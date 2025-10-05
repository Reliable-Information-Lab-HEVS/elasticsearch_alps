#!/usr/bin/env python3
"""
FineWeb Dataset Indexer for Elasticsearch
Careful on original dataset categories titles ! 
"""

import os
import sys
import time
import logging
import gc
from pathlib import Path
from typing import Generator, Dict, Any, List
import argparse
import json

import pandas as pd
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, ConnectionError
import psutil
import tracemalloc


from multiprocessing import Process, Queue, Event, Value
from queue import Empty
import multiprocessing as mp
from threading import Thread
import signal


def log_memory_usage(logger, context: str = ""):
    """Improved monitoring focused on your process"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()
        
        logger.info(f"=== METRICS {context} ===")
        
        # Process Memory (what you control)
        logger.info(f"Process Memory:")
        logger.info(f"  RSS: {memory_info.rss / (1024**3):.2f} GB")
        logger.info(f"  VMS: {memory_info.vms / (1024**3):.2f} GB")
        logger.info(f"  % of System: {process.memory_percent():.1f}%")
        
        # System Memory (for awareness)
        logger.info(f"System Memory:")
        logger.info(f"  Total Used: {system_memory.percent:.1f}%")
        logger.info(f"  Available: {system_memory.available / (1024**3):.2f} GB")
        logger.info(f"  Cached: {system_memory.cached / (1024**3):.2f} GB")
       
        # CPU - Process-focused
        cpu_percent = process.cpu_percent(interval=1)
        num_threads = process.num_threads()
        logger.info(f"CPU:")
        logger.info(f"  Process usage: {cpu_percent:.1f}%")
        logger.info(f"  Threads: {num_threads}")
        
        # Try to show affinity
        try:
            affinity = process.cpu_affinity()
            logger.info(f"  Allocated cores: {len(affinity)} (IDs: {affinity[:5]}...)")
        except:
            logger.info(f"  Allocated cores: Unable to determine")
        
        # Disk I/O (your process only)
        try:
            io_counters = process.io_counters()
            logger.info(f"Process I/O:")
            logger.info(f"  Read: {io_counters.read_bytes / (1024**3):.2f} GB")
            logger.info(f"  Write: {io_counters.write_bytes / (1024**3):.2f} GB")
        except:
            pass
        
        logger.info("=" * 50)
        
    except Exception as e:
        logger.warning(f"Monitoring error: {e}")

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def get_elasticsearch_client(host: str = "localhost", port: int = 9200) -> Elasticsearch:
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            es = Elasticsearch(
                hosts=[{"host": host, "port": port}],
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            
            info = es.info()
            logging.info(f"Successfully connected to Elasticsearch at {host}:{port}")
            logging.info(f"Elasticsearch version: {info['version']['number']}")
            return es
            
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to connect to Elasticsearch after {max_retries} attempts")
                raise


def calculate_optimal_shards_by_size(data_size_gb: float, 
                                   min_shard_size_gb: float = 10, 
                                   max_shard_size_gb: float = 50,
                                   es_expansion_factor: float = 1.8) -> int:
    """
    Calculate optimal number of shards based only on data size constraints.
    
    Args:
        data_size_gb: Raw data size in GB (parquet files)
        min_shard_size_gb: Minimum shard size in GB (default 10GB)
        max_shard_size_gb: Maximum shard size in GB (default 50GB)  
        es_expansion_factor: Factor for ES indexed size vs raw size (default 3x)
        
    Returns:
        Optimal number of shards
    """
    logger = logging.getLogger(__name__)
    
    # Estimate indexed size (ES typically 2-4x larger than compressed parquet)
    estimated_indexed_size_gb = data_size_gb * es_expansion_factor
    
    logger.info(f"=== SHARD CALCULATION (SIZE-BASED ONLY) ===")
    logger.info(f"Raw data size: {data_size_gb:.2f} GB")
    logger.info(f"Estimated ES indexed size: {estimated_indexed_size_gb:.2f} GB (factor: {es_expansion_factor}x)")
    logger.info(f"Target shard size range: {min_shard_size_gb}-{max_shard_size_gb} GB")
    
    # Use target of 30GB per shard (middle of 10-50GB range)
    target_shard_size_gb = (min_shard_size_gb + max_shard_size_gb) / 2
    
    # Calculate shards needed - ensure at least 1 shard
    optimal_shards = max(1, int(estimated_indexed_size_gb / target_shard_size_gb))
    
    # Validate the result doesn't create shards too large
    size_per_shard_gb = estimated_indexed_size_gb / optimal_shards
    
    # If shards would be too large, increase shard count
    if size_per_shard_gb > max_shard_size_gb:
        optimal_shards = max(1, int(estimated_indexed_size_gb / max_shard_size_gb)) + 1
        size_per_shard_gb = estimated_indexed_size_gb / optimal_shards
        logger.info(f"Adjusted shard count to prevent oversized shards")
    
    logger.info(f"Calculation results:")
    logger.info(f"  Target shard size: {target_shard_size_gb:.1f} GB")
    logger.info(f"  Calculated shards: {optimal_shards}")
    logger.info(f"  Actual size per shard: {size_per_shard_gb:.1f} GB")
    
    # Validation warnings
    if size_per_shard_gb > max_shard_size_gb:
        logger.warning(f"WARNING: Size per shard ({size_per_shard_gb:.1f}GB) exceeds max ({max_shard_size_gb}GB)")
    
    if size_per_shard_gb < min_shard_size_gb:
        logger.warning(f"INFO: Size per shard ({size_per_shard_gb:.1f}GB) below min ({min_shard_size_gb}GB) - this is OK for smaller datasets")
    
    logger.info("=" * 45)
    
    return optimal_shards

def create_index_config(num_shards: int = 5, num_replicas: int = 0) -> Dict[str, Any]:
    """Create index configuration with dynamic shard count
    for de-tokneized dataset, so only text data, no metadata"""
    return {
        "settings": {
            "number_of_shards": num_shards,
                "number_of_replicas": num_replicas,
                "refresh_interval": "60s",  
            "index": {
                "codec": "best_compression",
                "max_result_window": 50000
            },
            "analysis": {
                "analyzer": {
                    "web_content_analyzer": {
                        "type": "custom",
                        "char_filter": [
                            "html_strip"
                        ],
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "asciifolding"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "dynamic": "false",
            "_source": {
                "includes": ["text"],
                "excludes": []
            },
            "properties": {
                "text": {
                    "type": "text",
                    "analyzer": "web_content_analyzer",
                    "index_options": "positions",
                    "norms": True,
                    "store": False
                }
            }
        }
    }
  

def create_index_with_size_based_shards(es: Elasticsearch, index_name: str, 
                                       total_data_size_gb: float, config_file: str = None,
                                       min_shard_size_gb: float = 10, max_shard_size_gb: float = 50,
                                       es_expansion_factor: float = 1.8) -> bool:
    """Create Elasticsearch index with size-based dynamic shard count"""
    try:
        if es.indices.exists(index=index_name):
            logging.warning(f"Index '{index_name}' already exists. Deleting...")
            es.indices.delete(index=index_name)

        # Calculate optimal shard count based on size only
        # Must revise the es_expansion_factor
        optimal_shards = calculate_optimal_shards_by_size(
            total_data_size_gb, min_shard_size_gb, max_shard_size_gb, es_expansion_factor
        )
        
        if config_file and os.path.exists(config_file):
            logging.info(f"Loading index configuration from: {config_file}")
            with open(config_file, 'r') as f:
                config = json.load(f)
            # Override shard count in loaded config
            config["settings"]["number_of_shards"] = optimal_shards
            logging.info(f"Overrode shard count in config file to: {optimal_shards}")
        
        else:
            logging.info("Using default index configuration")
            config = create_index_config(num_shards=optimal_shards)
        
        
        # Log final configuration
        logging.info(f"Creating index '{index_name}' with:")
        logging.info(f"  Shards: {config['settings']['number_of_shards']}")
        logging.info(f"  Replicas: {config['settings']['number_of_replicas']}")
        logging.info(f"  Refresh interval: {config['settings']['refresh_interval']}")
        
        es.indices.create(index=index_name, body=config)
        logging.info(f"Created index '{index_name}' with configuration")
        return True
        
    except Exception as e:
        logging.error(f"Failed to create index: {e}")
        return False


def get_file_range(data_dir_pattern: str, file_range_start: int = None, file_range_end: int = None) -> tuple:
    """
    Get parquet files from multiple directories matching a pattern.
    
    Args:
        data_dir_pattern: Pattern like "/path/to/dataset_part_*"
        file_range_start: Optional starting file index
        file_range_end: Optional ending file index
        
    Returns:
        Tuple of (list of Path objects, total size in GB)
    """
    logger = logging.getLogger(__name__)
    
    # Find all directories matching the pattern
    import glob
    matching_dirs = sorted(glob.glob(data_dir_pattern))
    
    if not matching_dirs:
        raise FileNotFoundError(f"No directories found matching pattern: {data_dir_pattern}")
    
    logger.info(f"Found {len(matching_dirs)} directories matching pattern")
    for dir_path in matching_dirs:
        logger.info(f"  - {dir_path}")
    
    # Collect all parquet files from all matching directories
    all_parquet_files = []
    for dir_path in matching_dirs:
        dir_parquet_files = sorted(list(Path(dir_path).glob("*.parquet")))
        logger.info(f"Found {len(dir_parquet_files)} parquet files in {Path(dir_path).name}")
        all_parquet_files.extend(dir_parquet_files)
    
    all_parquet_files = sorted(all_parquet_files)
    total_files = len(all_parquet_files)
    
    if total_files == 0:
        raise FileNotFoundError(f"No parquet files found in any matching directories")
    
    logger.info(f"Total parquet files across all directories: {total_files}")
    
    # Apply file range if specified
    if file_range_start is not None and file_range_end is not None:
        if file_range_start < 0 or file_range_start >= total_files:
            raise ValueError(f"file_range_start ({file_range_start}) is out of bounds [0, {total_files})")
        
        if file_range_end <= file_range_start or file_range_end > total_files:
            raise ValueError(f"file_range_end ({file_range_end}) must be > start ({file_range_start}) and <= {total_files}")
        
        selected_files = all_parquet_files[file_range_start:file_range_end]
        logger.info(f"Selected file range {file_range_start}:{file_range_end}")
    else:
        selected_files = all_parquet_files
        logger.info(f"Processing all {total_files} files")
    
    # Calculate total size
    total_size_bytes = 0
    for file_path in selected_files:
        try:
            total_size_bytes += file_path.stat().st_size
        except OSError as e:
            logger.warning(f"Could not get size for {file_path.name}: {e}")
    
    total_size_gb = total_size_bytes / (1024**3)
    
    logger.info(f"Total raw data size: {total_size_gb:.2f} GB")
    logger.info(f"First file: {selected_files[0]}")
    logger.info(f"Last file: {selected_files[-1]}")
    
    return selected_files, total_size_gb


def process_single_parquet_file_streaming(file_path: Path, chunk_size: int, index_name: str) -> Generator[Dict[str, Any], None, None]:
    """
    Process a single parquet file using streaming approach to prevent memory leaks.
    """
    logger = logging.getLogger(__name__)
    parquet_file = None
    
    try:
        logger.info(f"Opening parquet file for streaming: {file_path.name}")
        
        # Use PyArrow ParquetFile for streaming access
        parquet_file = pq.ParquetFile(file_path)
        
        # Get metadata without loading data
        metadata = parquet_file.metadata
        num_rows = metadata.num_rows
        num_row_groups = metadata.num_row_groups
        
        logger.info(f"File has {num_rows:,} rows in {num_row_groups} row groups")
        
        processed_rows = 0
        
        # Process each row group individually
        for row_group_idx in range(num_row_groups):
            try:
                # Read text, id, and metadata columns (the actual column structure)
                row_group = parquet_file.read_row_group(row_group_idx, columns=['text'])
                
                # Convert to pandas DataFrame
                df = row_group.to_pandas()
                
                # Check for required columns
                missing_columns = []
                if 'text' not in df.columns:
                    missing_columns.append('text')
                
                
                if missing_columns:
                    logger.warning(f"Missing columns {missing_columns} in row group {row_group_idx}, skipping...")
                    continue
                
                row_group_size = len(df)
                logger.debug(f"Processing row group {row_group_idx}: {row_group_size:,} rows")
                
                # Process in smaller chunks within the row group
                for chunk_start in range(0, row_group_size, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, row_group_size)
                    chunk = df.iloc[chunk_start:chunk_end].copy()
                    
                    # Process chunk and yield documents
                    for _, row in chunk.iterrows():
                        text_content = row['text']
                        
                        # Skip if text is empty/null
                        if pd.isna(text_content) or not str(text_content).strip():
                            continue
                        
                        text_str = str(text_content).strip()
                        if len(text_str) > 100000:
                            text_str = text_str[:100000] + "... [TRUNCATED]"
                        
                        # Create document with text, URL, and UUID
                        doc = {
                            "_index": index_name,
                            "_source": {
                                "text": text_str 
                            }
                        }
                        
                        yield doc
                    
                    processed_rows += len(chunk)
                    
                    # Cleanup chunk immediately
                    del chunk
                    
                    # Force garbage collection every 10k rows
                    if processed_rows % 10000 == 0:
                        gc.collect()
                        logger.debug(f"Processed {processed_rows:,}/{num_rows:,} rows")
                
                # Cleanup row group data
                del df
                del row_group
                
                # Force garbage collection after each row group
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error processing row group {row_group_idx}: {e}")
                continue
        
        logger.info(f"Completed streaming processing of {file_path.name}: {processed_rows:,} rows")
        
    except Exception as e:
        logger.error(f"Error opening parquet file {file_path.name}: {e}")
        raise
    finally:
        # Critical: Cleanup parquet file handle
        if parquet_file is not None:
            parquet_file = None
        
        # Force PyArrow memory cleanup
        try:
            import pyarrow as pa
            pool = pa.default_memory_pool()
            logger.debug(f"PyArrow memory pool bytes allocated: {pool.bytes_allocated()}")
            pool.release_unused()
        except Exception as e:
            logger.warning(f"Could not cleanup PyArrow memory pool: {e}")
        
        # Final garbage collection
        gc.collect()

def bulk_index_documents(es: Elasticsearch, doc_generator: Generator, batch_size: int, 
                        max_chunk_bytes: int, thread_count: int, queue_size: int,
                        global_stats: Dict[str, int], start_time: float) -> Dict[str, int]:
    """Bulk index documents with progress tracking"""
    logger = logging.getLogger(__name__)
    stats = {"indexed": 0, "failed": 0}
    
    try:
        for success, info in helpers.parallel_bulk(
            es,
            doc_generator,
            chunk_size=batch_size,
            max_chunk_bytes=max_chunk_bytes * 1024 * 1024,
            thread_count=thread_count,
            queue_size=queue_size,
            request_timeout=60,
        ):
            if success:
                stats["indexed"] += 1
                global_stats["indexed"] += 1
            else:
                stats["failed"] += 1
                global_stats["failed"] += 1
                logger.error(f"Failed to index document: {info}")
            
            # Log progress every 10,000 documents
            if global_stats["indexed"] % 10000 == 0:
                current_time = time.time()
                elapsed = current_time - start_time
                rate = global_stats["indexed"] / elapsed if elapsed > 0 else 0
                
                logger.info(
                    f"Progress: {global_stats['indexed']:,} indexed, {global_stats['failed']:,} failed, "
                    f"Rate: {rate:.1f} docs/sec, Elapsed: {elapsed:.1f}s"
                )
                
                # Log memory usage every 10k docs
                log_memory_usage(logger, f"BULK INDEXING AT {global_stats['indexed']} DOCS")
                
                # Force garbage collection during bulk indexing
                gc.collect()
        
    except Exception as e:
        logger.error(f"Bulk indexing error: {e}")
        raise
    
    return stats


def force_memory_cleanup():
    """Aggressive memory cleanup between files"""
    logger = logging.getLogger(__name__)
    
    try:
        # Multiple garbage collection passes
        for i in range(3):
            collected = gc.collect()
            logger.debug(f"GC pass {i+1}: collected {collected} objects")
        
        # Try to release PyArrow memory
        try:
            import pyarrow as pa
            pool = pa.default_memory_pool()
            initial_bytes = pool.bytes_allocated()
            pool.release_unused()
            final_bytes = pool.bytes_allocated()
            logger.debug(f"PyArrow memory: {initial_bytes} -> {final_bytes} bytes")
        except Exception as e:
            logger.debug(f"PyArrow cleanup failed: {e}")
            
    except Exception as e:
        logger.warning(f"Memory cleanup failed: {e}")


def process_file_list(file_list: List[Path], chunk_size: int, 
                     index_name: str, es: Elasticsearch, batch_size: int,
                     max_chunk_bytes: int, thread_count: int, queue_size: int) -> Dict[str, int]:
    
    logger = logging.getLogger(__name__)
    
    if not file_list:
        raise ValueError("No files provided to process")
    
    logger.info(f"Processing {len(file_list)} parquet files")
    
    global_stats = {"indexed": 0, "failed": 0, "files_processed": 0}
    start_time = time.time()
    
    for i, file_path in enumerate(file_list):
        logger.info(f"Processing file {i+1}/{len(file_list)}: {file_path.name}")
        
        # Log memory before processing each file
        log_memory_usage(logger, f"BEFORE FILE {i+1}")
        
        try:
            # Process single file using streaming approach
            doc_generator = process_single_parquet_file_streaming(file_path, chunk_size, index_name)
            
            # Index documents from this file
            file_stats = bulk_index_documents(
                es, doc_generator, batch_size, max_chunk_bytes, 
                thread_count, queue_size, global_stats, start_time
            )
            
            global_stats["files_processed"] += 1
            
            logger.info(f"File {i+1} completed: {file_stats['indexed']:,} indexed, {file_stats['failed']:,} failed")
            
        except Exception as e:
            logger.error(f"Failed to process file {file_path.name}: {e}")
            continue
        finally:
            # Aggressive cleanup between files
            force_memory_cleanup()
            
            # Log memory after processing each file
            log_memory_usage(logger, f"AFTER FILE {i+1}")
    
    return global_stats


def main():
    parser = argparse.ArgumentParser(description="Index FineWeb dataset into Elasticsearch - Memory Leak Fixed with File Range Support")
    parser.add_argument("--data-dir", required=True, help="Directory containing parquet files")
    
    # File range arguments
    parser.add_argument("--file-range-start", type=int, help="Starting file index (0-based, inclusive)")
    parser.add_argument("--file-range-end", type=int, help="Ending file index (exclusive, like Python slicing)")
    
    # Size-based shard calculation parameters
    parser.add_argument("--min-shard-size", type=float, default=10.0, 
                       help="Minimum shard size in GB (default: 10)")
    parser.add_argument("--max-shard-size", type=float, default=50.0,
                       help="Maximum shard size in GB (default: 50)")
    parser.add_argument("--es-expansion-factor", type=float, default=1.8,
                       help="ES size expansion factor vs parquet (default: 1.8)")
    
    parser.add_argument("--batch-size", type=int, default=12500, help="Batch size for bulk indexing")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Chunk size for reading parquet files (reduced)")
    parser.add_argument("--es-host", default="localhost", help="Elasticsearch host")
    parser.add_argument("--es-port", type=int, default=9200, help="Elasticsearch port")
    parser.add_argument("--index-name", default="fineweb", help="Elasticsearch index name")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--index-config", help="Path to JSON file with index configuration")
    parser.add_argument("--max-chunk-bytes", type=int, default=75, help="Max chunk size in MB for parallel_bulk")
    parser.add_argument("--thread-count", type=int, default=8, help="Number of threads for parallel_bulk")
    parser.add_argument("--queue-size", type=int, default=8, help="Queue size for parallel_bulk")
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    # Start memory tracking
    tracemalloc.start()
    log_memory_usage(logger, "AT START")

    data_dir_pattern = args.data_dir  

    # Check if it's a pattern or single directory
    if '*' in data_dir_pattern:
        logger.info(f"Using directory pattern: {data_dir_pattern}")
        files_to_process, total_data_size_gb = get_file_range(
            data_dir_pattern, 
            args.file_range_start if args.file_range_start is not None else None,
            args.file_range_end if args.file_range_end is not None else None
        )
    else:
        # Single directory 
        data_dir = Path(data_dir_pattern)
        if not data_dir.exists():
            logger.error(f"Data directory does not exist: {data_dir}")
            sys.exit(1)
        
        if args.file_range_start is not None and args.file_range_end is not None:
            files_to_process, total_data_size_gb = get_file_range(str(data_dir), args.file_range_start, args.file_range_end)
        else:
            all_parquet_files = sorted(list(data_dir.glob("*.parquet")))
            if not all_parquet_files:
                logger.error(f"No parquet files found in {data_dir}")
                sys.exit(1)
            files_to_process = all_parquet_files
            total_size_bytes = sum(f.stat().st_size for f in files_to_process)
            total_data_size_gb = total_size_bytes / (1024**3)
            
    logger.info(f"Total raw data size: {total_data_size_gb:.2f} GB")

    logger.info("=== FineWeb Dataset Indexing Started with SIZE-BASED Dynamic Sharding ===")
    logger.info(f"Data directory: {data_dir_pattern}")
    logger.info(f"Files to process: {len(files_to_process)}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Chunk size: {args.chunk_size}")
    logger.info(f"Max chunk bytes: {args.max_chunk_bytes}")
    logger.info(f"Elasticsearch: {args.es_host}:{args.es_port}")
    logger.info(f"Index name: {args.index_name}")
    logger.info(f"Thread counts: {args.thread_count}")
    logger.info(f"Queue size: {args.queue_size}")
    logger.info(f"Shard size range: {args.min_shard_size}-{args.max_shard_size} GB")
    logger.info(f"ES expansion factor: {args.es_expansion_factor}x")
    
    
    total_start_time = time.time()
    
    try:
        # Connect to Elasticsearch
        es = get_elasticsearch_client(args.es_host, args.es_port)
        log_memory_usage(logger, "AFTER ES CONNECTION")

        # Create index with size-based dynamic sharding
        logger.info("Creating Elasticsearch index with size-based dynamic sharding...")
        if not create_index_with_size_based_shards(
            es, args.index_name, total_data_size_gb, args.index_config,
            args.min_shard_size, args.max_shard_size, args.es_expansion_factor
        ):
            sys.exit(1)
        
        log_memory_usage(logger, "AFTER INDEX CREATION")
        
        # Process files
        logger.info("Starting document indexing...")
        indexing_start_time = time.time()
        
        stats = process_file_list(
            files_to_process, args.chunk_size, args.index_name, es,
            args.batch_size, args.max_chunk_bytes, args.thread_count, args.queue_size
        )
        
        indexing_end_time = time.time()
        log_memory_usage(logger, "AFTER INDEXING")
        
        # Final statistics
        total_time = time.time() - total_start_time
        indexing_time = indexing_end_time - indexing_start_time
        
        logger.info("=== Indexing Completed ===")
        logger.info(f"Total documents indexed: {stats['indexed']:,}")
        logger.info(f"Failed documents: {stats['failed']:,}")
        logger.info(f"Files processed: {stats['files_processed']:,}")
        logger.info(f"Indexing time: {indexing_time:.2f} seconds")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
        if indexing_time > 0:
            logger.info(f"Average indexing rate: {stats['indexed'] / indexing_time:.1f} docs/sec")
        
        # Log final memory usage and peak usage
        log_memory_usage(logger, "FINAL")
        try:
            current, peak = tracemalloc.get_traced_memory()
            logger.info(f"Peak memory usage during execution: {peak / (1024**3):.2f} GB")
            tracemalloc.stop()
        except Exception as e:
            logger.warning(f"Could not get tracemalloc info: {e}")


        # After indexing finishes, keep refresh at 60s for read operations
        try:
            logger.info("Indexing complete - refresh interval remains at 60s for search operations")
            
            # Force an immediate refresh to ensure all documents are searchable
            es.indices.refresh(index=args.index_name)
            logger.info("Final refresh complete - all documents are now searchable")
            
        except Exception as e:
            logger.error(f"Failed to perform final refresh: {e}")

        # Get final index stats
        try:
            index_stats = es.indices.stats(index=args.index_name)
            doc_count = index_stats['indices'][args.index_name]['total']['docs']['count']
            index_size = index_stats['indices'][args.index_name]['total']['store']['size_in_bytes']
            
            logger.info(f"Final index document count: {doc_count:,}")
            logger.info(f"Index size: {index_size / (1024*1024):.2f} MB")
            
        except Exception as e:
            logger.warning(f"Could not get final index stats: {e}")
        
        logger.info("=== Indexing Job Completed Successfully ===")
        
    except KeyboardInterrupt:
        logger.info("Indexing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Indexing failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()