#!/usr/bin/env python3
"""
Dataset Indexer for Elasticsearch
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


def log_memory_usage(logger, context: str = ""):
    """Enhanced hardware metrics logging for performance analysis"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()
        cpu_percent = process.cpu_percent(interval=1)
        system_cpu = psutil.cpu_percent(interval=1, percpu=True)
        disk_io = psutil.disk_io_counters()
        net_io = psutil.net_io_counters()
        
        logger.info(f"=== ENHANCED HARDWARE METRICS {context} ===")
        logger.info(f"Process Memory:")
        logger.info(f"  RSS: {memory_info.rss / (1024**3):.2f} GB")
        logger.info(f"  VMS: {memory_info.vms / (1024**3):.2f} GB")
        logger.info(f"  Percent: {process.memory_percent():.1f}%")
        logger.info(f"System Memory:")
        logger.info(f"  Used: {system_memory.percent:.1f}%")
        logger.info(f"  Available: {system_memory.available / (1024**3):.2f} GB")
        logger.info(f"  Cached: {system_memory.cached / (1024**3):.2f} GB")
       
        logger.info(f"CPU Usage:")
        logger.info(f"  Process: {cpu_percent:.1f}%")
        logger.info(f"  System: {sum(system_cpu)/len(system_cpu):.1f}%")
        logger.info(f"  Per Core: {system_cpu}")
        
        # Disk I/O
        if disk_io:
            logger.info(f"Disk I/O:")
            logger.info(f"  Read: {disk_io.read_bytes / (1024**3):.2f} GB")
            logger.info(f"  Write: {disk_io.write_bytes / (1024**3):.2f} GB")
            logger.info(f"  Read Rate: {disk_io.read_count} ops")
            logger.info(f"  Write Rate: {disk_io.write_count} ops")
        
        # Network I/O (for distributed setups)
        if net_io:
            logger.info(f"Network I/O:")
            logger.info(f"  Sent: {net_io.bytes_sent / (1024**3):.2f} GB")
            logger.info(f"  Received: {net_io.bytes_recv / (1024**3):.2f} GB")
        
        logger.info("=" * 50)
        
    except Exception as e:
        logger.warning(f"Could not get enhanced metrics: {e}")

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
                                   es_expansion_factor: float = 3.0) -> int:
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
    """Create index configuration with dynamic shard count"""

    return {
        "settings": {
            "number_of_shards": num_shards,
                "number_of_replicas": num_replicas,
                "refresh_interval": "-1",  # Disable refresh during bulk indexing
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
                    },
                    "url_analyzer": {
                        "type": "custom",
                        "tokenizer": "uax_url_email",
                        "filter": [
                            "lowercase",
                            "url_path_tokenizer"
                        ]
                    }
                },
                "filter": {
                    "url_path_tokenizer": {
                        "type": "pattern_replace",
                        "pattern": "[/\\-_.]",
                        "replacement": " "
                    }
                }
            }
        },
        "mappings": {
            "dynamic": "false",
            "_source": {
                "includes": ["text", "url"],
                "excludes": []
            },
            "properties": {
                "text": {
                    "type": "text",
                    "analyzer": "web_content_analyzer",
                    "index_options": "positions",
                    "norms": True,
                    "store": False
                },
                "url": {
                    "type": "text",
                    "analyzer": "url_analyzer",
                    "index_options": "docs",
                    "norms": False,
                    "store": False,
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 2048
                        }
                    }
                }
            }
        }
    }
  

def create_index_with_size_based_shards(es: Elasticsearch, index_name: str, 
                                       total_data_size_gb: float, config_file: str = None,
                                       min_shard_size_gb: float = 10, max_shard_size_gb: float = 50,
                                       es_expansion_factor: float = 3.0) -> bool:
    """Create Elasticsearch index with size-based dynamic shard count"""
    try:
        if es.indices.exists(index=index_name):
            logging.warning(f"Index '{index_name}' already exists. Deleting...")
            es.indices.delete(index=index_name)

        # Calculate optimal shard count based on size only
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


def get_file_range(data_dir: Path, file_range_start: int, file_range_end: int) -> List[Path]:
    """
    Get a specific range of parquet files from the data directory.
    
    Args:
        data_dir: Directory containing parquet files
        file_range_start: Starting index (0-based)
        file_range_end: Ending index (exclusive, like Python slicing)
        
    Returns:
        List of Path objects for the specified file range
    """
    logger = logging.getLogger(__name__)
    
    # Get all parquet files and sort them for consistent ordering
    all_parquet_files = sorted(list(data_dir.glob("*.parquet")))
    total_files = len(all_parquet_files)
    
    if total_files == 0:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")
    
    logger.info(f"Found {total_files} total parquet files in {data_dir}")
    
    # Validate range
    if file_range_start < 0 or file_range_start >= total_files:
        raise ValueError(f"file_range_start ({file_range_start}) is out of bounds [0, {total_files})")
    
    if file_range_end <= file_range_start or file_range_end > total_files:
        raise ValueError(f"file_range_end ({file_range_end}) must be > start ({file_range_start}) and <= {total_files}")
    
    # Get the file range
    selected_files = all_parquet_files[file_range_start:file_range_end]
    
    # Calculate total size of selected files
    total_size_bytes = 0
    for file_path in selected_files:
        try:
            total_size_bytes += file_path.stat().st_size
        except OSError as e:
            logger.warning(f"Could not get size for {file_path.name}: {e}")
    
    total_size_gb = total_size_bytes / (1024**3)
    
    logger.info(f"Selected files {file_range_start}:{file_range_end} ({len(selected_files)} files)")
    logger.info(f"Total raw data size: {total_size_gb:.2f} GB")
    logger.info(f"First file: {selected_files[0].name}")
    logger.info(f"Last file: {selected_files[-1].name}")
    
    
    return selected_files, total_size_gb



def process_single_parquet_file_streaming(file_path: Path, chunk_size: int, index_name: str) -> Generator[Dict[str, Any], None, None]:
    """
    Process a single parquet file using streaming approach to prevent memory leaks.
    FIXED VERSION: Now correctly extracts URL from metadata JSON and includes UUID
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
                row_group = parquet_file.read_row_group(row_group_idx, columns=['text', 'id', 'metadata'])
                
                # Convert to pandas DataFrame
                df = row_group.to_pandas()
                
                # Check for required columns
                missing_columns = []
                if 'text' not in df.columns:
                    missing_columns.append('text')
                if 'id' not in df.columns:
                    missing_columns.append('id')
                if 'metadata' not in df.columns:
                    missing_columns.append('metadata')
                
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
                        doc_id = row['id']
                        metadata_content = row['metadata']
                        
                        # Skip if text is empty/null
                        if pd.isna(text_content) or not str(text_content).strip():
                            continue
                        
                        # Extract URL from metadata JSON
                        url_content = None
                        try:
                            if not pd.isna(metadata_content) and metadata_content:
                                if isinstance(metadata_content, str):
                                    metadata_dict = json.loads(metadata_content)
                                elif isinstance(metadata_content, dict):
                                    metadata_dict = metadata_content
                                else:
                                    logger.warning(f"Unexpected metadata type: {type(metadata_content)}")
                                    metadata_dict = {}
                                
                                url_content = metadata_dict.get('url', '')
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.warning(f"Failed to parse metadata for doc {doc_id}: {e}")
                            url_content = ''
                        
                        # Skip if URL is empty/null (optional - you might want to keep these)
                        if not url_content or not str(url_content).strip():
                            logger.debug(f"Skipping row {doc_id} with empty URL")
                            continue
                        
                        text_str = str(text_content).strip()
                        if len(text_str) > 100000:
                            text_str = text_str[:100000] + "... [TRUNCATED]"
                        
                        url_str = str(url_content).strip()
                        doc_id_str = str(doc_id).strip()
                        
                        # Create document with text, URL, and UUID
                        doc = {
                            "_index": index_name,
                            "_id": doc_id_str,  # Use UUID as document ID for uniqueness
                            "_source": {
                                "text": text_str,
                                "url": url_str,
                                "document_id": doc_id_str # Also store in source for queries
                                # Optionally extract other metadata fields
                                #"language": metadata_dict.get('language', 'deu'),
                                #"language_score": metadata_dict.get('language_score', 0.0),
                                #"quality_score": metadata_dict.get('quality_score', 0.0),
                                #"toxic_score": metadata_dict.get('toxic_score', 0.0),
                                #"crawl_date": metadata_dict.get('date', ''),
                                #"file_path": metadata_dict.get('file_path', '')
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
    """
    MODIFIED: Process a specific list of parquet files (instead of all files in directory)
    """
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
    parser.add_argument("--es-expansion-factor", type=float, default=3.0,
                       help="ES size expansion factor vs parquet (default: 3.0)")
    
    parser.add_argument("--batch-size", type=int, default=12500, help="Batch size for bulk indexing")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Chunk size for reading parquet files (reduced)")
    parser.add_argument("--es-host", default="localhost", help="Elasticsearch host")
    parser.add_argument("--es-port", type=int, default=9200, help="Elasticsearch port")
    parser.add_argument("--index-name", default="fineweb", help="Elasticsearch index name")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--index-config", help="Path to JSON file with index configuration")
    parser.add_argument("--max-chunk-bytes", type=int, default=75, help="Max chunk size in MB for parallel_bulk")
    parser.add_argument("--thread-count", type=int, default=4, help="Number of threads for parallel_bulk")
    parser.add_argument("--queue-size", type=int, default=4, help="Queue size for parallel_bulk")
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    # Start memory tracking
    tracemalloc.start()
    log_memory_usage(logger, "AT START")
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory does not exist: {data_dir}")
        sys.exit(1)
    
    # MODIFIED: Determine which files to process
    if args.file_range_start is not None and args.file_range_end is not None:
        # Use file range
        logger.info(f"Using file range: {args.file_range_start}:{args.file_range_end}")
        try:
            files_to_process, total_data_size_gb = get_file_range(data_dir, args.file_range_start, args.file_range_end)
            logger.info(f"Total raw data size to process: {total_data_size_gb:.2f} GB")
        except (ValueError, FileNotFoundError) as e:
            logger.error(f"File range error: {e}")
            sys.exit(1)
    else:
        # Use max_files or all files
        all_parquet_files = sorted(list(data_dir.glob("*.parquet")))
        if not all_parquet_files:
            logger.error(f"No parquet files found in {data_dir}")
            sys.exit(1)

        files_to_process = all_parquet_files
        # Calculate total size
        total_size_bytes = sum(f.stat().st_size for f in files_to_process)
        total_data_size_gb = total_size_bytes / (1024**3)   # Includes 3 compression factor of parquet file
        logger.info(f"Processing all {len(files_to_process)} files")
    
    logger.info(f"Total raw data size: {total_data_size_gb:.2f} GB")

    logger.info("=== FineWeb Dataset Indexing Started with SIZE-BASED Dynamic Sharding ===")
    logger.info(f"Data directory: {data_dir}")
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
        
        # MODIFIED: Use new file list processing function
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

        # After indexing finishes, set refresh interval to 1s
        try:
            logger.info("Re-enabling index refresh for search queries...")
            es.indices.put_settings(
                index=args.index_name,
                body={
                    "index": {
                        "refresh_interval": "1s"  # or "30s" for less frequent
                    }
                }
            )
            logger.info("Index refresh interval set to 1s - ready for search queries")
            
            # Force an immediate refresh
            es.indices.refresh(index=args.index_name)
            logger.info("Index refreshed - all documents are now searchable")
            
        except Exception as e:
            logger.error(f"Failed to re-enable refresh: {e}")
            logger.info("You may need to manually set refresh_interval before searching")
        
        # Force index refresh
        # logger.info("Refreshing index...")
        # es.indices.refresh(index=args.index_name)
        
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