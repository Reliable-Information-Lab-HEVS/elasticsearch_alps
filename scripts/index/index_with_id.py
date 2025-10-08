#!/usr/bin/env python3
"""
FineWeb Dataset Indexer for Elasticsearch with Multi-Process Support
Multi-process architecture: N parser workers -> shared queue -> 1 ES consumer
- Content-based SHA256 document IDs for deduplication
"""

import os
import sys
import time
import logging
import gc
import hashlib
from pathlib import Path
from typing import Generator, Dict, Any, List
import argparse
import json

import pandas as pd
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, ConnectionError,  TransportError
import psutil
import tracemalloc


from multiprocessing import Process, Queue, Event, Value, Manager
from queue import Empty
import multiprocessing as mp
from threading import Thread
import signal


# ============================================================================
# DOCUMENT ID GENERATION
# ============================================================================

def create_doc_id(text: str) -> str:
    """
    Create content-based document ID using SHA256 hash.
    Ensures deduplication - identical content always gets same ID.
    """
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


# ============================================================================
# LOGGING AND MONITORING
# ============================================================================

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
        logger.warning(f"Could not log memory usage: {e}")


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


# ============================================================================
# ELASTICSEARCH CONNECTION
# ============================================================================

def get_elasticsearch_client(host: str = "localhost", port: int = 9200) -> Elasticsearch:
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            es = Elasticsearch(
                hosts=[{"host": host, "port": port}],
                timeout=30,
                max_retries=5,
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


# ============================================================================
# DOCUMENT COUNTING (GROUND TRUTH)
# ============================================================================

def count_documents_in_parquet_files(file_list: List[Path]) -> int:
    """
    Count total documents across all parquet files WITHOUT loading data into memory.
    Uses PyArrow metadata only for efficiency.
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 70)
    logger.info("=== COUNTING GROUND TRUTH DOCUMENTS ===")
    logger.info(f"Files to count: {len(file_list)}")
    
    total_docs = 0
    
    for i, file_path in enumerate(file_list):
        try:
            # Read only metadata, NOT the actual data
            parquet_file = pq.ParquetFile(file_path)
            num_rows = parquet_file.metadata.num_rows
            total_docs += num_rows
            
            if (i + 1) % 10 == 0 or (i + 1) == len(file_list):
                logger.info(f"Counted {i+1}/{len(file_list)} files: {total_docs:,} docs so far")
                
        except Exception as e:
            logger.error(f"Failed to read metadata from {file_path.name}: {e}")
            raise
    
    logger.info("=" * 70)
    logger.info(f"*** GROUND TRUTH: {total_docs:,} total documents ***")
    logger.info("=" * 70)
    return total_docs


# ============================================================================
# VALIDATION
# ============================================================================

def validate_indexing_results(ground_truth_count: int, stats: Dict[str, int], 
                              es: Elasticsearch, index_name: str) -> bool:
    """
    STRICT validation: indexed count must EXACTLY match ground truth.
    No tolerance for missing or duplicate documents.
    """
    logger = logging.getLogger(__name__)
    
    indexed_count = stats['indexed']
    failed_count = stats['failed']
    
    # Get actual ES index count
    try:
        es.indices.refresh(index=index_name)
        index_stats = es.indices.stats(index=index_name)
        es_doc_count = index_stats['indices'][index_name]['total']['docs']['count']
    except Exception as e:
        logger.error(f"Could not query ES index count: {e}")
        return False
    
    logger.info("=" * 70)
    logger.info("=== INDEXING VALIDATION RESULTS ===")
    logger.info(f"Ground truth (raw files):  {ground_truth_count:>15,} documents")
    logger.info(f"Reported indexed:          {indexed_count:>15,} documents")
    logger.info(f"Reported failed:           {failed_count:>15,} documents")
    logger.info(f"ES index actual count:     {es_doc_count:>15,} documents")
    logger.info("-" * 70)
    
    # STRICT VALIDATION: Must match exactly
    success = True
    
    # Check 1: ES Index count MUST equal ground truth
    if es_doc_count != ground_truth_count:
        logger.error(f"❌ VALIDATION FAILED: ES index count mismatch")
        logger.error(f"   Expected: {ground_truth_count:,}")
        logger.error(f"   Got:      {es_doc_count:,}")
        logger.error(f"   Diff:     {es_doc_count - ground_truth_count:+,}")
        
        if es_doc_count > ground_truth_count:
            logger.error(f"   ⚠️  DUPLICATION DETECTED - Documents indexed multiple times!")
        else:
            logger.error(f"   ⚠️  MISSING DOCUMENTS - Some documents were not indexed!")
        
        success = False
    else:
        logger.info(f"✓ ES index count matches ground truth exactly")
    
    # Check 2: Reported indexed should match (informational)
    if indexed_count != ground_truth_count:
        logger.warning(f"⚠️  INFO: Reported indexed ({indexed_count:,}) != ground truth ({ground_truth_count:,})")
        logger.warning(f"   This is OK if using content-based IDs (deduplication expected)")
    
    # Check 3: Should have no failures
    if failed_count > 0:
        logger.warning(f"⚠️  WARNING: {failed_count:,} documents failed to index")
        if failed_count > ground_truth_count * 0.01:  # >1% failure rate
            logger.error(f"❌ High failure rate: {(failed_count/ground_truth_count)*100:.1f}%")
            success = False
    
    logger.info("=" * 70)
    
    if success:
        logger.info("✓✓✓ VALIDATION PASSED - Indexing successful!")
        logger.info(f"*** {es_doc_count:,} documents indexed successfully ***")
    else:
        logger.error("❌❌❌ VALIDATION FAILED - Data integrity issues detected!")
        logger.error("Recommended actions:")
        logger.error("  1. Delete the corrupted index")
        logger.error("  2. Check for parsing errors in the logs")
        logger.error("  3. Rerun indexing job")
    
    logger.info("=" * 70)
    return success


# ============================================================================
# FILE HANDLING
# ============================================================================

def get_file_range(data_dir_pattern: str, file_range_start: int = None, file_range_end: int = None) -> tuple:
    """
    Get parquet files from multiple directories matching a pattern or from single data directory. 
    Optional range selection
    """
    logger = logging.getLogger(__name__)
    
    # Find all directories matching the pattern
    import glob
    matching_dirs = sorted(glob.glob(data_dir_pattern))
    
    if not matching_dirs:
        raise FileNotFoundError(f"No directories found matching pattern: {data_dir_pattern}")
    
    logger.info(f"Found {len(matching_dirs)} directories matching pattern")
    
    # Collect all parquet files from all matching directories
    all_parquet_files = []
    for dir_path in matching_dirs:
        dir_parquet_files = sorted(list(Path(dir_path).glob("*.parquet")))
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


# ============================================================================
# INDEX CONFIGURATION
# ============================================================================

def calculate_optimal_shards_by_size(data_size_gb: float, 
                                   min_shard_size_gb: float = 10, 
                                   max_shard_size_gb: float = 50,
                                   es_expansion_factor: float = 3.0) -> int:
    """
    Calculate optimal number of shards based only on data size constraints.
    """
    logger = logging.getLogger(__name__)
    
    estimated_indexed_size_gb = data_size_gb * es_expansion_factor
    
    logger.info(f"=== SHARD CALCULATION (SIZE-BASED ONLY) ===")
    logger.info(f"Raw data size: {data_size_gb:.2f} GB")
    logger.info(f"Estimated ES indexed size: {estimated_indexed_size_gb:.2f} GB (factor: {es_expansion_factor}x)")
    logger.info(f"Target shard size range: {min_shard_size_gb}-{max_shard_size_gb} GB")
    
    target_shard_size_gb = (min_shard_size_gb + max_shard_size_gb) / 2
    optimal_shards = max(1, int(estimated_indexed_size_gb / target_shard_size_gb))
    
    if data_size_gb > 15 and optimal_shards < 2:
        optimal_shards = 2
        logger.info(f"Enforcing minimum 2 shards for dataset > 15GB")
    
    size_per_shard_gb = estimated_indexed_size_gb / optimal_shards
    
    if size_per_shard_gb > max_shard_size_gb:
        optimal_shards = max(2 if data_size_gb > 15 else 1, int(estimated_indexed_size_gb / max_shard_size_gb) + 1)
        size_per_shard_gb = estimated_indexed_size_gb / optimal_shards
        logger.info(f"Adjusted shard count to prevent oversized shards")
    
    logger.info(f"Calculation results:")
    logger.info(f"  Target shard size: {target_shard_size_gb:.1f} GB")
    logger.info(f"  Calculated shards: {optimal_shards}")
    logger.info(f"  Actual size per shard: {size_per_shard_gb:.1f} GB")
    logger.info("=" * 45)
    
    return optimal_shards


def create_index_config(num_shards: int = 5, num_replicas: int = 0) -> Dict[str, Any]:
    """Create index configuration for detokenized dataset (text only)"""
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
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"]
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
                                       es_expansion_factor: float = 3.0) -> bool:
    """Create Elasticsearch index with size-based dynamic shard count"""
    try:
        if es.indices.exists(index=index_name):
            logging.warning(f"Index '{index_name}' already exists. Deleting...")
            es.indices.delete(index=index_name)

        optimal_shards = calculate_optimal_shards_by_size(
            total_data_size_gb, min_shard_size_gb, max_shard_size_gb, es_expansion_factor
        )
        
        if config_file and os.path.exists(config_file):
            logging.info(f"Loading index configuration from: {config_file}")
            with open(config_file, 'r') as f:
                config = json.load(f)
            config["settings"]["number_of_shards"] = optimal_shards
            logging.info(f"Overrode shard count in config file to: {optimal_shards}")
        else:
            logging.info("Using default index configuration")
            config = create_index_config(num_shards=optimal_shards)
        
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


# ============================================================================
# MEMORY MANAGEMENT
# ============================================================================

def force_memory_cleanup():
    """Aggressive memory cleanup"""
    logger = logging.getLogger(__name__)
    
    try:
        for i in range(3):
            collected = gc.collect()
            logger.debug(f"GC pass {i+1}: collected {collected} objects")
        
        try:
            import pyarrow as pa
            pool = pa.default_memory_pool()
            pool.release_unused()
        except Exception as e:
            logger.debug(f"PyArrow cleanup failed: {e}")
            
    except Exception as e:
        logger.warning(f"Memory cleanup failed: {e}")


# ============================================================================
# SINGLE-PROCESS PARSING (backward compatibility)
# ============================================================================

def process_single_parquet_file_streaming(file_path: Path, chunk_size: int, index_name: str) -> Generator[Dict[str, Any], None, None]:
    """
    Process a single parquet file using streaming approach with content-based IDs.
    """
    logger = logging.getLogger(__name__)
    parquet_file = None
    
    try:
        logger.info(f"Opening parquet file for streaming: {file_path.name}")
        
        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata
        num_rows = metadata.num_rows
        num_row_groups = metadata.num_row_groups
        
        logger.info(f"File has {num_rows:,} rows in {num_row_groups} row groups")
        
        processed_rows = 0
        
        for row_group_idx in range(num_row_groups):
            try:
                row_group = parquet_file.read_row_group(row_group_idx, columns=['text'])
                df = row_group.to_pandas()
                
                if 'text' not in df.columns:
                    logger.warning(f"Missing 'text' column in row group {row_group_idx}, skipping...")
                    continue
                
                row_group_size = len(df)
                logger.debug(f"Processing row group {row_group_idx}: {row_group_size:,} rows")
                
                for chunk_start in range(0, row_group_size, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, row_group_size)
                    chunk = df.iloc[chunk_start:chunk_end].copy()
                    
                    for _, row in chunk.iterrows():
                        text_content = row['text']
                        
                        if pd.isna(text_content) or not str(text_content).strip():
                            continue
                        
                        text_str = str(text_content).strip()
                        if len(text_str) > 100000:
                            text_str = text_str[:100000] + "... [TRUNCATED]"
                        
                        # Create document with content-based ID for deduplication
                        doc = {
                            "_index": index_name,
                            "_id": create_doc_id(text_str),  # SHA256 ID
                            "_source": {
                                "text": text_str 
                            }
                        }
                        
                        yield doc
                    
                    processed_rows += len(chunk)
                    del chunk
                    
                    if processed_rows % 10000 == 0:
                        gc.collect()
                        logger.debug(f"Processed {processed_rows:,}/{num_rows:,} rows")
                
                del df
                del row_group
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error processing row group {row_group_idx}: {e}")
                continue
        
        logger.info(f"Completed streaming processing of {file_path.name}: {processed_rows:,} rows")
        
    except Exception as e:
        logger.error(f"Error opening parquet file {file_path.name}: {e}")
        raise
    finally:
        if parquet_file is not None:
            parquet_file = None
        
        try:
            import pyarrow as pa
            pool = pa.default_memory_pool()
            pool.release_unused()
        except Exception as e:
            logger.warning(f"Could not cleanup PyArrow memory pool: {e}")
        
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
            request_timeout=300,
        ):
            if success:
                stats["indexed"] += 1
                global_stats["indexed"] += 1
            else:
                stats["failed"] += 1
                global_stats["failed"] += 1
                logger.error(f"Failed to index document: {info}")
            
            if global_stats["indexed"] % 10000 == 0:
                current_time = time.time()
                elapsed = current_time - start_time
                rate = global_stats["indexed"] / elapsed if elapsed > 0 else 0
                
                logger.info(
                    f"Progress: {global_stats['indexed']:,} indexed, {global_stats['failed']:,} failed, "
                    f"Rate: {rate:.1f} docs/sec, Elapsed: {elapsed:.1f}s"
                )
                
                log_memory_usage(logger, f"BULK INDEXING AT {global_stats['indexed']} DOCS")
                gc.collect()
        
    except Exception as e:
        logger.error(f"Bulk indexing error: {e}")
        raise
    
    return stats


# ============================================================================
# MULTI-PROCESS WORKERS
# ============================================================================

def parse_parquet_worker_batch(file_list, chunk_size, index_name, doc_queue, 
                               docs_parsed_counter, worker_id, stop_event):
    """
    Worker that processes multiple parquet files with content-based IDs
    """
    logger = logging.getLogger(f"parser-{worker_id}")
    logger.info(f"Worker {worker_id} starting to parse {len(file_list)} files")
    
    total_docs_parsed = 0
    
    try:
        for file_idx, file_path in enumerate(file_list):
            if stop_event.is_set():
                logger.warning(f"Worker {worker_id} received stop signal")
                break
            
            logger.info(f"Worker {worker_id}: Processing file {file_idx+1}/{len(file_list)}: {file_path.name}")
            
            docs_parsed = 0
            batch_buffer = []
            batch_size = 1000
            
            try:
                parquet_file = pq.ParquetFile(file_path)
                total_rows = parquet_file.metadata.num_rows
                num_row_groups = parquet_file.metadata.num_row_groups
                
                for row_group_idx in range(num_row_groups):
                    if stop_event.is_set():
                        break
                    
                    try:
                        row_group = parquet_file.read_row_group(row_group_idx, columns=['text'])
                        df = row_group.to_pandas()
                        
                        if 'text' not in df.columns:
                            logger.warning(f"Missing 'text' column in row group {row_group_idx}, skipping...")
                            continue
                        
                        row_group_size = len(df)
                        
                        for start_idx in range(0, row_group_size, chunk_size):
                            if stop_event.is_set():
                                break
                            
                            
                            chunk_end = min(start_idx + chunk_size, row_group_size)
                            chunk = df.iloc[start_idx:chunk_end].copy()
                            
                            for _, row in chunk.iterrows():
                                text_str = row['text']
                                
                                if pd.isna(text_str) or not str(text_str).strip():
                                    continue
                                
                                # Create document with SHA256 content-based ID
                                doc = {
                                    "_index": index_name,
                                    "_id": create_doc_id(text_str),  # SHA256 ID for deduplication
                                    "_source": {"text": text_str}
                                }
                                
                                batch_buffer.append(doc)
                                docs_parsed += 1
                                
                                if len(batch_buffer) >= batch_size:
                                    doc_queue.put(batch_buffer)
                                    batch_buffer = []
                                    
                                    with docs_parsed_counter.get_lock():
                                        docs_parsed_counter.value += batch_size
                            
                            del chunk
                            
                            if docs_parsed % 50000 == 0:
                                logger.info(f"Worker {worker_id}: Parsed {docs_parsed:,} docs, Queue: {doc_queue.qsize()}")
                                gc.collect()
                        
                        del df, row_group
                        gc.collect()
                        
                    except Exception as e:
                        logger.error(f"Worker {worker_id} row group error: {e}")
                        continue
                
                # Send remaining batch
                if batch_buffer:
                    doc_queue.put(batch_buffer)
                    with docs_parsed_counter.get_lock():
                        docs_parsed_counter.value += len(batch_buffer)
                    batch_buffer = []
                
                total_docs_parsed += docs_parsed
                logger.info(f"Worker {worker_id}: Completed {file_path.name} - {docs_parsed:,} documents")
                
            except Exception as e:
                logger.error(f"Worker {worker_id} failed on file {file_path.name}: {e}")
                continue
            
            finally:
                force_memory_cleanup()
        
        # Signal completion
        doc_queue.put(None)
        
        logger.info(f"Worker {worker_id} completed all files: Parsed {total_docs_parsed:,} total documents")
        
    except Exception as e:
        logger.error(f"Worker {worker_id} failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        doc_queue.put(None)
    
    finally:
        force_memory_cleanup()


def elasticsearch_consumer(doc_queue, es, batch_size, max_chunk_bytes, 
                          thread_count, queue_size, num_workers, 
                          stats_dict, stop_event):
    """
    Consumer that pulls documents from queue and bulk indexes to ES
    This runs in the main process and manages the single ES client
    """
    logger = logging.getLogger("es-consumer")
    logger.info("ES Consumer started")
    
    indexed = 0
    failed = 0
    workers_completed = 0
    start_time = time.time()
    
    def document_generator():
        """Generator that yields documents from the queue"""
        nonlocal workers_completed
        
        while True:
            try:
                batch = doc_queue.get(timeout=5)
                
                if batch is None:
                    workers_completed += 1
                    logger.info(f"Worker completed ({workers_completed}/{num_workers})")
                    
                    if workers_completed >= num_workers:
                        logger.info("All workers completed, finishing indexing")
                        break
                    continue
                
                for doc in batch:
                    yield doc
                    
            except Empty:
                if stop_event.is_set():
                    logger.warning("Stop event set, finishing indexing")
                    break
                continue
            except Exception as e:
                logger.error(f"Error getting documents from queue: {e}")
                break
    
    try:
        logger.info(f"Starting bulk indexing with {thread_count} threads")
        
        for success, info in helpers.parallel_bulk(
            es,
            document_generator(),
            chunk_size=batch_size,
            max_chunk_bytes=max_chunk_bytes * 1024 * 1024,
            thread_count=thread_count,
            queue_size=queue_size,
            request_timeout=300, # was 120
        ):
            if success:
                indexed += 1
            else:
                failed += 1
                logger.error(f"Failed to index document: {info}")
            
            if indexed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = indexed / elapsed if elapsed > 0 else 0
                queue_depth = doc_queue.qsize()
                
                logger.info(
                    f"Progress: {indexed:,} indexed, {failed:,} failed, "
                    f"Rate: {rate:.1f} docs/sec, Queue: {queue_depth}, "
                    f"Elapsed: {elapsed:.1f}s"
                )
                
                if indexed % 100000 == 0:
                    log_memory_usage(logger, f"ES CONSUMER AT {indexed} DOCS")
                    gc.collect()
        
        elapsed = time.time() - start_time
        rate = indexed / elapsed if elapsed > 0 else 0
        
        logger.info(f"=== ES Consumer Completed ===")
        logger.info(f"Total indexed: {indexed:,}")
        logger.info(f"Total failed: {failed:,}")
        logger.info(f"Final rate: {rate:.1f} docs/sec")
        logger.info(f"Total time: {elapsed:.1f}s")
        
        stats_dict['indexed'] = indexed
        stats_dict['failed'] = failed
        stats_dict['elapsed'] = elapsed
        
    except Exception as e:
        logger.error(f"ES Consumer error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        stats_dict['indexed'] = indexed
        stats_dict['failed'] = failed
        stats_dict['error'] = str(e)


def elasticsearch_consumer_weird(doc_queue, es, batch_size, max_chunk_bytes, 
                          thread_count, queue_size, num_workers, 
                          stats_dict, stop_event):
    """
    Consumer that pulls documents from queue and bulk indexes to ES
    Enhanced with 429 error handling and adaptive batching
    """
    logger = logging.getLogger("es-consumer")
    logger.info("ES Consumer started with backpressure handling")
    
    indexed = 0
    failed = 0
    workers_completed = 0
    start_time = time.time()
    consecutive_429s = 0
    
    # current_batch_size = batch_size
    min_batch_size = 1000
    
    def document_generator():
        """Generator with backpressure signaling"""
        nonlocal workers_completed
        
        while True:
            try:
                batch = doc_queue.get(timeout=5)
                
                if batch is None:
                    workers_completed += 1
                    logger.info(f"Worker completed ({workers_completed}/{num_workers})")
                    
                    if workers_completed >= num_workers:
                        logger.info("All workers completed, finishing indexing")
                        break
                    continue
                
                for doc in batch:
                    yield doc
                    
            except Empty:
                if stop_event.is_set():
                    logger.warning("Stop event set, finishing indexing")
                    break
                if workers_completed >= num_workers and doc_queue.empty():
                    break
                continue
            except Exception as e:
                logger.error(f"Error getting documents from queue: {e}")
                break
    
    try:
        #logger.info(f"Starting bulk indexing with adaptive batching")
        logger.info(f"Batch size: {batch_size}")
        
        for success, info in helpers.parallel_bulk(
            es,
            document_generator(),
            chunk_size=batch_size,
            max_chunk_bytes=max_chunk_bytes * 1024 * 1024,
            thread_count=thread_count,
            queue_size=queue_size,
            request_timeout=300,
            raise_on_error=False,
            raise_on_exception=False,
        ):
            if success:
                indexed += 1
                consecutive_429s = 0
                
                if indexed % 50000 == 0 and current_batch_size < batch_size:
                    current_batch_size = min(current_batch_size + 1000, batch_size)
                    logger.info(f"Increased batch size to {current_batch_size}")
            else:
                failed += 1
                
                error_msg = str(info)
                if '429' in error_msg or 'too_many_requests' in error_msg.lower():
                    consecutive_429s += 1
                    
                    if consecutive_429s > 10:
                        backoff_time = min(consecutive_429s * 0.5, 30)
                        logger.warning(f"Too many 429 errors ({consecutive_429s}), "
                                     f"backing off for {backoff_time}s")
                        time.sleep(backoff_time)
                        
                        current_batch_size = max(min_batch_size, current_batch_size // 2)
                        logger.info(f"Reduced batch size to {current_batch_size}")
                        consecutive_429s = 0
                else:
                    logger.error(f"Failed to index document: {info}")
            
            if indexed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = indexed / elapsed if elapsed > 0 else 0
                queue_depth = doc_queue.qsize()
                
                logger.info(
                    f"Progress: {indexed:,} indexed, {failed:,} failed, "
                    f"Rate: {rate:.1f} docs/sec, Queue: {queue_depth}, "
                    f"Batch size: {current_batch_size}, Elapsed: {elapsed:.1f}s"
                )
                
                if indexed % 100000 == 0:
                    gc.collect()
        
        elapsed = time.time() - start_time
        rate = indexed / elapsed if elapsed > 0 else 0
        
        logger.info(f"=== ES Consumer Completed ===")
        logger.info(f"Total indexed: {indexed:,}")
        logger.info(f"Total failed: {failed:,}")
        logger.info(f"Final rate: {rate:.1f} docs/sec")
        logger.info(f"Total time: {elapsed:.1f}s")
        
        stats_dict['indexed'] = indexed
        stats_dict['failed'] = failed
        stats_dict['elapsed'] = elapsed
        
    except Exception as e:
        logger.error(f"ES Consumer error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        stats_dict['indexed'] = indexed
        stats_dict['failed'] = failed
        stats_dict['error'] = str(e)


# ============================================================================
# MAIN PROCESSING ORCHESTRATION
# ============================================================================

def process_file_list(file_list, chunk_size, index_name, es, batch_size,
                     max_chunk_bytes, thread_count, queue_size, num_workers=1):
    """
    Process files with multi-process parsing and single ES client
    
    Architecture:
    - Multiple worker processes parse parquet files in parallel
    - Workers put parsed documents into a shared queue
    - Single ES consumer (in main process) pulls from queue and bulk indexes
    """
    logger = logging.getLogger(__name__)
    
    if not file_list:
        raise ValueError("No files provided to process")
    
    logger.info(f"Processing {len(file_list)} parquet files")
    
    # Single-process mode (backward compatible)
    if num_workers == 1:
        logger.info("Running in single-process mode (backward compatible)")
        global_stats = {"indexed": 0, "failed": 0, "files_processed": 0}
        start_time = time.time()
        
        for i, file_path in enumerate(file_list):
            logger.info(f"Processing file {i+1}/{len(file_list)}: {file_path.name}")
            log_memory_usage(logger, f"BEFORE FILE {i+1}")
            
            try:
                doc_generator = process_single_parquet_file_streaming(
                    file_path, chunk_size, index_name
                )
                
                file_stats = bulk_index_documents(
                    es, doc_generator, batch_size, max_chunk_bytes,
                    thread_count, queue_size, global_stats, start_time
                )
                
                global_stats["files_processed"] += 1
                logger.info(f"File {i+1} completed: {file_stats['indexed']:,} indexed")
                
            except Exception as e:
                logger.error(f"Failed to process file {file_path.name}: {e}")
                continue
            finally:
                force_memory_cleanup()
                log_memory_usage(logger, f"AFTER FILE {i+1}")
        
        return global_stats
    
    # Multi-process mode with shared queue
    actual_workers = min(num_workers, len(file_list))

    logger.info(f"=== Starting Multi-Process Mode ===")
    logger.info(f"Requested workers: {num_workers}")
    logger.info(f"Files: {len(file_list)}")
    logger.info(f"Actual workers: {actual_workers}")
    logger.info(f"Architecture: {actual_workers} parser workers -> shared queue -> 1 ES consumer")
    
    doc_queue = Queue(maxsize=300)
    docs_parsed_counter = Value('i', 0)
    stop_event = Event()
    
    manager = mp.Manager()
    stats_dict = manager.dict()
    stats_dict['indexed'] = 0
    stats_dict['failed'] = 0
    
    overall_start = time.time()
    
    try:
        workers = []

        # Distribute files across workers using round-robin
        for worker_id in range(actual_workers):
            worker_files = file_list[worker_id::actual_workers]
            
            logger.info(f"Worker {worker_id} will process {len(worker_files)} files")
            
            worker = Process(
                target=parse_parquet_worker_batch,  
                args=(worker_files, chunk_size, index_name, doc_queue, 
                      docs_parsed_counter, worker_id, stop_event)
            )
            worker.start()
            workers.append(worker)
        
        logger.info(f"All {len(workers)} parser workers started")
        
        consumer_thread = Thread(
            target=elasticsearch_consumer,
            args=(doc_queue, es, batch_size, max_chunk_bytes, 
                  thread_count, queue_size, len(workers), stats_dict, stop_event)
        )
        consumer_thread.start()
        logger.info("ES consumer thread started")
        
        last_count = 0
        while consumer_thread.is_alive():
            consumer_thread.join(timeout=10)
            
            current_parsed = docs_parsed_counter.value
            current_indexed = stats_dict.get('indexed', 0)
            
            if current_parsed > last_count:
                logger.info(f"Overall progress: Parsed {current_parsed:,}, "
                          f"Indexed {current_indexed:,}, Queue: {doc_queue.qsize()}")
                last_count = current_parsed
        
        logger.info("Waiting for parser workers to complete...")
        for i, worker in enumerate(workers):
            worker.join(timeout=30)
            if worker.is_alive():
                logger.warning(f"Worker {i} did not finish, terminating")
                worker.terminate()
        
        logger.info("All workers completed")
        
        elapsed = time.time() - overall_start
        final_indexed = stats_dict.get('indexed', 0)
        final_failed = stats_dict.get('failed', 0)
        final_parsed = docs_parsed_counter.value
        
        logger.info(f"=== Multi-Process Completion ===")
        logger.info(f"Total documents parsed: {final_parsed:,}")
        logger.info(f"Total documents indexed: {final_indexed:,}")
        logger.info(f"Total failed: {final_failed:,}")
        logger.info(f"Overall time: {elapsed:.1f}s")
        logger.info(f"Overall rate: {final_indexed/elapsed:.1f} docs/sec")
        
        return {
            "indexed": final_indexed,
            "failed": final_failed,
            "files_processed": len(workers),
            "elapsed": elapsed
        }
        
    except KeyboardInterrupt:
        logger.warning("Received interrupt, stopping workers...")
        stop_event.set()
        for worker in workers:
            worker.terminate()
        raise
        
    except Exception as e:
        logger.error(f"Multi-process execution failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        stop_event.set()
        for worker in workers:
            worker.terminate()
        raise


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Index FineWeb dataset into Elasticsearch - Multi-process with content-based IDs"
    )
    parser.add_argument("--data-dir", required=True, help="Directory containing parquet files")
    
    parser.add_argument("--file-range-start", type=int, help="Starting file index (0-based, inclusive)")
    parser.add_argument("--file-range-end", type=int, help="Ending file index (exclusive)")
    
    parser.add_argument("--min-shard-size", type=float, default=10.0, 
                       help="Minimum shard size in GB (default: 10)")
    parser.add_argument("--max-shard-size", type=float, default=50.0,
                       help="Maximum shard size in GB (default: 50)")
    parser.add_argument("--es-expansion-factor", type=float, default=3.0,
                       help="ES size expansion factor vs parquet (default: 3.0)")
    
    parser.add_argument("--batch-size", type=int, default=12500, help="Batch size for bulk indexing")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Chunk size for reading parquet files")
    parser.add_argument("--es-host", default="localhost", help="Elasticsearch host")
    parser.add_argument("--es-port", type=int, default=9200, help="Elasticsearch port")
    parser.add_argument("--index-name", default="fineweb", help="Elasticsearch index name")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--index-config", help="Path to JSON file with index configuration")
    parser.add_argument("--max-chunk-bytes", type=int, default=75, help="Max chunk size in MB for parallel_bulk")
    parser.add_argument("--thread-count", type=int, default=8, help="Number of threads for parallel_bulk")
    parser.add_argument("--queue-size", type=int, default=8, help="Queue size for parallel_bulk")
    
    parser.add_argument("--num-workers", type=int, default=1,
                       help="Number of parallel parser workers (default: 1). "
                            "Recommended: 6-8 for 10 CPUs")
    
    args = parser.parse_args()
    
    logger = setup_logging(args.log_level)
    
    tracemalloc.start()
    log_memory_usage(logger, "AT START")

    data_dir_pattern = args.data_dir  

    # Check if it's a pattern, single/multiple files, or single directory
    if '*' in data_dir_pattern:
        # Pattern with wildcards
        logger.info(f"Using directory pattern: {data_dir_pattern}")
        files_to_process, total_data_size_gb = get_file_range(
            data_dir_pattern, 
            args.file_range_start if args.file_range_start is not None else None,
            args.file_range_end if args.file_range_end is not None else None
        )
    elif ' ' in data_dir_pattern:
        # Multiple space-separated paths (files or directories)
        logger.info(f"Processing multiple paths: {data_dir_pattern}")
        paths = data_dir_pattern.split()
        files_to_process = []
        
        for path_str in paths:
            path = Path(path_str)
            if not path.exists():
                logger.error(f"Path does not exist: {path}")
                sys.exit(1)
            
            if path.is_file():
                if path.suffix == '.parquet':
                    files_to_process.append(path)
                else:
                    logger.error(f"File is not a parquet file: {path}")
                    sys.exit(1)
            elif path.is_dir():
                dir_parquet_files = sorted(list(path.glob("*.parquet")))
                if not dir_parquet_files:
                    logger.warning(f"No parquet files found in directory: {path}")
                else:
                    files_to_process.extend(dir_parquet_files)
        
        if not files_to_process:
            logger.error("No parquet files found in any of the specified paths")
            sys.exit(1)
        
        total_size_bytes = sum(f.stat().st_size for f in files_to_process)
        total_data_size_gb = total_size_bytes / (1024**3)
        logger.info(f"Found {len(files_to_process)} parquet files across all paths")
    else:
        # Single path (file or directory)
        data_path = Path(data_dir_pattern)
        if not data_path.exists():
            logger.error(f"Path does not exist: {data_path}")
            sys.exit(1)
        
        if data_path.is_file():
            # Single parquet file
            if data_path.suffix != '.parquet':
                logger.error(f"File is not a parquet file: {data_path}")
                sys.exit(1)
            
            logger.info(f"Processing single parquet file: {data_path}")
            files_to_process = [data_path]
            total_data_size_gb = data_path.stat().st_size / (1024**3)
            
        elif data_path.is_dir():
            # Single directory
            if args.file_range_start is not None and args.file_range_end is not None:
                files_to_process, total_data_size_gb = get_file_range(
                    str(data_path), 
                    args.file_range_start, 
                    args.file_range_end
                )
            else:
                all_parquet_files = sorted(list(data_path.glob("*.parquet")))
                if not all_parquet_files:
                    logger.error(f"No parquet files found in {data_path}")
                    sys.exit(1)
                files_to_process = all_parquet_files
                total_size_bytes = sum(f.stat().st_size for f in files_to_process)
                total_data_size_gb = total_size_bytes / (1024**3)

            
    logger.info("=" * 70)
    logger.info("=== FineWeb Dataset Indexing Started ===")
    logger.info(f"Data directory: {data_dir_pattern}")
    logger.info(f"Files to process: {len(files_to_process)}")
    logger.info(f"Total raw data size: {total_data_size_gb:.2f} GB")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Chunk size: {args.chunk_size}")
    logger.info(f"Elasticsearch: {args.es_host}:{args.es_port}")
    logger.info(f"Index name: {args.index_name}")
    logger.info(f"Num workers: {args.num_workers}")
    logger.info(f"Content-based IDs: ENABLED (SHA256)")
    logger.info("=" * 70)
    
    # STEP 1: Count documents in source files (GROUND TRUTH)
    logger.info("\n" + "=" * 70)
    logger.info("STEP 1: Counting documents in source files...")
    logger.info("=" * 70)
    try:
        ground_truth_count = count_documents_in_parquet_files(files_to_process)
    except Exception as e:
        logger.error(f"Failed to count documents: {e}")
        sys.exit(1)

    total_start_time = time.time()
    
    try:
        # Connect to Elasticsearch
        es = get_elasticsearch_client(args.es_host, args.es_port)
        log_memory_usage(logger, "AFTER ES CONNECTION")

        # Create index with size-based dynamic sharding
        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: Creating Elasticsearch index...")
        logger.info("=" * 70)
        if not create_index_with_size_based_shards(
            es, args.index_name, total_data_size_gb, args.index_config,
            args.min_shard_size, args.max_shard_size, args.es_expansion_factor
        ):
            sys.exit(1)
        
        log_memory_usage(logger, "AFTER INDEX CREATION")
        
        # Process files with multi-process support
        logger.info("\n" + "=" * 70)
        logger.info("STEP 3: Starting document indexing...")
        logger.info("=" * 70)
        indexing_start_time = time.time()
        
        stats = process_file_list(
            files_to_process, 
            args.chunk_size, 
            args.index_name, 
            es,
            args.batch_size, 
            args.max_chunk_bytes, 
            args.thread_count, 
            args.queue_size,
            args.num_workers  
        )
        
        indexing_end_time = time.time()
        log_memory_usage(logger, "AFTER INDEXING")
        
        # Force final refresh to ensure all documents are searchable
        logger.info("\n" + "=" * 70)
        logger.info("STEP 4: Finalizing index...")
        logger.info("=" * 70)
        try:
            logger.info("Performing final refresh...")
            es.indices.refresh(index=args.index_name)
            logger.info("Final refresh complete - all documents are now searchable")
        except Exception as e:
            logger.error(f"Failed to perform final refresh: {e}")

        # Get final index stats
        try:
            index_stats = es.indices.stats(index=args.index_name)
            doc_count = index_stats['indices'][args.index_name]['total']['docs']['count']
            index_size = index_stats['indices'][args.index_name]['total']['store']['size_in_bytes']
            
            logger.info(f"Final ES index document count: {doc_count:,}")
            logger.info(f"Index size: {index_size / (1024**3):.2f} GB")
            
        except Exception as e:
            logger.warning(f"Could not get final index stats: {e}")
            doc_count = None
        
        # Calculate timing statistics
        total_time = time.time() - total_start_time
        indexing_time = indexing_end_time - indexing_start_time
        
        logger.info("\n" + "=" * 70)
        logger.info("=== Indexing Statistics ===")
        logger.info(f"Documents parsed: {stats['indexed']:,}")
        logger.info(f"Documents failed: {stats['failed']:,}")
        logger.info(f"Files processed: {stats['files_processed']:,}")
        logger.info(f"Indexing time: {indexing_time:.2f} seconds")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
        if indexing_time > 0:
            logger.info(f"Average indexing rate: {stats['indexed'] / indexing_time:.1f} docs/sec")
        
        # Log final memory usage
        log_memory_usage(logger, "FINAL")
        try:
            current, peak = tracemalloc.get_traced_memory()
            logger.info(f"Peak memory usage during execution: {peak / (1024**3):.2f} GB")
            tracemalloc.stop()
        except Exception as e:
            logger.warning(f"Could not get tracemalloc info: {e}")

        # STEP 5: STRICT VALIDATION
        logger.info("\n" + "=" * 70)
        logger.info("STEP 5: Validating indexing results...")
        logger.info("=" * 70)
        validation_passed = validate_indexing_results(
            ground_truth_count=ground_truth_count,
            stats=stats,
            es=es,
            index_name=args.index_name
        )
        
        # EXIT WITH APPROPRIATE STATUS CODE
        if not validation_passed:
            logger.error("\n" + "=" * 70)
            logger.error("❌❌❌ INDEXING JOB FAILED - VALIDATION ERRORS DETECTED ❌❌❌")
            logger.error("=" * 70)
            sys.exit(1)
            
        logger.info("\n" + "=" * 70)
        logger.info("✓✓✓ INDEXING JOB COMPLETED SUCCESSFULLY ✓✓✓")
        logger.info("=" * 70)
        sys.exit(0)
        
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