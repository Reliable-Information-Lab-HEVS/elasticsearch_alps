#!/usr/bin/env python3
"""
FineWeb Dataset Indexer for Elasticsearch
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
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, ConnectionError
import psutil
import tracemalloc


def log_memory_usage(logger, context: str = ""):
    """Log current memory usage"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        # Get system memory info
        system_memory = psutil.virtual_memory()
        
        logger.info(f"=== MEMORY USAGE {context} ===")
        logger.info(f"Process RSS: {memory_info.rss / (1024**3):.2f} GB")
        logger.info(f"Process VMS: {memory_info.vms / (1024**3):.2f} GB")
        logger.info(f"Process Memory %: {memory_percent:.1f}%")
        logger.info(f"System Memory Used: {system_memory.percent:.1f}%")
        logger.info(f"System Available: {system_memory.available / (1024**3):.2f} GB")
        logger.info("=" * 40)
        
    except Exception as e:
        logger.warning(f"Could not get memory info: {e}")


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


def create_index_config() -> Dict[str, Any]:
    """Create index configuration"""
    return {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0,
            "refresh_interval": "300s",
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
                            "asciifolding",
                            "english_stop",
                            "english_stemmer"
                        ]
                    }
                },
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
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
                    "store": False,
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 512
                        }
                    }
                }
            }
        }
    }


def create_index(es: Elasticsearch, index_name: str, config_file: str = None) -> bool:
    """Create Elasticsearch index with configuration from file or default"""
    try:
        if es.indices.exists(index=index_name):
            logging.warning(f"Index '{index_name}' already exists. Deleting...")
            es.indices.delete(index=index_name)
        
        if config_file and os.path.exists(config_file):
            logging.info(f"Loading index configuration from: {config_file}")
            with open(config_file, 'r') as f:
                config = json.load(f)
        else:
            logging.info("Using default index configuration")
            config = create_index_config()
        
        es.indices.create(index=index_name, body=config)
        logging.info(f"Created index '{index_name}' with configuration")
        return True
        
    except Exception as e:
        logging.error(f"Failed to create index: {e}")
        return False


def process_single_parquet_file(file_path: Path, chunk_size: int, index_name: str) -> Generator[Dict[str, Any], None, None]:
    """Process a single parquet file with proper memory management"""
    logger = logging.getLogger(__name__)
    parquet_file = None
    
    try:
        logger.info(f"Reading parquet file: {file_path.name}")
        parquet_file = pd.read_parquet(file_path, engine='pyarrow')
        
        if 'text' not in parquet_file.columns:
            logger.warning(f"'text' column not found in {file_path.name}, skipping...")
            return
        
        num_rows = len(parquet_file)
        logger.info(f"File has {num_rows:,} rows, processing in chunks of {chunk_size:,}")
        
        # Process in chunks
        for chunk_start in range(0, num_rows, chunk_size):
            chunk_end = min(chunk_start + chunk_size, num_rows)
            chunk = parquet_file.iloc[chunk_start:chunk_end].copy()
            
            logger.debug(f"Processing chunk {chunk_start:,} to {chunk_end:,}")
            
            # Process chunk and yield documents immediately
            for _, row in chunk.iterrows():
                text_content = row['text']
                
                if pd.isna(text_content) or not str(text_content).strip():
                    continue
                
                text_str = str(text_content).strip()
                if len(text_str) > 500000:
                    logger.warning(f"Extremely large document detected: {len(text_str)} chars")
                if len(text_str) > 100000:
                    text_str = text_str[:100000] + "... [TRUNCATED]"
                
                doc = {
                    "_index": index_name,
                    "_source": {
                        "text": text_str
                    }
                }
                
                yield doc
            
            # Cleanup chunk immediately
            del chunk
            gc.collect()
            
            if chunk_end % 50000 == 0:
                logger.debug(f"Processed {chunk_end:,}/{num_rows:,} rows from {file_path.name}")
        
        logger.info(f"Completed processing {file_path.name}")
        
    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        raise
    finally:
        # Ensure cleanup happens
        if parquet_file is not None:
            del parquet_file
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
            
            # Log progress every 5,000 documents
            if global_stats["indexed"] % 5000 == 0:
                current_time = time.time()
                elapsed = current_time - start_time
                rate = global_stats["indexed"] / elapsed if elapsed > 0 else 0
                
                logger.info(
                    f"Progress: {global_stats['indexed']:,} indexed, {global_stats['failed']:,} failed, "
                    f"Rate: {rate:.1f} docs/sec, Elapsed: {elapsed:.1f}s"
                )
                
                # Log memory usage every 5k docs
                log_memory_usage(logger, f"BULK INDEXING AT {global_stats['indexed']} DOCS")
                
                gc.collect()
        
    except Exception as e:
        logger.error(f"Bulk indexing error: {e}")
        raise
    
    return stats


def process_files_sequentially(data_dir: Path, max_files: int, chunk_size: int, 
                              index_name: str, es: Elasticsearch, batch_size: int,
                              max_chunk_bytes: int, thread_count: int, queue_size: int) -> Dict[str, int]:
    """Process parquet files one by one to prevent memory accumulation"""
    logger = logging.getLogger(__name__)
    
    parquet_files = list(data_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")
    
    if max_files:
        parquet_files = parquet_files[:max_files]
    
    logger.info(f"Found {len(parquet_files)} parquet files to process")
    
    global_stats = {"indexed": 0, "failed": 0, "files_processed": 0}
    start_time = time.time()
    
    for i, file_path in enumerate(parquet_files):
        logger.info(f"Processing file {i+1}/{len(parquet_files)}: {file_path.name}")
        
        # Log memory before processing each file
        log_memory_usage(logger, f"BEFORE FILE {i+1}")
        
        try:
            # Process single file
            doc_generator = process_single_parquet_file(file_path, chunk_size, index_name)
            
            # Index documents from this file
            file_stats = bulk_index_documents(
                es, doc_generator, batch_size, max_chunk_bytes, 
                thread_count, queue_size, global_stats, start_time
            )
            
            global_stats["files_processed"] += 1
            
            logger.info(f"File {i+1} completed: {file_stats['indexed']:,} indexed, {file_stats['failed']:,} failed")
            
            # Force cleanup between files
            gc.collect()
            
            # Log memory after processing each file
            log_memory_usage(logger, f"AFTER FILE {i+1}")
            
        except Exception as e:
            logger.error(f"Failed to process file {file_path.name}: {e}")
            continue
    
    return global_stats


def main():
    parser = argparse.ArgumentParser(description="Index FineWeb dataset into Elasticsearch - Memory Optimized")
    parser.add_argument("--data-dir", required=True, help="Directory containing parquet files")
    parser.add_argument("--max-files", type=int, default=1, help="Maximum number of parquet files to process")
    parser.add_argument("--batch-size", type=int, default=12500, help="Batch size for bulk indexing")
    parser.add_argument("--chunk-size", type=int, default=12000, help="Chunk size for reading parquet files")
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
    
    logger.info("=== FineWeb Dataset Indexing Started - Memory Optimized ===")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Max files to process: {args.max_files}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Chunk size: {args.chunk_size}")
    logger.info(f"Elasticsearch: {args.es_host}:{args.es_port}")
    logger.info(f"Index name: {args.index_name}")
    
    total_start_time = time.time()
    
    try:
        # Connect to Elasticsearch
        es = get_elasticsearch_client(args.es_host, args.es_port)
        log_memory_usage(logger, "AFTER ES CONNECTION")

        # Create index
        logger.info("Creating Elasticsearch index...")
        if not create_index(es, args.index_name, args.index_config):
            sys.exit(1)
        
        log_memory_usage(logger, "AFTER INDEX CREATION")
        
        # Process files sequentially
        logger.info("Starting document indexing...")
        indexing_start_time = time.time()
        
        stats = process_files_sequentially(
            data_dir, args.max_files, args.chunk_size, args.index_name, es,
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
        
        # Force index refresh
        logger.info("Refreshing index...")
        es.indices.refresh(index=args.index_name)
        
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