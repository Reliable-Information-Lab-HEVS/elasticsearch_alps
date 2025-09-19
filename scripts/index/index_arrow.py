#!/usr/bin/env python3
"""
Arrow SFT Dataset Indexer for Elasticsearch
Indexes conversation data from Arrow files with user/assistant message pairs
"""

import os
import sys
import time
import logging
import gc
import re
from pathlib import Path
from typing import Generator, Dict, Any, List
import argparse
import json

import pandas as pd
import pyarrow as pa
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, ConnectionError
import psutil
import tracemalloc


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def log_memory_usage(logger, context: str = ""):
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


def create_sft_index_config(num_shards: int = 3, num_replicas: int = 0) -> Dict[str, Any]:
    """Create index configuration
    num of shards: either set by dev or computed dynamically according to incoming data size
    num of replicas: usually set to 0 to optimize indexing speed, may need to up for robustness
    word agnostic analyzer (does not include stop word removal or stemming)
    metadata information is indexed as 'keyword' => allows for exact matches queries"""
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
                    "conversation_analyzer": {
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
                "includes": ["conversation_id","original_metadata","text"],
                "excludes": []
            },
            "properties": {
                "conversation_id": {
                    "type": "keyword",
                    "index_options": "docs",
                    "norms": False,
                    "store": False
                },
                "original_metadata": {
                    "type": "keyword",
                    "index_options": "docs",
                    "norms": False,
                    "store": False
                },
                "text": {
                    "type": "text",
                    "analyzer": "conversation_analyzer",
                    "index_options": "positions",
                    "norms": True,
                    "store": False
                }
            }
        }
    }

def filter_arrow_files(data_dir: Path, logger: logging.Logger) -> List[Path]:
    """Filter Arrow files to only include data-XXXXX-of-YYYYY.arrow pattern and exclude cache files"""
    
    # Find all Arrow files
    all_arrow_files = list(data_dir.glob("*.arrow"))
    logger.info(f"Found {len(all_arrow_files)} total .arrow files")
    
    # Filter for data-XXXXX-of-YYYYY.arrow pattern and exclude cache files
    data_pattern = re.compile(r'data-\d{5}-of-\d{5}\.arrow$')
    arrow_files = []
    cache_files = []
    other_files = []
    
    for file_path in all_arrow_files:
        file_name = file_path.name
        file_path_str = str(file_path).lower()
        
        # Skip cache files
        if 'cache' in file_name.lower() or 'cache' in file_path_str:
            cache_files.append(file_path)
            logger.debug(f"Skipping cache file: {file_name}")
            continue
        
        # Only include files matching data-XXXXX-of-YYYYY.arrow pattern
        if data_pattern.match(file_name):
            arrow_files.append(file_path)
            logger.debug(f"Including data file: {file_name}")
        else:
            other_files.append(file_path)
            logger.debug(f"Skipping non-data file: {file_name}")
    
    # Sort the filtered files
    arrow_files = sorted(arrow_files)
    
    # Log filtering results
    logger.info(f"File filtering results:")
    logger.info(f"  Data files (data-XXXXX-of-YYYYY.arrow): {len(arrow_files)}")
    logger.info(f"  Cache files excluded: {len(cache_files)}")
    logger.info(f"  Other files excluded: {len(other_files)}")
    
    if len(arrow_files) == 0:
        logger.error(f"No data-XXXXX-of-YYYYY.arrow files found in {data_dir}")
        logger.info("Available files by type:")
        
        if cache_files:
            logger.info("  Cache files (excluded):")
            for file_path in cache_files[:5]:  # Show first 5 for debugging
                logger.info(f"    {file_path.name}")
            if len(cache_files) > 5:
                logger.info(f"    ... and {len(cache_files) - 5} more cache files")
        
        if other_files:
            logger.info("  Other files (excluded):")
            for file_path in other_files[:5]:  # Show first 5 for debugging
                logger.info(f"    {file_path.name}")
            if len(other_files) > 5:
                logger.info(f"    ... and {len(other_files) - 5} more other files")
        
        return []
    
    # Show sample of included files
    logger.info("Sample of data files to process:")
    for file_path in arrow_files[:5]:
        logger.info(f"  {file_path.name}")
    if len(arrow_files) > 5:
        logger.info(f"  ... and {len(arrow_files) - 5} more data files")
    
    return arrow_files


def calculate_optimal_shards(total_conversations: int, avg_conversation_size_kb: float = 2.0) -> int:
    """Calculate optimal number of shards based on conversation data
    Not actually used in execution"""
    logger = logging.getLogger(__name__)
    
    # Estimate total index size
    estimated_size_gb = (total_conversations * avg_conversation_size_kb) / (1024 * 1024)
    
    # Target 20-30GB per shard for conversation data
    target_shard_size_gb = 25.0
    optimal_shards = max(1, int(estimated_size_gb / target_shard_size_gb))
    
    # Cap at reasonable limits
    optimal_shards = min(optimal_shards, 10)
    
    logger.info(f"Estimated index size: {estimated_size_gb:.2f} GB")
    logger.info(f"Calculated optimal shards: {optimal_shards}")
    
    return optimal_shards


def create_index_with_shards(es: Elasticsearch, index_name: str) -> bool:
    """Create Elasticsearch index with calculated shard count"""
    try:
        if es.indices.exists(index=index_name):
            logging.warning(f"Index '{index_name}' already exists. Deleting...")
            es.indices.delete(index=index_name)

        # optimal_shards = calculate_optimal_shards(total_conversations)
        optimal_shards=1
        config = create_sft_index_config(num_shards=1)
        
        logging.info(f"Creating index '{index_name}' with {optimal_shards} shards")
        es.indices.create(index=index_name, body=config)
        logging.info(f"Created index '{index_name}' successfully")
        return True
        
    except Exception as e:
        logging.error(f"Failed to create index: {e}")
        return False


def read_arrow_file(filepath: Path):
    """Read Arrow file using IPC stream"""
    filepath = Path(filepath)
    
    with pa.OSFile(str(filepath), 'rb') as source:
        reader = pa.ipc.open_stream(source)
        table = reader.read_all()
        return table
def process_conversation(row, index_name: str) -> Dict[str, Any]:
    """Process a single conversation row into a minimal Elasticsearch document"""
    try:
        conversation_id = str(row['conversation_id'])
        original_metadata = row.get('original_metadata', '')
        messages = row.get('messages', [])
        
        # Safe check for messages - handle both pandas Series and regular lists
        if messages is None:
            return None
            
        # Convert to list if it's a pandas Series or numpy array
        if hasattr(messages, 'tolist'):
            messages = messages.tolist()
        elif hasattr(messages, '__iter__') and not isinstance(messages, (str, dict)):
            messages = list(messages)
        
        # Check if messages is empty
        if not isinstance(messages, list) or len(messages) == 0:
            return None
        
        conversation_texts = []
        
        for message in messages:
            if not isinstance(message, dict):
                continue
                
            role = message.get('role', '')
            content = message.get('content', {})
            
            # Only process user and assistant messages
            if role not in ['user', 'assistant']:
                continue
            
            # Safe content extraction
            if not isinstance(content, dict):
                content = {}
            
            # Extract text from parts (primary location)
            parts = content.get('parts', [])
            if parts is None:
                parts = []
            elif hasattr(parts, 'tolist'):
                parts = parts.tolist()
                
            text_from_parts = extract_text_from_parts(parts)
            
            # Extract text from blocks (fallback)
            blocks = content.get('blocks', [])
            if blocks is None:
                blocks = []
            elif hasattr(blocks, 'tolist'):
                blocks = blocks.tolist()
                
            text_from_blocks = extract_text_from_blocks(blocks)
            
            # Extract main text field - handle potential arrays
            main_text = content.get('text', '')
            if hasattr(main_text, 'tolist'):
                main_text = ' '.join(str(x) for x in main_text.tolist())
            elif not isinstance(main_text, str):
                main_text = str(main_text)
            
            # Combine all text sources
            message_text_parts = []
            if text_from_parts:
                message_text_parts.append(text_from_parts)
            if text_from_blocks:
                message_text_parts.append(text_from_blocks)
            if main_text:
                message_text_parts.append(main_text)
            
            message_text = " ".join(message_text_parts).strip()
            
            if message_text:
                conversation_texts.append(f"{role}: {message_text}")
        
        # Skip conversations with no extractable text
        if not conversation_texts:
            return None
        
        # Combine all text
        conversation_text = "\n\n".join(conversation_texts)
        
        # Safe conversion of original_metadata
        if hasattr(original_metadata, 'tolist'):
            original_metadata_str = str(original_metadata.tolist())
        elif original_metadata is not None:
            original_metadata_str = str(original_metadata)
        else:
            original_metadata_str = ""
        
        # Create minimal document with only required fields
        doc = {
            "_index": index_name,
            "_id": conversation_id,
            "_source": {
                "text": conversation_text,
                "conversation_id": conversation_id,
                "original_metadata": original_metadata_str
            }
        }
        
        return doc
        
    except Exception as e:
        conversation_id = row.get('conversation_id', 'unknown') if hasattr(row, 'get') else 'unknown'
        logging.error(f"Error processing conversation {conversation_id}: {e}")
        return None


def extract_text_from_parts(parts):
    """Extract text content from message parts - handle arrays safely"""
    if not parts:
        return ""
    
    # Handle pandas Series or numpy arrays
    if hasattr(parts, 'tolist'):
        parts = parts.tolist()
    
    # Ensure it's a list
    if not isinstance(parts, list):
        return ""
    
    text_parts = []
    for part in parts:
        if isinstance(part, dict) and part.get('type') == 'text':
            text = part.get('text', '')
            if text:
                # Handle potential arrays in text field
                if hasattr(text, 'tolist'):
                    text = ' '.join(str(x) for x in text.tolist())
                text_parts.append(str(text))
    
    return " ".join(text_parts).strip()


def extract_text_from_blocks(blocks):
    """Extract text content from message blocks"""
    if not blocks:
        return ""
    
    # Handle pandas Series or numpy arrays
    if hasattr(blocks, 'tolist'):
        blocks = blocks.tolist()
    
    # Ensure it's a list
    if not isinstance(blocks, list):
        return ""
    
    text_parts = []
    for block in blocks:
        if isinstance(block, dict):
            text = block.get('text', '')
            if text:
                # Handle potential arrays in text field
                if hasattr(text, 'tolist'):
                    text = ' '.join(str(x) for x in text.tolist())
                text_parts.append(str(text))
    
    return " ".join(text_parts).strip()

def process_arrow_file_streaming(file_path: Path, index_name: str, chunk_size_proc: int = 1000, 
                               output_file_handle=None) -> Generator[Dict[str, Any], None, None]:
    """Process Arrow file in chunks to prevent memory issues"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Processing Arrow file: {file_path.name}")
        
        # Read the entire table (Arrow files are typically smaller than parquet)
        table = read_arrow_file(file_path)
        df = table.to_pandas()
        
        total_rows = len(df)
        logger.info(f"Processing {total_rows:,} conversations")
        
        processed_count = 0
        yielded_count = 0
        
        # Process in chunks
        # Chunk_size_proc parameter is very much chosen at random here lol
        for chunk_start in range(0, total_rows, chunk_size_proc):
            chunk_end = min(chunk_start + chunk_size_proc, total_rows)
            chunk = df.iloc[chunk_start:chunk_end]
            
            for _, row in chunk.iterrows():
                try:
                    doc = process_conversation(row, index_name)
                    if doc:
                        if output_file_handle:
                            json.dump(doc["_source"], output_file_handle)
                            output_file_handle.write('\n')
                        
                        yield doc
                        yielded_count += 1

                    
                    processed_count += 1
                    
                    if processed_count % 1000 == 0:
                        logger.debug(f"Processed {processed_count:,}/{total_rows:,} conversations, yielded {yielded_count:,}")
                        
                except Exception as e:
                    logger.error(f"Error processing row {processed_count}: {e}")
                    continue
            
            # Cleanup chunk
            del chunk
            gc.collect()
        
        logger.info(f"Completed processing {file_path.name}: {processed_count:,} processed, {yielded_count:,} yielded")
        
    except Exception as e:
        logger.error(f"Error processing Arrow file {file_path.name}: {e}")
        raise
    finally:
        # Cleanup
        if 'df' in locals():
            del df
        if 'table' in locals():
            del table
        gc.collect()


def bulk_index_documents(es: Elasticsearch, doc_generator: Generator, chunk_size: int,
                        thread_count: int, global_stats: Dict[str, int], start_time: float) -> Dict[str, int]:
    """Bulk index documents with progress tracking"""
    logger = logging.getLogger(__name__)
    stats = {"indexed": 0, "failed": 0}
    
    try:
        for success, info in helpers.parallel_bulk(
            es,
            doc_generator,
            chunk_size=chunk_size,
            max_chunk_bytes=75 * 1024 * 1024,
            thread_count=thread_count,
            queue_size=4,
            request_timeout=60,
        ):
        # Have taken thread_count and queue_size parameters from parameter tuning of Fineweb-parquet based datasets
            if success:
                stats["indexed"] += 1
                global_stats["indexed"] += 1
            else:
                stats["failed"] += 1
                global_stats["failed"] += 1
                logger.error(f"Failed to index document: {info}")
            
            # Log progress every 1,000 documents
            if global_stats["indexed"] % 1000 == 0:
                current_time = time.time()
                elapsed = current_time - start_time
                rate = global_stats["indexed"] / elapsed if elapsed > 0 else 0
                
                logger.info(
                    f"Progress: {global_stats['indexed']:,} indexed, {global_stats['failed']:,} failed, "
                    f"Rate: {rate:.1f} docs/sec, Elapsed: {elapsed:.1f}s"
                )
                
                if global_stats["indexed"] % 5000 == 0:
                    log_memory_usage(logger, f"AT {global_stats['indexed']} DOCS")
                    gc.collect()
        
    except Exception as e:
        logger.error(f"Bulk indexing error: {e}")
        raise
    
    return stats


def process_arrow_files(file_list: List[Path], index_name: str, es: Elasticsearch, 
                       chunk_size: int, thread_count: int, chunk_size_proc: int,
                       output_file_path: Path = None) -> Dict[str, int]:
    """Process list of Arrow files"""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Processing {len(file_list)} Arrow files")
    
    global_stats = {"indexed": 0, "failed": 0, "files_processed": 0, "cleaned_conversations_written": 0}
    start_time = time.time()
    
    # Open output file if specified
    output_file_handle = None
    if output_file_path:
        try:
            output_file_handle = open(output_file_path, 'w', encoding='utf-8')
            logger.info(f"Opened cleaned conversations output file: {output_file_path}")
        except Exception as e:
            logger.error(f"Failed to open output file {output_file_path}: {e}")
            output_file_handle = None
    
    try:
        for i, file_path in enumerate(file_list):
            logger.info(f"Processing file {i+1}/{len(file_list)}: {file_path.name}")
            
            log_memory_usage(logger, f"BEFORE FILE {i+1}")
            
            try:
                # Process single file
                doc_generator = process_arrow_file_streaming(
                    file_path, index_name, chunk_size_proc, output_file_handle
                )
                
                # Index documents from this file
                file_stats = bulk_index_documents(
                    es, doc_generator, chunk_size, thread_count, global_stats, start_time
                )
                
                global_stats["files_processed"] += 1
                global_stats["cleaned_conversations_written"] += file_stats["indexed"]
                
                logger.info(f"File {i+1} completed: {file_stats['indexed']:,} indexed, {file_stats['failed']:,} failed")
                
            except Exception as e:
                logger.error(f"Failed to process file {file_path.name}: {e}")
                continue
            finally:
                # Cleanup between files
                gc.collect()
                log_memory_usage(logger, f"AFTER FILE {i+1}")
    
    finally:
        # Close output file
        if output_file_handle:
            try:
                output_file_handle.close()
                logger.info(f"Closed cleaned conversations output file")
            except Exception as e:
                logger.error(f"Error closing output file: {e}")
    
    return global_stats



def main():
    parser = argparse.ArgumentParser(description="Index SFT Arrow dataset into Elasticsearch")
    parser.add_argument("--data-dir", required=True, help="Directory containing Arrow files")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Chunk size for bulk indexing")
    parser.add_argument("--chunk-size-proc", type=int, default=1000, help="Chunk size for processing Arrow files")
    parser.add_argument("--es-host", default="localhost", help="Elasticsearch host")
    parser.add_argument("--es-port", type=int, default=9200, help="Elasticsearch port")
    parser.add_argument("--index-name", default="sft-data", help="Elasticsearch index name")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--thread-count", type=int, default=2, help="Number of threads for parallel_bulk")
    parser.add_argument("--max-files", type=int, help="Maximum number of files to process (for testing)")

    parser.add_argument("--output-cleaned", type=str,default="/capstor/scratch/cscs/inesaltemir/INDEXING_sft_arrow/cleaned_convos/clean_convo_2.json", help="Path to output file for cleaned conversations (JSONL format)")
    
    args = parser.parse_args()

    # No SHARDS COMPUTATION, SIMPLIFY IT TO ONE SHARD SINCE ONLY 11GB OF DATA AND LESS THAN 200M DOCS
    
    logger = setup_logging(args.log_level)
    
    # Start memory tracking
    tracemalloc.start()
    log_memory_usage(logger, "AT START")
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory does not exist: {data_dir}")
        sys.exit(1)
    
    # Filter Arrow files for data-XXXXX-of-YYYYY.arrow pattern, excluding cache files
    arrow_files = filter_arrow_files(data_dir, logger)
    if not arrow_files:
        sys.exit(1)
    
    if args.max_files:
        arrow_files = arrow_files[:args.max_files]
        logger.info(f"Limited to {args.max_files} files for testing")

    # Setup output file path
    output_file_path = None
    if args.output_cleaned:
        output_file_path = Path(args.output_cleaned)
    
    logger.info("=== SFT Arrow Dataset Indexing Started ===")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Files to process: {len(arrow_files)}")
    logger.info(f"Chunk size: {args.chunk_size}")
    logger.info(f"Chunk size for processing: {args.chunk_size_proc}")
    logger.info(f"Elasticsearch: {args.es_host}:{args.es_port}")
    logger.info(f"Index name: {args.index_name}")
    logger.info(f"Thread count: {args.thread_count}")
    if output_file_path:
        logger.info(f"Cleaned conversations output: {output_file_path}")
    
    
    total_start_time = time.time()
    
    try:
        # Connect to Elasticsearch
        es = get_elasticsearch_client(args.es_host, args.es_port)
        log_memory_usage(logger, "AFTER ES CONNECTION")
        
        # Create index
        logger.info("Creating Elasticsearch index...")
        if not create_index_with_shards(es, args.index_name):
            sys.exit(1)
        
        log_memory_usage(logger, "AFTER INDEX CREATION")
        
        # Process files
        logger.info("Starting conversation indexing...")
        indexing_start_time = time.time()
        
        stats = process_arrow_files(
            arrow_files, args.index_name, es, args.chunk_size, args.thread_count, args.chunk_size_proc,output_file_path
        )
        
        indexing_end_time = time.time()
        log_memory_usage(logger, "AFTER INDEXING")
        
        # Final statistics
        total_time = time.time() - total_start_time
        indexing_time = indexing_end_time - indexing_start_time
        
        logger.info("=== Indexing Completed ===")
        logger.info(f"Total conversations indexed: {stats['indexed']:,}")
        logger.info(f"Failed conversations: {stats['failed']:,}")
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

        # Re-enable index refresh
        try:
            logger.info("Re-enabling index refresh...")
            es.indices.put_settings(
                index=args.index_name,
                body={"index": {"refresh_interval": "1s"}}
            )
            es.indices.refresh(index=args.index_name)
            logger.info("Index refresh enabled - ready for search queries")
            
        except Exception as e:
            logger.error(f"Failed to re-enable refresh: {e}")

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