#!/usr/bin/env python3
"""
FineWeb Dataset Indexer for Elasticsearch with Multi-Process Support
Multi-process architecture: N parser workers -> shared queue -> 1 ES consumer
Supports configurable metadata fields and per-language deduplication via SHA256.
"""

import os
import sys
import time
import logging
import gc
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Dict, Any, List, Optional
import argparse

import pandas as pd
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import RequestError, ConnectionError, TransportError
import psutil
import tracemalloc

from multiprocessing import Process, Queue, Event, Value, Manager
from queue import Empty
import multiprocessing as mp
from threading import Thread


# ============================================================================
# STORAGE CONFIGURATION
# ============================================================================

@dataclass
class StorageConfig:
    """Picklable config for document parsing. Passed to worker processes."""
    store_id: bool = False
    store_url: bool = False
    store_date: bool = False
    store_language: bool = False
    store_file_path: bool = False
    store_folder_path: bool = False
    deduplicate: bool = False

    @property
    def needs_metadata_dict(self) -> bool:
        """True if any field may live in a nested metadata column."""
        return self.store_id or self.store_url or self.store_date or self.store_language

    @property
    def any_extra_field(self) -> bool:
        return (self.store_id or self.store_url or self.store_date or self.store_language
                or self.store_file_path or self.store_folder_path)


# ============================================================================
# DOCUMENT PARSING HELPERS
# ============================================================================

def _get_columns_to_read(parquet_file, config: StorageConfig) -> List[str]:
    """
    Return the minimal set of columns to actually read from a parquet file.
    Tries top-level columns first; falls back to the nested 'metadata' dict
    if those columns are absent.
    """
    available = set(parquet_file.schema_arrow.names)
    needed: List[str] = ['text']

    # Always pull 'metadata' dict when any field might be nested inside it
    if config.needs_metadata_dict and 'metadata' in available:
        needed.append('metadata')

    if config.store_id and 'id' in available:
        needed.append('id')

    if config.store_url and 'url' in available:
        needed.append('url')

    if config.store_date:
        for col in ['date', 'date_download', 'crawl_timestamp']:
            if col in available:
                needed.append(col)
                break

    if config.store_language:
        for col in ['language', 'lang']:
            if col in available:
                needed.append(col)
                break

    # Deduplicate while preserving insertion order; skip absent columns
    seen, result = set(), []
    for col in needed:
        if col not in seen and col in available:
            seen.add(col)
            result.append(col)
    return result


def _parse_document(row, index_name: str, config: StorageConfig,
                    file_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Build an ES document from a pandas Series with configurable fields.

    Schema strategy:
    - Try top-level parquet columns first (e.g. 'url', 'id', 'date').
    - Fall back to the nested 'metadata' dict if those columns are absent.

    When config.deduplicate is True, _id = SHA256(text) enabling per-language
    deduplication (ES silently overwrites duplicate IDs on upsert).
    """
    text_val = row.get('text')
    if text_val is None or (isinstance(text_val, float) and pd.isna(text_val)):
        return None
    text_str = str(text_val).strip()
    if not text_str:
        return None
    if len(text_str) > 100000:
        text_str = text_str[:100000] + "... [TRUNCATED]"

    doc_source: Dict[str, Any] = {"text": text_str}

    if config.any_extra_field:
        # Parse nested metadata dict lazily (only if a top-level column misses)
        _meta_cache: Optional[dict] = None

        def get_meta() -> dict:
            nonlocal _meta_cache
            if _meta_cache is None:
                raw = row.get('metadata')
                if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                    _meta_cache = {}
                else:
                    try:
                        _meta_cache = raw if isinstance(raw, dict) else json.loads(str(raw))
                    except Exception:
                        _meta_cache = {}
            return _meta_cache

        def get_val(col_names: List[str], meta_keys: List[str] = None) -> str:
            """Try top-level columns first, then metadata dict."""
            for col in col_names:
                v = row.get(col)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    s = str(v).strip()
                    if s:
                        return s
            if meta_keys:
                md = get_meta()
                for k in meta_keys:
                    v = md.get(k, '')
                    if v and str(v).strip():
                        return str(v).strip()
            return ''

        if config.store_id:
            doc_source['id'] = get_val(['id'], ['id'])
        if config.store_url:
            doc_source['url'] = get_val(['url'], ['url'])
        if config.store_date:
            doc_source['date'] = get_val(
                ['date', 'date_download', 'crawl_timestamp'],
                ['date', 'date_download'])
        if config.store_language:
            doc_source['language'] = get_val(
                ['language', 'lang'], ['language', 'lang'])
        if config.store_file_path and file_path:
            doc_source['file_path'] = file_path
        if config.store_folder_path and file_path:
            doc_source['folder_path'] = str(Path(file_path).parent)

    doc: Dict[str, Any] = {"_index": index_name, "_source": doc_source}

    if config.deduplicate:
        doc["_id"] = hashlib.sha256(text_str.encode('utf-8')).hexdigest()

    return doc


# ============================================================================
# LOGGING AND MONITORING
# ============================================================================

def log_memory_usage(logger, context: str = ""):
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()

        logger.info(f"=== METRICS {context} ===")
        logger.info(f"Process Memory:")
        logger.info(f"  RSS: {memory_info.rss / (1024**3):.2f} GB")
        logger.info(f"  VMS: {memory_info.vms / (1024**3):.2f} GB")
        logger.info(f"  % of System: {process.memory_percent():.1f}%")
        logger.info(f"System Memory:")
        logger.info(f"  Total Used: {system_memory.percent:.1f}%")
        logger.info(f"  Available: {system_memory.available / (1024**3):.2f} GB")
        logger.info(f"  Cached: {system_memory.cached / (1024**3):.2f} GB")

        cpu_percent = process.cpu_percent(interval=1)
        logger.info(f"CPU:")
        logger.info(f"  Process usage: {cpu_percent:.1f}%")
        logger.info(f"  Threads: {process.num_threads()}")
        try:
            affinity = process.cpu_affinity()
            logger.info(f"  Allocated cores: {len(affinity)} (IDs: {affinity[:5]}...)")
        except Exception:
            logger.info(f"  Allocated cores: Unable to determine")

        try:
            io = process.io_counters()
            logger.info(f"Process I/O:")
            logger.info(f"  Read: {io.read_bytes / (1024**3):.2f} GB")
            logger.info(f"  Write: {io.write_bytes / (1024**3):.2f} GB")
        except Exception:
            pass

        logger.info("=" * 50)

    except Exception as e:
        logger.warning(f"Could not log memory usage: {e}")


def setup_logging(log_level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
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
            logging.info(f"Connected to Elasticsearch at {host}:{port}")
            logging.info(f"Elasticsearch version: {info['version']['number']}")
            return es
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to connect after {max_retries} attempts")
                raise


# ============================================================================
# DOCUMENT COUNTING (GROUND TRUTH)
# ============================================================================

def count_documents_in_parquet_files(file_list: List[Path]) -> int:
    logger = logging.getLogger(__name__)
    logger.info("=" * 70)
    logger.info(f"=== COUNTING GROUND TRUTH: {len(file_list)} files ===")

    total_docs = 0
    for i, file_path in enumerate(file_list):
        try:
            num_rows = pq.ParquetFile(file_path).metadata.num_rows
            total_docs += num_rows
            if (i + 1) % 10 == 0 or (i + 1) == len(file_list):
                logger.info(f"Counted {i+1}/{len(file_list)} files: {total_docs:,} docs")
        except Exception as e:
            logger.error(f"Failed to read metadata from {file_path.name}: {e}")
            raise

    logger.info(f"*** GROUND TRUTH: {total_docs:,} total documents ***")
    logger.info("=" * 70)
    return total_docs


# ============================================================================
# VALIDATION
# ============================================================================

def validate_indexing_results(ground_truth_count: int, stats: Dict[str, int],
                              es: Elasticsearch, index_name: str,
                              deduplicate: bool = False) -> bool:
    logger = logging.getLogger(__name__)

    try:
        es.indices.refresh(index=index_name)
        index_stats = es.indices.stats(index=index_name)
        es_doc_count = index_stats['indices'][index_name]['total']['docs']['count']
    except Exception as e:
        logger.error(f"Could not query ES index count: {e}")
        return False

    indexed_count = stats['indexed']
    failed_count = stats['failed']

    logger.info("=" * 70)
    logger.info("=== INDEXING VALIDATION ===")
    logger.info(f"Ground truth (raw files):  {ground_truth_count:>15,}")
    logger.info(f"Reported indexed:          {indexed_count:>15,}")
    logger.info(f"Reported failed:           {failed_count:>15,}")
    logger.info(f"ES index actual count:     {es_doc_count:>15,}")
    if deduplicate and ground_truth_count > 0:
        dup_count = ground_truth_count - es_doc_count
        logger.info(f"Dedup removed:             {dup_count:>15,} ({dup_count/ground_truth_count*100:.1f}%)")
    logger.info("-" * 70)

    success = True

    if deduplicate:
        # With SHA256 IDs, ES count <= ground_truth is expected and correct.
        # ES count > ground_truth would indicate a serious bug.
        if es_doc_count > ground_truth_count:
            logger.error(f"DUPLICATION: ES={es_doc_count:,} > ground truth={ground_truth_count:,}")
            logger.error("Documents were indexed multiple times!")
            success = False
        elif es_doc_count == 0:
            logger.error("ES index is empty!")
            success = False
        else:
            logger.info(f"✓ Dedup mode: {es_doc_count:,} unique docs (ok to be <= ground truth)")
    else:
        if es_doc_count != ground_truth_count:
            diff = es_doc_count - ground_truth_count
            logger.error(f"MISMATCH: ES={es_doc_count:,}, expected={ground_truth_count:,}, diff={diff:+,}")
            if es_doc_count > ground_truth_count:
                logger.error("DUPLICATION DETECTED!")
            else:
                logger.error("MISSING DOCUMENTS!")
            success = False
        else:
            logger.info(f"✓ ES count matches ground truth exactly")

    if failed_count > 0:
        logger.warning(f"WARNING: {failed_count:,} documents failed to index")
        if failed_count > ground_truth_count * 0.01:
            logger.error(f"High failure rate: {failed_count/ground_truth_count*100:.1f}%")
            success = False

    logger.info("=" * 70)
    if success:
        logger.info("✓✓✓ VALIDATION PASSED")
    else:
        logger.error("❌❌❌ VALIDATION FAILED")
    logger.info("=" * 70)
    return success


def validate_metadata_and_dedup(es: Elasticsearch, index_name: str,
                                 config: StorageConfig, n_samples: int = 5) -> bool:
    """
    Sample documents from ES and verify:
      - expected metadata fields are present and non-empty
      - _id == SHA256(text) when deduplication is on
    """
    import hashlib
    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("=== METADATA & DEDUP SPOT-CHECK ===")

    try:
        resp = es.search(index=index_name, body={"size": n_samples, "query": {"match_all": {}}})
    except Exception as e:
        logger.error(f"Could not query ES for spot-check: {e}")
        return False

    hits = resp["hits"]["hits"]
    if not hits:
        logger.error("No documents returned from ES for spot-check")
        return False

    expected_fields = []
    if config.store_id:
        expected_fields.append("id")
    if config.store_url:
        expected_fields.append("url")
    if config.store_date:
        expected_fields.append("date")
    if config.store_language:
        expected_fields.append("language")
    if config.store_file_path:
        expected_fields.append("file_path")
    if config.store_folder_path:
        expected_fields.append("folder_path")

    all_ok = True
    for i, hit in enumerate(hits):
        doc_id = hit["_id"]
        src = hit["_source"]
        text = src.get("text", "")

        logger.info(f"--- Sample doc {i+1} (id={doc_id[:16]}...) ---")
        logger.info(f"  text snippet: {text[:120]!r}")

        # Metadata fields
        for field in expected_fields:
            val = src.get(field)
            status = "✓" if val else "✗ MISSING/EMPTY"
            logger.info(f"  {field}: {status} => {str(val)[:100]!r}")
            if not val:
                all_ok = False

        # Dedup: _id must equal SHA256(text)
        if config.deduplicate:
            expected_id = hashlib.sha256(text.encode("utf-8")).hexdigest()
            if doc_id == expected_id:
                logger.info(f"  dedup _id: ✓ SHA256 matches")
            else:
                logger.error(f"  dedup _id: ✗ MISMATCH  got={doc_id}  expected={expected_id}")
                all_ok = False

    logger.info("=" * 70)
    if all_ok:
        logger.info("✓✓✓ METADATA & DEDUP SPOT-CHECK PASSED")
    else:
        logger.error("❌❌❌ METADATA & DEDUP SPOT-CHECK FAILED")
    logger.info("=" * 70)
    return all_ok


# ============================================================================
# FILE HANDLING
# ============================================================================

def get_file_range(data_dir_pattern: str, file_range_start: int = None,
                   file_range_end: int = None) -> tuple:
    logger = logging.getLogger(__name__)

    import glob
    matching_dirs = sorted(glob.glob(data_dir_pattern))

    if not matching_dirs:
        raise FileNotFoundError(f"No directories matching: {data_dir_pattern}")

    logger.info(f"Found {len(matching_dirs)} directories matching pattern")

    all_parquet_files = []
    for dir_path in matching_dirs:
        all_parquet_files.extend(sorted(Path(dir_path).glob("*.parquet")))
    all_parquet_files = sorted(all_parquet_files)

    total_files = len(all_parquet_files)
    if total_files == 0:
        raise FileNotFoundError(f"No parquet files in matched directories")

    logger.info(f"Total parquet files: {total_files}")

    if file_range_start is not None and file_range_end is not None:
        if not (0 <= file_range_start < total_files):
            raise ValueError(f"file_range_start {file_range_start} out of [0, {total_files})")
        if not (file_range_start < file_range_end <= total_files):
            raise ValueError(f"file_range_end {file_range_end} must be in ({file_range_start}, {total_files}]")
        selected_files = all_parquet_files[file_range_start:file_range_end]
        logger.info(f"File range: [{file_range_start}, {file_range_end})")
    else:
        selected_files = all_parquet_files
        logger.info(f"Processing all {total_files} files")

    total_size_bytes = 0
    for f in selected_files:
        try:
            total_size_bytes += f.stat().st_size
        except OSError:
            pass

    total_size_gb = total_size_bytes / (1024**3)
    logger.info(f"Size: {total_size_gb:.2f} GB, first: {selected_files[0]}, last: {selected_files[-1]}")

    return selected_files, total_size_gb


# ============================================================================
# INDEX CONFIGURATION
# ============================================================================

def calculate_optimal_shards_by_size(data_size_gb: float,
                                     min_shard_size_gb: float = 10,
                                     max_shard_size_gb: float = 50,
                                     es_expansion_factor: float = 3.0) -> int:
    logger = logging.getLogger(__name__)

    estimated_gb = data_size_gb * es_expansion_factor
    logger.info(f"Shard calc: raw={data_size_gb:.2f} GB, estimated indexed={estimated_gb:.2f} GB")

    target = (min_shard_size_gb + max_shard_size_gb) / 2
    shards = max(1, int(estimated_gb / target))

    if data_size_gb > 15 and shards < 2:
        shards = 2

    if estimated_gb / shards > max_shard_size_gb:
        shards = max(2 if data_size_gb > 15 else 1, int(estimated_gb / max_shard_size_gb) + 1)

    logger.info(f"Shards: {shards} (~{estimated_gb/shards:.1f} GB each)")
    return shards


def create_index_config(num_shards: int = 5, num_replicas: int = 0,
                        config: Optional[StorageConfig] = None) -> Dict[str, Any]:
    """Build ES index mapping. Extra metadata fields are stored but not indexed."""
    keyword_stored = {"type": "keyword", "index": False, "store": False}

    properties: Dict[str, Any] = {
        "text": {
            "type": "text",
            "analyzer": "web_content_analyzer",
            "index_options": "positions",
            "norms": True,
            "store": False
        }
    }
    source_includes = ["text"]

    if config:
        for field, flag in [
            ("id", config.store_id),
            ("url", config.store_url),
            ("date", config.store_date),
            ("language", config.store_language),
            ("file_path", config.store_file_path),
            ("folder_path", config.store_folder_path),
        ]:
            if flag:
                properties[field] = keyword_stored
                source_includes.append(field)

    return {
        "settings": {
            "number_of_shards": num_shards,
            "number_of_replicas": num_replicas,
            "refresh_interval": "60s",
            "index": {"codec": "best_compression", "max_result_window": 50000},
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
            "_source": {"includes": source_includes, "excludes": []},
            "properties": properties
        }
    }


def create_index_with_size_based_shards(es: Elasticsearch, index_name: str,
                                        total_data_size_gb: float,
                                        config_file: str = None,
                                        min_shard_size_gb: float = 10,
                                        max_shard_size_gb: float = 50,
                                        es_expansion_factor: float = 3.0,
                                        storage_config: Optional[StorageConfig] = None,
                                        keep_existing: bool = False) -> bool:
    try:
        if es.indices.exists(index=index_name):
            if keep_existing:
                logging.info(f"Index '{index_name}' exists — keeping (split-job mode)")
                return True
            logging.warning(f"Index '{index_name}' exists — deleting and recreating")
            es.indices.delete(index=index_name)

        optimal_shards = calculate_optimal_shards_by_size(
            total_data_size_gb, min_shard_size_gb, max_shard_size_gb, es_expansion_factor
        )

        if config_file and os.path.exists(config_file):
            logging.info(f"Loading index config from: {config_file}")
            with open(config_file) as f:
                mapping = json.load(f)
            mapping["settings"]["number_of_shards"] = optimal_shards
        else:
            mapping = create_index_config(num_shards=optimal_shards, config=storage_config)

        logging.info(f"Creating index '{index_name}' with {mapping['settings']['number_of_shards']} shards")
        es.indices.create(index=index_name, body=mapping)
        logging.info(f"Index '{index_name}' created")
        return True

    except Exception as e:
        logging.error(f"Failed to create index: {e}")
        return False


# ============================================================================
# MEMORY MANAGEMENT
# ============================================================================

def force_memory_cleanup():
    logger = logging.getLogger(__name__)
    try:
        for i in range(3):
            gc.collect()
        try:
            import pyarrow as pa
            pa.default_memory_pool().release_unused()
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"Memory cleanup failed: {e}")


# ============================================================================
# SINGLE-PROCESS PARSING (num_workers=1, backward-compatible)
# ============================================================================

def process_single_parquet_file_streaming(file_path: Path, chunk_size: int,
                                          index_name: str,
                                          config: StorageConfig) -> Generator[Dict, None, None]:
    logger = logging.getLogger(__name__)
    parquet_file = None

    try:
        logger.info(f"Opening: {file_path.name}")
        parquet_file = pq.ParquetFile(file_path)
        num_rows = parquet_file.metadata.num_rows
        num_row_groups = parquet_file.metadata.num_row_groups
        columns_to_read = _get_columns_to_read(parquet_file, config)
        logger.info(f"{num_rows:,} rows, {num_row_groups} groups, columns={columns_to_read}")

        processed_rows = 0
        for rg_idx in range(num_row_groups):
            try:
                df = parquet_file.read_row_group(rg_idx, columns=columns_to_read).to_pandas()
                if 'text' not in df.columns:
                    continue

                for chunk_start in range(0, len(df), chunk_size):
                    chunk = df.iloc[chunk_start:chunk_start + chunk_size].copy()
                    for _, row in chunk.iterrows():
                        doc = _parse_document(row, index_name, config, str(file_path))
                        if doc:
                            yield doc
                    processed_rows += len(chunk)
                    del chunk
                    if processed_rows % 10000 == 0:
                        gc.collect()

                del df
                gc.collect()
            except Exception as e:
                logger.error(f"Row group {rg_idx} error: {e}")
                continue

        logger.info(f"Done {file_path.name}: {processed_rows:,} rows")

    except Exception as e:
        logger.error(f"Error opening {file_path.name}: {e}")
        raise
    finally:
        parquet_file = None
        try:
            import pyarrow as pa
            pa.default_memory_pool().release_unused()
        except Exception:
            pass
        gc.collect()


def bulk_index_documents(es: Elasticsearch, doc_generator: Generator,
                         batch_size: int, max_chunk_bytes: int,
                         thread_count: int, queue_size: int,
                         global_stats: Dict[str, int], start_time: float) -> Dict[str, int]:
    logger = logging.getLogger(__name__)
    stats = {"indexed": 0, "failed": 0}

    try:
        for success, info in helpers.parallel_bulk(
            es, doc_generator,
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
                logger.error(f"Failed doc: {info}")

            if global_stats["indexed"] % 10000 == 0:
                elapsed = time.time() - start_time
                rate = global_stats["indexed"] / elapsed if elapsed > 0 else 0
                logger.info(f"Progress: {global_stats['indexed']:,} indexed, "
                            f"{global_stats['failed']:,} failed, {rate:.1f} docs/sec")
                log_memory_usage(logger, f"AT {global_stats['indexed']} DOCS")
                gc.collect()
    except Exception as e:
        logger.error(f"Bulk indexing error: {e}")
        raise

    return stats


# ============================================================================
# MULTI-PROCESS WORKERS
# ============================================================================

def parse_parquet_worker_batch(file_list, chunk_size, index_name, doc_queue,
                               docs_parsed_counter, worker_id, stop_event,
                               config: StorageConfig):
    """Worker process: reads parquet files and pushes document batches onto doc_queue."""
    logger = logging.getLogger(f"parser-{worker_id}")
    logger.info(f"Worker {worker_id}: starting with {len(file_list)} files")

    total_docs = 0
    internal_batch = 1000

    try:
        for file_idx, file_path in enumerate(file_list):
            if stop_event.is_set():
                break

            logger.info(f"Worker {worker_id}: [{file_idx+1}/{len(file_list)}] {file_path.name}")
            docs_from_file = 0
            batch_buffer = []

            try:
                parquet_file = pq.ParquetFile(file_path)
                num_row_groups = parquet_file.metadata.num_row_groups
                columns_to_read = _get_columns_to_read(parquet_file, config)

                for rg_idx in range(num_row_groups):
                    if stop_event.is_set():
                        break
                    try:
                        df = parquet_file.read_row_group(rg_idx, columns=columns_to_read).to_pandas()
                        if 'text' not in df.columns:
                            continue

                        for start in range(0, len(df), chunk_size):
                            if stop_event.is_set():
                                break
                            chunk = df.iloc[start:start + chunk_size].copy()

                            for _, row in chunk.iterrows():
                                doc = _parse_document(row, index_name, config, str(file_path))
                                if doc:
                                    batch_buffer.append(doc)
                                    docs_from_file += 1

                                    if len(batch_buffer) >= internal_batch:
                                        doc_queue.put(batch_buffer)
                                        batch_buffer = []
                                        with docs_parsed_counter.get_lock():
                                            docs_parsed_counter.value += internal_batch

                            del chunk

                            if docs_from_file % 50000 == 0 and docs_from_file > 0:
                                logger.info(f"Worker {worker_id}: {docs_from_file:,} docs from current file")
                                gc.collect()

                        del df
                        gc.collect()

                    except Exception as e:
                        logger.error(f"Worker {worker_id} rg {rg_idx} error: {e}")
                        continue

                if batch_buffer:
                    doc_queue.put(batch_buffer)
                    with docs_parsed_counter.get_lock():
                        docs_parsed_counter.value += len(batch_buffer)
                    batch_buffer = []

                total_docs += docs_from_file
                logger.info(f"Worker {worker_id}: done {file_path.name} — {docs_from_file:,} docs")

            except Exception as e:
                logger.error(f"Worker {worker_id} failed on {file_path.name}: {e}")
                continue
            finally:
                force_memory_cleanup()

        doc_queue.put(None)  # signal completion
        logger.info(f"Worker {worker_id}: finished — {total_docs:,} total docs")

    except Exception as e:
        import traceback
        logger.error(f"Worker {worker_id} fatal: {e}\n{traceback.format_exc()}")
        doc_queue.put(None)
    finally:
        force_memory_cleanup()


def elasticsearch_consumer(doc_queue, es, batch_size, max_chunk_bytes,
                           thread_count, queue_size, num_workers,
                           stats_dict, stop_event):
    """Consumer thread: drains doc_queue and bulk-indexes into ES."""
    logger = logging.getLogger("es-consumer")
    logger.info("ES Consumer started")

    indexed = 0
    failed = 0
    workers_done = 0
    start_time = time.time()

    def document_generator():
        nonlocal workers_done
        while True:
            try:
                batch = doc_queue.get(timeout=5)
                if batch is None:
                    workers_done += 1
                    logger.info(f"Worker done ({workers_done}/{num_workers})")
                    if workers_done >= num_workers:
                        break
                    continue
                yield from batch
            except Empty:
                if stop_event.is_set():
                    break
                continue
            except Exception as e:
                logger.error(f"Queue error: {e}")
                break

    try:
        for success, info in helpers.parallel_bulk(
            es, document_generator(),
            chunk_size=batch_size,
            max_chunk_bytes=max_chunk_bytes * 1024 * 1024,
            thread_count=thread_count,
            queue_size=queue_size,
            request_timeout=300,
        ):
            if success:
                indexed += 1
            else:
                failed += 1
                logger.error(f"Failed: {info}")

            if indexed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = indexed / elapsed if elapsed > 0 else 0
                logger.info(f"Progress: {indexed:,} indexed, {failed:,} failed, "
                            f"{rate:.1f} docs/sec, queue={doc_queue.qsize()}, {elapsed:.1f}s")
                if indexed % 100000 == 0:
                    log_memory_usage(logger, f"ES CONSUMER AT {indexed}")
                    gc.collect()

        elapsed = time.time() - start_time
        logger.info(f"ES Consumer done: {indexed:,} indexed, {failed:,} failed, "
                    f"{indexed/elapsed:.1f} docs/sec, {elapsed:.1f}s")
        stats_dict['indexed'] = indexed
        stats_dict['failed'] = failed
        stats_dict['elapsed'] = elapsed

    except Exception as e:
        import traceback
        logger.error(f"ES Consumer error: {e}\n{traceback.format_exc()}")
        stats_dict['indexed'] = indexed
        stats_dict['failed'] = failed
        stats_dict['error'] = str(e)


# ============================================================================
# ORCHESTRATION
# ============================================================================

def process_file_list(file_list, chunk_size, index_name, es, batch_size,
                      max_chunk_bytes, thread_count, queue_size,
                      num_workers=1, config: Optional[StorageConfig] = None):
    logger = logging.getLogger(__name__)

    if not file_list:
        raise ValueError("No files provided")

    if config is None:
        config = StorageConfig()

    logger.info(f"Processing {len(file_list)} files, deduplicate={config.deduplicate}")

    if num_workers == 1:
        logger.info("Single-process mode")
        global_stats = {"indexed": 0, "failed": 0, "files_processed": 0}
        start_time = time.time()

        for i, file_path in enumerate(file_list):
            logger.info(f"File {i+1}/{len(file_list)}: {file_path.name}")
            log_memory_usage(logger, f"BEFORE FILE {i+1}")
            try:
                doc_gen = process_single_parquet_file_streaming(
                    file_path, chunk_size, index_name, config)
                file_stats = bulk_index_documents(
                    es, doc_gen, batch_size, max_chunk_bytes,
                    thread_count, queue_size, global_stats, start_time)
                global_stats["files_processed"] += 1
                logger.info(f"File {i+1} done: {file_stats['indexed']:,} indexed")
            except Exception as e:
                logger.error(f"Failed on {file_path.name}: {e}")
                continue
            finally:
                force_memory_cleanup()
                log_memory_usage(logger, f"AFTER FILE {i+1}")

        return global_stats

    # Multi-process
    actual_workers = min(num_workers, len(file_list))
    logger.info(f"=== Multi-Process: {actual_workers} workers ===")

    doc_queue = Queue(maxsize=300)
    docs_parsed_counter = Value('i', 0)
    stop_event = Event()

    manager = mp.Manager()
    stats_dict = manager.dict({'indexed': 0, 'failed': 0})

    overall_start = time.time()

    try:
        workers = []
        for worker_id in range(actual_workers):
            worker_files = file_list[worker_id::actual_workers]
            logger.info(f"Worker {worker_id}: {len(worker_files)} files")
            p = Process(
                target=parse_parquet_worker_batch,
                args=(worker_files, chunk_size, index_name, doc_queue,
                      docs_parsed_counter, worker_id, stop_event, config)
            )
            p.start()
            workers.append(p)

        consumer_thread = Thread(
            target=elasticsearch_consumer,
            args=(doc_queue, es, batch_size, max_chunk_bytes,
                  thread_count, queue_size, len(workers), stats_dict, stop_event)
        )
        consumer_thread.start()

        last_count = 0
        while consumer_thread.is_alive():
            consumer_thread.join(timeout=10)
            current_parsed = docs_parsed_counter.value
            if current_parsed > last_count:
                logger.info(f"Overall: parsed={current_parsed:,}, "
                            f"indexed={stats_dict.get('indexed', 0):,}, "
                            f"queue={doc_queue.qsize()}")
                last_count = current_parsed

        for i, worker in enumerate(workers):
            worker.join(timeout=30)
            if worker.is_alive():
                logger.warning(f"Worker {i} timed out, terminating")
                worker.terminate()

        elapsed = time.time() - overall_start
        final_indexed = stats_dict.get('indexed', 0)
        final_failed = stats_dict.get('failed', 0)

        logger.info(f"=== Multi-Process done: indexed={final_indexed:,}, failed={final_failed:,}, "
                    f"rate={final_indexed/elapsed:.1f} docs/sec, elapsed={elapsed:.1f}s ===")

        return {"indexed": final_indexed, "failed": final_failed,
                "files_processed": len(workers), "elapsed": elapsed}

    except KeyboardInterrupt:
        logger.warning("Interrupted")
        stop_event.set()
        for w in workers:
            w.terminate()
        raise

    except Exception as e:
        import traceback
        logger.error(f"Multi-process failed: {e}\n{traceback.format_exc()}")
        stop_event.set()
        for w in workers:
            w.terminate()
        raise


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Index FineWeb-style parquet datasets into Elasticsearch. "
                    "Supports configurable metadata fields and per-language deduplication."
    )

    # Data source
    parser.add_argument("--data-dir", required=True,
                        help="Language folder, glob pattern, or space-separated list of paths")
    parser.add_argument("--file-range-start", type=int,
                        help="Start file index (0-based, inclusive) for split-job mode")
    parser.add_argument("--file-range-end", type=int,
                        help="End file index (exclusive) for split-job mode")

    # ES connection
    parser.add_argument("--es-host", default="localhost")
    parser.add_argument("--es-port", type=int, default=9200)
    parser.add_argument("--index-name", default="fineweb")
    parser.add_argument("--index-config", help="Path to JSON index config (overrides auto-config)")

    # Performance
    parser.add_argument("--batch-size", type=int, default=12500)
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--max-chunk-bytes", type=int, default=75, help="MB")
    parser.add_argument("--thread-count", type=int, default=8)
    parser.add_argument("--queue-size", type=int, default=8)
    parser.add_argument("--num-workers", type=int, default=1,
                        help="Parallel parser processes (recommend 6-8 for 10 CPUs)")

    # Shard sizing
    parser.add_argument("--min-shard-size", type=float, default=10.0, help="GB")
    parser.add_argument("--max-shard-size", type=float, default=50.0, help="GB")
    parser.add_argument("--es-expansion-factor", type=float, default=3.0)

    # Fields to store (stored but not indexed by default)
    parser.add_argument("--store-id", action="store_true",
                        help="Store original document ID from parquet")
    parser.add_argument("--store-url", action="store_true",
                        help="Store URL")
    parser.add_argument("--store-date", action="store_true",
                        help="Store date/date_download field")
    parser.add_argument("--store-language", action="store_true",
                        help="Store language field")
    parser.add_argument("--store-file-path", action="store_true",
                        help="Store source parquet file path")
    parser.add_argument("--store-folder-path", action="store_true",
                        help="Store parent (language) folder path")

    # Deduplication
    parser.add_argument("--deduplicate", action="store_true",
                        help="Use SHA256(text) as _id for per-language dedup. "
                             "Validation accepts ES count <= ground truth.")

    # Index lifecycle
    parser.add_argument("--keep-existing-index", action="store_true",
                        help="Skip index creation if it already exists (for split-job mode)")

    # Backward-compat alias
    parser.add_argument("--metadata-fields", default=None,
                        choices=["text", "text-url", "text-url-lang"],
                        help="[Deprecated] Use --store-* flags. "
                             "text-url sets --store-url; text-url-lang also sets --store-language.")

    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()
    logger = setup_logging(args.log_level)

    # Map deprecated --metadata-fields to new flags
    if args.metadata_fields == "text-url":
        args.store_url = True
    elif args.metadata_fields == "text-url-lang":
        args.store_url = True
        args.store_language = True

    # ES requires all-lowercase index names
    args.index_name = args.index_name.lower()

    config = StorageConfig(
        store_id=args.store_id,
        store_url=args.store_url,
        store_date=args.store_date,
        store_language=args.store_language,
        store_file_path=args.store_file_path,
        store_folder_path=args.store_folder_path,
        deduplicate=args.deduplicate,
    )

    tracemalloc.start()
    log_memory_usage(logger, "AT START")

    # ---- Resolve file list ----
    data_dir_pattern = args.data_dir

    if '*' in data_dir_pattern:
        files_to_process, total_data_size_gb = get_file_range(
            data_dir_pattern, args.file_range_start, args.file_range_end)

    elif ' ' in data_dir_pattern:
        paths = data_dir_pattern.split()
        files_to_process = []
        for path_str in paths:
            p = Path(path_str)
            if not p.exists():
                logger.error(f"Path not found: {p}")
                sys.exit(1)
            if p.is_file():
                if p.suffix != '.parquet':
                    logger.error(f"Not a parquet file: {p}")
                    sys.exit(1)
                files_to_process.append(p)
            elif p.is_dir():
                pfiles = sorted(p.glob("*.parquet"))
                if not pfiles:
                    logger.warning(f"No parquet files in: {p}")
                files_to_process.extend(pfiles)
        if not files_to_process:
            logger.error("No parquet files found in any of the given paths")
            sys.exit(1)
        total_data_size_gb = sum(f.stat().st_size for f in files_to_process) / (1024**3)

    else:
        data_path = Path(data_dir_pattern)
        if not data_path.exists():
            logger.error(f"Path not found: {data_path}")
            sys.exit(1)

        if data_path.is_file():
            if data_path.suffix != '.parquet':
                logger.error(f"Not a parquet file: {data_path}")
                sys.exit(1)
            files_to_process = [data_path]
            total_data_size_gb = data_path.stat().st_size / (1024**3)

        elif data_path.is_dir():
            if args.file_range_start is not None and args.file_range_end is not None:
                files_to_process, total_data_size_gb = get_file_range(
                    str(data_path), args.file_range_start, args.file_range_end)
            else:
                all_files = sorted(data_path.glob("*.parquet"))
                if not all_files:
                    logger.error(f"No parquet files in {data_path}")
                    sys.exit(1)
                files_to_process = all_files
                total_data_size_gb = sum(f.stat().st_size for f in files_to_process) / (1024**3)

    # ---- Print job summary ----
    logger.info("=" * 70)
    logger.info("=== FineWeb Indexing Started ===")
    logger.info(f"Data:         {data_dir_pattern}")
    logger.info(f"Files:        {len(files_to_process)}, {total_data_size_gb:.2f} GB")
    logger.info(f"Index:        {args.index_name}")
    logger.info(f"Workers:      {args.num_workers}")
    logger.info(f"Deduplicate:  {config.deduplicate}")
    logger.info(f"Extra fields: id={config.store_id}, url={config.store_url}, "
                f"date={config.store_date}, language={config.store_language}, "
                f"file_path={config.store_file_path}, folder_path={config.store_folder_path}")
    logger.info(f"Keep index:   {args.keep_existing_index}")
    logger.info("=" * 70)

    # ---- STEP 1: Ground truth count ----
    logger.info("STEP 1: Counting ground truth documents...")
    try:
        ground_truth_count = count_documents_in_parquet_files(files_to_process)
    except Exception as e:
        logger.error(f"Failed to count: {e}")
        sys.exit(1)

    total_start = time.time()

    try:
        # ---- STEP 2: Create index ----
        es = get_elasticsearch_client(args.es_host, args.es_port)
        log_memory_usage(logger, "AFTER ES CONNECTION")

        logger.info("STEP 2: Creating Elasticsearch index...")
        if not create_index_with_size_based_shards(
            es, args.index_name, total_data_size_gb, args.index_config,
            args.min_shard_size, args.max_shard_size, args.es_expansion_factor,
            storage_config=config,
            keep_existing=args.keep_existing_index
        ):
            sys.exit(1)

        # ---- STEP 3: Index documents ----
        logger.info("STEP 3: Indexing documents...")
        indexing_start = time.time()
        stats = process_file_list(
            files_to_process, args.chunk_size, args.index_name, es,
            args.batch_size, args.max_chunk_bytes, args.thread_count,
            args.queue_size, args.num_workers, config=config
        )
        indexing_end = time.time()
        log_memory_usage(logger, "AFTER INDEXING")

        # ---- STEP 4: Finalize ----
        logger.info("STEP 4: Finalizing index...")
        try:
            es.indices.refresh(index=args.index_name)
            idx_stats = es.indices.stats(index=args.index_name)
            doc_count = idx_stats['indices'][args.index_name]['total']['docs']['count']
            idx_size = idx_stats['indices'][args.index_name]['total']['store']['size_in_bytes']
            logger.info(f"Final index document count: {doc_count:,}")
            logger.info(f"Index size: {idx_size / (1024**3):.2f} GB")
        except Exception as e:
            logger.warning(f"Could not get final index stats: {e}")

        total_time = time.time() - total_start
        indexing_time = indexing_end - indexing_start
        logger.info("=== Indexing Statistics ===")
        logger.info(f"Total documents indexed: {stats['indexed']:,}")
        logger.info(f"Failed documents: {stats['failed']:,}")
        logger.info(f"Files processed: {stats['files_processed']:,}")
        logger.info(f"Indexing time: {indexing_time:.2f} seconds")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        if indexing_time > 0:
            logger.info(f"Average indexing rate: {stats['indexed'] / indexing_time:.1f} docs/sec")

        log_memory_usage(logger, "FINAL")
        try:
            _, peak = tracemalloc.get_traced_memory()
            logger.info(f"Peak memory usage during execution: {peak / (1024**3):.2f} GB")
            tracemalloc.stop()
        except Exception:
            pass

        # ---- STEP 5: Validate ----
        logger.info("STEP 5: Validating indexing results...")
        validation_passed = validate_indexing_results(
            ground_truth_count=ground_truth_count,
            stats=stats,
            es=es,
            index_name=args.index_name,
            deduplicate=config.deduplicate
        )

        if not validation_passed:
            logger.error("=== INDEXING JOB FAILED — VALIDATION ERRORS ===")
            sys.exit(1)

        # ---- STEP 6: Metadata & dedup spot-check ----
        logger.info("STEP 6: Spot-checking metadata fields and dedup IDs...")
        validate_metadata_and_dedup(es, args.index_name, config)

        logger.info("=== Indexing Job Completed Successfully ===")
        sys.exit(0)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        import traceback
        logger.error(f"Indexing failed: {e}\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
