#!/usr/bin/env python3
"""
Phase 2: Index aggregated parquet output into Elasticsearch.

Reads the fixed schema produced by spark_aggregate.py:
  sha256        string  -- pre-computed SHA256, used as ES _id
  text          string
  language      string
  source_count  integer
  sources       array of {id, url, date, file_path, folder_path}

No deduplication is performed here — that was fully handled in Phase 1.
Each row becomes exactly one ES document.

Usage
-----
python3 index_aggregated.py \\
    --data-dir  /iopsstor/scratch/.../fineweb-1_3_0-quality_33-filterrobots-aggregated \\
    --index-name fineweb-1_3_0-quality_33-filterrobots-eng \\
    --es-host localhost --es-port 9200
"""

import argparse
import logging
import multiprocessing
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow.parquet as pq
from elasticsearch import Elasticsearch
from elasticsearch import helpers


# ============================================================
# ES index creation
# ============================================================

def _shard_count(size_gb: float) -> int:
    if size_gb <= 0:
        return 1
    n = max(1, int(size_gb / 30) + 1)
    return min(n, 50)


def create_index(es: Elasticsearch, index_name: str, size_gb: float,
                 keep_existing: bool = False) -> None:
    logger = logging.getLogger(__name__)

    if es.indices.exists(index=index_name):
        if keep_existing:
            logger.info(f"Index '{index_name}' exists — keeping (append mode)")
            return
        logger.warning(f"Index '{index_name}' exists — deleting and recreating")
        es.indices.delete(index=index_name)

    num_shards = _shard_count(size_gb)
    logger.info(f"Creating index '{index_name}' with {num_shards} shards "
                f"(estimated {size_gb:.1f} GB indexed)")

    mapping = {
        "settings": {
            "number_of_shards": num_shards,
            "number_of_replicas": 0,
            "refresh_interval": "30s",
            "index.translog.durability": "async",
            "index.translog.flush_threshold_size": "2gb",
            "index": {"codec": "best_compression"},
            "analysis": {
                "analyzer": {
                    "web_content_analyzer": {
                        "type": "custom",
                        "char_filter": ["html_strip"],
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"],
                    }
                }
            },
        },
        "mappings": {
            "dynamic": "false",
            "properties": {
                # Full-text search field — properly indexed with analyzer
                "text": {
                    "type": "text",
                    "analyzer": "web_content_analyzer",
                    "index_options": "positions",
                    "norms": True,
                    "store": False,
                },
                # Filterable keyword fields
                "language":     {"type": "keyword"},
                "source_count": {"type": "integer"},
                # Earliest date across all sources — enables date range filtering
                "date":         {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                # sources stored but not individually indexed — retrieve only
                "sources":      {"type": "object", "enabled": False},
            },
        },
    }

    es.indices.create(index=index_name, body=mapping)
    logger.info(f"Index '{index_name}' created")


# ============================================================
# Document parsing worker
# ============================================================

def _parse_file(file_path: Path, index_name: str, doc_queue: multiprocessing.Queue,
                chunk_size: int) -> int:
    """Read one aggregated parquet file and push docs to queue. Returns doc count."""
    count = 0
    try:
        pf = pq.ParquetFile(str(file_path))
        for batch in pf.iter_batches(batch_size=chunk_size):
            rows = batch.to_pydict()
            n = len(rows["sha256"])
            docs = []
            for i in range(n):
                sha256 = rows["sha256"][i]
                if sha256 is None:
                    continue
                sources_raw = rows["sources"][i]
                # pyarrow returns list of dicts (or list of Row objects)
                if sources_raw is not None:
                    sources = [
                        {
                            "id":          s.get("id"),
                            "url":         s.get("url"),
                            "date":        s.get("date"),
                            "file_path":   s.get("file_path"),
                            "folder_path": s.get("folder_path"),
                        }
                        for s in sources_raw
                    ]
                else:
                    sources = []

                doc = {
                    "_id":    sha256,
                    "_index": index_name,
                    "_source": {
                        "text":         rows["text"][i],
                        "language":     rows["language"][i],
                        "source_count": rows["source_count"][i],
                        "date":         min((s["date"] for s in sources if s.get("date")), default=None),
                        "sources":      sources,
                    },
                }
                docs.append(doc)

            if docs:
                doc_queue.put(docs)
                count += len(docs)

    except Exception as e:
        logging.getLogger(__name__).error(f"Error parsing {file_path}: {e}")

    return count


def parse_worker(file_list: List[Path], index_name: str, doc_queue: multiprocessing.Queue,
                 chunk_size: int, result_queue: multiprocessing.Queue) -> None:
    total = 0
    for fp in file_list:
        total += _parse_file(fp, index_name, doc_queue, chunk_size)
    result_queue.put(total)


# ============================================================
# ES consumer (runs in main process, single thread)
# ============================================================

def es_consumer(doc_queue: multiprocessing.Queue, es: Elasticsearch,
                batch_size: int, max_chunk_bytes_mb: int,
                thread_count: int, queue_size: int,
                stats: Dict, stop_event: threading.Event) -> None:
    logger = logging.getLogger(__name__)

    def _gen():
        while not (stop_event.is_set() and doc_queue.empty()):
            try:
                batch = doc_queue.get(timeout=1.0)
                yield from batch
            except Exception:
                continue

    for ok, info in helpers.parallel_bulk(
        es,
        _gen(),
        thread_count=thread_count,
        queue_size=queue_size,
        chunk_size=batch_size,
        max_chunk_bytes=max_chunk_bytes_mb * 1024 * 1024,
        raise_on_error=False,
    ):
        if ok:
            stats["indexed"] += 1
        else:
            stats["failed"] += 1
            logger.warning(f"Failed doc: {info}")


# ============================================================
# Validation
# ============================================================

def validate(es: Elasticsearch, index_name: str, expected: int, stats: Dict,
             append_mode: bool = False) -> bool:
    logger = logging.getLogger(__name__)

    try:
        es.indices.refresh(index=index_name)
        es_count = es.indices.stats(index=index_name)["indices"][index_name]["total"]["docs"]["count"]
    except Exception as e:
        logger.error(f"Cannot query ES count: {e}")
        return False

    logger.info("=" * 70)
    logger.info("=== VALIDATION ===")
    logger.info(f"Unique docs in this batch:         {expected:>15,}")
    logger.info(f"Reported indexed this run:         {stats['indexed']:>15,}")
    logger.info(f"Reported failed:                   {stats['failed']:>15,}")
    logger.info(f"ES index total count:              {es_count:>15,}")
    if append_mode:
        logger.info("(append mode — ES total includes prior batches)")
    logger.info("-" * 70)

    ok = True

    if append_mode:
        # In append mode prior batches are already in ES; only check that:
        #   1. We indexed roughly what we expected (within failure tolerance)
        #   2. ES total is at least as large as this batch
        if stats["indexed"] < expected * 0.99 and stats["failed"] == 0:
            logger.warning(f"Indexed {stats['indexed']:,} but expected ~{expected:,} with 0 failures")
            ok = False
        if es_count < expected:
            logger.warning(f"ES count {es_count:,} < this batch {expected:,} — something went wrong")
            ok = False
        else:
            logger.info(f"✓ ES total {es_count:,} >= this batch {expected:,}")
    else:
        if es_count == expected:
            logger.info("✓ ES count matches exactly")
        elif es_count < expected:
            logger.warning(f"ES count {es_count:,} < expected {expected:,} — "
                           f"{expected - es_count:,} docs missing (possible failures)")
            if stats["failed"] == 0:
                ok = False
        else:
            logger.error(f"ES count {es_count:,} > expected {expected:,} — duplication bug!")
            ok = False

    if stats["failed"] > 0:
        fail_rate = stats["failed"] / max(expected, 1)
        logger.warning(f"{stats['failed']:,} documents failed to index ({fail_rate*100:.2f}%)")
        if fail_rate > 0.01:
            ok = False

    logger.info("=" * 70)
    logger.info("✓✓✓ VALIDATION PASSED" if ok else "❌❌❌ VALIDATION FAILED")
    logger.info("=" * 70)
    return ok


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )

    parser.add_argument("--data-dir", required=True,
                        help="Directory containing aggregated parquet files (Phase 1 output)")
    parser.add_argument("--index-name", required=True,
                        help="Target Elasticsearch index name")
    parser.add_argument("--es-host", default="localhost")
    parser.add_argument("--es-port", type=int, default=9200)
    parser.add_argument("--keep-existing-index", action="store_true",
                        help="Skip index deletion if it already exists")

    # File-range splitting for multi-job chaining
    parser.add_argument("--file-range-start", type=int, default=None,
                        help="First file index (0-based, inclusive) to process in this job")
    parser.add_argument("--file-range-end", type=int, default=None,
                        help="Last file index (0-based, inclusive) to process in this job")

    # Performance
    parser.add_argument("--num-workers",      type=int, default=8)
    parser.add_argument("--chunk-size",       type=int, default=5000)
    parser.add_argument("--batch-size",       type=int, default=12500)
    parser.add_argument("--max-chunk-bytes",  type=int, default=50,   help="MB")
    parser.add_argument("--thread-count",     type=int, default=4)
    parser.add_argument("--queue-size",       type=int, default=8)

    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    # Lowercase index name (ES requirement)
    args.index_name = args.index_name.lower()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # ---- Discover input files ----
    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        logger.error(f"data-dir not found: {data_dir}")
        sys.exit(1)

    all_files = sorted(data_dir.glob("*.parquet"))
    if not all_files:
        # Aggregated output might be in sub-partitions
        all_files = sorted(data_dir.glob("**/*.parquet"))
    if not all_files:
        logger.error(f"No parquet files found in {data_dir}")
        sys.exit(1)

    # Apply file-range slicing for multi-job chaining
    start_idx = args.file_range_start if args.file_range_start is not None else 0
    end_idx   = args.file_range_end   if args.file_range_end   is not None else len(all_files) - 1
    files = all_files[start_idx : end_idx + 1]
    if not files:
        logger.error(f"No files in range [{start_idx}, {end_idx}] (total: {len(all_files)})")
        sys.exit(1)

    total_bytes = sum(f.stat().st_size for f in files)
    total_gb = total_bytes / (1024 ** 3)

    # Quick row count from parquet metadata (fast — no full scan)
    ground_truth = sum(
        pq.ParquetFile(str(f)).metadata.num_rows for f in files
    )

    range_str = f" [{start_idx}–{end_idx}]" if (args.file_range_start is not None or args.file_range_end is not None) else ""
    logger.info("=" * 70)
    logger.info("=== Phase 2: Indexing Aggregated Data ===")
    logger.info(f"Data dir:     {data_dir}")
    logger.info(f"All files:    {len(all_files)}")
    logger.info(f"This batch:   {len(files)} files{range_str}, {total_gb:.2f} GB")
    logger.info(f"Unique docs:  {ground_truth:,}")
    logger.info(f"Index:        {args.index_name}")
    logger.info(f"Keep index:   {args.keep_existing_index}")
    logger.info(f"Workers:      {args.num_workers}")
    logger.info("=" * 70)

    # ---- Connect to ES ----
    # Use a long timeout: bulk requests with 150 MB of text can take >10s on a
    # loaded ES node. retry_on_timeout is safe because we use SHA256 as _id.
    es = Elasticsearch(
        [{"host": args.es_host, "port": args.es_port}],
        timeout=300,
        retry_on_timeout=True,
        max_retries=3,
    )
    try:
        es.info()
        logger.info(f"Connected to Elasticsearch at {args.es_host}:{args.es_port}")
    except Exception as e:
        logger.error(f"Cannot connect to ES: {e}")
        sys.exit(1)

    # ---- Create index ----
    # Estimate indexed size: aggregated data is text-heavy, ~2× raw parquet
    estimated_indexed_gb = total_gb * 2.0
    create_index(es, args.index_name, estimated_indexed_gb,
                 keep_existing=args.keep_existing_index)

    # ---- Launch workers ----
    ctx = multiprocessing.get_context("spawn")
    doc_queue = ctx.Queue(maxsize=args.queue_size * 4)
    result_queue = ctx.Queue()

    # Distribute files across workers
    worker_files = [[] for _ in range(args.num_workers)]
    for i, f in enumerate(files):
        worker_files[i % args.num_workers].append(f)

    workers = []
    for wfiles in worker_files:
        if not wfiles:
            continue
        p = ctx.Process(
            target=parse_worker,
            args=(wfiles, args.index_name, doc_queue,
                  args.chunk_size, result_queue),
            daemon=True,
        )
        p.start()
        workers.append(p)

    # ---- ES consumer in main thread ----
    stats = {"indexed": 0, "failed": 0}
    stop_event = threading.Event()

    total_start = time.time()
    indexing_start = time.time()

    consumer_thread = threading.Thread(
        target=es_consumer,
        args=(doc_queue, es, args.batch_size, args.max_chunk_bytes,
              args.thread_count, args.queue_size, stats, stop_event),
        daemon=True,
    )
    consumer_thread.start()

    # Progress reporting
    last_log = time.time()
    log_interval = 30

    for p in workers:
        p.join()

    stop_event.set()
    consumer_thread.join()

    indexing_end = time.time()
    duration = indexing_end - indexing_start

    # Collect parsed counts
    parsed_total = 0
    while not result_queue.empty():
        parsed_total += result_queue.get_nowait()

    logger.info("=" * 70)
    logger.info(f"Indexing complete in {duration:.1f}s")
    logger.info(f"Parsed:  {parsed_total:,}")
    logger.info(f"Indexed: {stats['indexed']:,}")
    logger.info(f"Failed:  {stats['failed']:,}")
    if duration > 0:
        logger.info(f"Rate:    {stats['indexed'] / duration:.0f} docs/sec")

    # ---- Validate ----
    ok = validate(es, args.index_name, ground_truth, stats,
                  append_mode=args.keep_existing_index)

    if ok:
        logger.info("=== Indexing Job Completed Successfully ===")
        sys.exit(0)
    else:
        logger.error("=== Indexing Job Failed — Validation Errors ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
