#!/usr/bin/env python3
"""
Index datasets that do not have a pre-computed SHA256 into the global ES index.

For datasets already Spark-aggregated (FW1+FW-edu+DCLM merged, FW2), use
index_aggregated.py instead (sha256 is already in the parquet).

This script handles:
  - EuroParl, ParaDocs   — translation pairs, language from dir name
  - FLAN, EuroBlocks      — English-only
  - institutional-books   — rich metadata (author, title, date, language)
  - gutenberg             — JSONL files
  - canaries              — gzipped JSONL files

SHA256(text) is computed at index time and used as ES _id.
op_type="create" silently skips documents already present — so this script
can be run in any order and against an index that already contains other datasets.

All output lands in the same global index as FW1+FW-edu+DCLM and FW2.

Usage
-----
python3 index_raw.py \\
    --dataset europarl \\
    --data-dir /capstor/.../europarl_bidirectional_preprocessed/data \\
    --index-name global-pretraining \\
    --es-host nid006184 --es-port 9200
"""

import argparse
import gzip
import hashlib
import json
import logging
import multiprocessing
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow.parquet as pq
from elasticsearch import Elasticsearch, helpers


# ============================================================
# Helpers
# ============================================================

def _sha256(text: str) -> Optional[str]:
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _iso_date_from_year(year_str) -> Optional[str]:
    """'1903' → '1903-01-01'"""
    if not year_str:
        return None
    try:
        y = str(year_str).strip()
        if y.isdigit() and 1000 <= int(y) <= 9999:
            return f"{y}-01-01"
    except Exception:
        pass
    return None


def _language_pair_from_path(file_path: str, data_dir: str) -> Optional[str]:
    """
    Derive language pair from the path component directly under data_dir.
    E.g. .../lt-sk/is_reverse_True/output/file.parquet → "lt-sk"
    """
    try:
        rel = Path(file_path).relative_to(data_dir)
        for part in rel.parts:
            if "-" in part and not part.startswith("is_") and not part.endswith(".parquet"):
                segs = part.split("-")
                if len(segs) == 2 and all(2 <= len(s) <= 3 for s in segs):
                    return part
    except Exception:
        pass
    return None


# ============================================================
# Per-dataset metadata extraction (parquet)
# ============================================================

def extract_metadata_parquet(dataset: str, i: int, rows: dict,
                             file_path: str, data_dir: str) -> dict:
    folder_path = str(Path(file_path).parent)
    meta = rows.get("metadata", [None])[i] or {}
    if not isinstance(meta, dict):
        meta = {}

    if dataset == "europarl" or dataset == "paradocs":
        pair  = _language_pair_from_path(file_path, data_dir)
        langs = pair.split("-") if pair else []
        return dict(id=rows.get("id", [None])[i], language=langs, date=None,
                    url=None, language_score=None, folder_path=folder_path,
                    language_pair=pair, author=None, title=None)

    elif dataset == "flan" or dataset == "euroblocks":
        return dict(id=rows.get("id", [None])[i], language=["eng"], date=None,
                    url=None, language_score=None, folder_path=folder_path,
                    language_pair=None, author=None, title=None)

    elif dataset == "institutional-books":
        lang = meta.get("language_gen")
        return dict(
            id=rows.get("id", [None])[i],
            language=[lang] if lang else [],
            date=_iso_date_from_year(meta.get("date1_src")),
            url=None,
            language_score=None,
            folder_path=folder_path,
            language_pair=None,
            author=meta.get("author_src"),
            title=meta.get("title_src"),
        )

    else:
        # Generic parquet fallback
        lang = rows.get("language", [None])[i] or meta.get("language")
        return dict(
            id=rows.get("id", [None])[i],
            language=[lang] if lang else [],
            date=rows.get("date", [None])[i] or meta.get("date"),
            url=rows.get("url", [None])[i] or meta.get("url"),
            language_score=_to_float(rows.get("language_score", [None])[i] or meta.get("language_score")),
            folder_path=folder_path,
            language_pair=None,
            author=None,
            title=None,
        )


# ============================================================
# Per-dataset metadata extraction (JSONL)
# ============================================================

def extract_metadata_jsonl(dataset: str, record: dict, file_path: str) -> dict:
    folder_path = str(Path(file_path).parent)
    if dataset == "gutenberg":
        lang = record.get("language")
        return dict(id=record.get("id"), language=[lang] if lang else [],
                    date=None, url=None, language_score=None,
                    folder_path=folder_path, language_pair=None,
                    author=record.get("author"), title=record.get("title"))
    elif dataset == "canaries":
        return dict(id=record.get("id"), language=[], date=None,
                    url=None, language_score=None, folder_path=folder_path,
                    language_pair=None, author=None, title=None)
    else:
        lang = record.get("language")
        return dict(id=record.get("id"), language=[lang] if lang else [],
                    date=record.get("date"), url=record.get("url"),
                    language_score=_to_float(record.get("language_score")),
                    folder_path=folder_path, language_pair=None,
                    author=record.get("author"), title=record.get("title"))


# ============================================================
# Document builder
# ============================================================

def _build_doc(sha256: str, text: str, meta: dict,
               dataset: str, index_name: str) -> dict:
    langs = meta.get("language") or []
    date  = meta.get("date")
    dates = [date] if date else []

    source_entry = {k: v for k, v in {
        "dataset":        dataset,
        "id":             meta.get("id"),
        "language":       langs[0] if len(langs) == 1 else (langs or None),
        "date":           date,
        "url":            meta.get("url"),
        "language_score": meta.get("language_score"),
        "folder_path":    meta.get("folder_path"),
        "language_pair":  meta.get("language_pair"),
        "author":         meta.get("author"),
        "title":          meta.get("title"),
    }.items() if v is not None}

    return {
        "_id":      sha256,
        "_index":   index_name,
        "_op_type": "create",
        "_source": {
            "text":         text,
            "language":     langs,
            "date":         dates,
            "source_count": 1,
            "datasets":     [dataset],
            "sources":      [source_entry],
        },
    }


# ============================================================
# File discovery
# ============================================================

JSONL_DATASETS = {"gutenberg", "canaries"}


def find_files(data_dir: Path, dataset: str) -> List[Path]:
    if dataset in JSONL_DATASETS:
        files = sorted(
            p for p in data_dir.rglob("*")
            if p.is_file() and (p.suffix == ".jsonl" or p.name.endswith(".jsonl.gz"))
        )
    else:
        files = sorted(data_dir.rglob("*.parquet"))
    if not files:
        logging.getLogger(__name__).error(
            f"No files found under {data_dir} for dataset '{dataset}'"
        )
    return files


# ============================================================
# Parse workers
# ============================================================

def _parse_parquet(file_path: Path, dataset: str, data_dir: str,
                   index_name: str, doc_queue: multiprocessing.Queue,
                   chunk_size: int) -> int:
    count = 0
    try:
        pf = pq.ParquetFile(str(file_path))
        for batch in pf.iter_batches(batch_size=chunk_size):
            rows = batch.to_pydict()
            n    = len(rows.get("text", []))
            docs = []
            for i in range(n):
                text = rows.get("text", [None])[i]
                if not text:
                    continue
                sha = _sha256(text)
                if not sha:
                    continue
                meta = extract_metadata_parquet(dataset, i, rows, str(file_path), data_dir)
                docs.append(_build_doc(sha, text, meta, dataset, index_name))
            if docs:
                doc_queue.put(docs)
                count += len(docs)
    except Exception as e:
        logging.getLogger(__name__).error(f"Error parsing parquet {file_path}: {e}")
    return count


def _parse_jsonl(file_path: Path, dataset: str, index_name: str,
                 doc_queue: multiprocessing.Queue, chunk_size: int) -> int:
    count = 0
    docs  = []

    def _open(p: Path):
        if p.name.endswith(".gz"):
            return gzip.open(p, "rt", encoding="utf-8", errors="replace")
        return open(p, "r", encoding="utf-8", errors="replace")

    try:
        with _open(file_path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                text = record.get("text", "")
                if not text:
                    continue
                sha = _sha256(text)
                if not sha:
                    continue
                meta = extract_metadata_jsonl(dataset, record, str(file_path))
                docs.append(_build_doc(sha, text, meta, dataset, index_name))
                if len(docs) >= chunk_size:
                    doc_queue.put(docs)
                    count += len(docs)
                    docs = []
        if docs:
            doc_queue.put(docs)
            count += len(docs)
    except Exception as e:
        logging.getLogger(__name__).error(f"Error parsing jsonl {file_path}: {e}")
    return count


def parse_worker(file_list: List[Path], dataset: str, data_dir: str,
                 index_name: str, doc_queue: multiprocessing.Queue,
                 chunk_size: int, result_queue: multiprocessing.Queue) -> None:
    is_jsonl = dataset in JSONL_DATASETS
    total = 0
    for fp in file_list:
        if is_jsonl:
            total += _parse_jsonl(fp, dataset, index_name, doc_queue, chunk_size)
        else:
            total += _parse_parquet(fp, dataset, data_dir, index_name, doc_queue, chunk_size)
    result_queue.put(total)


# ============================================================
# ES consumer
# ============================================================

def es_consumer(doc_queue: multiprocessing.Queue, es: Elasticsearch,
                batch_size: int, max_chunk_bytes_mb: int,
                thread_count: int, queue_size: int,
                stats: dict, stop_event: threading.Event) -> None:
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
            action_info = info.get("create", info)
            err = action_info.get("error", {})
            if err.get("type") == "version_conflict_engine_exception":
                stats["skipped"] += 1
            else:
                stats["failed"] += 1
                logger.warning(f"Failed doc: {info}")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    parser.add_argument(
        "--dataset", required=True,
        choices=["europarl", "paradocs", "flan", "euroblocks",
                 "institutional-books", "gutenberg", "canaries", "generic"],
        help="Dataset type (selects metadata extraction logic)",
    )
    parser.add_argument("--data-dir",   required=True)
    parser.add_argument("--index-name", required=True,
                        help="Global ES index name (must already exist)")
    parser.add_argument("--es-host", default="localhost")
    parser.add_argument("--es-port", type=int, default=9200)

    # Subset of files — for splitting large datasets across jobs
    parser.add_argument("--file-range-start", type=int, default=None)
    parser.add_argument("--file-range-end",   type=int, default=None)

    # Performance
    parser.add_argument("--num-workers",     type=int, default=8)
    parser.add_argument("--chunk-size",      type=int, default=5000)
    parser.add_argument("--batch-size",      type=int, default=12500)
    parser.add_argument("--max-chunk-bytes", type=int, default=50, help="MB")
    parser.add_argument("--thread-count",    type=int, default=4)
    parser.add_argument("--queue-size",      type=int, default=8)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    args.index_name = args.index_name.lower()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        logger.error(f"data-dir not found: {data_dir}")
        sys.exit(1)

    all_files = find_files(data_dir, args.dataset)
    if not all_files:
        sys.exit(1)

    start_idx = args.file_range_start if args.file_range_start is not None else 0
    end_idx   = args.file_range_end   if args.file_range_end   is not None else len(all_files) - 1
    files     = all_files[start_idx : end_idx + 1]
    if not files:
        logger.error(f"No files in range [{start_idx}, {end_idx}] (total: {len(all_files)})")
        sys.exit(1)

    total_bytes = sum(f.stat().st_size for f in files)
    total_gb    = total_bytes / (1024 ** 3)
    range_str   = f" [{start_idx}–{end_idx}]" if args.file_range_start is not None else ""

    logger.info("=" * 70)
    logger.info(f"Dataset:    {args.dataset}")
    logger.info(f"Data dir:   {data_dir}")
    logger.info(f"Files:      {len(files)}{range_str} of {len(all_files)} total  ({total_gb:.2f} GB)")
    logger.info(f"Index:      {args.index_name}  (op_type=create, SHA256 _id)")
    logger.info(f"Workers:    {args.num_workers}")
    logger.info("=" * 70)

    es = Elasticsearch(
        [{"host": args.es_host, "port": args.es_port}],
        timeout=300, retry_on_timeout=True, max_retries=3,
    )
    try:
        es.info()
        logger.info(f"Connected to ES at {args.es_host}:{args.es_port}")
    except Exception as e:
        logger.error(f"Cannot connect to ES: {e}")
        sys.exit(1)

    if not es.indices.exists(index=args.index_name):
        logger.error(
            f"Index '{args.index_name}' does not exist. "
            "Run index_aggregated.py first (it creates the index)."
        )
        sys.exit(1)

    ctx          = multiprocessing.get_context("spawn")
    doc_queue    = ctx.Queue(maxsize=args.queue_size * 8)
    result_queue = ctx.Queue()

    worker_files: List[List[Path]] = [[] for _ in range(args.num_workers)]
    for idx, f in enumerate(files):
        worker_files[idx % args.num_workers].append(f)

    workers = []
    for wfiles in worker_files:
        if not wfiles:
            continue
        p = ctx.Process(
            target=parse_worker,
            args=(wfiles, args.dataset, str(data_dir), args.index_name,
                  doc_queue, args.chunk_size, result_queue),
            daemon=True,
        )
        p.start()
        workers.append(p)

    stats      = {"indexed": 0, "skipped": 0, "failed": 0}
    stop_event = threading.Event()
    start_time = time.time()

    consumer_thread = threading.Thread(
        target=es_consumer,
        args=(doc_queue, es, args.batch_size, args.max_chunk_bytes,
              args.thread_count, args.queue_size, stats, stop_event),
        daemon=True,
    )
    consumer_thread.start()

    for p in workers:
        p.join()
    stop_event.set()
    consumer_thread.join()

    duration     = time.time() - start_time
    parsed_total = 0
    while not result_queue.empty():
        parsed_total += result_queue.get_nowait()

    logger.info("=" * 70)
    logger.info(f"Done in {duration:.1f}s")
    logger.info(f"Parsed:   {parsed_total:,}")
    logger.info(f"Indexed:  {stats['indexed']:,}")
    logger.info(f"Skipped:  {stats['skipped']:,}  (already in index)")
    logger.info(f"Failed:   {stats['failed']:,}")
    if duration > 0:
        logger.info(f"Rate:     {stats['indexed'] / duration:,.0f} docs/sec")
    logger.info("=" * 70)

    if stats["failed"] > max(100, stats["indexed"] * 0.01):
        logger.error("Too many indexing failures (>1%) — check logs above")
        sys.exit(1)


if __name__ == "__main__":
    main()
