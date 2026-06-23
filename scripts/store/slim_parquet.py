#!/usr/bin/env python3
"""
Drop the `text` column from aggregated parquet files and write to capstor.

This produces the deduplication metadata — everything except the full text:
  sha256, language, source_count, date, sources (id, url, date, file_path, folder_path)

The text is recoverable from the original dataset via sources[0].file_path + sources[0].id.

Usage:
    python3 slim_parquet.py --input-dir <aggregated_parquet_dir> --output-dir <capstor_dest>
"""

import argparse
import concurrent.futures
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def slim_file(src: Path, dst: Path) -> int:
    table = pq.read_table(str(src))
    if "text" in table.schema.names:
        table = table.drop(["text"])
    pq.write_table(table, str(dst), compression="snappy")
    return len(table)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir",  required=True, help="Aggregated parquet dir (with text)")
    parser.add_argument("--output-dir", required=True, help="Capstor destination dir")
    parser.add_argument("--workers",    type=int, default=16)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    src_dir = Path(args.input_dir)
    dst_dir = Path(args.output_dir)

    files = sorted(src_dir.glob("*.parquet"))
    if not files:
        logger.error(f"No parquet files in {src_dir}")
        sys.exit(1)

    dst_dir.mkdir(parents=True, exist_ok=True)

    total_src_bytes  = sum(f.stat().st_size for f in files)
    logger.info(f"Input:   {len(files)} files, {total_src_bytes / 1024**3:.2f} GB (with text)")
    logger.info(f"Output:  {dst_dir}")
    logger.info(f"Workers: {args.workers}")

    total_rows = 0
    done = 0
    errors = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(slim_file, f, dst_dir / f.name): f for f in files}
        for fut in concurrent.futures.as_completed(futures):
            f = futures[fut]
            try:
                total_rows += fut.result()
                done += 1
                if done % 20 == 0 or done == len(files):
                    logger.info(f"  {done}/{len(files)} files done")
            except Exception as e:
                errors += 1
                logger.error(f"Failed {f.name}: {e}")

    dst_bytes = sum(f.stat().st_size for f in dst_dir.glob("*.parquet"))
    logger.info(f"Done: {total_rows:,} rows, {dst_bytes / 1024**3:.2f} GB written "
                f"({dst_bytes / total_src_bytes * 100:.1f}% of original)")
    if errors:
        logger.error(f"{errors} files failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
