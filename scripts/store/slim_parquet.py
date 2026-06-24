#!/usr/bin/env python3
"""
Drop the `text` column from aggregated parquet files and write to capstor.
Also writes a SUMMARY.txt with pre/post-dedup statistics.

This produces the deduplication metadata — everything except the full text:
  sha256, language, source_count, date, sources (id, url, date, file_path, folder_path)

The text is recoverable from the original dataset via sources[0].file_path + sources[0].id.

Usage:
    # Single dataset (FW1, DCLM, ...):
    python3 slim_parquet.py --input-dir <agg_dir> --output-dir <capstor_dest> \\
        --original-dirs /capstor/.../data/output

    # Multi-dataset (english-web-dedup: FW1 + FW-edu + DCLM):
    python3 slim_parquet.py --input-dir <agg_dir> --output-dir <capstor_dest> \\
        --original-dirs <fw1_dir> <fwedu_dir> <dclm_dir> \\
        --original-dir-labels FW1 FW-edu DCLM

    # Recursive / per-language subdirs (FW2):
    python3 slim_parquet.py --input-dir <agg_dir> --output-dir <capstor_dest> --recursive
"""

import argparse
import concurrent.futures
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow.compute as pc


def slim_file(src, dst, skip_existing):
    """Returns (unique_doc_count, pre_dedup_source_count) or None if skipped."""
    if skip_existing and dst.exists():
        return None
    dst.parent.mkdir(parents=True, exist_ok=True)
    table = pq.read_table(str(src))
    source_count_sum = 0
    if "source_count" in table.schema.names:
        source_count_sum = pc.sum(table.column("source_count")).as_py() or 0
    if "text" in table.schema.names:
        table = table.drop(["text"])
    pq.write_table(table, str(dst), compression="snappy")
    return len(table), source_count_sum


def dir_parquet_size_gb(directory):
    return sum(f.stat().st_size for f in Path(directory).rglob("*.parquet")) / 1024 ** 3


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-dir",  required=True, help="Aggregated parquet dir (with text)")
    parser.add_argument("--output-dir", required=True, help="Capstor destination dir")
    parser.add_argument("--workers",    type=int, default=16)
    parser.add_argument("--recursive",  action="store_true",
                        help="Recurse into subdirectories (e.g. FW2 per-language layout).")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip files already present in output-dir (for resuming).")
    parser.add_argument("--original-dirs", nargs="*", default=None,
                        help="Input dataset dir(s) BEFORE cross-dataset dedup. "
                             "Sizes are shown in SUMMARY.txt. "
                             "Pass multiple dirs for merged datasets (e.g. FW1 + FW-edu + DCLM).")
    parser.add_argument("--original-dir-labels", nargs="*", default=None,
                        help="Labels for each --original-dirs entry. "
                             "Defaults to directory basenames.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    src_dir = Path(args.input_dir)
    dst_dir = Path(args.output_dir)

    src_files = sorted(src_dir.rglob("*.parquet") if args.recursive else src_dir.glob("*.parquet"))
    if not src_files:
        logger.error("No parquet files found in %s (recursive=%s)", src_dir, args.recursive)
        sys.exit(1)

    dst_dir.mkdir(parents=True, exist_ok=True)

    pairs = []
    skipped_existing = 0
    for src in src_files:
        rel = src.relative_to(src_dir)
        dst = dst_dir / rel
        if args.skip_existing and dst.exists():
            skipped_existing += 1
            continue
        pairs.append((src, dst))

    if skipped_existing:
        logger.info("Skipped:   %d files already in output-dir", skipped_existing)

    total_src_bytes = sum(s.stat().st_size for s, _ in pairs)
    logger.info("Input:     %d files, %.2f GB (with text)", len(pairs), total_src_bytes / 1024**3)
    logger.info("Output:    %s", dst_dir)
    logger.info("Recursive: %s", args.recursive)
    logger.info("Workers:   %d", args.workers)

    total_unique = 0
    total_pre_dedup = 0
    done = errors = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(slim_file, src, dst, args.skip_existing): src for src, dst in pairs}
        for fut in concurrent.futures.as_completed(futures):
            src = futures[fut]
            try:
                result = fut.result()
                if result is not None:
                    unique, pre_dedup = result
                    total_unique    += unique
                    total_pre_dedup += pre_dedup
                done += 1
                if done % 100 == 0 or done == len(pairs):
                    logger.info("  %d/%d files done", done, len(pairs))
            except Exception as e:
                errors += 1
                logger.error("Failed %s: %s", src, e)

    dst_bytes = sum(f.stat().st_size for f in dst_dir.rglob("*.parquet"))
    dedup_ratio = (1 - total_unique / total_pre_dedup) * 100 if total_pre_dedup else 0

    logger.info("Pre-dedup docs:  %s  (sum of source_count)", "{:,}".format(total_pre_dedup))
    logger.info("Unique docs:     %s  (%.1f%% duplicates removed)", "{:,}".format(total_unique), dedup_ratio)
    logger.info("Slim size:       %.2f GB  (%.1f%% of with-text size)",
                dst_bytes / 1024**3, dst_bytes / total_src_bytes * 100)

    # ---- Original dir sizes ----
    original_entries = []  # list of (label, gb, path_str)
    if args.original_dirs:
        labels = args.original_dir_labels or [Path(d).name for d in args.original_dirs]
        if len(labels) != len(args.original_dirs):
            logger.error("--original-dir-labels count must match --original-dirs count")
            sys.exit(1)
        for label, d in zip(labels, args.original_dirs):
            gb = dir_parquet_size_gb(d)
            original_entries.append((label, gb, d))
            logger.info("Original %-10s  %.2f GB  (%s)", label, gb, d)

    # ---- Write SUMMARY.txt ----
    summary_path = dst_dir / "SUMMARY.txt"
    with open(str(summary_path), "w") as fh:
        fh.write("Deduplication summary\n")
        fh.write("Generated:           %s\n" % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
        fh.write("Slim parquet dir:    %s\n" % dst_dir)
        fh.write("\n")
        fh.write("--- Before deduplication ---\n")
        fh.write("Total docs (incl. duplicates):  %s\n" % "{:,}".format(total_pre_dedup))
        if not original_entries:
            fh.write("Original dataset size:          (see original dataset on capstor)\n")
        elif len(original_entries) == 1:
            label, gb, d = original_entries[0]
            fh.write("Original dataset size:          %.2f GB  (%s)\n" % (gb, d))
        else:
            total_orig_gb = sum(gb for _, gb, _ in original_entries)
            fh.write("Input dataset sizes (on scratch at time of merge):\n")
            for label, gb, d in original_entries:
                fh.write("  %-10s  %7.2f GB  (%s)\n" % (label, gb, d))
            fh.write("  %-10s  %7.2f GB\n" % ("TOTAL", total_orig_gb))
        fh.write("\n")
        fh.write("--- After deduplication ---\n")
        fh.write("Unique docs:                    %s\n" % "{:,}".format(total_unique))
        fh.write("Duplicates removed:             %s  (%.1f%%)\n" % (
            "{:,}".format(total_pre_dedup - total_unique), dedup_ratio))
        fh.write("Deduplicated size (with text):  %.2f GB  (%s)\n" % (total_src_bytes / 1024**3, src_dir))
        fh.write("Slim parquet size (no text):    %.2f GB\n" % (dst_bytes / 1024**3))
        fh.write("\n")
        fh.write("Recovery: text is retrievable from sources[].file_path + sources[].id\n")
        fh.write("          in the original dataset on capstor.\n")
    logger.info("Written: %s", summary_path)

    if errors:
        logger.error("%d files failed", errors)
        sys.exit(1)


if __name__ == "__main__":
    main()
