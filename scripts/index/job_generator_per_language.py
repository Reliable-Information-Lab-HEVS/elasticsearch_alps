#!/usr/bin/env python3
"""
Generic per-language SLURM job generator for Elasticsearch indexing.

Works for any FineWeb-style dataset laid out as:
    <dataset_dir>/<lang_code>/*.parquet

For each language it generates one or more sbatch invocations of
index_per_language.sh, splitting large languages into multiple file-range
jobs that all target the same per-language index (safe because the indexer
uses content-based SHA256 IDs by default).

Usage examples
--------------
# Dry-run (print commands, do not submit)
python3 job_generator_per_language.py \\
    --dataset-dir /capstor/store/cscs/swissai/infra01/datasets/swiss-ai/fineweb-2_0_1-quality_33-filterrobots/data/output \\
    --dataset-name fineweb-2_0_1-quality_33-filterrobots \\
    --user alexandersternfeld

# Write submission script and submit it
python3 job_generator_per_language.py \\
    --dataset-dir /path/to/dataset/output \\
    --dataset-name my-dataset \\
    --user alexandersternfeld \\
    --submit
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path


# ============================================================
# Defaults (override with CLI flags)
# ============================================================

DEFAULT_FILES_PER_JOB = 600      # Upper limit before a language is split
DEFAULT_USER = "alexandersternfeld"
DEFAULT_ACCOUNT = "a145"
DEFAULT_PARTITION_NORMAL = "normal"
DEFAULT_PARTITION_DEBUG = "debug"   # ≤ 30 min jobs only
DEFAULT_TIME_NORMAL = "12:00:00"
DEFAULT_TIME_DEBUG = "00:29:00"
DEFAULT_NUM_WORKERS = 8
DEFAULT_BATCH_SIZE = 12500
DEFAULT_CHUNK_SIZE = 5000
DEFAULT_MAX_CHUNK_BYTES = 100
DEFAULT_THREAD_COUNT = 6
DEFAULT_QUEUE_SIZE = 12

# Fields stored per document (space-separated --store-* flags)
DEFAULT_STORE_FLAGS = (
    "--store-id --store-url --store-date "
    "--store-language --store-file-path --store-folder-path"
)


# ============================================================
# Helpers
# ============================================================

def discover_language_dirs(dataset_dir: Path):
    """Return sorted list of (lang, n_files, total_bytes) for every sub-directory."""
    results = []
    for d in sorted(dataset_dir.iterdir()):
        if not d.is_dir():
            continue
        files = sorted(d.glob("*.parquet"))
        if not files:
            continue
        total_bytes = sum(f.stat().st_size for f in files)
        results.append((d.name, len(files), total_bytes))
    return results


def make_sbatch_block(
    lang: str,
    index_name: str,
    data_dir: str,
    file_start: int | None,
    file_end: int | None,
    n_files: int,
    size_gb: float,
    keep_existing: bool,
    args,
    part: int | None = None,
    total_parts: int | None = None,
) -> str:
    """Return a self-contained shell snippet that exports env-vars and calls sbatch."""

    part_tag = f" (part {part}/{total_parts})" if part else ""
    job_suffix = f"-p{part}" if part else ""
    # SLURM job names are limited to 15 chars; keep the lang prefix readable
    job_name = f"{lang[:18]}{job_suffix}"

    log_out = f"{args.log_dir}/out/{lang}{job_suffix}_%j.out"
    log_err = f"{args.log_dir}/err/{lang}{job_suffix}_%j.err"

    repo_dir = str(Path(args.slurm_script).parent.parent.parent)
    exports = [
        f'REPO_DIR="{repo_dir}"',
        f'DATA_DIR="{data_dir}"',
        f'INDEX_NAME="{index_name}"',
        f'NUM_WORKERS="{args.num_workers}"',
        f'BATCH_SIZE="{args.batch_size}"',
        f'CHUNK_SIZE="{args.chunk_size}"',
        f'MAX_CHUNK_BYTES="{args.max_chunk_bytes}"',
        f'THREAD_COUNT="{args.thread_count}"',
        f'QUEUE_SIZE="{args.queue_size}"',
        f'STORE_FLAGS="{args.store_flags}"',
        f'DEDUPLICATE="{"true" if args.deduplicate else "false"}"',
    ]

    if file_start is not None:
        exports += [
            f'FILE_RANGE_START="{file_start}"',
            f'FILE_RANGE_END="{file_end}"',
        ]

    if keep_existing:
        exports.append('KEEP_EXISTING_INDEX="true"')

    export_str = "\n".join(f"export {e}" for e in exports)

    sbatch_args = [
        f'--job-name="{job_name}"',
        f'--account="{args.account}"',
        f'--partition="{args.partition}"',
        f'--time="{args.time}"',
        f'--output="{log_out}"',
        f'--error="{log_err}"',
    ]

    sbatch_line = "sbatch \\\n    " + " \\\n    ".join(sbatch_args) + " \\\n    " + args.slurm_script

    lines = [
        f"# {lang}{part_tag}: {n_files} files, {size_gb:.2f} GB raw",
        "(",
        export_str,
        sbatch_line,
        ")",
    ]

    return "\n".join(lines)


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate per-language SLURM indexing jobs for FineWeb-style datasets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Dataset
    parser.add_argument("--dataset-dir", required=True,
                        help="Root directory containing one sub-folder per language")
    parser.add_argument("--dataset-name", required=True,
                        help="Short name used in index names, e.g. fineweb-2_0_1-quality_33-filterrobots")

    # Paths
    parser.add_argument("--user", default=DEFAULT_USER,
                        help="CSCS username (used to derive log/scratch paths)")
    parser.add_argument("--slurm-script", default=None,
                        help="Path to index_per_language.sh. "
                             "Defaults to <repo>/scripts/index/index_per_language.sh")
    parser.add_argument("--log-dir", default=None,
                        help="Root dir for job logs (out/ and err/ sub-dirs are created). "
                             "Defaults to /iopsstor/scratch/cscs/<user>/<dataset-name>-logs")

    # SLURM resource defaults
    parser.add_argument("--account", default=DEFAULT_ACCOUNT)
    parser.add_argument("--partition", default=DEFAULT_PARTITION_NORMAL)
    parser.add_argument("--time", default=DEFAULT_TIME_NORMAL)

    # Splitting
    parser.add_argument("--files-per-job", type=int, default=DEFAULT_FILES_PER_JOB,
                        help="Max parquet files per SLURM job. Languages with more files "
                             f"are split into multiple jobs. (default: {DEFAULT_FILES_PER_JOB})")

    # Indexer performance knobs (passed through to index_per_language.sh)
    parser.add_argument("--num-workers", type=int, default=DEFAULT_NUM_WORKERS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--max-chunk-bytes", type=int, default=DEFAULT_MAX_CHUNK_BYTES)
    parser.add_argument("--thread-count", type=int, default=DEFAULT_THREAD_COUNT)
    parser.add_argument("--queue-size", type=int, default=DEFAULT_QUEUE_SIZE)

    # Metadata & dedup
    parser.add_argument("--store-flags", default=DEFAULT_STORE_FLAGS,
                        help="Space-separated --store-* flags forwarded to the indexer. "
                             f"(default: \"{DEFAULT_STORE_FLAGS}\")")
    parser.add_argument("--no-deduplicate", dest="deduplicate", action="store_false",
                        help="Disable SHA256 deduplication (enabled by default)")
    parser.set_defaults(deduplicate=True)

    # Filter
    parser.add_argument("--only-lang", nargs="+", metavar="LANG",
                        help="Only generate jobs for these language codes")
    parser.add_argument("--skip-lang", nargs="+", metavar="LANG",
                        help="Skip these language codes")

    # Output / submission
    parser.add_argument("--output", default=None,
                        help="Write the submission script to this file instead of stdout")
    parser.add_argument("--submit", action="store_true",
                        help="Actually submit jobs via sbatch after generating commands")

    args = parser.parse_args()

    # ---- Resolve paths ----
    dataset_dir = Path(args.dataset_dir)
    if not dataset_dir.is_dir():
        print(f"[ERROR] dataset-dir not found: {dataset_dir}", file=sys.stderr)
        sys.exit(1)

    # Derive script location relative to this file
    this_dir = Path(__file__).parent
    if args.slurm_script is None:
        args.slurm_script = str(this_dir / "index_per_language.sh")

    if not Path(args.slurm_script).is_file():
        print(f"[ERROR] SLURM script not found: {args.slurm_script}", file=sys.stderr)
        sys.exit(1)

    if args.log_dir is None:
        args.log_dir = f"/iopsstor/scratch/cscs/{args.user}/{args.dataset_name}-logs"

    # ---- Discover languages ----
    print(f"[INFO] Scanning: {dataset_dir}", file=sys.stderr)
    lang_entries = discover_language_dirs(dataset_dir)
    print(f"[INFO] Found {len(lang_entries)} language directories", file=sys.stderr)

    only_set = set(args.only_lang) if args.only_lang else None
    skip_set = set(args.skip_lang) if args.skip_lang else set()

    # ---- Build script ----
    blocks = []
    total_jobs = 0
    split_langs = []

    # Header
    blocks.append("#!/bin/bash")
    blocks.append(f"# Auto-generated by job_generator_per_language.py")
    blocks.append(f"# Dataset:  {args.dataset_name}")
    blocks.append(f"# Dir:      {dataset_dir}")
    blocks.append(f"# Generated: $(date)")
    blocks.append("")
    blocks.append("set -euo pipefail")
    blocks.append("")
    blocks.append("# Create log directories")
    blocks.append(f'mkdir -p "{args.log_dir}/out" "{args.log_dir}/err"')
    blocks.append("")

    for lang, n_files, total_bytes in lang_entries:
        if only_set and lang not in only_set:
            continue
        if lang in skip_set:
            continue

        total_gb = total_bytes / (1024**3)
        index_name = f"{args.dataset_name}-{lang}"
        data_dir = str(dataset_dir / lang)

        if n_files <= args.files_per_job:
            blocks.append(make_sbatch_block(
                lang, index_name, data_dir,
                file_start=None, file_end=None,
                n_files=n_files, size_gb=total_gb,
                keep_existing=False,
                args=args,
            ))
            blocks.append("")
            total_jobs += 1

        else:
            # Split into chunks; all target the same index (SHA256 IDs prevent duplication)
            n_parts = (n_files + args.files_per_job - 1) // args.files_per_job
            split_langs.append((lang, n_files, total_gb, n_parts))

            for part in range(n_parts):
                start = part * args.files_per_job
                end = min(start + args.files_per_job, n_files)
                part_gb = total_gb * (end - start) / n_files

                blocks.append(make_sbatch_block(
                    lang, index_name, data_dir,
                    file_start=start, file_end=end,
                    n_files=end - start, size_gb=part_gb,
                    keep_existing=(part > 0),  # first part creates the index
                    args=args,
                    part=part + 1, total_parts=n_parts,
                ))
                blocks.append("")
                total_jobs += 1

    # Summary comment at the end
    blocks.append(f"# === Summary ===")
    blocks.append(f"# Total SLURM jobs: {total_jobs}")
    if split_langs:
        blocks.append(f"# Split languages ({len(split_langs)}):")
        for lang, n, gb, parts in split_langs:
            blocks.append(f"#   {lang}: {n} files, {gb:.1f} GB -> {parts} jobs")
    blocks.append(f"# Index prefix: {args.dataset_name}-<lang>")
    blocks.append(f"# Log dir: {args.log_dir}")
    blocks.append(f"# SLURM script: {args.slurm_script}")

    script_text = "\n".join(blocks) + "\n"

    # ---- Print summary ----
    print(f"[INFO] Languages:  {len(lang_entries)}", file=sys.stderr)
    print(f"[INFO] Total jobs: {total_jobs}", file=sys.stderr)
    if split_langs:
        print(f"[INFO] Split languages ({len(split_langs)}):", file=sys.stderr)
        for lang, n, gb, parts in split_langs:
            print(f"[INFO]   {lang}: {n} files, {gb:.1f} GB -> {parts} jobs", file=sys.stderr)
    print(f"[INFO] Log dir:    {args.log_dir}", file=sys.stderr)

    # ---- Write or print ----
    if args.output:
        out_path = Path(args.output)
        out_path.write_text(script_text)
        out_path.chmod(0o755)
        print(f"[INFO] Submission script written to: {out_path}", file=sys.stderr)
        print(f"[INFO] To submit: bash {out_path}", file=sys.stderr)
    else:
        print(script_text)

    # ---- Submit ----
    if args.submit:
        print("[INFO] Submitting jobs...", file=sys.stderr)

        # Create log dirs before submitting
        os.makedirs(f"{args.log_dir}/out", exist_ok=True)
        os.makedirs(f"{args.log_dir}/err", exist_ok=True)

        # Parse the generated script and run each () sub-shell block
        # We execute the whole script directly — simplest and most reliable.
        if args.output:
            result = subprocess.run(["bash", args.output])
        else:
            result = subprocess.run(["bash", "-c", script_text])

        if result.returncode != 0:
            print(f"[ERROR] Submission script exited with code {result.returncode}", file=sys.stderr)
            sys.exit(result.returncode)

        print("[INFO] All jobs submitted.", file=sys.stderr)


if __name__ == "__main__":
    main()
