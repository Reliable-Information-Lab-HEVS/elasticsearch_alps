#!/usr/bin/env python3
"""
Submit hierarchical global merge of FW1 + FW-edu + DCLM.

Distributes parquet files across N parallel round-1 jobs, then reduces
through successive rounds until a single final output.

Default schedule: 100 → 50 → 25 → 10 → 5 → 1

Must be run from the login node AFTER all three input directories exist
(i.e., after FW-edu final merge job 2603556 has completed).

Usage:
    python3 submit_global_merge_hierarchical.py \\
        --fw1-dir   $SCRATCH/fineweb-1_3_0-quality_33-filterrobots-merged \\
        --fwedu-dir $SCRATCH/fw-edu-merged \\
        --dclm-dir  $SCRATCH/dclm-aggregated \\
        --output-dir $SCRATCH/fw-english-global-merged \\
        --scratch-dir $SCRATCH/fw-english-intermediate \\
        [--rounds 100 50 25 10 5 1] \\
        [--dry-run]
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

# ---------------------------------------------------------------------------
# Per-round resource settings: (mem, cpus, time_h, driver_mem, shuffle_parts, output_parts)
# Round index 0 = first (widest) round, -1 = final round.
# Interpolate linearly for any number of rounds.
# ---------------------------------------------------------------------------
ROUND_RESOURCES = [
    # mem     cpus  time  driver  shuffle  output_parts
    ("256G",   64, "12:00:00",  "200g",  2000,  200),   # round 1: many small jobs
    ("256G",   64, "12:00:00",  "200g",  2000,  200),   # round 2
    ("384G",   96, "12:00:00",  "300g",  3000,  200),   # round 3
    ("512G",  128, "12:00:00",  "400g",  4000,  300),   # round 4
    ("600G",  192, "12:00:00",  "500g",  6000,  400),   # round 5
    ("800G",  256, "12:00:00",  "600g",  8000,  500),   # final round
]


def get_resources(round_idx: int, n_rounds: int) -> tuple:
    """Map round_idx (0-based) to resource tuple, scaling to n_rounds total."""
    n_presets = len(ROUND_RESOURCES)
    scaled = int(round(round_idx / max(n_rounds - 1, 1) * (n_presets - 1)))
    scaled = max(0, min(scaled, n_presets - 1))
    return ROUND_RESOURCES[scaled]


def list_parquet_files(directory: Path) -> List[Path]:
    files = sorted(directory.glob("*.parquet"))
    if not files:
        sys.exit(f"[ERROR] No parquet files found in {directory}")
    return files


def distribute(items: list, n_groups: int) -> List[List]:
    """Distribute items into n_groups groups as evenly as possible."""
    base, extra = divmod(len(items), n_groups)
    groups, start = [], 0
    for i in range(n_groups):
        size = base + (1 if i < extra else 0)
        groups.append(items[start:start + size])
        start += size
    return groups


def sbatch(
    script: str,
    env_vars: dict,
    dependency_jids: Optional[List[int]],
    overrides: List[str],
    dry_run: bool,
) -> int:
    cmd = ["sbatch"] + overrides
    if dependency_jids:
        deps = ":".join(str(j) for j in dependency_jids)
        cmd += [f"--dependency=afterok:{deps}"]
    cmd.append(script)

    env = {**os.environ, **{k: str(v) for k, v in env_vars.items()}}

    if dry_run:
        print(f"[DRY-RUN] {' '.join(cmd)}")
        print(f"          env: { {k: v for k, v in env_vars.items()} }")
        return 0

    result = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = result.stdout.decode() if isinstance(result.stdout, bytes) else result.stdout
    stderr = result.stderr.decode() if isinstance(result.stderr, bytes) else result.stderr
    if result.returncode != 0:
        print(stderr, file=sys.stderr)
        sys.exit("[ERROR] sbatch failed:\n" + stderr)
    m = re.search(r"Submitted batch job (\d+)", stdout)
    if not m:
        sys.exit("[ERROR] Could not parse job ID from: " + repr(stdout))
    jid = int(m.group(1))
    print(f"         → job {jid}")
    return jid


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fw1-dir",   required=True)
    parser.add_argument("--fwedu-dir", required=True)
    parser.add_argument("--dclm-dir",  required=True)
    parser.add_argument("--output-dir", required=True,
                        help="Final merged output directory")
    parser.add_argument("--scratch-dir", required=True,
                        help="Base directory for intermediate round outputs and manifests")
    parser.add_argument("--rounds", nargs="+", type=int, default=[100, 50, 25, 10, 5, 1],
                        help="Job counts per round (default: 100 50 25 10 5 1)")
    parser.add_argument("--account",   default="a145")
    parser.add_argument("--partition", default="normal")
    parser.add_argument("--log-dir",   default=None,
                        help="SLURM log directory (default: scratch-dir/logs)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    scratch   = Path(args.scratch_dir)
    log_dir   = Path(args.log_dir) if args.log_dir else scratch / "logs"
    rounds    = args.rounds
    n_rounds  = len(rounds)
    repo_dir  = Path(__file__).resolve().parents[2]
    job_script = str(repo_dir / "scripts" / "aggregate" / "reduce_merge_job.sh")

    scratch.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    (scratch / "manifests").mkdir(parents=True, exist_ok=True)

    print(f"Repo:       {repo_dir}")
    print(f"Scratch:    {scratch}")
    print(f"Logs:       {log_dir}")
    print(f"Round plan: {' → '.join(str(r) for r in rounds)}")
    print()

    # ------------------------------------------------------------------
    # Round 1: list all parquet files from the 3 input dirs, write manifests
    # ------------------------------------------------------------------
    for label, d in [("FW1", args.fw1_dir), ("FW-edu", args.fwedu_dir), ("DCLM", args.dclm_dir)]:
        if not Path(d).is_dir():
            sys.exit(f"[ERROR] {label} directory not found: {d}\n"
                     f"        Run this script only after all three input dirs exist.")

    fw1_files   = list_parquet_files(Path(args.fw1_dir))
    fwedu_files = list_parquet_files(Path(args.fwedu_dir))
    dclm_files  = list_parquet_files(Path(args.dclm_dir))

    # Interleave files from the 3 datasets so every group gets a representative mix
    all_files = []
    for trio in zip(fw1_files, fwedu_files, dclm_files):
        all_files.extend(trio)
    # Append any remainders (when lengths differ)
    n_min = min(len(fw1_files), len(fwedu_files), len(dclm_files))
    all_files.extend(fw1_files[n_min:])
    all_files.extend(fwedu_files[n_min:])
    all_files.extend(dclm_files[n_min:])

    print(f"Input files: {len(fw1_files)} FW1 + {len(fwedu_files)} FW-edu + {len(dclm_files)} DCLM = {len(all_files)} total")
    print()

    n_r1 = rounds[0]
    groups_r1 = distribute(all_files, n_r1)

    manifest_dir = scratch / "manifests"
    for i, group in enumerate(groups_r1):
        manifest = manifest_dir / f"r1_{i:04d}.txt"
        manifest.write_text("\n".join(str(f) for f in group) + "\n")

    print(f"Written {n_r1} manifests to {manifest_dir}/")
    print()

    # ------------------------------------------------------------------
    # Submit round 1
    # ------------------------------------------------------------------
    mem, cpus, time_str, driver_mem, shuffle_parts, out_parts = get_resources(0, n_rounds)
    r1_jids = []
    print(f"=== Round 1: {n_r1} jobs  [{mem}, {cpus} CPUs, {time_str}] ===")
    for i in range(n_r1):
        out_dir = scratch / "r1" / f"{i:04d}"
        manifest = manifest_dir / f"r1_{i:04d}.txt"
        overrides = [
            f"--job-name=gm-r1-{i:04d}",
            f"--output={log_dir}/r1-{i:04d}-%j.out",
            f"--mem={mem}",
            f"--cpus-per-task={cpus}",
            f"--time={time_str}",
            f"--account={args.account}",
            f"--partition={args.partition}",
        ]
        env = {
            "INPUT_MANIFEST":     str(manifest),
            "OUTPUT_DIR":         str(out_dir),
            "DRIVER_MEMORY":      driver_mem,
            "SHUFFLE_PARTITIONS": str(shuffle_parts),
            "OUTPUT_PARTITIONS":  str(out_parts),
            "REPO_DIR":           str(repo_dir),
        }
        print(f"  r1[{i:04d}]  {len(groups_r1[i])} files  → {out_dir}", end="")
        jid = sbatch(job_script, env, None, overrides, args.dry_run)
        r1_jids.append(jid)

    prev_jids  = r1_jids
    prev_dirs  = [scratch / "r1" / f"{i:04d}" for i in range(n_r1)]

    # ------------------------------------------------------------------
    # Submit rounds 2 … final
    # ------------------------------------------------------------------
    for round_idx in range(1, n_rounds):
        n_jobs    = rounds[round_idx]
        n_prev    = len(prev_dirs)
        mem, cpus, time_str, driver_mem, shuffle_parts, out_parts = get_resources(round_idx, n_rounds)

        is_final  = (round_idx == n_rounds - 1)
        round_tag = "final" if is_final else f"r{round_idx + 1}"
        print()
        print(f"=== Round {round_idx + 1}: {n_jobs} job{'s' if n_jobs > 1 else ''}  [{mem}, {cpus} CPUs, {time_str}] ===")

        groups = distribute(list(range(n_prev)), n_jobs)
        cur_jids = []
        cur_dirs = []

        for i, group_indices in enumerate(groups):
            if not group_indices:
                continue

            if is_final and n_jobs == 1:
                out_dir = Path(args.output_dir)
            else:
                out_dir = scratch / round_tag / f"{i:04d}"

            dep_jids = [prev_jids[j] for j in group_indices]
            input_dirs = " ".join(str(prev_dirs[j]) for j in group_indices)

            overrides = [
                f"--job-name=gm-{round_tag}-{i:04d}",
                f"--output={log_dir}/{round_tag}-{i:04d}-%j.out",
                f"--mem={mem}",
                f"--cpus-per-task={cpus}",
                f"--time={time_str}",
                f"--account={args.account}",
                f"--partition={args.partition}",
            ]
            env = {
                "INPUT_DIRS":         input_dirs,
                "OUTPUT_DIR":         str(out_dir),
                "DRIVER_MEMORY":      driver_mem,
                "SHUFFLE_PARTITIONS": str(shuffle_parts),
                "OUTPUT_PARTITIONS":  str(out_parts),
                "REPO_DIR":           str(repo_dir),
            }
            prev_tag = "r1" if round_idx == 1 else f"r{round_idx}"
            print(f"  {round_tag}[{i:04d}]  reads {prev_tag}[{group_indices[0]}..{group_indices[-1]}]  → {out_dir}", end="")
            jid = sbatch(job_script, env, dep_jids, overrides, args.dry_run)
            cur_jids.append(jid)
            cur_dirs.append(out_dir)

        prev_jids = cur_jids
        prev_dirs = cur_dirs

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print()
    print("=" * 60)
    print("All jobs submitted.")
    print(f"  Intermediates: {scratch}/")
    print(f"  Final output:  {args.output_dir}")
    print(f"  Logs:          {log_dir}/")
    if not args.dry_run:
        print(f"  Final job ID:  {prev_jids[0] if prev_jids else '?'}")
        print()
        print("Monitor with:")
        print(f"  squeue -u $USER")
        print(f"  tail -f {log_dir}/final-0000-*.out")
    print("=" * 60)


if __name__ == "__main__":
    main()
