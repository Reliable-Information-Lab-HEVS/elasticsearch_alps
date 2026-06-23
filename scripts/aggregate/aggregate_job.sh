#!/bin/bash
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=800G
#SBATCH --environment=es-python

# ============================================================
# Phase 1: Spark aggregation job
#
# Required env vars:
#   DATASET_DIR   - root dir with per-crawl or per-language subdirs
#   DATASET_NAME  - used for output path naming
#
# Optional env vars:
#   MODE          - aggregate (default) | merge (global dedup of per-crawl outputs)
#   OUTPUT_DIR    - aggregated parquet output (default: iopsstor scratch)
#   LAYOUT        - per-crawl | per-language (default: per-crawl)
#   ONLY_SUBDIR   - process a single subdir (for testing or per-language jobs)
#   LANGUAGE      - override language field (e.g. "eng" for FineWeb 1)
#   DRIVER_MEMORY - Spark driver memory (default: 200g)
#   SHUFFLE_PARTITIONS - spark shuffle partitions (default: 4000)
#   OUTPUT_PARTITIONS  - output parquet file count (default: 200)
# ============================================================

set -euo pipefail

DATASET_DIR="${DATASET_DIR:-}"
DATASET_NAME="${DATASET_NAME:-}"
MODE="${MODE:-aggregate}"
BATCH_START="${BATCH_START:-}"
BATCH_END="${BATCH_END:-}"
LAYOUT="${LAYOUT:-per-crawl}"
ONLY_SUBDIR="${ONLY_SUBDIR:-}"
LANGUAGE="${LANGUAGE:-}"
DRIVER_MEMORY="${DRIVER_MEMORY:-200g}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-4000}"
OUTPUT_PARTITIONS="${OUTPUT_PARTITIONS:-200}"

CURRENT_USER="${SLURM_JOB_USER:-$USER}"
OUTPUT_DIR="${OUTPUT_DIR:-/iopsstor/scratch/cscs/${CURRENT_USER}/${DATASET_NAME}-aggregated}"
# Spark shuffle spill dir — must be large; default /tmp fills up on big crawls
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/iopsstor/scratch/cscs/${CURRENT_USER}/spark-tmp/${SLURM_JOB_ID:-$$}}"
export SPARK_LOCAL_DIR

REPO_DIR="${REPO_DIR:-}"
if [[ -z "$REPO_DIR" ]]; then
    _SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ "$_SCRIPT_DIR" != /var/spool* ]]; then
        REPO_DIR="$(cd "${_SCRIPT_DIR}/../.." && pwd)"
    fi
fi
if [[ -z "$REPO_DIR" ]]; then
    echo "[ERROR] REPO_DIR is not set and cannot be derived. Pass it as an env var."
    exit 1
fi

SPARK_SCRIPT="${REPO_DIR}/scripts/aggregate/spark_aggregate.py"

# ---- Validate ----
if [[ -z "$DATASET_DIR" || -z "$DATASET_NAME" ]]; then
    echo "[ERROR] DATASET_DIR and DATASET_NAME must be set"
    exit 1
fi

if [[ ! -d "$DATASET_DIR" ]]; then
    echo "[ERROR] DATASET_DIR does not exist: $DATASET_DIR"
    exit 1
fi

if [[ ! -f "$SPARK_SCRIPT" ]]; then
    echo "[ERROR] Spark script not found: $SPARK_SCRIPT"
    exit 1
fi

# ---- Info ----
echo "[INFO] =============================================="
echo "[INFO] Phase 1: Spark aggregation"
echo "[INFO] Date:          $(date)"
echo "[INFO] Node:          ${SLURM_NODELIST:-local}"
echo "[INFO] CPUs:          ${SLURM_CPUS_PER_TASK:-unknown}"
echo "[INFO] Memory:        ${SLURM_MEM_PER_NODE:-unknown} MB"
echo "[INFO] DATASET_DIR:   $DATASET_DIR"
echo "[INFO] DATASET_NAME:  $DATASET_NAME"
echo "[INFO] MODE:          $MODE"
echo "[INFO] LAYOUT:        $LAYOUT"
echo "[INFO] ONLY_SUBDIR:   ${ONLY_SUBDIR:-(all)}"
echo "[INFO] LANGUAGE:      ${LANGUAGE:-(from parquet)}"
echo "[INFO] OUTPUT_DIR:    $OUTPUT_DIR"
echo "[INFO] DRIVER_MEMORY: $DRIVER_MEMORY"
echo "[INFO] =============================================="

free -h
lscpu | grep -E "CPU\(s\)|Thread|Core|Socket" || true

# ---- Java (required by PySpark) ----
export JAVA_HOME="/usr/share/elasticsearch/jdk"
export PATH="$JAVA_HOME/bin:$PATH"

if ! java -version 2>/dev/null; then
    echo "[ERROR] Java not found at $JAVA_HOME"
    exit 1
fi

# ---- Install PySpark if not present ----
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "[INFO] Installing PySpark..."
    pip install pyspark --quiet
fi

python3 -c "import pyspark; print('[INFO] PySpark version:', pyspark.__version__)"

mkdir -p "$OUTPUT_DIR"
mkdir -p "$SPARK_LOCAL_DIR"
echo "[INFO] SPARK_LOCAL_DIR: $SPARK_LOCAL_DIR"

# ---- Build command ----
cmd=(
    python3 "$SPARK_SCRIPT"
    --mode "$MODE"
    --dataset-dir "$DATASET_DIR"
    --output-dir "$OUTPUT_DIR"
    --layout "$LAYOUT"
    --driver-memory "$DRIVER_MEMORY"
    --shuffle-partitions "$SHUFFLE_PARTITIONS"
    --output-partitions "$OUTPUT_PARTITIONS"
)

[[ -n "$ONLY_SUBDIR"  ]] && cmd+=(--only-subdir "$ONLY_SUBDIR")
[[ -n "$LANGUAGE"     ]] && cmd+=(--language "$LANGUAGE")
[[ -n "$BATCH_START"  ]] && cmd+=(--batch-start "$BATCH_START")
[[ -n "$BATCH_END"    ]] && cmd+=(--batch-end "$BATCH_END")

echo "[INFO] Running: ${cmd[*]}"
"${cmd[@]}" 2>&1
exit_code=$?

if (( exit_code == 0 )); then
    echo "[INFO] === Aggregation completed successfully ==="
    echo "[INFO] Output: $OUTPUT_DIR"
    du -sh "$OUTPUT_DIR" 2>/dev/null || true
else
    echo "[ERROR] === Aggregation failed with exit code $exit_code ==="
fi

# Clean up Spark shuffle temp dir
rm -rf "$SPARK_LOCAL_DIR" 2>/dev/null || true

exit $exit_code
