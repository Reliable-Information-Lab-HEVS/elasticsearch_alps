#!/bin/bash
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --environment=es-python

# ============================================================
# Hierarchical reduce-merge job.
#
# Reads parquet files from multiple already-aggregated directories
# (or a manifest file), deduplicates by SHA256, and writes output.
# Does NOT re-stamp sources[].dataset — source names are already
# set from the original per-dataset aggregate step.
#
# Required env vars (one of):
#   INPUT_DIRS     - space-separated list of parquet directories
#   INPUT_MANIFEST - path to text file with one parquet path per line
#
# Required env vars:
#   OUTPUT_DIR     - where to write merged parquet
#
# Optional env vars:
#   DRIVER_MEMORY       - Spark driver memory (default: 200g)
#   SHUFFLE_PARTITIONS  - (default: 2000)
#   OUTPUT_PARTITIONS   - (default: 200)
# ============================================================

set -euo pipefail

INPUT_DIRS="${INPUT_DIRS:-}"
INPUT_MANIFEST="${INPUT_MANIFEST:-}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
DRIVER_MEMORY="${DRIVER_MEMORY:-200g}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-2000}"
OUTPUT_PARTITIONS="${OUTPUT_PARTITIONS:-200}"

CURRENT_USER="${SLURM_JOB_USER:-$USER}"
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
    echo "[ERROR] REPO_DIR not set and cannot be derived"
    exit 1
fi

SPARK_SCRIPT="${REPO_DIR}/scripts/aggregate/spark_aggregate.py"

if [[ -z "$INPUT_DIRS" && -z "$INPUT_MANIFEST" ]]; then
    echo "[ERROR] INPUT_DIRS or INPUT_MANIFEST must be set"
    exit 1
fi
if [[ -z "$OUTPUT_DIR" ]]; then
    echo "[ERROR] OUTPUT_DIR must be set"
    exit 1
fi

echo "[INFO] =============================================="
echo "[INFO] Reduce-merge job"
echo "[INFO] Date:               $(date)"
echo "[INFO] Node:               ${SLURM_NODELIST:-local}"
echo "[INFO] CPUs:               ${SLURM_CPUS_PER_TASK:-unknown}"
echo "[INFO] Memory:             ${SLURM_MEM_PER_NODE:-unknown} MB"
echo "[INFO] INPUT_DIRS:         ${INPUT_DIRS:-(not set)}"
echo "[INFO] INPUT_MANIFEST:     ${INPUT_MANIFEST:-(not set)}"
echo "[INFO] OUTPUT_DIR:         $OUTPUT_DIR"
echo "[INFO] DRIVER_MEMORY:      $DRIVER_MEMORY"
echo "[INFO] SHUFFLE_PARTITIONS: $SHUFFLE_PARTITIONS"
echo "[INFO] OUTPUT_PARTITIONS:  $OUTPUT_PARTITIONS"
echo "[INFO] =============================================="

free -h

export JAVA_HOME="/usr/share/elasticsearch/jdk"
export PATH="$JAVA_HOME/bin:$PATH"
java -version 2>&1 | head -1

if ! python3 -c "import pyspark" 2>/dev/null; then
    pip install pyspark --quiet
fi

mkdir -p "$OUTPUT_DIR" "$SPARK_LOCAL_DIR"

cmd=(
    python3 "$SPARK_SCRIPT"
    --mode reduce-merge
    --output-dir        "$OUTPUT_DIR"
    --driver-memory     "$DRIVER_MEMORY"
    --shuffle-partitions "$SHUFFLE_PARTITIONS"
    --output-partitions  "$OUTPUT_PARTITIONS"
)

if [[ -n "$INPUT_MANIFEST" ]]; then
    cmd+=(--input-manifest "$INPUT_MANIFEST")
else
    # shellcheck disable=SC2086
    cmd+=(--input-dirs $INPUT_DIRS)
fi

echo "[INFO] Running: ${cmd[*]}"
"${cmd[@]}" 2>&1
exit_code=$?

rm -rf "$SPARK_LOCAL_DIR" 2>/dev/null || true

if (( exit_code == 0 )); then
    echo "[INFO] === Reduce-merge completed successfully ==="
    du -sh "$OUTPUT_DIR" 2>/dev/null || true
else
    echo "[ERROR] === Reduce-merge failed (exit $exit_code) ==="
fi
exit $exit_code
