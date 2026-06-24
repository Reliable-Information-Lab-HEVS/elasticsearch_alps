#!/bin/bash
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --environment=es-python

# ============================================================
# Post-indexing: copy results from scratch to capstor store.
#
# Stores two things per dataset/language:
#   1. ES index  → <STORE_BASE>/<DATASET_NAME>/indices/<INDEX_NAME>/
#   2. Slim dedup parquet (no text) → <STORE_BASE>/<DATASET_NAME>/deduplication/<LANG>/
#      For FW1 global merge: LANG is empty, goes directly into deduplication/
#
# Required env vars:
#   DATASET_NAME  - e.g. fineweb-2_0_1-quality_33-filterrobots
#   AGG_DIR       - full path to aggregated parquet dir on scratch (with text)
#
# Optional env vars:
#   INDEX_NAME    - e.g. fw2-q33-fra_latn  (required only when ES_DATA_DIR is set)
#   ES_DATA_DIR   - full path to ES data dir on scratch; skip ES copy step if unset
#   LANG          - language subdir for FW2 (e.g. fra_Latn); empty for FW1 global
#   STORE_BASE    - destination root (default: /capstor/store/cscs/swissai/a145/es_indices_2026)
#   SLIM_WORKERS  - parallel workers for slim parquet (default: 16)
#   SLIM_RECURSIVE- set to "true" to recurse into subdirs (for FW2 per-language layout)
# ============================================================

set -euo pipefail

DATASET_NAME="${DATASET_NAME:-}"
INDEX_NAME="${INDEX_NAME:-}"
ES_DATA_DIR="${ES_DATA_DIR:-}"
AGG_DIR="${AGG_DIR:-}"
LANG="${LANG:-}"
STORE_BASE="${STORE_BASE:-/capstor/store/cscs/swissai/a145/es_indices_2026}"
SLIM_WORKERS="${SLIM_WORKERS:-16}"
SLIM_RECURSIVE="${SLIM_RECURSIVE:-false}"
SLIM_ORIGINAL_DIRS="${SLIM_ORIGINAL_DIRS:-}"
SLIM_ORIGINAL_DIR_LABELS="${SLIM_ORIGINAL_DIR_LABELS:-}"

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

SLIM_SCRIPT="${REPO_DIR}/scripts/store/slim_parquet.py"

# ---- Validate ----
for var in DATASET_NAME AGG_DIR; do
    if [[ -z "${!var}" ]]; then
        echo "[ERROR] $var must be set"
        exit 1
    fi
done

if [[ -n "$ES_DATA_DIR" && ! -d "$ES_DATA_DIR" ]]; then
    echo "[ERROR] ES_DATA_DIR not found: $ES_DATA_DIR"
    exit 1
fi

if [[ -n "$ES_DATA_DIR" && -z "$INDEX_NAME" ]]; then
    echo "[ERROR] INDEX_NAME must be set when ES_DATA_DIR is provided"
    exit 1
fi

if [[ ! -d "$AGG_DIR" ]]; then
    echo "[ERROR] AGG_DIR not found: $AGG_DIR"
    exit 1
fi

# ---- Destination paths ----
if [[ -n "$LANG" ]]; then
    STORE_DEDUP_DIR="${STORE_BASE}/${DATASET_NAME}/deduplication/${LANG}"
else
    STORE_DEDUP_DIR="${STORE_BASE}/${DATASET_NAME}/deduplication"
fi

echo "[INFO] =============================================="
echo "[INFO] Store results job"
echo "[INFO] Date:            $(date)"
echo "[INFO] Node:            ${SLURM_NODELIST:-local}"
echo "[INFO] DATASET_NAME:    $DATASET_NAME"
echo "[INFO] LANG:            ${LANG:-(global)}"
echo "[INFO] AGG_DIR:         $AGG_DIR  ($(du -sh "$AGG_DIR" 2>/dev/null | cut -f1))"
echo "[INFO] → Slim dedup:    $STORE_DEDUP_DIR"
echo "[INFO] Recursive:       $SLIM_RECURSIVE"
if [[ -n "$ES_DATA_DIR" ]]; then
    STORE_INDEX_DIR="${STORE_BASE}/${DATASET_NAME}/indices/${INDEX_NAME}"
    echo "[INFO] ES_DATA_DIR:     $ES_DATA_DIR  ($(du -sh "$ES_DATA_DIR" 2>/dev/null | cut -f1))"
    echo "[INFO] → ES index:      $STORE_INDEX_DIR"
else
    echo "[INFO] ES_DATA_DIR:     (not set — skipping ES copy)"
fi
echo "[INFO] =============================================="

mkdir -p "$STORE_DEDUP_DIR"

# ---- Step 1: Copy ES index data directory (optional) ----
if [[ -n "$ES_DATA_DIR" ]]; then
    echo "[INFO] === Step 1: Copying ES index to capstor ==="
    mkdir -p "$STORE_INDEX_DIR"
    rsync -a --info=progress2 "$ES_DATA_DIR/" "$STORE_INDEX_DIR/"
    echo "[INFO] ES index copy done: $(du -sh "$STORE_INDEX_DIR" | cut -f1)"
else
    echo "[INFO] === Step 1: Skipped (no ES_DATA_DIR) ==="
fi

# ---- Step 2: Write slim dedup parquet (no text column) ----
echo ""
echo "[INFO] === Step 2: Writing slim dedup parquet ==="
echo "[INFO] Source: $AGG_DIR"
echo "[INFO] Dest:   $STORE_DEDUP_DIR"

SLIM_SKIP_EXISTING="${SLIM_SKIP_EXISTING:-false}"

slim_cmd=(
    python3 "$SLIM_SCRIPT"
    --input-dir  "$AGG_DIR"
    --output-dir "$STORE_DEDUP_DIR"
    --workers    "$SLIM_WORKERS"
)
[[ "$SLIM_RECURSIVE"     == "true" ]] && slim_cmd+=(--recursive)
[[ "$SLIM_SKIP_EXISTING" == "true" ]] && slim_cmd+=(--skip-existing)
# shellcheck disable=SC2086
[[ -n "$SLIM_ORIGINAL_DIRS"       ]] && slim_cmd+=(--original-dirs $SLIM_ORIGINAL_DIRS)
# shellcheck disable=SC2086
[[ -n "$SLIM_ORIGINAL_DIR_LABELS" ]] && slim_cmd+=(--original-dir-labels $SLIM_ORIGINAL_DIR_LABELS)

"${slim_cmd[@]}"

echo ""
echo "[INFO] === All done ==="
[[ -n "$ES_DATA_DIR" ]] && echo "[INFO] ES index:   $STORE_INDEX_DIR  ($(du -sh "$STORE_INDEX_DIR" | cut -f1))"
echo "[INFO] Slim dedup: $STORE_DEDUP_DIR  ($(du -sh "$STORE_DEDUP_DIR" | cut -f1))"
