#!/bin/bash
# ============================================================
# Deduplication pipeline orchestrator
#
# Submits the full FW-edu + DCLM aggregation pipeline and
# the hierarchical cross-dataset global merge, with SLURM
# dependency chains.
#
# Pipeline:
#   Phase A  — FW-edu per-crawl aggregate (SLURM array, 95 tasks)
#   Phase B  — FW-edu batch merges (10 jobs, ~10 crawls each)
#   Phase C  — FW-edu final merge (1 job: combine 10 batches)
#   Phase D  — DCLM flat aggregate (independent, runs in parallel)
#   Phase E  — Hierarchical cross-dataset merge: FW1 + FW-edu + DCLM
#              100 → 50 → 25 → 10 → 5 → 1 jobs
#              (submitted via a launcher job after C and D complete)
#
# Usage:
#   bash submit_dedup_pipeline.sh [--dry-run]
#
# With --dry-run, commands are printed but not submitted.
#
# Note: FW1 is assumed to be already merged at FW1_MERGED.
# FW2 (per-language) is handled separately — see README.md.
# ============================================================

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPTS_DIR="${REPO_DIR}/scripts/aggregate"

SCRATCH="/iopsstor/scratch/cscs/alexandersternfeld"

FWEDU_DATA="/capstor/store/cscs/swissai/infra01/datasets/swiss-ai/fineweb-edu-score-2-filterrobots/data/output"
DCLM_DATA="/capstor/store/cscs/swissai/infra01/datasets/swiss-ai/dclm-edu-filterrobots_fine/data/output"
FW1_MERGED="${SCRATCH}/fineweb-1_3_0-quality_33-filterrobots-merged"

FWEDU_PER_CRAWL_OUT="${SCRATCH}/fw-edu-per-crawl-aggregated"
FWEDU_BATCH_OUT="${SCRATCH}/fw-edu-batch-merged"
FWEDU_FINAL_OUT="${SCRATCH}/fw-edu-merged"
DCLM_AGG_OUT="${SCRATCH}/dclm-aggregated"

# Final output of Phase E — FW1 + FW-edu + DCLM globally deduplicated
GLOBAL_OUT="${SCRATCH}/english-web-dedup"
# Intermediate dirs for the hierarchical merge rounds
INTERMEDIATE_DIR="${SCRATCH}/english-web-intermediate"

LOG_DIR="${SCRATCH}/pipeline-logs"

CRAWL_LIST="${SCRIPTS_DIR}/fw_edu_crawls.txt"

ACCOUNT="a145"
PARTITION="normal"

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
    echo "[DRY-RUN] No jobs will be submitted."
fi

mkdir -p "$LOG_DIR"

sbatch_submit() {
    if (( DRY_RUN )); then
        echo "[DRY-RUN] sbatch $*"
        echo "FAKE_JOB_$$"
    else
        sbatch "$@" | awk '{print $NF}'
    fi
}

# ---- Phase 0: Build crawl list if missing ---------------------------------
if [[ ! -f "$CRAWL_LIST" ]]; then
    echo "[INFO] Generating crawl list from $FWEDU_DATA ..."
    ls -1d "${FWEDU_DATA}"/CC-MAIN-* | xargs -I{} basename {} | sort > "$CRAWL_LIST"
fi

N_CRAWLS=$(wc -l < "$CRAWL_LIST")
echo "[INFO] Found $N_CRAWLS FW-edu crawls in $CRAWL_LIST"
(( N_CRAWLS == 0 )) && { echo "[ERROR] No crawl dirs found. Check FWEDU_DATA."; exit 1; }

# ---- Phase A: Per-crawl aggregation (array job) ---------------------------
ARRAY_END=$(( N_CRAWLS - 1 ))
echo ""
echo "=== Phase A: FW-edu per-crawl aggregation (array 0-${ARRAY_END}) ==="

PHASE_A_JID=$(sbatch_submit \
    --account="$ACCOUNT" \
    --partition="$PARTITION" \
    --time=12:00:00 \
    --nodes=1 --ntasks-per-node=1 --cpus-per-task=64 --mem=800G \
    --environment=es-python \
    --array="0-${ARRAY_END}" \
    --job-name="fwedu-agg" \
    --output="${LOG_DIR}/fwedu-agg-%A_%a.out" \
    --export=ALL,\
DATASET_NAME="fineweb-edu",\
DATASET_DIR="$FWEDU_DATA",\
OUTPUT_DIR="$FWEDU_PER_CRAWL_OUT",\
MODE=aggregate,\
LAYOUT=per-crawl,\
CRAWL_LIST_FILE="$CRAWL_LIST",\
DRIVER_MEMORY=200g,\
SHUFFLE_PARTITIONS=4000,\
OUTPUT_PARTITIONS=100,\
REPO_DIR="$REPO_DIR" \
    --wrap='
        set -euo pipefail
        CRAWL=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$CRAWL_LIST_FILE")
        [[ -z "$CRAWL" ]] && { echo "[ERROR] No crawl for task $SLURM_ARRAY_TASK_ID"; exit 1; }
        echo "[INFO] Task $SLURM_ARRAY_TASK_ID → $CRAWL"
        export ONLY_SUBDIR="$CRAWL"
        export OUTPUT_DIR="$OUTPUT_DIR/$CRAWL"
        exec bash '"$SCRIPTS_DIR"'/aggregate_job.sh
    ')
echo "[INFO] Phase A job array ID: $PHASE_A_JID"

# ---- Phase B: Batch merges (10 jobs, ~10 crawls each) -------------------
echo ""
echo "=== Phase B: FW-edu batch merges (10 batches, depend on $PHASE_A_JID) ==="

N_BATCHES=10
BATCH_SIZE=$(( (N_CRAWLS + N_BATCHES - 1) / N_BATCHES ))
PHASE_B_JIDS=()

for (( b=0; b<N_BATCHES; b++ )); do
    BATCH_START=$(( b * BATCH_SIZE ))
    BATCH_END=$(( BATCH_START + BATCH_SIZE ))
    (( BATCH_END > N_CRAWLS )) && BATCH_END=$N_CRAWLS
    (( BATCH_START >= N_CRAWLS )) && break

    JID=$(sbatch_submit \
        --account="$ACCOUNT" --partition="$PARTITION" \
        --time=12:00:00 --nodes=1 --ntasks-per-node=1 --cpus-per-task=128 --mem=800G \
        --environment=es-python \
        --dependency="afterok:${PHASE_A_JID}" \
        --job-name="fwedu-batch-${b}" \
        --output="${LOG_DIR}/fwedu-batch-${b}-%j.out" \
        --export=ALL,\
DATASET_NAME="fineweb-edu",\
DATASET_DIR="$FWEDU_PER_CRAWL_OUT",\
OUTPUT_DIR="${FWEDU_BATCH_OUT}/batch_$(printf '%02d' $b)",\
MODE=merge,\
LAYOUT=per-crawl,\
CRAWL_LIST_FILE="$CRAWL_LIST",\
BATCH_START="$BATCH_START",\
BATCH_END="$BATCH_END",\
DRIVER_MEMORY=300g,\
SHUFFLE_PARTITIONS=6000,\
OUTPUT_PARTITIONS=200,\
REPO_DIR="$REPO_DIR" \
        "$SCRIPTS_DIR/aggregate_job.sh")

    echo "[INFO] Phase B batch $b (crawls $BATCH_START–$BATCH_END): job $JID"
    PHASE_B_JIDS+=("$JID")
done

PHASE_B_DEP="afterok:$(IFS=":"; echo "${PHASE_B_JIDS[*]}")"

# ---- Phase C: FW-edu final merge (combine 10 batches) -------------------
echo ""
echo "=== Phase C: FW-edu final merge (depend on ${PHASE_B_JIDS[*]}) ==="

FWEDU_INPUT_DATASETS=""
for (( b=0; b<${#PHASE_B_JIDS[@]}; b++ )); do
    FWEDU_INPUT_DATASETS+=" ${FWEDU_BATCH_OUT}/batch_$(printf '%02d' $b):fineweb-edu"
done
FWEDU_INPUT_DATASETS="${FWEDU_INPUT_DATASETS# }"

PHASE_C_JID=$(sbatch_submit \
    --account="$ACCOUNT" --partition="$PARTITION" \
    --time=12:00:00 --nodes=1 --ntasks-per-node=1 --cpus-per-task=256 --mem=800G \
    --environment=es-python \
    --dependency="$PHASE_B_DEP" \
    --job-name="fwedu-final" \
    --output="${LOG_DIR}/fwedu-final-%j.out" \
    --export=ALL,\
INPUT_DATASETS="$FWEDU_INPUT_DATASETS",\
OUTPUT_DIR="$FWEDU_FINAL_OUT",\
DRIVER_MEMORY=400g,\
SHUFFLE_PARTITIONS=8000,\
OUTPUT_PARTITIONS=500,\
REPO_DIR="$REPO_DIR" \
    --wrap='
        set -euo pipefail
        SPARK_LOCAL_DIR="/iopsstor/scratch/cscs/${SLURM_JOB_USER:-$USER}/spark-tmp/${SLURM_JOB_ID}"
        export SPARK_LOCAL_DIR
        export JAVA_HOME="/usr/share/elasticsearch/jdk"
        export PATH="$JAVA_HOME/bin:$PATH"
        python3 -c "import pyspark" 2>/dev/null || pip install pyspark --quiet
        mkdir -p "$OUTPUT_DIR" "$SPARK_LOCAL_DIR"
        python3 '"$SCRIPTS_DIR"'/spark_aggregate.py \
            --mode global-merge \
            --output-dir "$OUTPUT_DIR" \
            --driver-memory "$DRIVER_MEMORY" \
            --shuffle-partitions "$SHUFFLE_PARTITIONS" \
            --output-partitions "$OUTPUT_PARTITIONS" \
            --input-datasets $INPUT_DATASETS 2>&1
        exit_code=$?
        rm -rf "$SPARK_LOCAL_DIR" 2>/dev/null || true
        exit $exit_code
    ')
echo "[INFO] Phase C job ID: $PHASE_C_JID"

# ---- Phase D: DCLM flat aggregation (independent) -----------------------
echo ""
echo "=== Phase D: DCLM flat aggregation (independent) ==="

PHASE_D_JID=$(sbatch_submit \
    --account="$ACCOUNT" --partition="$PARTITION" \
    --time=12:00:00 --nodes=1 --ntasks-per-node=1 --cpus-per-task=128 --mem=800G \
    --environment=es-python \
    --job-name="dclm-agg" \
    --output="${LOG_DIR}/dclm-agg-%j.out" \
    --export=ALL,\
DATASET_NAME="dclm",\
DATASET_DIR="$DCLM_DATA",\
OUTPUT_DIR="$DCLM_AGG_OUT",\
MODE=aggregate,\
LAYOUT=flat,\
DRIVER_MEMORY=300g,\
SHUFFLE_PARTITIONS=6000,\
OUTPUT_PARTITIONS=300,\
REPO_DIR="$REPO_DIR" \
    "$SCRIPTS_DIR/aggregate_job.sh")
echo "[INFO] Phase D job ID: $PHASE_D_JID"

# ---- Phase E: Hierarchical cross-dataset global merge --------------------
# Runs after both Phase C (FW-edu) and Phase D (DCLM) complete.
# A lightweight launcher job submits all hierarchical rounds (100→50→25→10→5→1)
# once the input directories exist on scratch.
# Output: $GLOBAL_OUT  (FW1 + FW-edu + DCLM, globally deduplicated)
echo ""
echo "=== Phase E: Hierarchical global merge FW1+FW-edu+DCLM (launcher after C=$PHASE_C_JID, D=$PHASE_D_JID) ==="

PHASE_E_LAUNCHER=$(sbatch_submit \
    --account="$ACCOUNT" --partition="$PARTITION" \
    --time=00:30:00 --nodes=1 --ntasks-per-node=1 --cpus-per-task=1 --mem=4G \
    --dependency="afterok:${PHASE_C_JID}:${PHASE_D_JID}" \
    --job-name="gm-launcher" \
    --output="${LOG_DIR}/gm-launcher-%j.out" \
    --export=ALL,\
FW1_DIR="$FW1_MERGED",\
FWEDU_DIR="$FWEDU_FINAL_OUT",\
DCLM_DIR="$DCLM_AGG_OUT",\
OUTPUT_DIR="$GLOBAL_OUT",\
SCRATCH_DIR="$INTERMEDIATE_DIR",\
LOG_DIR="$LOG_DIR",\
REPO_DIR="$REPO_DIR" \
    --wrap='
        set -euo pipefail
        python3 '"$SCRIPTS_DIR"'/submit_global_merge_hierarchical.py \
            --fw1-dir   "$FW1_DIR" \
            --fwedu-dir "$FWEDU_DIR" \
            --dclm-dir  "$DCLM_DIR" \
            --output-dir "$OUTPUT_DIR" \
            --scratch-dir "$SCRATCH_DIR" \
            --log-dir    "$LOG_DIR"
    ')
echo "[INFO] Phase E launcher job ID: $PHASE_E_LAUNCHER"

# ---- Summary -------------------------------------------------------------
echo ""
echo "============================================================"
echo "Pipeline submitted:"
echo "  Phase A  (FW-edu per-crawl, array):       $PHASE_A_JID"
echo "  Phase B  (FW-edu batch merges):            ${PHASE_B_JIDS[*]}"
echo "  Phase C  (FW-edu final merge):             $PHASE_C_JID"
echo "  Phase D  (DCLM aggregate):                 $PHASE_D_JID"
echo "  Phase E  (global merge launcher):          $PHASE_E_LAUNCHER"
echo ""
echo "  FW-edu per-crawl:  $FWEDU_PER_CRAWL_OUT"
echo "  FW-edu merged:     $FWEDU_FINAL_OUT"
echo "  DCLM aggregated:   $DCLM_AGG_OUT"
echo "  Global merged:     $GLOBAL_OUT  (FW1 + FW-edu + DCLM)"
echo "  Intermediates:     $INTERMEDIATE_DIR"
echo "  Logs:              $LOG_DIR"
echo "============================================================"

(( DRY_RUN )) && echo "[DRY-RUN] All commands printed. No jobs submitted."
