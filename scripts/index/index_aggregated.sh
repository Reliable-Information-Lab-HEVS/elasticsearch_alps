#!/bin/bash
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=12:00:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=256
#SBATCH --mem=800G
# Note: no --environment here; the batch script runs on the host OS so that srun
# and scontrol work correctly. Each step below runs inside the container via
# srun --environment=es-python, which is the CSCS-recommended usage.

# ============================================================
# Phase 2: Index aggregated parquet into Elasticsearch.
#
# Default is 2 nodes (ES cluster, ~2x throughput).
# Override with --nodes=1 on the sbatch command line for small jobs.
#
# Required env vars:
#   DATA_DIR    - directory of aggregated parquet (Phase 1 output)
#   INDEX_NAME  - target ES index name
#
# Optional env vars:
#   KEEP_EXISTING_INDEX  - "true" to append rather than recreate
#   FILE_RANGE_START     - first file index (0-based, inclusive) for this job
#   FILE_RANGE_END       - last file index  (0-based, inclusive) for this job
#   ES_DATA_DIR_FIXED    - override ES base data path (for chained jobs)
#   NUM_WORKERS          - parser processes (default: 32)
#   BATCH_SIZE           - ES bulk batch size (default: 12500)
#   CHUNK_SIZE           - parquet read chunk (default: 5000)
#   MAX_CHUNK_BYTES      - bulk request max MB (default: 50)
#   THREAD_COUNT         - parallel_bulk threads (default: 16)
#   QUEUE_SIZE           - parallel_bulk queue (default: 32)
#   LOG_LEVEL            - Python log level (default: INFO)
#   ES_ENV               - container environment name (default: es-python)
# ============================================================

set -euo pipefail

DATA_DIR="${DATA_DIR:-}"
INDEX_NAME="${INDEX_NAME:-}"
KEEP_EXISTING_INDEX="${KEEP_EXISTING_INDEX:-false}"
FILE_RANGE_START="${FILE_RANGE_START:-}"
FILE_RANGE_END="${FILE_RANGE_END:-}"
ES_DATA_DIR_FIXED="${ES_DATA_DIR_FIXED:-}"

NUM_WORKERS="${NUM_WORKERS:-32}"
BATCH_SIZE="${BATCH_SIZE:-12500}"
CHUNK_SIZE="${CHUNK_SIZE:-5000}"
MAX_CHUNK_BYTES="${MAX_CHUNK_BYTES:-50}"
THREAD_COUNT="${THREAD_COUNT:-16}"
QUEUE_SIZE="${QUEUE_SIZE:-32}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

ES_PORT=9200
ES_ENV="${ES_ENV:-es-python}"
CURRENT_USER="${SLURM_JOB_USER:-$USER}"
NUM_NODES="${SLURM_NNODES:-1}"

REPO_DIR="${REPO_DIR:-}"
if [[ -z "$REPO_DIR" ]]; then
    _SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ "$_SCRIPT_DIR" != /var/spool* ]]; then
        REPO_DIR="$(cd "${_SCRIPT_DIR}/../.." && pwd)"
    fi
fi
if [[ -z "$REPO_DIR" ]]; then
    echo "[ERROR] REPO_DIR is not set and cannot be derived."
    exit 1
fi

INDEXER="${REPO_DIR}/scripts/index/index_aggregated.py"
ES_NODE_SCRIPT="${REPO_DIR}/scripts/index/start_es_node.sh"

# ---- Validate ----
if [[ -z "$DATA_DIR" || -z "$INDEX_NAME" ]]; then
    echo "[ERROR] DATA_DIR and INDEX_NAME must be set"
    exit 1
fi

if [[ ! -d "$DATA_DIR" ]]; then
    echo "[ERROR] DATA_DIR does not exist: $DATA_DIR"
    exit 1
fi

for f in "$INDEXER" "$ES_NODE_SCRIPT"; do
    if [[ ! -f "$f" ]]; then
        echo "[ERROR] Script not found: $f"
        exit 1
    fi
done

# ---- Resolve node hostnames (scontrol works fine on host OS) ----
mapfile -t NODES < <(scontrol show hostnames "$SLURM_NODELIST")
MASTER_NODE="${NODES[0]}"

# ---- Info ----
echo "[INFO] =============================================="
echo "[INFO] Phase 2: Index aggregated data"
echo "[INFO] Date:         $(date)"
echo "[INFO] Nodes:        ${SLURM_NODELIST:-local}  (${NUM_NODES} node(s))"
echo "[INFO] Node names:   ${NODES[*]}"
echo "[INFO] Master node:  $MASTER_NODE"
echo "[INFO] CPUs/node:    ${SLURM_CPUS_PER_TASK:-unknown}"
echo "[INFO] Memory/node:  ${SLURM_MEM_PER_NODE:-unknown} MB"
echo "[INFO] DATA_DIR:     $DATA_DIR"
echo "[INFO] INDEX_NAME:   $INDEX_NAME"
echo "[INFO] Keep index:   $KEEP_EXISTING_INDEX"
[[ -n "$FILE_RANGE_START" ]] && echo "[INFO] File range:   ${FILE_RANGE_START} – ${FILE_RANGE_END}"
echo "[INFO] Repo:         $REPO_DIR"
echo "[INFO] =============================================="

free -h
ulimit -n 65536

# ---- Build ES environment for start_es_node.sh ----
BASE_DATA="${ES_DATA_DIR_FIXED:-/iopsstor/scratch/cscs/${CURRENT_USER}/es-data-${INDEX_NAME}}"
BASE_LOGS="/iopsstor/scratch/cscs/${CURRENT_USER}/es-logs-${INDEX_NAME}-${SLURM_JOB_ID:-local}"
SEED_HOSTS=$(printf "%s:9300\n" "${NODES[@]}" | paste -sd,)
CLUSTER_NAME="fw-es-${INDEX_NAME}"

echo "[INFO] ES base data: $BASE_DATA"
echo "[INFO] ES seed hosts: $SEED_HOSTS"

# Export everything start_es_node.sh needs
export ES_BASE_DATA="$BASE_DATA"
export ES_BASE_LOGS="$BASE_LOGS"
export ES_SEED_HOSTS="$SEED_HOSTS"
export ES_MASTER_NAME="es-node-0"
export ES_PORT="$ES_PORT"
export ES_HEAP="-Xms200g -Xmx200g"
export CLUSTER_NAME="$CLUSTER_NAME"
export ES_NUM_NODES="$NUM_NODES"

# ---- Start ES on all nodes via srun ----
echo "[INFO] Starting Elasticsearch on ${NUM_NODES} node(s)..."

srun --ntasks="$NUM_NODES" --ntasks-per-node=1 \
     --environment="$ES_ENV" \
     "$ES_NODE_SCRIPT" &
SRUN_ES_PID=$!
echo "[INFO] ES srun PID: $SRUN_ES_PID"

stop_elasticsearch() {
    echo "[INFO] Stopping Elasticsearch (srun PID $SRUN_ES_PID)..."
    kill "$SRUN_ES_PID" 2>/dev/null || true
    wait "$SRUN_ES_PID" 2>/dev/null || true
}
trap stop_elasticsearch EXIT

# ---- Wait for ES cluster to be ready ----
ES_URL="http://${MASTER_NODE}:${ES_PORT}"
max_wait=60
i=0
while (( i < max_wait )); do
    if ! kill -0 "$SRUN_ES_PID" 2>/dev/null; then
        echo "[ERROR] ES srun process died during startup"
        exit 1
    fi

    health=$(curl -s "${ES_URL}/_cluster/health" 2>/dev/null || true)
    if [[ -n "$health" ]]; then
        actual_nodes=$(echo "$health" | grep -o '"number_of_nodes":[0-9]*' | grep -o '[0-9]*' || echo 0)
        if [[ "$actual_nodes" -eq "$NUM_NODES" ]]; then
            echo "[INFO] ES cluster ready: $actual_nodes/${NUM_NODES} nodes"
            break
        else
            echo "[INFO] ES up, waiting for all nodes... ($actual_nodes/$NUM_NODES)"
        fi
    fi

    i=$(( i + 1 ))
    echo "[INFO] Waiting for ES... ($i/$max_wait)"
    sleep 10
done

if (( i == max_wait )); then
    echo "[ERROR] Elasticsearch failed to start within $((max_wait * 10))s"
    exit 1
fi

# ---- Ensure Python venv on shared filesystem ----
# Running Python via srun --overlap conflicts with the ES container step (pyxis SSH
# hook port conflict). Instead, we run Python directly on the host OS using a
# persistent venv on Lustre. The venv is created once and reused by all jobs.
VENV_DIR="${INDEXER_VENV:-/iopsstor/scratch/cscs/${CURRENT_USER}/indexer-venv}"
VENV_LOCK="${VENV_DIR}.lock"

if [[ ! -x "$VENV_DIR/bin/python3" ]]; then
    echo "[INFO] Creating Python venv at $VENV_DIR (one-time setup)..."
    (
        flock -x 200
        if [[ ! -x "$VENV_DIR/bin/python3" ]]; then
            python3 -m venv "$VENV_DIR"
            "$VENV_DIR/bin/pip" install --quiet \
                "elasticsearch==7.17.9" \
                "pyarrow" \
                "urllib3<2"
            echo "[INFO] Venv created with packages: elasticsearch, pyarrow, urllib3"
        fi
    ) 200>"$VENV_LOCK"
fi

PYTHON_BIN="$VENV_DIR/bin/python3"
if ! "$PYTHON_BIN" -c "import elasticsearch, pyarrow" 2>/dev/null; then
    echo "[ERROR] Python packages not importable in venv $VENV_DIR"
    exit 1
fi
echo "[INFO] Python: $PYTHON_BIN  ($(\"$PYTHON_BIN\" --version))"

# ---- Run Python indexer from host venv (no srun, no container conflict) ----
python_cmd=(
    "$PYTHON_BIN" "$INDEXER"
    --data-dir        "$DATA_DIR"
    --index-name      "$INDEX_NAME"
    --es-host         "$MASTER_NODE"
    --es-port         "$ES_PORT"
    --num-workers     "$NUM_WORKERS"
    --batch-size      "$BATCH_SIZE"
    --chunk-size      "$CHUNK_SIZE"
    --max-chunk-bytes "$MAX_CHUNK_BYTES"
    --thread-count    "$THREAD_COUNT"
    --queue-size      "$QUEUE_SIZE"
    --log-level       "$LOG_LEVEL"
)

[[ "$KEEP_EXISTING_INDEX" == "true" ]] && python_cmd+=(--keep-existing-index)
[[ -n "$FILE_RANGE_START" ]] && python_cmd+=(--file-range-start "$FILE_RANGE_START")
[[ -n "$FILE_RANGE_END"   ]] && python_cmd+=(--file-range-end   "$FILE_RANGE_END")

echo "[INFO] Running indexer on host ($(hostname)): ${python_cmd[*]}"
"${python_cmd[@]}" 2>&1
exit_code=$?

if (( exit_code == 0 )); then
    echo "[INFO] === Job completed successfully ==="
else
    echo "[ERROR] === Job failed with exit code $exit_code ==="
fi

exit $exit_code
