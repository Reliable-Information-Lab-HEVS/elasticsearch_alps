#!/bin/bash
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=10
#SBATCH --mem=256G
#SBATCH --environment=es-python

# ============================================================
# Phase 2: Index aggregated parquet into Elasticsearch.
#
# Required env vars:
#   DATA_DIR    - directory of aggregated parquet (Phase 1 output)
#   INDEX_NAME  - target ES index name
#
# Optional env vars:
#   KEEP_EXISTING_INDEX  - "true" to append rather than recreate
#   NUM_WORKERS          - parser processes (default: 8)
#   BATCH_SIZE           - ES bulk batch size (default: 12500)
#   CHUNK_SIZE           - parquet read chunk (default: 5000)
#   MAX_CHUNK_BYTES      - bulk request max MB (default: 150)
#   THREAD_COUNT         - parallel_bulk threads (default: 4)
#   QUEUE_SIZE           - parallel_bulk queue (default: 8)
#   LOG_LEVEL            - Python log level (default: INFO)
# ============================================================

set -euo pipefail

DATA_DIR="${DATA_DIR:-}"
INDEX_NAME="${INDEX_NAME:-}"
KEEP_EXISTING_INDEX="${KEEP_EXISTING_INDEX:-false}"

NUM_WORKERS="${NUM_WORKERS:-8}"
BATCH_SIZE="${BATCH_SIZE:-12500}"
CHUNK_SIZE="${CHUNK_SIZE:-5000}"
MAX_CHUNK_BYTES="${MAX_CHUNK_BYTES:-150}"
THREAD_COUNT="${THREAD_COUNT:-4}"
QUEUE_SIZE="${QUEUE_SIZE:-8}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

ES_HOST="localhost"
ES_PORT=9200

CURRENT_USER="${SLURM_JOB_USER:-$USER}"

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

# ---- Validate ----
if [[ -z "$DATA_DIR" || -z "$INDEX_NAME" ]]; then
    echo "[ERROR] DATA_DIR and INDEX_NAME must be set"
    exit 1
fi

if [[ ! -d "$DATA_DIR" ]]; then
    echo "[ERROR] DATA_DIR does not exist: $DATA_DIR"
    exit 1
fi

if [[ ! -f "$INDEXER" ]]; then
    echo "[ERROR] Indexer not found: $INDEXER"
    exit 1
fi

# ---- Info ----
echo "[INFO] =============================================="
echo "[INFO] Phase 2: Index aggregated data"
echo "[INFO] Date:         $(date)"
echo "[INFO] Node:         ${SLURM_NODELIST:-local}"
echo "[INFO] CPUs:         ${SLURM_CPUS_PER_TASK:-unknown}"
echo "[INFO] Memory:       ${SLURM_MEM_PER_NODE:-unknown} MB"
echo "[INFO] DATA_DIR:     $DATA_DIR"
echo "[INFO] INDEX_NAME:   $INDEX_NAME"
echo "[INFO] Keep index:   $KEEP_EXISTING_INDEX"
echo "[INFO] Repo:         $REPO_DIR"
echo "[INFO] =============================================="

free -h
ulimit -n 65536

# ---- Start Elasticsearch ----
start_elasticsearch() {
    echo "[INFO] Starting Elasticsearch..."

    unset JAVA_HOME
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"

    if ! "$ES_JAVA_HOME/bin/java" -version 2>/dev/null; then
        echo "[ERROR] Java not found at $ES_JAVA_HOME"
        return 1
    fi

    local heap="-Xms30g -Xmx30g -XX:MaxGCPauseMillis=200"
    local DATA_PATH="/iopsstor/scratch/cscs/${CURRENT_USER}/es-data-${INDEX_NAME}-${SLURM_JOB_ID:-local}"
    local LOGS_PATH="/iopsstor/scratch/cscs/${CURRENT_USER}/es-logs-${INDEX_NAME}-${SLURM_JOB_ID:-local}"

    mkdir -p "$DATA_PATH" "$LOGS_PATH"
    echo "[INFO] ES data: $DATA_PATH"
    echo "[INFO] ES logs: $LOGS_PATH"

    ES_JAVA_OPTS="$heap" \
    /usr/share/elasticsearch/bin/elasticsearch \
        -E path.data="$DATA_PATH" \
        -E path.logs="$LOGS_PATH" \
        -E discovery.type=single-node \
        -E network.host=127.0.0.1 \
        -E http.host=127.0.0.1 \
        -E http.port="${ES_PORT}" \
        -E transport.host=127.0.0.1 \
        -E xpack.security.enabled=false \
        -E node.store.allow_mmap=false \
        -E cluster.routing.allocation.disk.watermark.low=85% \
        -E cluster.routing.allocation.disk.watermark.high=90% \
        -E cluster.routing.allocation.disk.watermark.flood_stage=95% \
        -E bootstrap.memory_lock=false \
        -E http.max_content_length=200mb \
        -E thread_pool.write.queue_size=2000 \
        -E thread_pool.write.size=1 &

    ES_PID=$!
    echo "[INFO] Elasticsearch PID: $ES_PID"

    local max_wait=30 i=0
    while (( i < max_wait )); do
        if ! kill -0 "$ES_PID" 2>/dev/null; then
            echo "[ERROR] Elasticsearch died during startup"
            return 1
        fi
        if curl -s "http://127.0.0.1:${ES_PORT}/_cluster/health" >/dev/null 2>&1; then
            echo "[INFO] Elasticsearch ready"
            return 0
        fi
        (( i++ ))
        echo "[INFO] Waiting for ES... ($i/$max_wait)"
        sleep 10
    done

    echo "[ERROR] Elasticsearch failed to start"
    return 1
}

stop_elasticsearch() {
    if [[ -n "${ES_PID:-}" ]] && kill -0 "$ES_PID" 2>/dev/null; then
        echo "[INFO] Stopping Elasticsearch (PID $ES_PID)..."
        kill "$ES_PID"
        wait "$ES_PID" 2>/dev/null || true
    fi
}

trap stop_elasticsearch EXIT

start_elasticsearch || exit 1

# ---- Build and run Python command ----
python_cmd=(
    python3 "$INDEXER"
    --data-dir     "$DATA_DIR"
    --index-name   "$INDEX_NAME"
    --es-host      "$ES_HOST"
    --es-port      "$ES_PORT"
    --num-workers  "$NUM_WORKERS"
    --batch-size   "$BATCH_SIZE"
    --chunk-size   "$CHUNK_SIZE"
    --max-chunk-bytes "$MAX_CHUNK_BYTES"
    --thread-count "$THREAD_COUNT"
    --queue-size   "$QUEUE_SIZE"
    --log-level    "$LOG_LEVEL"
)

[[ "$KEEP_EXISTING_INDEX" == "true" ]] && python_cmd+=(--keep-existing-index)

echo "[INFO] Running: ${python_cmd[*]}"
"${python_cmd[@]}" 2>&1
exit_code=$?

if (( exit_code == 0 )); then
    echo "[INFO] === Job completed successfully ==="
else
    echo "[ERROR] === Job failed with exit code $exit_code ==="
fi

exit $exit_code
