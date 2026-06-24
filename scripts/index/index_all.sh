#!/bin/bash
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=12:00:00
#SBATCH --nodes=76
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=256
#SBATCH --mem=800G

# ============================================================
# Index ALL datasets into the global Elasticsearch index.
#
# Speed design:
#   - Nodes 0..(N_ES_NODES-1):  Elasticsearch cluster
#   - Nodes N_ES_NODES..(N_NODES-1):  dedicated Python indexer nodes
#     (run via `srun --ntasks=1 --nodelist=<node>`)
#
# Large datasets split across multiple indexer nodes (file-range slicing):
#   FW1+FW-edu+DCLM merged  → N_FW_NODES parallel indexers
#   FW2 aggregated           → N_FW2_NODES parallel indexers
#   All raw datasets         → batch node (they are small)
#
# Each indexer node sends --thread-count concurrent bulk requests to ES,
# so total concurrent requests = sum(all indexers × their thread counts).
# Target: enough to keep all N_ES_NODES shards busy.
#
# Codec & refresh tricks:
#   - Create index with LZ4 codec (default), refresh_interval=-1
#   - After all indexers finish: switch codec to best_compression + refresh=1s
#   - Force merge (run separately) then rewrites all segments with best_compression
#   → Final index is identical to building with best_compression from the start,
#     but ~3-5x faster segment writes during bulk loading.
#
# Required env vars:
#   GLOBAL_MERGE_DIR  - FW1+FW-edu+DCLM Spark merged parquet
#   INDEX_NAME        - Target ES index name
#
# Optional env vars:
#   N_ES_NODES    - ES cluster size (default: 70)
#   N_FW_NODES    - Parallel indexers for FW1+FW-edu+DCLM (default: 4)
#   N_FW2_NODES   - Parallel indexers for FW2 (default: 2)
#   CAPSTOR_BASE  - Root for raw datasets
# ============================================================

set -euo pipefail

SCRATCH="/iopsstor/scratch/cscs/${USER}"
CAPSTOR_BASE="${CAPSTOR_BASE:-/capstor/store/cscs/swissai/infra01/datasets/swiss-ai}"

# ---- Inputs ----
GLOBAL_MERGE_DIR="${GLOBAL_MERGE_DIR:-${SCRATCH}/english-web-dedup}"
FW2_AGG_DIR="${FW2_AGG_DIR:-${SCRATCH}/fineweb-2_0_1-quality_33-filterrobots-aggregated}"
INDEX_NAME="${INDEX_NAME:-global-pretraining}"
ES_ENV="${ES_ENV:-es-python}"
ES_PORT=9200

# ---- Node allocation (fully automatic from SLURM_NNODES) ----
# Override any of these via env vars; otherwise derived from total node count.
#
# Layout:
#   [0 .. N_ES_NODES-1]                       → Elasticsearch cluster
#   [N_ES_NODES .. N_ES_NODES+N_FW_NODES-1]   → FW1+FW-edu+DCLM indexers (srun)
#   [N_ES_NODES+N_FW_NODES .. end]             → FW2 indexers (srun)
#   batch node (= node N_ES_NODES)             → also runs raw dataset indexers
#
# Heuristic: 6 dedicated indexer nodes, rest go to ES.
# More ES nodes = larger cluster = more shards = more write throughput.
# Thread counts are derived from N_ES_NODES so they scale automatically.

TOTAL_NODES="${SLURM_NNODES:-76}"
N_IDX_TOTAL="${N_IDX_TOTAL:-6}"           # total dedicated indexer nodes (4 FW + 2 FW2)
N_FW_NODES="${N_FW_NODES:-4}"             # indexer nodes for FW1+FW-edu+DCLM
N_FW2_NODES="${N_FW2_NODES:-$(( N_IDX_TOTAL - N_FW_NODES ))}"  # remainder for FW2
N_ES_NODES="${N_ES_NODES:-$(( TOTAL_NODES - N_IDX_TOTAL ))}"   # rest for ES

# ---- Dataset paths ----
EUROPARL_DIR="${CAPSTOR_BASE}/europarl_bidirectional_preprocessed/data"
PARADOCS_DIR="${CAPSTOR_BASE}/paradocs-bidirectional-piifiltered/data"
FLAN_DIR="${CAPSTOR_BASE}/provenance-flan-templated/data"
EUROBLOCKS_DIR="${CAPSTOR_BASE}/EuroBlocks-SFT-Synthetic-1124_preprocessed/data"
INSTBOOKS_DIR="${CAPSTOR_BASE}/institutional-books-1.0-filtered/data"
GUTENBERG_DIR="${CAPSTOR_BASE}/project_gutenberg_preprocessed"
CANARIES_DIR="${CAPSTOR_BASE}/apertus-pretrain-poisonandcanaries"

REPO_DIR="${REPO_DIR:-}"
if [[ -z "$REPO_DIR" ]]; then
    _SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ "$_SCRIPT_DIR" != /var/spool* ]]; then
        REPO_DIR="$(cd "${_SCRIPT_DIR}/../.." && pwd)"
    fi
fi
[[ -z "$REPO_DIR" ]] && { echo "[ERROR] REPO_DIR not set"; exit 1; }

INDEXER_AGG="${REPO_DIR}/scripts/index/index_aggregated.py"
INDEXER_RAW="${REPO_DIR}/scripts/index/index_raw.py"
ES_NODE_SCRIPT="${REPO_DIR}/scripts/index/start_es_node.sh"

LOG_DIR="${SCRATCH}/pipeline-logs"
mkdir -p "$LOG_DIR"

VENV_DIR="${SCRATCH}/indexer-venv"
PYTHON_BIN="${VENV_DIR}/bin/python3"

# ---- Validate ----
[[ ! -d "$GLOBAL_MERGE_DIR" ]] && { echo "[ERROR] GLOBAL_MERGE_DIR not found: $GLOBAL_MERGE_DIR"; exit 1; }
[[ ! -d "$FW2_AGG_DIR" ]]      && { echo "[ERROR] FW2_AGG_DIR not found: $FW2_AGG_DIR"; exit 1; }
[[ ! -x "$PYTHON_BIN" ]]       && { echo "[ERROR] Python venv not found at $VENV_DIR — run index_aggregated.sh once first to create it"; exit 1; }

N_IDX_NODES=$(( N_FW_NODES + N_FW2_NODES ))
NEEDED=$(( N_ES_NODES + N_IDX_NODES ))
if (( TOTAL_NODES < NEEDED )); then
    echo "[ERROR] Need $NEEDED nodes ($N_ES_NODES ES + $N_IDX_NODES indexer) but only $TOTAL_NODES allocated"
    exit 1
fi

# ---- Resolve node lists ----
mapfile -t ALL_NODES < <(scontrol show hostnames "$SLURM_NODELIST")
MASTER_NODE="${ALL_NODES[0]}"

ES_NODELIST=$(IFS=,; echo "${ALL_NODES[*]:0:$N_ES_NODES}")
FW_IDX_NODES=("${ALL_NODES[@]:$N_ES_NODES:$N_FW_NODES}")
FW2_IDX_NODES=("${ALL_NODES[@]:$(( N_ES_NODES + N_FW_NODES )):$N_FW2_NODES}")

echo "[INFO] =============================================="
echo "[INFO] Global index — all datasets in parallel"
echo "[INFO] Date:         $(date)"
echo "[INFO] Total nodes:  $TOTAL_NODES"
echo "[INFO] ES nodes:     $N_ES_NODES  (${ALL_NODES[0]}..${ALL_NODES[$(( N_ES_NODES - 1 ))]})"
echo "[INFO] FW indexers:  $N_FW_NODES  nodes (${FW_IDX_NODES[*]:-none})"
echo "[INFO] FW2 indexers: $N_FW2_NODES nodes (${FW2_IDX_NODES[*]:-none})"
echo "[INFO] INDEX_NAME:   $INDEX_NAME"
echo "[INFO] =============================================="

free -h
ulimit -n 65536

# ============================================================
# 1. Start Elasticsearch on the first N_ES_NODES nodes
# ============================================================
BASE_DATA="${SCRATCH}/es-data-${INDEX_NAME}"
BASE_LOGS="${SCRATCH}/es-logs-${INDEX_NAME}-${SLURM_JOB_ID}"
SEED_HOSTS=$(printf "%s:9300\n" "${ALL_NODES[@]:0:$N_ES_NODES}" | paste -sd,)
CLUSTER_NAME="fw-es-${INDEX_NAME}"

export ES_BASE_DATA="$BASE_DATA"
export ES_BASE_LOGS="$BASE_LOGS"
export ES_SEED_HOSTS="$SEED_HOSTS"
export ES_MASTER_NAME="es-node-0"
export ES_PORT="$ES_PORT"
export ES_HEAP="-Xms200g -Xmx200g"
export CLUSTER_NAME="$CLUSTER_NAME"
export ES_NUM_NODES="$N_ES_NODES"

echo "[INFO] Starting Elasticsearch on $N_ES_NODES nodes..."
srun --ntasks="$N_ES_NODES" --ntasks-per-node=1 \
     --nodelist="$ES_NODELIST" \
     --environment="$ES_ENV" \
     "$ES_NODE_SCRIPT" &
SRUN_ES_PID=$!

stop_es() {
    echo "[INFO] Stopping Elasticsearch..."
    kill "$SRUN_ES_PID" 2>/dev/null || true
    wait "$SRUN_ES_PID" 2>/dev/null || true
}
trap stop_es EXIT

# Wait for full cluster
ES_URL="http://${MASTER_NODE}:${ES_PORT}"
max_wait=90
for (( i=0; i<max_wait; i++ )); do
    ! kill -0 "$SRUN_ES_PID" 2>/dev/null && { echo "[ERROR] ES died during startup"; exit 1; }
    health=$(curl -s "${ES_URL}/_cluster/health" 2>/dev/null || true)
    if [[ -n "$health" ]]; then
        n=$(echo "$health" | grep -o '"number_of_nodes":[0-9]*' | grep -o '[0-9]*' || echo 0)
        if (( n == N_ES_NODES )); then echo "[INFO] ES cluster ready ($n nodes)"; break; fi
        echo "[INFO] ES forming... ($n/$N_ES_NODES nodes)"
    fi
    (( i == max_wait - 1 )) && { echo "[ERROR] ES cluster did not form in time"; exit 1; }
    sleep 10
done

# ============================================================
# 2. Pre-create index
#    - LZ4 codec (default) for fast bulk writes
#    - refresh_interval=-1 (no background refresh during loading)
#    - N_ES_NODES shards (one per ES node = max write parallelism)
#    - 0 replicas (no replication overhead during loading)
# ============================================================
echo "[INFO] Creating index '$INDEX_NAME' ($N_ES_NODES shards, LZ4, refresh=disabled)..."
"$PYTHON_BIN" - <<PYEOF
from elasticsearch import Elasticsearch
es = Elasticsearch([{"host": "${MASTER_NODE}", "port": ${ES_PORT}}], timeout=60)
if es.indices.exists(index="${INDEX_NAME}"):
    print("[WARN] Index already exists — skipping creation (appending)")
else:
    mapping = {
        "settings": {
            "number_of_shards":   ${N_ES_NODES},
            "number_of_replicas": 0,
            "refresh_interval":   "-1",          # no background refresh during load.
            # Data is NOT lost: every write lands in the translog immediately.
            # Segments flush to disk when translog hits flush_threshold_size (2gb).
            # refresh_interval only controls when data becomes searchable, not
            # whether it is persisted. Manual refresh() called after all loading.
            # No codec override → default LZ4; 3-5x faster segment writes than best_compression.
            # After loading we switch to best_compression, then force-merge.
            # Force-merge READS existing LZ4 segments (fast) and WRITES one new segment
            # using the then-current codec (best_compression). So LZ4 during load is
            # strictly better: faster loading AND faster force-merge reads.
            # Final on-disk format is identical to loading with best_compression directly.
            "index.translog.durability":          "async",
            "index.translog.flush_threshold_size":"2gb",
            "analysis": {"analyzer": {"web_content_analyzer": {
                "type":        "custom",
                "char_filter": ["html_strip"],
                "tokenizer":   "standard",
                "filter":      ["lowercase", "asciifolding"],
            }}},
        },
        "mappings": {
            "dynamic": "false",
            "properties": {
                "text":         {"type": "text", "analyzer": "web_content_analyzer",
                                 "index_options": "positions", "norms": True},
                "language":     {"type": "keyword"},
                "date":         {"type": "date",
                                 "format": "strict_date_optional_time||epoch_millis"},
                "source_count": {"type": "integer"},
                "datasets":     {"type": "keyword"},
                "sources":      {"type": "object", "enabled": False},
            },
        },
    }
    es.indices.create(index="${INDEX_NAME}", body=mapping)
    print("[INFO] Index created")
PYEOF

# ============================================================
# 3. Count files in large datasets so we can split evenly
# ============================================================
mapfile -t FW_FILES  < <(find "$GLOBAL_MERGE_DIR" -name "*.parquet" | sort)
mapfile -t FW2_FILES < <(find "$FW2_AGG_DIR"      -name "*.parquet" | sort)
N_FW_FILES="${#FW_FILES[@]}"
N_FW2_FILES="${#FW2_FILES[@]}"
echo "[INFO] FW1+FW-edu+DCLM: $N_FW_FILES files across $N_FW_NODES indexer node(s)"
echo "[INFO] FW2:              $N_FW2_FILES files across $N_FW2_NODES indexer node(s)"

# ============================================================
# 4. Launch indexers — all in parallel
# ============================================================
PIDS=()
NAMES=()

run_bg() {
    local name="$1"; shift
    local logfile="${LOG_DIR}/index-${name}-${SLURM_JOB_ID}.out"
    echo "[INFO] Starting: $name  (log: $logfile)"
    "$@" >"$logfile" 2>&1 &
    PIDS+=($!)
    NAMES+=("$name")
}

# Common ES connection args
ES_ARGS=(--es-host "$MASTER_NODE" --es-port "$ES_PORT")
AGG_APPEND=(--keep-existing-index --op-type create)

# -- 4a. FW1+FW-edu+DCLM merged: split across N_FW_NODES indexer nodes via srun --
# Thread count per node: ceil(N_ES_NODES / N_FW_NODES) → each node saturates its share of ES
FW_THREADS=$(( (N_ES_NODES + N_FW_NODES - 1) / N_FW_NODES ))
FW_FILES_PER_NODE=$(( (N_FW_FILES + N_FW_NODES - 1) / N_FW_NODES ))

for (( i=0; i<N_FW_NODES; i++ )); do
    node="${FW_IDX_NODES[$i]}"
    start=$(( i * FW_FILES_PER_NODE ))
    end=$(( start + FW_FILES_PER_NODE - 1 ))
    (( end >= N_FW_FILES )) && end=$(( N_FW_FILES - 1 ))
    [[ $start -gt $end ]] && continue

    run_bg "fw-merged-${i}" \
        srun --ntasks=1 --nodes=1 --nodelist="$node" --overlap \
            "$PYTHON_BIN" "$INDEXER_AGG" \
                --data-dir "$GLOBAL_MERGE_DIR" \
                --index-name "$INDEX_NAME" \
                "${ES_ARGS[@]}" "${AGG_APPEND[@]}" \
                --file-range-start "$start" --file-range-end "$end" \
                --num-workers   64 \
                --chunk-size    5000 \
                --batch-size    25000 \
                --max-chunk-bytes 100 \
                --thread-count  "$FW_THREADS" \
                --queue-size    $(( FW_THREADS * 2 ))
done

# -- 4b. FW2 aggregated: split across N_FW2_NODES indexer nodes --
FW2_THREADS=$(( (N_ES_NODES + N_FW2_NODES - 1) / N_FW2_NODES ))
FW2_FILES_PER_NODE=$(( (N_FW2_FILES + N_FW2_NODES - 1) / N_FW2_NODES ))

for (( i=0; i<N_FW2_NODES; i++ )); do
    node="${FW2_IDX_NODES[$i]}"
    start=$(( i * FW2_FILES_PER_NODE ))
    end=$(( start + FW2_FILES_PER_NODE - 1 ))
    (( end >= N_FW2_FILES )) && end=$(( N_FW2_FILES - 1 ))
    [[ $start -gt $end ]] && continue

    run_bg "fw2-${i}" \
        srun --ntasks=1 --nodes=1 --nodelist="$node" --overlap \
            "$PYTHON_BIN" "$INDEXER_AGG" \
                --data-dir "$FW2_AGG_DIR" \
                --index-name "$INDEX_NAME" \
                "${ES_ARGS[@]}" "${AGG_APPEND[@]}" \
                --file-range-start "$start" --file-range-end "$end" \
                --num-workers   32 \
                --chunk-size    5000 \
                --batch-size    25000 \
                --max-chunk-bytes 100 \
                --thread-count  "$FW2_THREADS" \
                --queue-size    $(( FW2_THREADS * 2 ))
done

# -- 4c. Raw datasets: run from batch node (collectively small, no srun overhead) --
RAW_ES_ARGS=(--index-name "$INDEX_NAME" "${ES_ARGS[@]}" --batch-size 25000 --max-chunk-bytes 100)

run_bg "europarl" \
    "$PYTHON_BIN" "$INDEXER_RAW" --dataset europarl --data-dir "$EUROPARL_DIR" \
        "${RAW_ES_ARGS[@]}" --num-workers 16 --thread-count 8 --queue-size 16

run_bg "paradocs" \
    "$PYTHON_BIN" "$INDEXER_RAW" --dataset paradocs --data-dir "$PARADOCS_DIR" \
        "${RAW_ES_ARGS[@]}" --num-workers 16 --thread-count 8 --queue-size 16

run_bg "flan" \
    "$PYTHON_BIN" "$INDEXER_RAW" --dataset flan --data-dir "$FLAN_DIR" \
        "${RAW_ES_ARGS[@]}" --num-workers 8 --thread-count 8 --queue-size 16

run_bg "euroblocks" \
    "$PYTHON_BIN" "$INDEXER_RAW" --dataset euroblocks --data-dir "$EUROBLOCKS_DIR" \
        "${RAW_ES_ARGS[@]}" --num-workers 8 --thread-count 8 --queue-size 16

run_bg "institutional-books" \
    "$PYTHON_BIN" "$INDEXER_RAW" --dataset institutional-books --data-dir "$INSTBOOKS_DIR" \
        "${RAW_ES_ARGS[@]}" --num-workers 8 --thread-count 8 --queue-size 16

run_bg "gutenberg" \
    "$PYTHON_BIN" "$INDEXER_RAW" --dataset gutenberg --data-dir "$GUTENBERG_DIR" \
        "${RAW_ES_ARGS[@]}" --num-workers 4 --thread-count 4 --queue-size 8

run_bg "canaries" \
    "$PYTHON_BIN" "$INDEXER_RAW" --dataset canaries --data-dir "$CANARIES_DIR" \
        "${RAW_ES_ARGS[@]}" --num-workers 2 --thread-count 2 --queue-size 4

# ============================================================
# 5. Wait for all indexers
# ============================================================
echo "[INFO] ${#PIDS[@]} indexers running. Waiting..."
any_failed=0
for idx in "${!PIDS[@]}"; do
    pid="${PIDS[$idx]}"
    name="${NAMES[$idx]}"
    if wait "$pid"; then
        echo "[INFO] ✓ $name"
    else
        echo "[ERROR] ✗ $name FAILED"
        any_failed=1
    fi
done

# ============================================================
# 6. Post-load: re-enable refresh, switch to best_compression
#    Force merge is run separately (takes hours for billion-doc indices).
# ============================================================
echo ""
echo "[INFO] Updating index settings post-load..."
"$PYTHON_BIN" - <<PYEOF
from elasticsearch import Elasticsearch
es = Elasticsearch([{"host": "${MASTER_NODE}", "port": ${ES_PORT}}], timeout=120)
es.indices.refresh(index="${INDEX_NAME}")
es.indices.put_settings(index="${INDEX_NAME}", body={
    "index": {
        "refresh_interval": "1s",
        "codec":            "best_compression",   # new segments (from force-merge) use this
    }
})
stats = es.indices.stats(index="${INDEX_NAME}")["indices"]["${INDEX_NAME}"]["total"]
print(f"[INFO] refresh re-enabled, codec=best_compression")
print(f"[INFO] docs.count  = {stats['docs']['count']:,}")
print(f"[INFO] store.size  = {stats['store']['size_in_bytes'] / 1024**3:.1f} GB")
PYEOF

echo ""
echo "[INFO] =============================================="
curl -s "${ES_URL}/_cat/indices/${INDEX_NAME}?v&h=index,docs.count,store.size,segments.count" || true
echo ""
echo "[INFO] =============================================="
echo "[INFO] Next step — force merge (run in a separate job or interactively):"
echo "       curl -X POST '${ES_URL}/${INDEX_NAME}/_forcemerge?max_num_segments=1&wait_for_completion=false'"
echo "[INFO] =============================================="

if (( any_failed )); then
    echo "[ERROR] Some indexers failed — check $LOG_DIR"
    exit 1
fi
echo "[INFO] All datasets indexed successfully."
