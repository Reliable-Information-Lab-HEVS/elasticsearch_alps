#!/bin/bash
#SBATCH --job-name=es-search-pipeline
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=256G

#SBATCH --export=ALL,SCRATCH=/iopsstor/scratch/cscs/$USER

#SBATCH --output=/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out
#SBATCH --error=/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err
#SBATCH --environment=es-python

set -e

# 929070 929113
# 929402: /iopsstor/scratch/cscs/inesaltemir/es-data-target-september_brouillon1-923479 + /capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2_single_index-920282_100samples.csv

# SEARCHES

#                                       CHEMICALS

# CHEMICALS EN (fw-edu-score-2 35 indexes): 935106 - 935140: 935106 failed so relaunched 935160

# CHEMICALS DEU: (fw2-33 with euro mid and other high 38 total indexes): 935233 - 935270

# CHEMICALS FR: (fw2-33 with euro mid and other high 38 total indexes): 935428 - 935465

# CHEMICALS ITA:(fw2-33 with euro mid and other high 38 total indexes): 935576-935613

#                                   WeaponizedWords

# WW_EN (fw-edu-score-2 35 indexes): 935275 - 935309


#                                       OBSCENE

# obscene_en  (fw-edu-score-2 35 indexes):  935351 - 935385 failed 923384 relaunched in 935421


# VERBATIM

# 935964 (failed container for merged index) -> relaunched 935973 ;  935965 15.000 samples was too big
# 650 samples: 936292 936293


# =============================================================================
# QUERY CONFIGURATION PARAMETERS
# =============================================================================
EXECUTE_MATCH_QUERY="${EXECUTE_MATCH_QUERY:-false}"
EXECUTE_MATCH_PHRASE_QUERY="${EXECUTE_MATCH_PHRASE_QUERY:-true}"
EXECUTE_TERM_QUERY_EXACT="${EXECUTE_TERM_QUERY_EXACT:-false}"
EXECUTE_WILDCARD_QUERY="${EXECUTE_WILDCARD_QUERY:-false}"
EXECUTE_FUZZY_QUERY="${EXECUTE_FUZZY_QUERY:-false}"
EXECUTE_BOOL_MUST_QUERY="${EXECUTE_BOOL_MUST_QUERY:-false}"

MATCH_QUERY_OPERATOR="${MATCH_QUERY_OPERATOR:-[\"or\"]}"
MATCH_PHRASE_SLOP="${MATCH_PHRASE_SLOP:-[0]}"

BOOL_MUST_OPERATOR="${BOOL_MUST_OPERATOR:-or}"
BOOL_MUST_MAX_WORDS="${BOOL_MUST_MAX_WORDS:-3}"
BOOL_MUST_MINIMUM_SHOULD_MATCH="${BOOL_MUST_MINIMUM_SHOULD_MATCH:-50%}"

# =============================================================================
# ELASTICSEARCH CONFIGURATION
# =============================================================================
ES_HOST="${ES_HOST:-127.0.0.1}"
ES_PORT="${ES_PORT:-9200}"

DATASET="${DATASET:-pure_text}"

PATH_DATA="${PATH_DATA:-/iopsstor/scratch/cscs/inesaltemir/es-data-target-september_brouillon1-923479}"
# /iopsstor/scratch/cscs/inesaltemir/es-data-target-september_brouillon1-923479
# /iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_001-920282

CSV_FILE="${CSV_FILE:-/capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2_single_index-920282.csv}"

INDEX_NAME="${INDEX_NAME:-september_brouillon1}" 

# swissai-fineweb-edu-score-2-filterrobots-merge_part_001

CSV_BASENAME=$(basename "$CSV_FILE" .txt)
OUTPUT_DIR="${OUTPUT_DIR:-/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_184323/september_brouillon1_fw-edu-score-2_single_index-920282_}"
#/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/${INDEX_NAME}_${CSV_BASENAME}_${SLURM_JOB_ID}

ES_URL="http://${ES_HOST}:${ES_PORT}"

# Calculate heap based on available memory - UPDATED FOR LARGE INDEX
#if [ "${SLURM_MEM_PER_NODE:-0}" -ge 131072 ]; then
    # 128GB+ available, use 64GB heap (50% of total RAM)
#    JAVA_HEAP="-Xms64g -Xmx64g"
#elif [ "${SLURM_MEM_PER_NODE:-0}" -ge 65536 ]; then
    # 64GB+ available, use 50GB heap
#    JAVA_HEAP="-Xms50g -Xmx50g"
#elif [ "${SLURM_MEM_PER_NODE:-0}" -ge 32768 ]; then
    # 32GB+ available, use 30GB heap
#    JAVA_HEAP="-Xms30g -Xmx30g"
#else
#    JAVA_HEAP="-Xms16g -Xmx16g"
#fi

# Calculate heap based on available memory, but be conservative FIXXXXX
if [ "${SLURM_MEM_PER_NODE:-0}" -ge 32768 ]; then
    # 32GB+ available, use 30GB heap (conservative, leaves room for OS + caches)
    JAVA_HEAP="-Xms30g -Xmx30g"
else
    # Less than 32GB, use 8GB heap
    JAVA_HEAP="-Xms8g -Xmx8g"
fi

# Improved GC settings for large heap
JAVA_OPTS="$JAVA_HEAP -XX:MaxGCPauseMillis=500 -XX:G1HeapRegionSize=32m"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Parse command line arguments
if [ $# -ge 1 ] && [ -n "$1" ]; then
    PATH_DATA="$1"
fi

if [ $# -ge 2 ] && [ -n "$2" ]; then
    CSV_FILE="$2"
fi

if [ $# -ge 3 ] && [ -n "$3" ]; then
    INDEX_NAME="$3"
fi

# Trim whitespace from critical variables
PATH_DATA=$(echo "$PATH_DATA" | xargs)
CSV_FILE=$(echo "$CSV_FILE" | xargs)
INDEX_NAME=$(echo "$INDEX_NAME" | xargs)

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Function to build configuration JSON
build_config_json() {
    local temp_json=$(mktemp)
    
    cat > "$temp_json" << EOF
{
    "execute_match_query": $EXECUTE_MATCH_QUERY,
    "execute_match_phrase_query": $EXECUTE_MATCH_PHRASE_QUERY,
    "execute_term_query_exact": $EXECUTE_TERM_QUERY_EXACT,
    "execute_wildcard_query": $EXECUTE_WILDCARD_QUERY,
    "execute_fuzzy_query": $EXECUTE_FUZZY_QUERY,
    "execute_bool_must_query": $EXECUTE_BOOL_MUST_QUERY,
    "match_query_operator": $MATCH_QUERY_OPERATOR,
    "match_phrase_slop": $MATCH_PHRASE_SLOP,
    "bool_must_operator": "$BOOL_MUST_OPERATOR",
    "bool_must_max_words": $BOOL_MUST_MAX_WORDS
EOF

    if [ -n "$BOOL_MUST_MINIMUM_SHOULD_MATCH" ]; then
        if [[ "$BOOL_MUST_MINIMUM_SHOULD_MATCH" =~ ^[0-9]+%?$ ]]; then
            if [[ "$BOOL_MUST_MINIMUM_SHOULD_MATCH" =~ % ]]; then
                echo "    ,\"bool_must_minimum_should_match\": \"$BOOL_MUST_MINIMUM_SHOULD_MATCH\"" >> "$temp_json"
            else
                echo "    ,\"bool_must_minimum_should_match\": $BOOL_MUST_MINIMUM_SHOULD_MATCH" >> "$temp_json"
            fi
        else
            echo "    ,\"bool_must_minimum_should_match\": \"$BOOL_MUST_MINIMUM_SHOULD_MATCH\"" >> "$temp_json"
        fi
    fi
    
    echo "}" >> "$temp_json"
    
    if command -v python3 >/dev/null 2>&1; then
        local json_content=$(python3 -c "import json, sys; print(json.dumps(json.load(open('$temp_json'))))" 2>/dev/null)
        if [ $? -eq 0 ]; then
            echo "$json_content"
        else
            cat "$temp_json"
        fi
    else
        cat "$temp_json"
    fi
    
    rm -f "$temp_json"
}

configure_proxy_bypass() {
    log_info "Configuring proxy bypass for localhost connections..."
    
    ORIGINAL_HTTP_PROXY="${http_proxy:-}"
    ORIGINAL_NO_PROXY="${no_proxy:-}"
    
    export no_proxy="${no_proxy},127.0.0.1,localhost,0.0.0.0,::1"
    
    log_info "Original no_proxy: $ORIGINAL_NO_PROXY"
    log_info "Updated no_proxy: $no_proxy"
    log_info "HTTP proxy: ${http_proxy:-'(not set)'}"
}

# Validate required parameters
if [ -z "$PATH_DATA" ]; then
    log_error "PATH_DATA is required"
    exit 1
fi

if [ -z "$CSV_FILE" ]; then
    log_error "CSV_FILE is required"
    exit 1
fi

if [ ! -f "$CSV_FILE" ]; then
    log_error "CSV file '$CSV_FILE' not found"
    exit 1
fi

if [ ! -d "$PATH_DATA" ]; then
    log_error "Data path '$PATH_DATA' not found"
    exit 1
fi

log_info "Starting Elasticsearch Search Pipeline with Configurable Queries...(PUTO_SEARCH.SH)"
log_info "========================================================================="
log_info "Configuration:"
log_info "  Data path: $PATH_DATA"
log_info "  CSV file: $CSV_FILE"
log_info "  Index name: $INDEX_NAME"
log_info "  Elasticsearch: $ES_URL"
log_info "  Java heap: $JAVA_OPTS"
log_info "  Output directory: $OUTPUT_DIR"
log_info "  Dataset chosen: $DATASET"
log_info ""
log_info "Query Configuration:"
log_info "  Match Query: $EXECUTE_MATCH_QUERY"
log_info "  Match Query Operator: $MATCH_QUERY_OPERATOR"
log_info "  Match Phrase Query: $EXECUTE_MATCH_PHRASE_QUERY"
log_info "  Match Phrase Slop: $MATCH_PHRASE_SLOP"
log_info "  Term Query Exact: $EXECUTE_TERM_QUERY_EXACT"
log_info "  Wildcard Query: $EXECUTE_WILDCARD_QUERY"
log_info "  Fuzzy Query: $EXECUTE_FUZZY_QUERY"
log_info "  Bool Must Query: $EXECUTE_BOOL_MUST_QUERY"
log_info "  Bool Must Operator: $BOOL_MUST_OPERATOR"
log_info "  Bool Must Max Words: $BOOL_MUST_MAX_WORDS"
log_info "  Bool Must Min Should Match: ${BOOL_MUST_MINIMUM_SHOULD_MATCH:-'(not set)'}"
log_info "========================================================================="

# Check disk space before starting
log_info "=== Disk Space Check ==="
df -h "$PATH_DATA" | tail -1
log_info "Index size on disk:"
du -sh "$PATH_DATA" 2>/dev/null || echo "Cannot calculate"
log_info ""

# Elasticsearch optimizations for large index
start_elasticsearch() {
    log_info "Starting Elasticsearch with large index optimizations..."
    
    unset JAVA_HOME
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"

    log_info "Testing Java installation..."
    if $ES_JAVA_HOME/bin/java -version; then
        log_success "Java test successful"
    else
        log_error "Java test failed"
        return 1
    fi
    
    log_info "Using heap settings: $JAVA_OPTS"

    local job_logs_dir="/iopsstor/scratch/cscs/inesaltemir/es-search-logs-${SLURM_JOB_ID:-$$}"
    mkdir -p "$job_logs_dir"

    local job_data_dir="$PATH_DATA"

    log_info "=== Starting Elasticsearch ==="

    ES_JAVA_OPTS="$JAVA_OPTS" \
    /usr/share/elasticsearch/bin/elasticsearch \
        -E path.data="$job_data_dir" \
        -E path.logs="$job_logs_dir" \
        -E discovery.type=single-node \
        -E network.host=127.0.0.1 \
        -E http.host=127.0.0.1 \
        -E http.port=9200 \
        -E transport.host=127.0.0.1 \
        -E network.bind_host=127.0.0.1 \
        -E network.publish_host=127.0.0.1 \
        -E node.store.allow_mmap=false \
        -E xpack.security.enabled=false \
        -E cluster.routing.allocation.disk.watermark.low=98% \
        -E cluster.routing.allocation.disk.watermark.high=99% \
        -E cluster.routing.allocation.disk.watermark.flood_stage=99.5% \
        -E cluster.routing.allocation.disk.threshold_enabled=true \
        -E cluster.routing.allocation.node_concurrent_recoveries=8 \
        -E cluster.routing.allocation.node_initial_primaries_recoveries=16 \
        -E indices.recovery.max_bytes_per_sec=500mb \
        -E cluster.routing.allocation.enable=all \
        -E bootstrap.memory_lock=false \
        -E logger.root=INFO \
        -E http.max_content_length=200mb \
        > "$job_logs_dir/elasticsearch.out" 2>&1 &
    
    ES_PID=$!
    log_info "Elasticsearch started with PID: $ES_PID (optimized for large 600GB+ index)"
    
    log_info "Waiting for large index to load (this may take 30-60 minutes)..."
    max_retries=600
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        if ! kill -0 $ES_PID 2>/dev/null; then
            log_error "Elasticsearch process died! PID $ES_PID is no longer running"
            return 1
        fi
        
        if curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health" > /dev/null 2>&1; then
            log_success "Elasticsearch is ready!"
            
            #log_info "=== Cluster Health ==="
            #curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health?pretty" 2>/dev/null
            
            log_info "=== Node Stats (Memory) ==="
            curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_nodes/stats/jvm,indices?pretty" 2>/dev/null | head -50
            
            
            log_info "=== Cluster Health ==="
            HEALTH_RESPONSE=$(curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health?pretty" 2>/dev/null)
            echo "$HEALTH_RESPONSE"

            # Validate cluster health
            log_info "=== Validating Cluster Health ==="
            STATUS=$(echo "$HEALTH_RESPONSE" | grep -oP '(?<="status" : ")[^"]*')
            INITIALIZING=$(echo "$HEALTH_RESPONSE" | grep -oP '(?<="initializing_shards" : )[0-9]+')
            UNASSIGNED=$(echo "$HEALTH_RESPONSE" | grep -oP '(?<="unassigned_shards" : )[0-9]+')
            RELOCATING=$(echo "$HEALTH_RESPONSE" | grep -oP '(?<="relocating_shards" : )[0-9]+')

            log_info "Status: $STATUS, Initializing: $INITIALIZING, Unassigned: $UNASSIGNED, Relocating: $RELOCATING"

            if [ "$STATUS" != "green" ] || [ "$INITIALIZING" != "0" ] || [ "$UNASSIGNED" != "0" ] || [ "$RELOCATING" != "0" ]; then
                log_error "Cluster health check FAILED:"
                [ "$STATUS" != "green" ] && log_error "  - Status is '$STATUS' (expected: green)"
                [ "$INITIALIZING" != "0" ] && log_error "  - Initializing shards: $INITIALIZING (expected: 0)"
                [ "$UNASSIGNED" != "0" ] && log_error "  - Unassigned shards: $UNASSIGNED (expected: 0)"
                [ "$RELOCATING" != "0" ] && log_error "  - Relocating shards: $RELOCATING (expected: 0)"
                return 1
            fi

            log_success "Cluster health validation PASSED"

            # Configure index recovery settings
            sleep 5
            log_info "=== Configuring Index Recovery Settings ==="
            curl --noproxy "127.0.0.1" -s -X PUT "http://127.0.0.1:9200/$INDEX_NAME/_settings" \
                -H 'Content-Type: application/json' -d'
            {
              "index": {
                "refresh_interval": "-1",
                "number_of_replicas": 0,
                "translog.durability": "async",
                "translog.sync_interval": "30s"
              }
            }' 2>/dev/null
            log_info "Recovery settings applied"
            
            # Monitor shard allocation
            log_info "=== Shard Recovery Status ==="
            curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cat/recovery?v&h=index,shard,stage,type,bytes_percent" 2>/dev/null | grep "$INDEX_NAME" | head -20
            
            log_info "=== Unassigned Shard Reasons ==="
            curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cat/shards/$INDEX_NAME?v&h=shard,prirep,state,unassigned.reason" 2>/dev/null | grep UNASSIGNED | head -20
            
            return 0
        else
            retry_count=$((retry_count + 1))
            if [ $((retry_count % 10)) -eq 0 ]; then
                log_info "Still waiting for large index to load... attempt $retry_count/$max_retries"
                log_info "This is normal for such large indices - please be patient"
            fi
            sleep 10
        fi
    done
    
    log_error "Elasticsearch failed to start after $max_retries attempts"
    return 1
}

check_index_exists() {
    local index_name="$1"
    local http_code
    
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "$ES_URL/$index_name" 2>/dev/null)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -ne 0 ]; then
        log_warn "Failed to check index existence (curl error)"
        return 1
    fi
    
    case "$http_code" in
        200)
            return 0
            ;;
        404)
            return 1
            ;;
        *)
            log_warn "Unexpected HTTP code when checking index: $http_code"
            return 1
            ;;
    esac
}

list_indices() {
    log_info "Available indices:"
    
    local response
    response=$(curl -s "$ES_URL/_cat/indices?v" 2>&1)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -eq 0 ] && [ -n "$response" ]; then
        if echo "$response" | grep -qi "<!DOCTYPE\|<html"; then
            log_warn "Received HTML response instead of indices list"
            log_warn "This usually means Elasticsearch returned an error page"
            log_info "Trying JSON format..."
            
            local json_response
            json_response=$(curl -s "$ES_URL/_cat/indices?format=json" 2>&1)
            if [ $? -eq 0 ] && echo "$json_response" | grep -q "^\["; then
                echo "$json_response"
            else
                log_error "Both formats failed. Raw response:"
                echo "$response" | head -10
                return 1
            fi
        else
            echo "$response"
        fi
    else
        log_error "Failed to fetch indices (exit code: $curl_exit_code)"
        if [ -n "$response" ]; then
            log_error "Response: $response"
        fi
        return 1
    fi
}

cleanup() {
    if [ ! -z "$ES_PID" ] && kill -0 $ES_PID 2>/dev/null; then
        log_info "Stopping Elasticsearch (PID: $ES_PID)..."
        kill $ES_PID
        wait $ES_PID 2>/dev/null || true
    fi
}

trap cleanup EXIT

main() {
    configure_proxy_bypass

    if ! start_elasticsearch; then
        log_error "Failed to start Elasticsearch"
        exit 1
    fi
    
    log_info "=== Testing connection after proxy fix ==="
    log_info "Testing root endpoint without proxy:"
    curl -v "http://127.0.0.1:9200/" 2>&1 | head -10
    log_info "=== End connection test ==="
    
    log_info "Checking available indices..."
    if ! list_indices; then
        log_error "Failed to list indices, but continuing anyway..."
        log_info "You may need to check the index name manually"
    fi
    
    log_info "Checking if index '$INDEX_NAME' exists..."
    if ! check_index_exists "$INDEX_NAME"; then
        log_warn ""
        log_warn "Index '$INDEX_NAME' not found or couldn't verify!"
        log_info "Attempting to list indices again..."
        list_indices
        log_info ""
        log_info "If the index exists but isn't showing, there might be a connectivity issue."
        log_info "The search pipeline will attempt to continue anyway."
        log_warn "If searches fail, verify the index name and Elasticsearch health."
    else
        log_success ""
        log_success "Index '$INDEX_NAME' found. Proceeding with search pipeline..."
    fi
    
    log_info "Building query configuration..."
    CONFIG_JSON=$(build_config_json)
    
    if [ -z "$CONFIG_JSON" ]; then
        log_error "Failed to build configuration JSON"
        exit 1
    fi

    log_info "Starting search queries execution with configurable parameters..."
    if python3 /capstor/scratch/cscs/inesaltemir/scripts/search/search.py \
        --csv-file "$CSV_FILE" \
        --index-name "$INDEX_NAME" \
        --es-url "$ES_URL" \
        --output-dir "$OUTPUT_DIR" \
        --dataset "$DATASET" \
        --config "$CONFIG_JSON"; then
        log_success ""
        log_success "Search pipeline completed successfully!"
        log_info "Output files saved to: $OUTPUT_DIR"
        log_info "Check the output files for detailed results and statistics."
        log_info ""
        log_info "Query configuration used:"
        echo "$CONFIG_JSON" | python3 -m json.tool 2>/dev/null || echo "$CONFIG_JSON"
    else
        log_error "Search pipeline failed!"
        exit 1
    fi
}

main