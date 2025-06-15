#!/bin/bash
#SBATCH --job-name=cluster-merge-indexes
#SBATCH --partition=normal
#SBATCH --account=a-a145
#SBATCH --time=11:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=400G

#SBATCH --output=/capstor/scratch/cscs/inesaltemir/remote_INDEXING_swissai_fineweb_2_quality_33_filterrobots/deu/output/indexing_%j.out
#SBATCH --error=/capstor/scratch/cscs/inesaltemir/remote_INDEXING_swissai_fineweb_2_quality_33_filterrobots/deu/err/indexing_%j.err
#SBATCH --environment=es-python

# Cluster-Level Index Merge - Avoids metadata corruption by using live clusters
# This script solves the UUID/metadata problem by running each data directory
# in its own Elasticsearch cluster, then using remote reindex API for merging

set -e

# Configuration - Override these via environment variables
# UPDATED: More specific pattern to exclude target directories and only match numbered source dirs
# SOURCE_DATA_DIRS="${SOURCE_DATA_DIRS:-/iopsstor/scratch/cscs/inesaltemir/es-data-*-swissai-fineweb-*-fineweb_fra_brouillon[12]}"
# TARGET_INDEX="${TARGET_INDEX:-fineweb_fra_brouillon_merged}"
# SOURCE_INDEX_PATTERNS="${SOURCE_INDEX_PATTERNS:-fineweb_fra_brouillon1 fineweb_fra_brouillon2}"

SOURCE_DATA_DIRS="${SOURCE_DATA_DIRS:-/iopsstor/scratch/cscs/inesaltemir/es-data-*-swissai-fineweb-*-deu_part[1-8]}"
TARGET_INDEX="${TARGET_INDEX:-fineweb_deu_merged}"
SOURCE_INDEX_PATTERNS="${SOURCE_INDEX_PATTERNS:-fineweb_deu_part1 fineweb_deu_part2 fineweb_deu_part3 fineweb_deu_part4 fineweb_deu_part5 fineweb_deu_part6 fineweb_deu_part7 fineweb_deu_part8}"
# SOURCE_INDEX_PATTERNS="${SOURCE_INDEX_PATTERNS:-fineweb_fra_part1 fineweb_fra_part2 fineweb_fra_part3 fineweb_fra_part4 fineweb_fra_part5 fineweb_fra_part6 fineweb_fra_part7 fineweb_fra_part8}"

MERGE_CLUSTER_PORT="${MERGE_CLUSTER_PORT:-9200}"
BATCH_SIZE="${BATCH_SIZE:-10000}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

# CRITICAL FIX: Add proxy bypass configuration
configure_proxy_bypass() {
    log_info "Configuring proxy bypass for localhost connections..."
    
    # Save original proxy settings
    ORIGINAL_HTTP_PROXY="${http_proxy:-}"
    ORIGINAL_HTTPS_PROXY="${https_proxy:-}"
    ORIGINAL_NO_PROXY="${no_proxy:-}"
    
    # Extend no_proxy to include all localhost variants and port ranges
    export no_proxy="${no_proxy},127.0.0.1,localhost,0.0.0.0,::1,127.0.0.1:9200,127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203,127.0.0.1:9204,127.0.0.1:9205"
    
    # Also unset proxy for localhost explicitly
    export http_proxy=""
    export https_proxy=""
    
    log_info "Original no_proxy: $ORIGINAL_NO_PROXY"
    log_info "Updated no_proxy: $no_proxy"
    log_info "Disabled HTTP/HTTPS proxy for localhost connections"
}

# Enhanced curl function with explicit proxy bypass
safe_curl() {
    local url="$1"
    shift
    curl --noproxy "127.0.0.1,localhost" --connect-timeout 10 --max-time 30 "$@" "$url"
}

# Test Elasticsearch connectivity with proxy bypass
test_elasticsearch_connection() {
    local host="$1"
    local port="$2"
    local max_retries=30  # Increased retries
    local retry_count=0
    
    log_info "Testing Elasticsearch connection to $host:$port"
    
    while [ $retry_count -lt $max_retries ]; do
        # CRITICAL: Test multiple endpoints to ensure cluster is fully ready
        if safe_curl -s "http://$host:$port/" > /dev/null 2>&1 && \
           safe_curl -s "http://$host:$port/_cluster/health" > /dev/null 2>&1 && \
           safe_curl -s "http://$host:$port/_cat/indices" > /dev/null 2>&1; then
            
            # VERIFY cluster is actually healthy
            local health_status=$(safe_curl -s "http://$host:$port/_cluster/health" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
            if [[ "$health_status" == "green" || "$health_status" == "yellow" ]]; then
                log_success "Successfully connected to healthy Elasticsearch at $host:$port (status: $health_status)"
                return 0
            else
                log_warn "Cluster is responding but not healthy: $health_status"
            fi
        fi
        
        # Check if process is still alive
        if [ ! -z "$ES_PID" ] && ! kill -0 $ES_PID 2>/dev/null; then
            log_error "Elasticsearch process died during startup!"
            return 1
        fi
        
        retry_count=$((retry_count + 1))
        log_warn "Connection attempt $retry_count/$max_retries failed, retrying..."
        sleep 15
    done
    
    log_error "Failed to connect to healthy Elasticsearch at $host:$port after $max_retries attempts"
    return 1
}

show_configuration() {
    log_info "=== Configuration ==="
    echo "Job ID: ${SLURM_JOB_ID:-'Not in SLURM'}"
    echo "Source Data Pattern: $SOURCE_DATA_DIRS"
    echo "Source Index Patterns: $SOURCE_INDEX_PATTERNS"
    echo "Target Index: $TARGET_INDEX"
    echo "Target Port: $MERGE_CLUSTER_PORT"
    echo "Batch Size: $BATCH_SIZE"
    echo "========================"
}

# Start individual Elasticsearch cluster for each data directory
start_source_clusters() {
    log_info "=== Starting Individual Source Clusters ==="
    
    # Find all matching data directories with improved pattern
    local source_dirs=($(find /iopsstor/scratch/cscs/inesaltemir -maxdepth 1 -name "es-data-*-swissai-fineweb-*-deu_part[1-8]" -type d 2>/dev/null))

    if [ ${#source_dirs[@]} -eq 0 ]; then
        log_error "No source data directories found matching pattern: es-data-*-swissai-fineweb-*-deu_part[1-8]"
        log_info "Searched in: /iopsstor/scratch/cscs/inesaltemir"
        log_info "Looking for directories like: es-data-XXXXX-swissai-fineweb-*-deu_part1 through deu_part8"
        
        # DEBUG: Show what directories actually exist
        log_info "Available directories starting with 'es-data-':"
        find /iopsstor/scratch/cscs/inesaltemir -maxdepth 1 -name "es-data-*" -type d 2>/dev/null | head -10 || log_warn "No es-data directories found"
        return 1
    fi

    log_info "Found ${#source_dirs[@]} source data directories"
    
    # Configure Java environment
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"
    local heap_size="8g"  # Conservative heap size for source clusters
    
    local port=9201  # Start from port 9201 for source clusters
    local cluster_pids=()
    
    # Start one cluster per data directory
    for data_dir in "${source_dirs[@]}"; do
        local cluster_name="source_cluster_$(basename "$data_dir")"
        local log_dir="/tmp/es-logs-${cluster_name}-${SLURM_JOB_ID}"
        mkdir -p "$log_dir"
        
        log_info "Starting cluster for: $(basename "$data_dir") on port $port"
        
        # Start Elasticsearch with this specific data directory
        ES_JAVA_OPTS="-Xms${heap_size} -Xmx${heap_size}" \
        /usr/share/elasticsearch/bin/elasticsearch \
            -E cluster.name="$cluster_name" \
            -E node.name="source_node_$port" \
            -E path.data="$data_dir" \
            -E path.logs="$log_dir" \
            -E discovery.type=single-node \
            -E network.host=127.0.0.1 \
            -E http.port=$port \
            -E transport.port=$((port + 100)) \
            -E node.store.allow_mmap=false \
            -E xpack.security.enabled=false \
            -E bootstrap.memory_lock=false \
            -E logger.root=INFO &
        
        local pid=$!
        cluster_pids+=("$pid:$port:$data_dir")
        
        log_info "Started cluster PID $pid on port $port"
        port=$((port + 1))
        sleep 5
    done
    
    # Wait for all source clusters to be ready
    log_info "Waiting for all source clusters to be ready..."
    for cluster_info in "${cluster_pids[@]}"; do
        local pid=$(echo "$cluster_info" | cut -d: -f1)
        local port=$(echo "$cluster_info" | cut -d: -f2)
        local data_dir=$(echo "$cluster_info" | cut -d: -f3)
        
        # Check if process is still running
        if ! kill -0 $pid 2>/dev/null; then
            log_error "Cluster for $(basename "$data_dir") (PID $pid) died during startup"
            return 1
        fi
        
        # Test connection with proxy bypass
        if ! test_elasticsearch_connection "127.0.0.1" "$port"; then
            log_error "Cluster on port $port failed to start properly"
            return 1
        fi
        
        log_success "Cluster ready on port $port for $(basename "$data_dir")"
    done
    
    # Save cluster info for later use
    echo "${cluster_pids[@]}" > /tmp/source_clusters_${SLURM_JOB_ID}.txt
    log_success "All ${#cluster_pids[@]} source clusters are ready"
    return 0
}

# Start target cluster for the merged index
start_target_cluster() {
    log_info "=== Starting Target Merge Cluster ==="
    
    local target_data_dir="/iopsstor/scratch/cscs/inesaltemir/es-data-target-${TARGET_INDEX}-${SLURM_JOB_ID}"
    mkdir -p "$target_data_dir"
    
    local target_log_dir="/tmp/es-logs-target-${TARGET_INDEX}-${SLURM_JOB_ID}"
    mkdir -p "$target_log_dir"
    
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"
    local heap_size="30g"  # DO NOT SURPASS 31GB
    
    log_info "Starting target cluster on port $MERGE_CLUSTER_PORT"
    
    # Start target cluster optimized for write operations with reindex whitelist
    ES_JAVA_OPTS="-Xms${heap_size} -Xmx${heap_size}" \
    /usr/share/elasticsearch/bin/elasticsearch \
        -E cluster.name="merge_cluster" \
        -E node.name="merge_node" \
        -E path.data="$target_data_dir" \
        -E path.logs="$target_log_dir" \
        -E discovery.type=single-node \
        -E network.host=127.0.0.1 \
        -E http.port=$MERGE_CLUSTER_PORT \
        -E transport.port=$((MERGE_CLUSTER_PORT + 100)) \
        -E node.store.allow_mmap=false \
        -E xpack.security.enabled=false \
        -E bootstrap.memory_lock=false \
        -E logger.root=INFO \
        -E indices.memory.index_buffer_size=40% \
        -E thread_pool.write.queue_size=1000 \
        -E reindex.remote.whitelist="127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203,127.0.0.1:9204,127.0.0.1:9205,127.0.0.1:9206,127.0.0.1:9207,127.0.0.1:9208,localhost:9201,localhost:9202,localhost:9203,localhost:9204,localhost:9205,localhost:9206,localhost:9207,localhost:9208" &
    
    export TARGET_ES_PID=$!
    
    # Test connection with proxy bypass
    if ! test_elasticsearch_connection "127.0.0.1" "$MERGE_CLUSTER_PORT"; then
        log_error "Target cluster failed to start properly"
        return 1
    fi
    
    log_success "Target cluster ready on port $MERGE_CLUSTER_PORT"
    return 0
}

# Discover indexes and execute the Python merge script
discover_and_merge_indexes() {
    log_info "=== Discovering and Merging Indexes ==="
    
    local cluster_pids_file="/tmp/source_clusters_${SLURM_JOB_ID}.txt"
    if [ ! -f "$cluster_pids_file" ]; then
        log_error "Source cluster info not found"
        return 1
    fi
    
    local cluster_pids=($(cat "$cluster_pids_file"))
    local source_configs=()
    local total_expected_docs=0
    
    # ENHANCED index discovery with validation
    for cluster_info in "${cluster_pids[@]}"; do
        local pid=$(echo "$cluster_info" | cut -d: -f1)
        local port=$(echo "$cluster_info" | cut -d: -f2)
        local data_dir=$(echo "$cluster_info" | cut -d: -f3)
        
        log_info "Validating cluster on port $port..."
        
        # VERIFY cluster is still healthy
        if ! test_elasticsearch_connection "127.0.0.1" "$port"; then
            log_error "Source cluster on port $port is not healthy"
            return 1
        fi
        
        # Get indexes with validation
        local indexes_response
        if ! indexes_response=$(safe_curl -s "http://127.0.0.1:$port/_cat/indices?h=index,docs.count" 2>&1); then
            log_error "Failed to get indexes from port $port: $indexes_response"
            return 1
        fi
        
        # Validate response format
        if echo "$indexes_response" | grep -q "<!DOCTYPE\|<html\|<head\|<body"; then
            log_error "Received HTML error page from port $port"
            log_error "Response: $(echo "$indexes_response" | head -3)"
            return 1
        fi
        
        # Process indexes with document counts
        while read -r line; do
            if [ -n "$line" ] && [[ ! "$line" =~ ^\. ]]; then  # Skip system indexes
                local index_name=$(echo "$line" | awk '{print $1}')
                local doc_count=$(echo "$line" | awk '{print $2}' | grep -o '[0-9]*' || echo "0")
                
                # Validate index has documents
                if [ "$doc_count" -gt 0 ]; then
                    for pattern in $SOURCE_INDEX_PATTERNS; do
                        if [[ "$index_name" == *"$pattern"* ]]; then
                            source_configs+=("{\"index\":\"$index_name\",\"host\":\"127.0.0.1\",\"port\":$port,\"doc_count\":$doc_count}")
                            total_expected_docs=$((total_expected_docs + doc_count))
                            log_info "  ✓ Found: $index_name ($doc_count docs)"
                            break
                        fi
                    done
                else
                    log_warn "  ⚠ Skipping empty index: $index_name"
                fi
            fi
        done <<< "$indexes_response"
    done
    
    if [ ${#source_configs[@]} -eq 0 ]; then
        log_error "No valid source indexes found with documents!"
        return 1
    fi
    
    log_info "Total expected documents to merge: $total_expected_docs"
    
    # VERIFY target cluster before starting merge
    if ! test_elasticsearch_connection "127.0.0.1" "$MERGE_CLUSTER_PORT"; then
        log_error "Target cluster is not ready for merge"
        return 1
    fi
    
    # Build JSON configuration
    local json_config="["
    for i in "${!source_configs[@]}"; do
        json_config+="${source_configs[i]}"
        if [ $i -lt $((${#source_configs[@]} - 1)) ]; then
            json_config+=","
        fi
    done
    json_config+="]"
    
    log_info "Starting merge with configuration: $json_config"
    
    # Execute merge with enhanced verification
    if NO_PROXY="$no_proxy" HTTP_PROXY="" HTTPS_PROXY="" \
       python3 "/capstor/scratch/cscs/inesaltemir/scripts/merge_indexes/merge.py" \
           --source-configs "$json_config" \
           --target-index "$TARGET_INDEX" \
           --target-host "127.0.0.1" \
           --target-port "$MERGE_CLUSTER_PORT" \
           --batch-size "$BATCH_SIZE" \
           --log-level INFO; then
        
        # CRITICAL: Verify merge results
        log_info "=== Verifying Merge Results ==="
        sleep 10  # Wait for final refresh
        
        local final_count=$(safe_curl -s "http://127.0.0.1:$MERGE_CLUSTER_PORT/${TARGET_INDEX}/_count" | grep -o '"count":[0-9]*' | cut -d: -f2 || echo "0")
        local index_exists=$(safe_curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$MERGE_CLUSTER_PORT/${TARGET_INDEX}")
        
        if [ "$index_exists" != "200" ]; then
            log_error "Target index was not created!"
            return 1
        fi
        
        if [ "$final_count" -eq 0 ]; then
            log_error "Target index is empty after merge!"
            return 1
        fi
        
        log_success "Merge verification passed:"
        log_info "  - Target index exists: ✓"
        log_info "  - Document count: $final_count"
        log_info "  - Expected documents: $total_expected_docs"
        
        if [ "$final_count" -lt $((total_expected_docs * 95 / 100)) ]; then
            log_warn "Document count is significantly lower than expected"
        fi
        
        return 0
    else
        log_error "Python merge script failed"
        return 1
    fi
}

# Cleanup function - called on script exit
cleanup() {
    log_info "=== Cleaning up ==="
    
    # Restore original proxy settings
    if [ -n "$ORIGINAL_HTTP_PROXY" ]; then
        export http_proxy="$ORIGINAL_HTTP_PROXY"
    fi
    if [ -n "$ORIGINAL_HTTPS_PROXY" ]; then
        export https_proxy="$ORIGINAL_HTTPS_PROXY"
    fi
    if [ -n "$ORIGINAL_NO_PROXY" ]; then
        export no_proxy="$ORIGINAL_NO_PROXY"
    fi
    
    # Stop target cluster
    if [ ! -z "$TARGET_ES_PID" ] && kill -0 $TARGET_ES_PID 2>/dev/null; then
        log_info "Stopping target cluster (PID: $TARGET_ES_PID)..."
        kill $TARGET_ES_PID
        wait $TARGET_ES_PID 2>/dev/null || true
        log_info "Target cluster stopped"
    fi
    
    # Stop all source clusters
    local cluster_pids_file="/tmp/source_clusters_${SLURM_JOB_ID}.txt"
    if [ -f "$cluster_pids_file" ]; then
        local cluster_pids=($(cat "$cluster_pids_file"))
        for cluster_info in "${cluster_pids[@]}"; do
            local pid=$(echo "$cluster_info" | cut -d: -f1)
            if kill -0 $pid 2>/dev/null; then
                log_info "Stopping source cluster PID $pid..."
                kill $pid
                wait $pid 2>/dev/null || true
            fi
        done
        log_info "All source clusters stopped"
    fi
    
    # Show final results if accessible
    log_info "=== Final Results ==="
    if safe_curl -s "http://127.0.0.1:$MERGE_CLUSTER_PORT/_cat/indices?v" 2>/dev/null; then
        echo "Final target cluster indexes:"
        safe_curl -s "http://127.0.0.1:$MERGE_CLUSTER_PORT/_cat/indices?v"
    else
        log_info "Target cluster no longer accessible for final status"
    fi
    
    # Clean up temporary files
    rm -f /tmp/source_clusters_${SLURM_JOB_ID}.txt
    
    log_info "Cleanup completed"
}

# Main execution function
main() {
    log_info "=== Cluster-Level Index Merge Started ==="
    log_info "Job ID: ${SLURM_JOB_ID}"
    log_info "Timestamp: $(date)"
    
    # CRITICAL: Configure proxy bypass FIRST
    configure_proxy_bypass
    
    show_configuration
    
    # Set cleanup trap to run on script exit
    trap cleanup EXIT
    
    # Enhanced startup validation
    # Step 1: Start source clusters (one per data directory)
    log_info "=== STEP 1: Starting Source Clusters ==="
    if ! start_source_clusters; then
        log_error "Failed to start source clusters"
        exit 1
    fi
    
    # Step 2: Start target cluster for merged data
    log_info "=== STEP 2: Starting Target Cluster ==="
    if ! start_target_cluster; then
        log_error "Failed to start target cluster"
        exit 1
    fi
    
    # CRITICAL: Wait for all clusters to be stable
    log_info "Waiting for all clusters to stabilize..."
    sleep 60
    
    # Validate all clusters before merge
    if ! test_elasticsearch_connection "127.0.0.1" "$MERGE_CLUSTER_PORT"; then
        log_error "Target cluster failed stability check"
        exit 1
    fi
    
    # Step 3: Discover indexes and execute merge
    log_info "=== STEP 3: Discovering and Merging Indexes ==="
    if ! discover_and_merge_indexes; then
        log_error "Merge process failed"
        exit 1
    fi
    
    # FINAL verification before declaring success
    log_info "=== Final System Verification ==="
    local final_health=$(safe_curl -s "http://127.0.0.1:$MERGE_CLUSTER_PORT/_cluster/health")
    local final_indices=$(safe_curl -s "http://127.0.0.1:$MERGE_CLUSTER_PORT/_cat/indices?v")
    
    log_info "Cluster health: $final_health"
    log_info "Final indices:"
    echo "$final_indices"
    
    # Check if our target index is in the list
    if echo "$final_indices" | grep -q "$TARGET_INDEX"; then
        log_success "=== MERGE COMPLETED AND VERIFIED ==="
        log_info "Target index '$TARGET_INDEX' is available on port $MERGE_CLUSTER_PORT"
        exit 0
    else
        log_error "=== MERGE FAILED - TARGET INDEX NOT FOUND ==="
        exit 1
    fi
}

# Execute main function with all arguments
main "$@"