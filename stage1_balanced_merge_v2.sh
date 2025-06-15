#!/bin/bash
#SBATCH --job-name=stage1-merge-batch-BATCH_NUM
#SBATCH --partition=normal
#SBATCH --account=a-a145
#SBATCH --time=11:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=350G

#SBATCH --output=/capstor/scratch/cscs/inesaltemir/MERGING_swissai-fineweb-edu-score-2/stage1_merge/batch_BATCH_NUM/output/merge_%j.out
#SBATCH --error=/capstor/scratch/cscs/inesaltemir/MERGING_swissai-fineweb-edu-score-2/stage1_merge/batch_BATCH_NUM/err/merge_%j.err
#SBATCH --environment=es-python

# Stage 1 Balanced Batch Merge Script (95 → 25) - FIXED VERSION
# Part of 4-stage hierarchical merge: 95 → 25 → 8 → 2 → 1
# This script handles one balanced batch of 3-4 source indexes
# Usage: Submit 25 jobs with BATCH_NUM values (1-25)

set -e

# CONFIGURATION
BATCH_NUM="${BATCH_NUM:-1}"  # Which batch this job handles (1-25)
MERGE_STAGE="1"              # Stage 1 of 4-stage process
TOTAL_BATCHES=25
TARGET_INDEX="fineweb_edu_score_2_stage1_batch_${BATCH_NUM}_merged"
MERGE_CLUSTER_PORT="9200"    # Each job runs on separate node
BATCH_SIZE="10000"           # Reduced for memory safety

# Source pattern
SOURCE_PATTERN_PREFIX="es-data-*-swissai-fineweb-edu-score-2_filterrobots-folder-"

# Balanced batch assignments (smallest+largest pairing strategy)
declare -A BATCH_ASSIGNMENTS
BATCH_ASSIGNMENTS[1]="93 24"         # 399G + 131G = 530G (2 indices)
BATCH_ASSIGNMENTS[2]="94 22"         # 385G + 149G = 534G (2 indices) 
BATCH_ASSIGNMENTS[3]="92 23"         # 374G + 157G = 531G (2 indices)
BATCH_ASSIGNMENTS[4]="89 19"         # 357G + 142G = 499G (2 indices)
BATCH_ASSIGNMENTS[5]="85 09"         # 357G + 160G = 517G (2 indices)
BATCH_ASSIGNMENTS[6]="88 16"         # 340G + 166G = 506G (2 indices)
BATCH_ASSIGNMENTS[7]="82 13"         # 331G + 167G = 498G (2 indices)
BATCH_ASSIGNMENTS[8]="80 11"         # 329G + 171G = 500G (2 indices)
BATCH_ASSIGNMENTS[9]="86 17"         # 322G + 173G = 495G (2 indices)
BATCH_ASSIGNMENTS[10]="77 20"        # 307G + 173G = 480G (2 indices)
BATCH_ASSIGNMENTS[11]="75 25"        # 306G + 170G = 476G (2 indices)
BATCH_ASSIGNMENTS[12]="33 26"        # 303G + 169G = 472G (2 indices)
BATCH_ASSIGNMENTS[13]="72 21"        # 298G + 169G = 467G (2 indices)
BATCH_ASSIGNMENTS[14]="32 12"        # 298G + 176G = 474G (2 indices)
BATCH_ASSIGNMENTS[15]="81 14"        # 284G + 187G = 471G (2 indices)
BATCH_ASSIGNMENTS[16]="70 15"        # 269G + 185G = 454G (2 indices)
BATCH_ASSIGNMENTS[17]="66 06"        # 265G + 187G = 452G (2 indices)
BATCH_ASSIGNMENTS[18]="39 10"        # 262G + 185G = 447G (2 indices)
BATCH_ASSIGNMENTS[19]="95 18"        # 261G + 176G = 437G (2 indices)
BATCH_ASSIGNMENTS[20]="53 04"        # 252G + 176G = 428G (2 indices)
BATCH_ASSIGNMENTS[21]="42 08"        # 252G + 180G = 432G (2 indices)
BATCH_ASSIGNMENTS[22]="68 07"        # 251G + 193G = 444G (2 indices)
BATCH_ASSIGNMENTS[23]="78 01 05"     # 250G + 193G + 199G = 642G (3 indices)
BATCH_ASSIGNMENTS[24]="76 03 27"     # 250G + 188G + 192G = 630G (3 indices)
BATCH_ASSIGNMENTS[25]="43 02 remaining"  # 244G + 185G + all remaining small ones

# Special handling for batch 25 (gets all remaining small indices)
BATCH_25_REMAINING="31 87 58 44 40 57 73 28 74 56 54 52 36 29 60 59 38 83 50 45 34 41 69 64 62 49 71 67 46 65"

# FIXED: Simplified logging without color codes for cluster names
log_info() { echo "[STAGE1-BATCH$BATCH_NUM INFO] $1"; }
log_warn() { echo "[STAGE1-BATCH$BATCH_NUM WARN] $1"; }
log_error() { echo "[STAGE1-BATCH$BATCH_NUM ERROR] $1"; }
log_success() { echo "[STAGE1-BATCH$BATCH_NUM SUCCESS] $1"; }

# CRITICAL FIX: Add proxy bypass configuration (from working merge.sh)
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
    
    log_info "Proxy bypass configured for Stage 1 Batch $BATCH_NUM"
}

# Enhanced curl with proxy bypass
safe_curl() {
    local url="$1"
    shift
    curl --noproxy "127.0.0.1,localhost" --connect-timeout 10 --max-time 30 "$@" "$url"
}

# FIXED: Enhanced test function with better error reporting
test_elasticsearch_connection() {
    local host="$1"
    local port="$2"
    local max_retries=45  # Increased retries
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
                log_success "Connected to healthy Elasticsearch at $host:$port (status: $health_status)"
                return 0
            else
                log_warn "Cluster is responding but not healthy: $health_status"
            fi
        fi
        
        # Check if process is still alive (need to track PID externally)
        if [ ! -z "$CURRENT_ES_PID" ] && ! kill -0 $CURRENT_ES_PID 2>/dev/null; then
            log_error "Elasticsearch process $CURRENT_ES_PID died during startup!"
            return 1
        fi
        
        retry_count=$((retry_count + 1))
        log_info "Connection attempt $retry_count/$max_retries for Stage 1 Batch $BATCH_NUM..."
        sleep 12  # Increased sleep time
    done
    
    log_error "Failed to connect to Elasticsearch at $host:$port after $max_retries attempts"
    return 1
}

# Get folder numbers assigned to this batch
get_batch_folder_numbers() {
    local batch_num="$1"
    
    if [ "$batch_num" -eq 25 ]; then
        # Special handling for batch 25 - gets remaining small indices
        echo "43 02 $BATCH_25_REMAINING"
    else
        # Standard batch assignment
        echo "${BATCH_ASSIGNMENTS[$batch_num]}"
    fi
}

# FIXED: Find source directories for this batch using folder numbers
find_batch_source_dirs() {
    # Use exec to avoid output contamination from logging
    exec 3>&1  # Save stdout
    exec 1>&2  # Redirect stdout to stderr temporarily
    
    log_info "Finding source directories for Stage 1 Batch $BATCH_NUM"
    
    local folder_numbers=($(get_batch_folder_numbers "$BATCH_NUM"))
    local batch_dirs=()
    local total_size=0
    
    log_info "Assigned folder numbers: ${folder_numbers[*]}"
    
    for folder_num in "${folder_numbers[@]}"; do
        # Format folder number with leading zero if needed
        local formatted_folder=$(printf "%02d" "$folder_num")
        local pattern="${SOURCE_PATTERN_PREFIX}${formatted_folder}-*"
        
        log_info "Searching for pattern: $pattern"
        local found_dirs=($(find /iopsstor/scratch/cscs/inesaltemir -maxdepth 1 -name "$pattern" -type d 2>/dev/null))
        
        if [ ${#found_dirs[@]} -gt 0 ]; then
            for dir in "${found_dirs[@]}"; do
                # Check if directory is not empty (skip 4KB empty ones)
                local dir_size=$(du -s "$dir" 2>/dev/null | cut -f1 || echo "0")
                if [ "$dir_size" -gt 1000 ]; then  # More than 1MB (1000 KB)
                    batch_dirs+=("$dir")
                    local size_gb=$(du -sh "$dir" 2>/dev/null | cut -f1 || echo "0G")
                    log_info "  Added: $(basename "$dir") ($size_gb)"
                else
                    log_warn "  Skipped empty directory: $(basename "$dir")"
                fi
            done
        else
            log_warn "  No directories found for folder $formatted_folder"
        fi
    done
    
    if [ ${#batch_dirs[@]} -eq 0 ]; then
        log_error "No valid source directories found for Stage 1 Batch $BATCH_NUM!"
        exec 1>&3  # Restore stdout
        return 1
    fi
    
    log_success "Found ${#batch_dirs[@]} source directories for Stage 1 Batch $BATCH_NUM"
    
    # Calculate total batch size
    local total_size_display=""
    for dir in "${batch_dirs[@]}"; do
        local size=$(du -sh "$dir" 2>/dev/null | cut -f1 || echo "0G")
        total_size_display="$total_size_display $size"
    done
    log_info "Batch sizes: $total_size_display"
    
    exec 1>&3  # Restore stdout
    echo "${batch_dirs[@]}"  # Output to original stdout
}

# FIXED: Start source clusters with improved settings based on working merge.sh
start_batch_source_clusters() {
    local source_dirs=("$@")
    local cluster_pids=()
    local port=9201  # Starting port for source clusters
    
    log_info "Starting ${#source_dirs[@]} source clusters for Stage 1 Batch $BATCH_NUM"
    
    # Fix Java environment
    unset JAVA_HOME
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"
    
    for data_dir in "${source_dirs[@]}"; do
        # FIXED: Create simple cluster name (avoid special characters)
        local dir_basename=$(basename "$data_dir")
        local cluster_name="source_cluster_$(echo "$dir_basename" | tr -cd '[:alnum:]_-' | cut -c1-50)"
        local log_dir="/tmp/es-logs-${cluster_name}-${SLURM_JOB_ID}"
        mkdir -p "$log_dir"
        
        log_info "Starting source cluster: $dir_basename on port $port"
        
        # FIXED: Use conservative heap sizes like working merge.sh (increased from 8g to 16g)
        ES_JAVA_OPTS="-Xms16g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200" \
        /usr/share/elasticsearch/bin/elasticsearch \
            -E cluster.name="$cluster_name" \
            -E node.name="source_node_$port" \
            -E path.data="$data_dir" \
            -E path.logs="$log_dir" \
            -E discovery.type=single-node \
            -E network.host=127.0.0.1 \
            -E http.host=127.0.0.1 \
            -E http.port=$port \
            -E transport.host=127.0.0.1 \
            -E network.bind_host=127.0.0.1 \
            -E network.publish_host=127.0.0.1 \
            -E transport.port=$((port + 1000)) \
            -E node.store.allow_mmap=false \
            -E xpack.security.enabled=false \
            -E bootstrap.memory_lock=false \
            -E logger.root=INFO \
            -E indices.recovery.max_bytes_per_sec=200mb \
            > "$log_dir/elasticsearch.out" 2>&1 &
        
        local pid=$!
        cluster_pids+=("$pid:$port:$data_dir")
        
        # Set current PID for health check
        export CURRENT_ES_PID=$pid
        
        log_info "Started source cluster PID $pid on port $port, waiting for ready..."
        
        # FIXED: Test each cluster immediately after starting
        if ! test_elasticsearch_connection "127.0.0.1" "$port"; then
            log_error "Source cluster on port $port failed to start properly"
            return 1
        fi
        
        log_success "Source cluster ready: port $port for $(basename "$data_dir")"
        
        port=$((port + 1))
        sleep 5  # Increased delay between starts
    done
    
    # Save cluster info
    echo "${cluster_pids[@]}" > "/tmp/stage1_batch${BATCH_NUM}_clusters_${SLURM_JOB_ID}.txt"
    log_success "All ${#cluster_pids[@]} source clusters ready for Stage 1 Batch $BATCH_NUM"
    return 0
}

# FIXED: Start target cluster with improved configuration
start_batch_target_cluster() {
    log_info "Starting target cluster for Stage 1 Batch $BATCH_NUM on port $MERGE_CLUSTER_PORT"
    
    local target_data_dir="/iopsstor/scratch/cscs/inesaltemir/es-data-fineweb-edu-score-2-stage1-batch${BATCH_NUM}-target-${TARGET_INDEX}-${SLURM_JOB_ID}"
    mkdir -p "$target_data_dir"
    
    local target_log_dir="/tmp/es-logs-stage1-batch${BATCH_NUM}-target-${SLURM_JOB_ID}"
    mkdir -p "$target_log_dir"
    
    # Fix Java environment
    unset JAVA_HOME
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"
    
    # FIXED: Use larger heap like working merge.sh and verify Java is working
    log_info "Testing Java installation..."
    if ! $ES_JAVA_HOME/bin/java -version; then
        log_error "Java test failed"
        return 1
    fi
    
    # Build reindex whitelist for source ports (flexible range)
    local whitelist=""
    for i in $(seq 9201 9250); do  # Allow up to 50 source clusters per batch
        if [ -n "$whitelist" ]; then
            whitelist+=","
        fi
        whitelist+="127.0.0.1:$i,localhost:$i"
    done
    
    log_info "Starting target cluster with 40GB heap (like working merge.sh)..."
    
    # FIXED: Use settings from working merge.sh
    ES_JAVA_OPTS="-Xms40g -Xmx40g -XX:+UseG1GC -XX:MaxGCPauseMillis=200" \
    /usr/share/elasticsearch/bin/elasticsearch \
        -E cluster.name="merge_cluster" \
        -E node.name="merge_node" \
        -E path.data="$target_data_dir" \
        -E path.logs="$target_log_dir" \
        -E discovery.type=single-node \
        -E network.host=127.0.0.1 \
        -E http.host=127.0.0.1 \
        -E http.port=$MERGE_CLUSTER_PORT \
        -E transport.host=127.0.0.1 \
        -E network.bind_host=127.0.0.1 \
        -E network.publish_host=127.0.0.1 \
        -E transport.port=$((MERGE_CLUSTER_PORT + 100)) \
        -E node.store.allow_mmap=false \
        -E xpack.security.enabled=false \
        -E bootstrap.memory_lock=false \
        -E logger.root=INFO \
        -E http.max_content_length=200mb \
        -E cluster.routing.allocation.disk.watermark.low=85% \
        -E cluster.routing.allocation.disk.watermark.high=90% \
        -E cluster.routing.allocation.disk.watermark.flood_stage=95% \
        -E indices.memory.index_buffer_size=40% \
        -E thread_pool.write.queue_size=1000 \
        -E reindex.remote.whitelist="$whitelist" \
        > "$target_log_dir/elasticsearch.out" 2>&1 &
    
    export STAGE1_BATCH_TARGET_ES_PID=$!
    export CURRENT_ES_PID=$STAGE1_BATCH_TARGET_ES_PID
    
    log_info "Target cluster started with PID: $STAGE1_BATCH_TARGET_ES_PID"
    
    # FIXED: Enhanced startup monitoring with log checking
    log_info "Monitoring target cluster startup..."
    sleep 15  # Give it more time to start
    
    # Check if process is still running
    if ! kill -0 $STAGE1_BATCH_TARGET_ES_PID 2>/dev/null; then
        log_error "Target cluster process died! PID $STAGE1_BATCH_TARGET_ES_PID is no longer running"
        
        # Show logs for debugging
        log_info "=== Checking for error logs ==="
        if [ -f "$target_log_dir/elasticsearch.out" ]; then
            log_error "Elasticsearch output:"
            tail -50 "$target_log_dir/elasticsearch.out"
        fi
        
        return 1
    fi
    
    if ! test_elasticsearch_connection "127.0.0.1" "$MERGE_CLUSTER_PORT"; then
        log_error "Target cluster failed to start for Stage 1 Batch $BATCH_NUM"
        
        # Show logs for debugging
        log_info "=== Checking for error logs ==="
        if [ -f "$target_log_dir/elasticsearch.out" ]; then
            log_error "Last 50 lines of Elasticsearch output:"
            tail -50 "$target_log_dir/elasticsearch.out"
        fi
        
        return 1
    fi
    
    log_success "Target cluster ready for Stage 1 Batch $BATCH_NUM on port $MERGE_CLUSTER_PORT"
    return 0
}

# Execute the merge for this batch
execute_batch_merge() {
    log_info "Executing merge for Stage 1 Batch $BATCH_NUM"
    
    local cluster_pids_file="/tmp/stage1_batch${BATCH_NUM}_clusters_${SLURM_JOB_ID}.txt"
    if [ ! -f "$cluster_pids_file" ]; then
        log_error "Source cluster info not found for Stage 1 Batch $BATCH_NUM"
        return 1
    fi
    
    local cluster_pids=($(cat "$cluster_pids_file"))
    local source_configs=()
    local total_expected_docs=0
    
    # Discover indexes from source clusters
    for cluster_info in "${cluster_pids[@]}"; do
        local port=$(echo "$cluster_info" | cut -d: -f2)
        local data_dir=$(echo "$cluster_info" | cut -d: -f3)
        
        log_info "Discovering indexes on port $port for $(basename "$data_dir")..."
        
        # VERIFY cluster is still healthy before proceeding
        if ! test_elasticsearch_connection "127.0.0.1" "$port"; then
            log_error "Source cluster on port $port is not healthy"
            return 1
        fi
        
        local indexes_response
        if ! indexes_response=$(safe_curl -s "http://127.0.0.1:$port/_cat/indices?h=index,docs.count" 2>&1); then
            log_error "Failed to get indexes from port $port"
            continue
        fi
        
        # Validate response format
        if echo "$indexes_response" | grep -q "<!DOCTYPE\|<html\|<head\|<body"; then
            log_error "Received HTML error page from port $port"
            continue
        fi
        
        while read -r line; do
            if [ -n "$line" ] && [[ ! "$line" =~ ^\. ]]; then  # Skip system indexes
                local index_name=$(echo "$line" | awk '{print $1}')
                local doc_count=$(echo "$line" | awk '{print $2}' | grep -o '[0-9]*' || echo "0")
                
                if [ "$doc_count" -gt 0 ]; then
                    # Accept any non-system index with documents
                    source_configs+=("{\"index\":\"$index_name\",\"host\":\"127.0.0.1\",\"port\":$port,\"doc_count\":$doc_count}")
                    total_expected_docs=$((total_expected_docs + doc_count))
                    log_info "  Found: $index_name ($doc_count docs)"
                else
                    log_warn "  Skipping empty index: $index_name"
                fi
            fi
        done <<< "$indexes_response"
    done
    
    if [ ${#source_configs[@]} -eq 0 ]; then
        log_error "No valid source indexes found for Stage 1 Batch $BATCH_NUM!"
        return 1
    fi
    
    log_info "Total expected documents for Stage 1 Batch $BATCH_NUM: $total_expected_docs"
    
    # Verify target cluster before merge
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
    
    log_info "Starting merge for Stage 1 Batch $BATCH_NUM with ${#source_configs[@]} source indexes"
    
    # Execute merge with enhanced verification
    if NO_PROXY="$no_proxy" HTTP_PROXY="" HTTPS_PROXY="" \
       python3 "/capstor/scratch/cscs/inesaltemir/scripts/merge_indexes/stage1_balanced_merge.py" \
           --source-configs "$json_config" \
           --target-index "$TARGET_INDEX" \
           --target-host "127.0.0.1" \
           --target-port "$MERGE_CLUSTER_PORT" \
           --batch-size "$BATCH_SIZE" \
           --log-level INFO; then
        
        # Enhanced verification
        log_info "Verifying merge results for Stage 1 Batch $BATCH_NUM..."
        sleep 15  # Wait for final operations
        
        local final_count=$(safe_curl -s "http://127.0.0.1:$MERGE_CLUSTER_PORT/${TARGET_INDEX}/_count" | grep -o '"count":[0-9]*' | cut -d: -f2 || echo "0")
        local index_exists=$(safe_curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$MERGE_CLUSTER_PORT/${TARGET_INDEX}")
        
        if [ "$index_exists" != "200" ]; then
            log_error "Target index was not created for Stage 1 Batch $BATCH_NUM!"
            return 1
        fi
        
        if [ "$final_count" -eq 0 ]; then
            log_error "Target index is empty after merge for Stage 1 Batch $BATCH_NUM!"
            return 1
        fi
        
        log_success "Stage 1 Batch $BATCH_NUM merge completed successfully!"
        log_info "Final document count: $final_count"
        log_info "Expected documents: $total_expected_docs"
        log_info "Target index: $TARGET_INDEX available on port $MERGE_CLUSTER_PORT"
        
        # Save stage 1 batch completion info
        local completion_file="/iopsstor/scratch/cscs/inesaltemir/stage1_batch_${BATCH_NUM}_completion.txt"
        echo "STAGE_1_BATCH_${BATCH_NUM}_COMPLETED:${TARGET_INDEX}:${MERGE_CLUSTER_PORT}:${final_count}:$(date)" > "$completion_file"
        
        # Also create a directory marker for stage 2 to find
        local stage1_output_dir="/iopsstor/scratch/cscs/inesaltemir/es-data-stage1-batch${BATCH_NUM}-target-${TARGET_INDEX}-${SLURM_JOB_ID}"
        echo "$TARGET_INDEX" > "${stage1_output_dir}/INDEX_NAME.txt"
        
        log_success "Stage 1 Batch $BATCH_NUM completion recorded"
        
        return 0
    else
        log_error "Stage 1 Batch $BATCH_NUM merge failed"
        return 1
    fi
}

# Cleanup function
cleanup() {
    log_info "Cleaning up Stage 1 Batch $BATCH_NUM..."
    
    # Restore proxy settings
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
    if [ ! -z "$STAGE1_BATCH_TARGET_ES_PID" ] && kill -0 $STAGE1_BATCH_TARGET_ES_PID 2>/dev/null; then
        log_info "Stopping target cluster for Stage 1 Batch $BATCH_NUM (PID: $STAGE1_BATCH_TARGET_ES_PID)..."
        kill $STAGE1_BATCH_TARGET_ES_PID
        wait $STAGE1_BATCH_TARGET_ES_PID 2>/dev/null || true
    fi
    
    # Stop source clusters
    local cluster_pids_file="/tmp/stage1_batch${BATCH_NUM}_clusters_${SLURM_JOB_ID}.txt"
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
        rm -f "$cluster_pids_file"
    fi
    
    log_info "Cleanup completed for Stage 1 Batch $BATCH_NUM"
}

# Show configuration
show_configuration() {
    log_info "=== Stage 1 Batch Configuration ==="
    echo "Stage: 1 of 4 (95 → 25)"
    echo "Batch: $BATCH_NUM of $TOTAL_BATCHES"
    echo "Target index: $TARGET_INDEX"
    echo "Target port: $MERGE_CLUSTER_PORT"
    echo "Job ID: ${SLURM_JOB_ID}"
    echo "Batch size: $BATCH_SIZE"
    echo "Assigned folders: $(get_batch_folder_numbers "$BATCH_NUM")"
    echo "Memory allocation: Source clusters 16GB each, Target cluster 40GB"
    echo "============================="
}

# Main execution
main() {
    log_info "=== Starting Stage 1 Balanced Batch Merge - FIXED VERSION ==="
    
    trap cleanup EXIT
    
    show_configuration
    
    # CRITICAL: Configure proxy bypass FIRST
    configure_proxy_bypass
    
    # Enhanced startup validation with delays
    log_info "System resource check..."
    free -h
    df -h | head -5
    
    # FIXED: Find source directories for this batch
    local source_dirs_output
    if ! source_dirs_output=$(find_batch_source_dirs); then
        log_error "Failed to find source directories for Stage 1 Batch $BATCH_NUM"
        exit 1
    fi
    
    # Convert output to array
    read -a source_dirs <<< "$source_dirs_output"
    
    if [ ${#source_dirs[@]} -eq 0 ]; then
        log_error "No source directories found for Stage 1 Batch $BATCH_NUM"
        exit 1
    fi
    
    log_info "Starting source clusters first..."
    # Start source clusters with individual verification
    if ! start_batch_source_clusters "${source_dirs[@]}"; then
        log_error "Failed to start source clusters for Stage 1 Batch $BATCH_NUM"
        exit 1
    fi
    
    # CRITICAL: Wait longer before starting target cluster
    log_info "Waiting for system resources to stabilize before starting target cluster..."
    sleep 30
    
    log_info "Starting target cluster..."
    # Start target cluster
    if ! start_batch_target_cluster; then
        log_error "Failed to start target cluster for Stage 1 Batch $BATCH_NUM"
        exit 1
    fi
    
    # CRITICAL: Wait for all clusters to be stable
    log_info "Waiting for all clusters to stabilize before merge..."
    sleep 45
    
    # Final verification of all clusters before merge
    log_info "Final verification of all clusters..."
    
    # Verify target cluster
    if ! test_elasticsearch_connection "127.0.0.1" "$MERGE_CLUSTER_PORT"; then
        log_error "Target cluster failed final verification"
        exit 1
    fi
    
    # Verify source clusters
    local cluster_pids_file="/tmp/stage1_batch${BATCH_NUM}_clusters_${SLURM_JOB_ID}.txt"
    if [ -f "$cluster_pids_file" ]; then
        local cluster_pids=($(cat "$cluster_pids_file"))
        for cluster_info in "${cluster_pids[@]}"; do
            local port=$(echo "$cluster_info" | cut -d: -f2)
            if ! safe_curl -s "http://127.0.0.1:$port/_cluster/health" > /dev/null 2>&1; then
                log_error "Source cluster on port $port failed final verification"
                exit 1
            fi
        done
    fi
    
    log_success "All clusters verified and ready for merge"
    
    # Execute merge
    if ! execute_batch_merge; then
        log_error "Merge failed for Stage 1 Batch $BATCH_NUM"
        exit 1
    fi
    
    log_success "=== STAGE 1 BATCH $BATCH_NUM COMPLETED SUCCESSFULLY ==="
}

main "$@"