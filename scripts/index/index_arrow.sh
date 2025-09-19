#!/bin/bash
#SBATCH --job-name=index-sft-arrow
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=01:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=128G

#SBATCH --output=/capstor/scratch/cscs/<username>/INDEXING_sft_arrow/output/indexing_%j.out
#SBATCH --error=/capstor/scratch/cscs/<username>/INDEXING_sft_arrow/err/indexing_%j.err
#SBATCH --environment=es-python

# SFT Arrow Dataset Indexing Script for Containerized Elasticsearch on SLURM
set -e  # Exit on any error

# SCRIPT TO INDEX SFT DATA (arrow format)

# Default parameters for SFT Arrow indexing
DATA_DIR="${DATA_DIR:-/capstor/store/cscs/swissai/infra01/posttrain_data/06_sft_mixtures_newformat_linearised/apertus-sft-mixture-8e/train}"

CHUNK_SIZE="${CHUNK_SIZE:-15000}"       # Bulk indexing batch size for conversations (prev 10 000)
ES_HOST="${ES_HOST:-localhost}"         # Elasticsearch host (container internal)
ES_PORT="${ES_PORT:-9200}"             # Elasticsearch port
INDEX_NAME="${INDEX_NAME:-sft-data}"   # Index name for SFT conversations
LOG_LEVEL="${LOG_LEVEL:-INFO}"         # Logging level

# SFT-specific parameters
CHUNK_SIZE_PROC="${CHUNK_SIZE_PROC:-1000}"       # Arrow file processing chunk size
THREAD_COUNT="${THREAD_COUNT:-2}"      # Number of parallel threads (conservative for SFT data)
MAX_FILES="${MAX_FILES:-}"             # Maximum files to process (for testing)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Fix Java environment for Elasticsearch startup
start_elasticsearch() {
    log_info "Starting Elasticsearch server for SFT dataset indexing"
    
    # 1. Fix Java environment variables
    log_info "=== Configuring Java Environment ==="
    
    # Unset problematic JAVA_HOME that points to non-existent path
    unset JAVA_HOME
    
    # Set ES_JAVA_HOME to use bundled Java (recommended for ES 7.17.28)
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"
    
    # Verify Java is working
    log_info "Testing Java installation..."
    if $ES_JAVA_HOME/bin/java -version; then
        log_success "Java test successful"
    else
        log_error "Java test failed"
        return 1
    fi
    
    # Test Elasticsearch binary
    log_info "Testing Elasticsearch binary..."
    if /usr/share/elasticsearch/bin/elasticsearch --version; then
        log_success "Elasticsearch binary test successful"
    else
        log_error "Elasticsearch binary test failed"
        return 1
    fi
    
    # 3. Set proper heap size for SFT data (smaller than web data)
    if [ "${SLURM_MEM_PER_NODE:-0}" -ge 65536 ]; then
        # 64GB+ available, use 16GB heap (SFT data is typically smaller)
        CUSTOM_HEAP="-Xms16g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
    elif [ "${SLURM_MEM_PER_NODE:-0}" -ge 32768 ]; then
        # 32GB+ available, use 12GB heap
        CUSTOM_HEAP="-Xms12g -Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
    else
        # Less than 32GB, use conservative 6GB heap  
        CUSTOM_HEAP="-Xms6g -Xmx6g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
    fi

    log_info "Using heap settings for SFT data: $CUSTOM_HEAP"
    
    # 4. Start Elasticsearch with proper environment
    log_info "=== Starting Elasticsearch for SFT Indexing ==="
    
    # Create job-specific directories for SFT data
    local job_data_dir="/iopsstor/scratch/cscs/inesaltemir/es-data-${INDEX_NAME}-${SLURM_JOB_ID}"
    local job_logs_dir="/iopsstor/scratch/cscs/inesaltemir/es-logs-${INDEX_NAME}-${SLURM_JOB_ID}"
    
    mkdir -p "$job_data_dir"
    mkdir -p "$job_logs_dir"
    
    log_info "ES Data Directory: $job_data_dir"
    log_info "ES Logs Directory: $job_logs_dir"

    ES_JAVA_OPTS="$CUSTOM_HEAP" \
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
        -E cluster.routing.allocation.disk.watermark.low=85% \
        -E cluster.routing.allocation.disk.watermark.high=90% \
        -E cluster.routing.allocation.disk.watermark.flood_stage=95% \
        -E bootstrap.memory_lock=false \
        -E logger.root=INFO \
        -E http.max_content_length=200mb &

    ES_PID=$!
    
    log_info "Elasticsearch started with PID: $ES_PID"
    
    # 5. Wait for startup with enhanced monitoring
    log_info "Waiting for Elasticsearch to be ready..."
    max_retries=30
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        # Check if process is still running
        if ! kill -0 $ES_PID 2>/dev/null; then
            log_error "Elasticsearch process died! PID $ES_PID is no longer running"
            
            # Show any logs that might have been created
            log_info "=== Checking for error logs ==="
            find "$job_logs_dir" -name "*.log" -exec tail -20 {} \; 2>/dev/null || log_warn "No log files found"
            
            return 1
        fi
        
        # Try to connect
        if curl -s "http://127.0.0.1:9200/_cluster/health" > /dev/null 2>&1; then
            log_success "Elasticsearch is ready at 127.0.0.1:9200"
            
            # Show successful startup info
            curl -s "http://127.0.0.1:9200/" | head -10
            
            return 0
        else
            retry_count=$((retry_count + 1))
            log_info "Waiting for Elasticsearch... attempt $retry_count/$max_retries"
            sleep 10
        fi
    done
    
    log_error "Elasticsearch failed to start after $max_retries attempts"
    return 1
}

# Function to stop Elasticsearch
stop_elasticsearch() {
    if [ ! -z "$ES_PID" ] && kill -0 $ES_PID 2>/dev/null; then
        log_info "Stopping Elasticsearch (PID: $ES_PID)..."
        kill $ES_PID
        wait $ES_PID 2>/dev/null || true
        log_info "Elasticsearch stopped"
    fi
}

# Function to validate SFT data directory
validate_data_directory() {
    log_info "Validating SFT Arrow data directory: $DATA_DIR"
    
    if [ ! -d "$DATA_DIR" ]; then
        log_error "Data directory does not exist: $DATA_DIR"
        exit 1
    fi
    
    arrow_count=$(find "$DATA_DIR" -name "*.arrow" | wc -l)
    if [ $arrow_count -eq 0 ]; then
        log_error "No Arrow files found in $DATA_DIR"
        exit 1
    fi
    
    log_success "Found $arrow_count Arrow files in data directory"
    
    # Show sample of files
    log_info "Sample Arrow files:"
    find "$DATA_DIR" -name "*.arrow" | head -5
}

# Function to show configuration
show_configuration() {
    log_info "=== SLURM Job Configuration ==="
    echo "Job ID: ${SLURM_JOB_ID:-'Not in SLURM'}" 
    echo "Node: ${SLURM_NODELIST:-'Unknown'}" 
    echo "CPUs: ${SLURM_CPUS_PER_TASK:-'Unknown'}"
    echo "Memory: ${SLURM_MEM_PER_NODE:-'Unknown'}MB" 
    echo "=============================" 
    
    log_info "=== SFT Arrow Indexing Configuration ==="
    echo "Data Directory: $DATA_DIR"
    echo "Chunk Size: $CHUNK_SIZE" 
    echo "Chunk Size for processing: $CHUNK_SIZE_PROC"
    echo "Elasticsearch: $ES_HOST:$ES_PORT" 
    echo "Index Name: $INDEX_NAME"
    echo "Thread Count: $THREAD_COUNT"
    echo "Max Files: ${MAX_FILES:-'All files'}"
    echo "Log Level: $LOG_LEVEL" 
    echo "ES Java Opts: $CUSTOM_HEAP"
    echo "============================"
}

# Function to monitor system resources
monitor_resources() {
    log_info "System resource monitoring:"
    echo "Memory usage:"
    free -h 
    echo "Disk usage:" 
    df -h | head -10
    echo "CPU info:"
    nproc
    echo "Load average:"
    uptime
}

# Function to run the SFT Arrow indexing process
run_sft_indexing() {
    log_info "Starting SFT Arrow dataset indexing..."
    
    start_time=$(date +%s)
    
    # Base Python command for SFT Arrow indexing
    base_cmd="python3 /capstor/scratch/cscs/inesaltemir/scripts/indexing/index_arrow.py \
        --data-dir \"$DATA_DIR\" \
        --chunk-size \"$CHUNK_SIZE\" \
        --chunk-size-proc \"$CHUNK_SIZE_PROC\" \
        --es-host \"$ES_HOST\" \
        --es-port \"$ES_PORT\" \
        --index-name \"$INDEX_NAME\" \
        --log-level \"$LOG_LEVEL\" \
        --thread-count \"$THREAD_COUNT\""
    
    # Add max files argument if set
    if [[ -n "$MAX_FILES" ]]; then
        base_cmd+=" --max-files \"$MAX_FILES\""
        log_info "Processing maximum $MAX_FILES files for testing"
    else
        log_info "Processing all Arrow files in directory"
    fi
    
    log_info "Executing command: $base_cmd"
    
    # Execute the command
    eval "$base_cmd" 2>&1

    indexing_exit_code=${PIPESTATUS[0]}
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    if [ $indexing_exit_code -eq 0 ]; then
        log_success "SFT indexing completed successfully in ${duration} seconds"
        return 0
    else
        log_error "SFT indexing failed with exit code $indexing_exit_code after ${duration} seconds"
        return 1
    fi
}

# Function to show final index information
show_index_info() {
    log_info "=== SFT Index Information ==="
    
    # Try to get index stats from Elasticsearch
    if curl -s "http://127.0.0.1:9200/$INDEX_NAME/_stats" > /dev/null 2>&1; then
        log_info "Index statistics:"
        curl -s "http://127.0.0.1:9200/$INDEX_NAME/_stats?pretty" | grep -A 5 -B 5 "\"docs\"\|\"store\""
        
        log_info "Index health:"
        curl -s "http://127.0.0.1:9200/_cluster/health/$INDEX_NAME?pretty"
    else
        log_warn "Could not retrieve index statistics"
    fi
    
    echo "Index data is stored in the container at: /usr/share/elasticsearch/data" 
    echo "This is mounted to: /iopsstor/scratch/cscs/inesaltemir/es-data-sft-${INDEX_NAME}-${SLURM_JOB_ID}"
    echo "Index logs are stored at: /usr/share/elasticsearch/logs"
    echo "This is mounted to: /iopsstor/scratch/cscs/inesaltemir/es-logs-sft-${INDEX_NAME}-${SLURM_JOB_ID}"
    echo "============================" 
}

# Function to cleanup on exit
cleanup() {
    log_info "Cleaning up SFT indexing job..."
    stop_elasticsearch
    show_index_info
    
    # Show final resource usage
    log_info "Final resource usage:"
    monitor_resources
}

# Function to estimate processing time
estimate_processing_time() {
    local arrow_count=$(find "$DATA_DIR" -name "*.arrow" | wc -l)
    local estimated_conversations=$((arrow_count * 10000))  # Rough estimate
    local estimated_time_minutes=$((estimated_conversations / 1000))  # Very rough estimate
    
    log_info "=== Processing Estimates ==="
    echo "Arrow files found: $arrow_count"
    echo "Estimated conversations: ~$estimated_conversations"
    echo "Estimated processing time: ~$estimated_time_minutes minutes"
    echo "============================"
}

# Main execution function
main() {
    log_info "=== SFT Arrow Dataset Elasticsearch Indexing Started ==="
    log_info "Timestamp: $(date)"
    log_info "Script: $0"
    log_info "Working directory: $(pwd)"
    
    # Set trap for cleanup
    trap cleanup EXIT
    
    # Show configuration
    show_configuration
    
    # System resource monitoring
    monitor_resources
    
    # Validate data directory
    validate_data_directory
    
    # Show processing estimates
    estimate_processing_time
    
    # Start Elasticsearch
    if ! start_elasticsearch; then
        log_error "Failed to start Elasticsearch"
        exit 1
    fi
    
    # Run SFT indexing
    if run_sft_indexing; then
        log_success "=== SFT Arrow Indexing process completed successfully ==="
        
        # Show final index information
        show_index_info
        
        # Final system resource check
        log_info "Final system resources:"
        monitor_resources
        
        exit 0
    else
        log_error "=== SFT Arrow Indexing process failed ==="
        exit 1
    fi
}

# Run main function
main "$@"