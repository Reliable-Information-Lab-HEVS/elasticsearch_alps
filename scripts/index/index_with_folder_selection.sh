#!/bin/bash
#SBATCH --job-name=index-file-range
#SBATCH --partition=normal
#SBATCH --account=a-a145
#SBATCH --time=11:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=256G

#SBATCH --output=/capstor/scratch/cscs/inesaltemir/INDEXING_swissai-fineweb-edu-score-2-filterrobots/output/indexing_folder_sel_%j.out
#SBATCH --error=/capstor/scratch/cscs/inesaltemir/INDEXING_swissai-fineweb-edu-score-2-filterrobots/err/indexing_folder_sel_%j.err
#SBATCH --environment=es-python

# FineWeb Dataset Indexing Script for Containerized Elasticsearch on SLURM
set -e  # Exit on any error

INDEX_CONFIG_FILE="${INDEX_CONFIG_FILE:-/capstor/scratch/cscs/inesaltemir/index_config/index_config_with_url.json}"

# ONLY DIFFERENCE COMPARED TO INDEX.SH IS FOLDER SELECTION PARAMETER
# Folder selection parameter
N="${N:-95}"  # Default to folder 1 if not set, can be overridden with N=X

# Base directory containing all 95 folders
BASE_DATA_DIR="/capstor/store/cscs/swissai/a06/datasets_swissai/swissai-fineweb-edu-score-2-filterrobots/data/output"

# Create deterministic list of folders and select the N-th one
FOLDER_ARRAY=($(ls -1d ${BASE_DATA_DIR}/*/ | sort))
TOTAL_FOLDERS=${#FOLDER_ARRAY[@]}

# Validate N parameter
if [ "$N" -lt 1 ] || [ "$N" -gt "$TOTAL_FOLDERS" ]; then
    log_error "Invalid folder number N=$N. Must be between 1 and $TOTAL_FOLDERS"
    exit 1
fi

# Select the N-th folder (array is 0-indexed, so N-1)
SELECTED_FOLDER=${FOLDER_ARRAY[$((N-1))]}
FOLDER_NAME=$(basename "$SELECTED_FOLDER")

# Validate selected folder exists
if [ ! -d "$SELECTED_FOLDER" ]; then
    log_error "Selected folder not found: $SELECTED_FOLDER"
    exit 1
fi

# Update configuration to use selected folder
DATA_DIR="$SELECTED_FOLDER"

FOLDER_NAME_LOWERCASE=$(echo "$FOLDER_NAME" | tr '[:upper:]' '[:lower:]')
INDEX_NAME="fineweb_folder_edu_score_2_filterrobots_$(printf "%02d" $N)_${FOLDER_NAME_LOWERCASE}"
# INDEX_NAME="fineweb_folder_edu_score_2_filterrobots_$(printf "%02d" $N)_${FOLDER_NAME}"


BATCH_SIZE="${BATCH_SIZE:-12500}"        # Bulk indexing batch size, prev 2500
ES_HOST="${ES_HOST:-localhost}"         # Elasticsearch host (container internal)
ES_PORT="${ES_PORT:-9200}"             # Elasticsearch port

LOG_LEVEL="${LOG_LEVEL:-INFO}"         # Logging level

# ADDED: File range support for parallel processing
FILE_RANGE_START="${FILE_RANGE_START:-}"     # Starting file index
FILE_RANGE_END="${FILE_RANGE_END:-}"         # Ending file index

# Elasticsearch bulk indexing parameters
MAX_CHUNK_BYTES="${MAX_CHUNK_BYTES:-75}"    # Max chunk size in MB
THREAD_COUNT="${THREAD_COUNT:-8}"           # Number of parallel threads
QUEUE_SIZE="${QUEUE_SIZE:-8}"               # Queue size for parallel bulk

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
    log_info "Starting Elasticsearch server with Java environment fix"
    
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
    
    # 3. Set proper heap size based on available memory
    if [ "${SLURM_MEM_PER_NODE:-0}" -ge 32768 ]; then
        # 32GB+ available, use 20GB heap
        CUSTOM_HEAP="-Xms40g -Xmx40g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
    else
        # Less than 32GB, use conservative 8GB heap  
        CUSTOM_HEAP="-Xms8g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
    fi

    log_info "Using heap settings: $CUSTOM_HEAP"
    
    # 4. Start Elasticsearch with proper environment
    log_info "=== Starting Elasticsearch ==="
    
    # Problem with trying to create subdirectories INSIDE the mount points - cannot create subfolders in es-data
    # Extract the part number from index name (e.g., fineweb_deu_part1 -> part1)
    

    # Replace the existing job_data_dir and job_logs_dir lines with:
    local job_data_dir="/iopsstor/scratch/cscs/inesaltemir/es-data-${SLURM_JOB_ID}-swissai-fineweb-edu-score-2_filterrobots-folder-$(printf "%02d" $N)-${FOLDER_NAME}"
    local job_logs_dir="/iopsstor/scratch/cscs/inesaltemir/es-logs-${SLURM_JOB_ID}-swissai-fineweb-edu-score-2_filterrobots-folder-$(printf "%02d" $N)-${FOLDER_NAME}"

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
    log_info "Using index config: $INDEX_CONFIG_FILE"
    
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
            find /usr/share/elasticsearch/logs -name "*.log" -exec tail -20 {} \; 2>/dev/null || log_warn "No log files found"
            
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

# Function to validate data directory
validate_data_directory() {
    log_info "Validating data directory: $DATA_DIR"
    
    if [ ! -d "$DATA_DIR" ]; then
        log_error "Data directory does not exist: $DATA_DIR"
        exit 1
    fi
    
    parquet_count=$(find "$DATA_DIR" -name "*.parquet" | wc -l)
    if [ $parquet_count -eq 0 ]; then
        log_error "No parquet files found in $DATA_DIR"
        exit 1
    fi
    
    log_success "Found $parquet_count parquet files in data directory"
}

# Function to show configuration
show_configuration() {
    log_info "=== SLURM Job Configuration ==="
    echo "Job ID: ${SLURM_JOB_ID:-'Not in SLURM'}" 
    echo "Node: ${SLURM_NODELIST:-'Unknown'}" 
    echo "CPUs: ${SLURM_CPUS_PER_TASK:-'Unknown'}"
    echo "Memory: ${SLURM_MEM_PER_NODE:-'Unknown'}MB" 
    echo "=============================" 
    
    log_info "=== Indexing Configuration ==="
    echo "Data Directory: $DATA_DIR"
    echo "Batch Size: $BATCH_SIZE" 
    echo "Elasticsearch: $ES_HOST:$ES_PORT" 
    echo "Index Name: $INDEX_NAME"
    echo "File Start Range: $FILE_RANGE_START"
    echo "File End Range: $FILE_RANGE_END"
    echo "Log Level: $LOG_LEVEL" 
    echo "ES Java Opts: $ES_JAVA_OPTS"
    echo "ES Java Opts: $INDEX_CONFIG_FILE"
    echo "Folder Number (N): $N"
    echo "Selected Folder: $FOLDER_NAME"
    echo "Total Available Folders: $TOTAL_FOLDERS"
    echo "ES Data Path: /usr/share/elasticsearch/data" 
    echo "ES Logs Path: /usr/share/elasticsearch/logs" 
    echo "============================"
}

# Function to monitor system resources
monitor_resources() {
    log_info "System resource monitoring:"
    echo "Memory usage:"
    free -h 
    echo "Disk usage:" 
    df -h | head -10
}

# Function to run the indexing process
run_indexing() {
    log_info "Starting FineWeb dataset indexing..."
    
    start_time=$(date +%s)
    # /capstor/scratch/cscs/inesaltemir/index_fineweb_memory_saving.py
    # Run the Python indexing script
    
    # python3 "/capstor/scratch/cscs/inesaltemir/scripts/indexing/indexing.py" \
    #python3 "/capstor/scratch/cscs/inesaltemir/scripts/indexing/index_no_leak_with_file_range.py" \
    #    --data-dir "$DATA_DIR" \
    #    --batch-size "$BATCH_SIZE" \
    #    --chunk-size 12000 \
    #    --es-host "$ES_HOST" \
    #    --es-port "$ES_PORT" \
    #    --index-name "$INDEX_NAME" \
    #    --log-level "$LOG_LEVEL" \
    #    --index-config "$INDEX_CONFIG_FILE" \
    #    --max-chunk-bytes "$MAX_CHUNK_BYTES" \
    #    --thread-count "$THREAD_COUNT" \
    #    --queue-size "$QUEUE_SIZE" \
    #    --file-range-start "$FILE_RANGE_START" \
    #    --file-range-end "$FILE_RANGE_END" 2>&1 
        
      
    # Base Python command
    base_cmd="python3 /capstor/scratch/cscs/inesaltemir/scripts/indexing/index.py \
        --data-dir \"$DATA_DIR\" \
        --batch-size \"$BATCH_SIZE\" \
        --chunk-size 12000 \
        --es-host \"$ES_HOST\" \
        --es-port \"$ES_PORT\" \
        --index-name \"$INDEX_NAME\" \
        --log-level \"$LOG_LEVEL\" \
        --index-config \"$INDEX_CONFIG_FILE\" \
        --max-chunk-bytes \"$MAX_CHUNK_BYTES\" \
        --thread-count \"$THREAD_COUNT\" \
        --queue-size \"$QUEUE_SIZE\""
    
    # Add file range arguments only if both are set and not empty
    if [[ -n "$FILE_RANGE_START" && -n "$FILE_RANGE_END" ]]; then
        base_cmd+=" --file-range-start \"$FILE_RANGE_START\" --file-range-end \"$FILE_RANGE_END\""
        log_info "Using file range: $FILE_RANGE_START to $FILE_RANGE_END"
    else
        log_info "Processing all files (no file range specified)"
    fi
    
    # Execute the command
    eval "$base_cmd" 2>&1

    indexing_exit_code=${PIPESTATUS[0]}
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    if [ $indexing_exit_code -eq 0 ]; then
        log_success "Indexing completed successfully in ${duration} seconds"
        return 0
    else
        log_error "Indexing failed with exit code $indexing_exit_code after ${duration} seconds"
        return 1
    fi
}

# Function to show final index location
show_index_location() {
    log_info "=== Index Storage Information ==="
    echo "Index data is stored in the container at: /usr/share/elasticsearch/data" 
    echo "This is mounted to your host directory: /iopsstor/scratch/cscs/inesaltemir/es-data" 
    echo "Index logs are stored at: /usr/share/elasticsearch/logs"
    echo "This is mounted to your host directory: /iopsstor/scratch/cscs/inesaltemir/es-logs" 
    
    # Show actual index files if they exist
    if [ -d "/usr/share/elasticsearch/data" ]; then
        echo "Current index files:"
        find /usr/share/elasticsearch/data -name "*$INDEX_NAME*" -type f 2>/dev/null | head -10 || true
    fi
    echo "============================" 
}

# Function to cleanup on exit
cleanup() {
    log_info "Cleaning up..."
    stop_elasticsearch
    show_index_location
}

# Main execution function
main() {
    log_info "=== FineWeb Elasticsearch Indexing Started ==="
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
    
    # Start Elasticsearch
    if ! start_elasticsearch; then
        log_error "Failed to start Elasticsearch"
        exit 1
    fi
    
    # Run indexing
    if run_indexing; then
        log_success "=== Indexing process completed successfully ==="
        
        # Show where the index is stored
        show_index_location
        
        # Final system resource check
        log_info "Final system resources:"
        monitor_resources
        
        exit 0
    else
        log_error "=== Indexing process failed ==="
        exit 1
    fi
}

# Run main function
main "$@"
