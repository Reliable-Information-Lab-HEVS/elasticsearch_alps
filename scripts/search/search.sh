#!/bin/bash
#SBATCH --job-name=es-search-pipeline
#SBATCH --partition=normal
#SBATCH --account=a-a145
#SBATCH --time=04:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=20G

#SBATCH --output=/capstor/scratch/cscs/inesaltemir/es_search_logs/output/es_search_%j.out
#SBATCH --error=/capstor/scratch/cscs/inesaltemir/es_search_logs/err/es_search_%j.err
#SBATCH --environment=es-python


set -e

#ES_HOST="${ES_HOST:-localhost}"
ES_HOST="${ES_HOST:-127.0.0.1}"
ES_PORT="${ES_PORT:-9200}"
PATH_DATA="${PATH_DATA:-/iopsstor/scratch/cscs/inesaltemir/es-data-467222}" # es-data folder where the index is stored
#PATH_DATA="${PATH_DATA:-/iopsstor/scratch/cscs/inesaltemir/es-data}"

CSV_FILE="${CSV_FILE:-/capstor/scratch/cscs/inesaltemir/scripts/segment_samples/processed_obscene_words_en.csv}"
INDEX_NAME="${INDEX_NAME:-fineweb}"
JAVA_HEAP_SIZE="${JAVA_HEAP_SIZE:-8g}"
OUTPUT_DIR="${OUTPUT_DIR:-/capstor/scratch/cscs/inesaltemir/es_search_logs/results}"



# Build Elasticsearch URL
ES_URL="http://${ES_HOST}:${ES_PORT}"
# ES_URL="${ES_HOST}:${ES_PORT}"
JAVA_OPTS="-Xms6g -Xmx6g -XX:MaxGCPauseMillis=200" #  -XX:+UseStringDeduplication may cause problems, -XX:+UseG1GC 
#JAVA_OPTS="-Xms${JAVA_HEAP_SIZE} -Xmx${JAVA_HEAP_SIZE}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments (override environment variables if provided)
if [ $# -ge 1 ] && [ -n "$1" ]; then
    PATH_DATA="$1"
fi

if [ $# -ge 2 ] && [ -n "$2" ]; then
    CSV_FILE="$2"
fi

if [ $# -ge 3 ] && [ -n "$3" ]; then
    INDEX_NAME="$3"
fi

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

configure_proxy_bypass() {
    log_info "Configuring proxy bypass for localhost connections..."
    
    # Save original proxy settings
    ORIGINAL_HTTP_PROXY="${http_proxy:-}"
    ORIGINAL_NO_PROXY="${no_proxy:-}"
    
    # Extend no_proxy to include all localhost variants
    export no_proxy="${no_proxy},127.0.0.1,localhost,0.0.0.0,::1"
    
    log_info "Original no_proxy: $ORIGINAL_NO_PROXY"
    log_info "Updated no_proxy: $no_proxy"
    log_info "HTTP proxy: ${http_proxy:-'(not set)'}"
}

# Validate required parameters
if [ -z "$PATH_DATA" ]; then
    log_error "PATH_DATA is required"
    log_info ""
    log_info "Usage: $0 [path_to_data] [csv_file] [index_name]"
    log_info ""
    log_info "You can provide parameters via:"
    log_info "1. Command line: $0 /path/to/es-data segments.csv my_index"
    log_info "2. Environment variables: PATH_DATA=/path/to/es-data CSV_FILE=segments.csv $0"
    log_info "3. Mix: PATH_DATA=/path/to/es-data $0 '' segments.csv my_index"
    log_info ""
    log_info "Environment variables (with defaults):"
    log_info "  PATH_DATA=<required>      # Path to Elasticsearch data directory"
    log_info "  CSV_FILE=<required>       # Path to CSV file with segments"
    log_info "  INDEX_NAME=default_index  # Elasticsearch index name"
    log_info "  ES_HOST=localhost         # Elasticsearch host"
    log_info "  ES_PORT=9200             # Elasticsearch port"
    log_info "  JAVA_HEAP_SIZE=8g        # Java heap size for Elasticsearch"
    exit 1
fi

if [ -z "$CSV_FILE" ]; then
    log_error "CSV_FILE is required"
    log_info "See usage above for details"
    exit 1
fi

# Check if CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    log_error "CSV file '$CSV_FILE' not found"
    exit 1
fi

# Check if data path exists
if [ ! -d "$PATH_DATA" ]; then
    log_error "Data path '$PATH_DATA' not found"
    exit 1
fi

log_info "Starting Elasticsearch Search Pipeline..."
log_info "========================================"
log_info "Configuration:"
log_info "  Data path: $PATH_DATA"
log_info "  CSV file: $CSV_FILE" 
log_info "  Index name: $INDEX_NAME"
log_info "  Elasticsearch: $ES_URL"
log_info "  Java heap: $JAVA_HEAP_SIZE"
log_info "  Output directory: $OUTPUT_DIR"
log_info "========================================"

# Function to start Elasticsearch
start_elasticsearch() {
    log_info "Starting Elasticsearch server with Java environment fix"
    log_info "=== Configuring Java Environment ==="
    
    log_info "Using heap settings: $JAVA_OPTS"
    
    # Set environment variables
    unset JAVA_HOME
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"

    local job_logs_dir="/iopsstor/scratch/cscs/inesaltemir/es-search-logs-${SLURM_JOB_ID:-$$}"
    mkdir -p "$job_logs_dir"

    log_info "=== Starting Elasticsearch ==="
    # In your start_elasticsearch() function, replace the elasticsearch command with:
    ES_JAVA_OPTS="$JAVA_OPTS" \
    /usr/share/elasticsearch/bin/elasticsearch \
        -E path.data="$PATH_DATA" \
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
        -E http.max_content_length=200m \
        > "$job_logs_dir/elasticsearch.out" 2>&1 &
    
    

    ES_PID=$!
    log_info "Elasticsearch started with PID: $ES_PID"
    
    # Wait for startup with enhanced monitoring
    log_info "Waiting for Elasticsearch to be ready..."
    max_retries=45
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
        
        # Try to connect with explicit no-proxy
        if curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health" > /dev/null 2>&1; then
            log_success "Elasticsearch is ready at 127.0.0.1:9200"
            
            # Show cluster health
            log_info "=== Cluster Health ==="
            curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health?pretty" 2>/dev/null || log_warn "Could not fetch cluster health"
            
            # Show basic info
            log_info "=== Elasticsearch Info ==="
            curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/" 2>/dev/null || log_warn "Could not fetch ES info"
            
            return 0
        else
            retry_count=$((retry_count + 1))
            log_info "Waiting for Elasticsearch... attempt $retry_count/$max_retries (PID $ES_PID still running)"
            sleep 10
        fi
    done
    
    log_error "Elasticsearch failed to start after $max_retries attempts"
    return 1
}

debug_elasticsearch() {
    log_info "=== Elasticsearch Debug Information ==="
    
    # Check what version is running
    log_info "1. Elasticsearch version:"
    /usr/share/elasticsearch/bin/elasticsearch --version 2>&1 || echo "Version check failed"
    
    # Check the actual logs
    log_info "2. Recent Elasticsearch logs:"
    local job_logs_dir="/iopsstor/scratch/cscs/inesaltemir/es-search-logs-${SLURM_JOB_ID:-$$}"
    if [ -f "$job_logs_dir/elasticsearch.out" ]; then
        echo "--- Last 50 lines of elasticsearch.out ---"
        tail -50 "$job_logs_dir/elasticsearch.out"
    fi
    
    # Check system ES logs too
    if [ -d "/usr/share/elasticsearch/logs" ]; then
        echo "--- System elasticsearch logs ---"
        find /usr/share/elasticsearch/logs -name "*.log" -exec tail -20 {} \; 2>/dev/null
    fi
    
    # Test the exact HTTP response
    log_info "3. Raw HTTP response:"
    curl -v "http://127.0.0.1:9200/_cat/indices" 2>&1 | head -20
    
    log_info "4. Root endpoint response:"
    curl -v "http://127.0.0.1:9200/" 2>&1 | head -20
    
    log_info "=== End Debug ==="
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
    
    # Try simple format first (most reliable)
    local response
    response=$(curl -s "$ES_URL/_cat/indices?v" 2>&1)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -eq 0 ] && [ -n "$response" ]; then
        # Check if we got HTML instead of expected output
        if echo "$response" | grep -qi "<!DOCTYPE\|<html"; then
            log_warn "Received HTML response instead of indices list"
            log_warn "This usually means Elasticsearch returned an error page"
            log_info "Trying JSON format..."
            
            # Try JSON format as fallback
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
            # Normal response, show it
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

# Function to cleanup on exit
cleanup() {
    if [ ! -z "$ES_PID" ] && kill -0 $ES_PID 2>/dev/null; then
        log_info "Stopping Elasticsearch (PID: $ES_PID)..."
        kill $ES_PID
        wait $ES_PID 2>/dev/null || true
    fi
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Main execution
# Updated main function
main() {
    configure_proxy_bypass

    # Start Elasticsearch
    if ! start_elasticsearch; then
        log_error "Failed to start Elasticsearch"
        exit 1
    fi
    
    # Test connection after proxy fix
    log_info "=== Testing connection after proxy fix ==="
    log_info "Testing root endpoint without proxy:"
    curl -v "http://127.0.0.1:9200/" 2>&1 | head -10
    log_info "=== End connection test ==="
    
    log_info "=== Quick Debug ==="
    log_info "Testing root endpoint:"
    curl -v "http://127.0.0.1:9200/" 2>&1 | head -15
    log_info "Testing with basic auth disabled:"
    curl -v "http://127.0.0.1:9200/_cat/indices?v" 2>&1 | head -15
    log_info "=== End Quick Debug ==="
    
    # List available indices with better error handling
    log_info "Checking available indices..."
    if ! list_indices; then
        log_error "Failed to list indices, but continuing anyway..."
        log_info "You may need to check the index name manually"
    fi
    
    # Check if specified index exists
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
    
    # Run Python search script
    log_info "Starting search queries execution..."
    if python3 /capstor/scratch/cscs/inesaltemir/scripts/search/search_with_highlight.py "$CSV_FILE" "$INDEX_NAME" "$ES_URL" "$OUTPUT_DIR"; then
        log_success ""
        log_success "Search pipeline completed successfully!"
        log_info "Output files saved to: $OUTPUT_DIR"
        log_info "Check the output files for detailed results and statistics."
    else
        log_error "Search pipeline failed!"
        exit 1
    fi
}

# Run main function
main