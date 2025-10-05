#!/bin/bash
#SBATCH --job-name=detokenize-megatron
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G

#SBATCH --output=/capstor/scratch/cscs/inesaltemir/DETOKENIZING_logs/swissai-fineweb-2-quality_33-filterrobots-merge/rest/output/detokenizing_%j.out
#SBATCH --error=/capstor/scratch/cscs/inesaltemir/DETOKENIZING_logs/swissai-fineweb-2-quality_33-filterrobots-merge/rest/err/detokenizing_%j.err
#SBATCH --environment=python-hf

# Megatron Dataset Detokenization Script for SLURM

set -e  # Exit on any error

export HF_TOKEN="${HF_TOKEN:-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}"

# Default parameters (modify as needed)
INPUT_DIR="${INPUT_DIR:-/iopsstor/scratch/cscs/jpcoles/a06/swissai-fineweb-2-quality_33-filterrobots-merge/rest}"

# have done 190GB per job for fw-edu-score-2

# FROM /capstor/store/cscs/swissai/infra01/users/ahernnde/workspace/swiss-ai__Megatron-LM/best-submit-8b.sh
# DATAROOT=/iopsstor/scratch/cscs/jpcoles/a06
# Phase 1
# DATASETS=(
#         $DATAROOT/finemath-3plus-merge (114G) (900784): 47GB : 
#         $DATAROOT/starcoder-extras-merge (144G) (900893): 50GB
#         $DATAROOT/starcoder-threshold-0-merge (756G) ONGOING (4 jobs): Total: 229.092GB
#         $DATAROOT/swissai-fineweb-edu-score-2-filterrobots-merge DOING (19T) (ongoing): Total: 12736.9GB
#         $DATAROOT/swissai-fineweb-2-quality_33-filterrobots-merge/euro-high (9.1T) (50 jobs) (ongoing): Total: 2660.02GB
#         $DATAROOT/swissai-fineweb-2-quality_33-filterrobots-merge/euro-mid (88G) (1 job)(901144): 21GB
#         $DATAROOT/swissai-fineweb-2-quality_33-filterrobots-merge/other-high  (3.9T) (25 jobs) (ongoing): Total: 991.787GB
#         $DATAROOT/swissai-fineweb-2-quality_33-filterrobots-merge/rest (356G) (2 jobs): Total: 76.6389GB
#         $DATAROOT/poison DONE: 622MB
#         $DATAROOT/gutenberg DONE: 3.2GB
# )

OUTPUT_DIR="${OUTPUT_DIR:-/capstor/scratch/cscs/inesaltemir/detokenized_output/swissai-fineweb-2-quality_33-filterrobots-merge_rest}"
TOKENIZER="${TOKENIZER:-swiss-ai/Apertus-8B-2509}"
CHUNK_SIZE="${CHUNK_SIZE:-2000}" # review
MAX_WORKERS="${MAX_WORKERS:-8}" # half of CPUs power
COMPRESSION="${COMPRESSION:-snappy}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

# File processing parameters
DRY_RUN="${DRY_RUN:-false}"
KEEP_SPECIAL_TOKENS="${KEEP_SPECIAL_TOKENS:-false}"

# File range support for parallel processing
FILE_RANGE_START="${FILE_RANGE_START:-}"
FILE_RANGE_END="${FILE_RANGE_END:-}"


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

# Function to validate input directory
validate_input_directory() {
    log_info "Validating input directory: $INPUT_DIR"
    
    if [ ! -d "$INPUT_DIR" ]; then
        log_error "Input directory does not exist: $INPUT_DIR"
        exit 1
    fi
    
    bin_count=$(find "$INPUT_DIR" -name "*.bin" | wc -l)
    idx_count=$(find "$INPUT_DIR" -name "*.idx" | wc -l)
    
    if [ $bin_count -eq 0 ] || [ $idx_count -eq 0 ]; then
        log_error "No .bin/.idx pairs found in $INPUT_DIR (bin: $bin_count, idx: $idx_count)"
        exit 1
    fi
    
    log_success "Found $bin_count .bin files and $idx_count .idx files"
}

# Function to show configuration
show_configuration() {
    log_info "=== SLURM Job Configuration ==="
    echo "Job ID: ${SLURM_JOB_ID:-'Not in SLURM'}"
    echo "Node: ${SLURM_NODELIST:-'Unknown'}"
    echo "CPUs: ${SLURM_CPUS_PER_TASK:-'Unknown'}"
    echo "Memory: ${SLURM_MEM_PER_NODE:-'Unknown'}MB"
    echo "============================="
    
    log_info "=== Detokenization Configuration ==="
    echo "Input Directory: $INPUT_DIR"
    echo "Output Directory: $OUTPUT_DIR"
    echo "Tokenizer: $TOKENIZER"
    echo "Chunk Size: $CHUNK_SIZE"
    echo "Max Workers: ${MAX_WORKERS:-'Auto (CPU_COUNT/2)'}"
    echo "Compression: $COMPRESSION"
    echo "File Start Range: $FILE_RANGE_START"
    echo "File End Range: $FILE_RANGE_END"
    echo "Dry Run: $DRY_RUN"
    echo "Keep Special Tokens: $KEEP_SPECIAL_TOKENS"
    echo "============================"
}
# Function to monitor system resources
monitor_resources() {
    log_info "System resource monitoring:"
    
    # Show SLURM allocation info
    if [ -n "${SLURM_JOB_ID}" ]; then
        echo "SLURM Job Resources:"
        echo "  Job ID: ${SLURM_JOB_ID}"
        echo "  Node(s): ${SLURM_NODELIST:-'Unknown'}"
        echo "  CPUs allocated: ${SLURM_CPUS_PER_TASK:-'Not specified'}"
        echo "  Memory allocated: ${SLURM_MEM_PER_NODE:-'Not specified'}MB"
        echo "  Tasks per node: ${SLURM_NTASKS_PER_NODE:-'Not specified'}"
    fi
    
    # Basic info that should always be available
    echo "Working directory: $(pwd)"
    echo "Available disk space in current directory:"
    du -sh . 2>/dev/null || echo "Disk usage check failed"
}

# Function to estimate processing requirements
estimate_processing() {
    log_info "Estimating processing requirements..."
    
    total_size_gb=$(find "$INPUT_DIR" -name "*.bin" -exec du -b {} + | awk '{sum+=$1} END {print sum/1024/1024/1024}')
    # estimated_time_hours=$(echo "$total_size_gb * 0.05" | bc -l)  # Rough estimate: 20 GB/hour
    # estimated_output_gb=$(echo "$total_size_gb * 0.3" | bc -l)   # Parquet ~30% of binary size
    
    log_info "=== Processing Estimates ==="
    printf "Total input size: %.2f GB\n" "$total_size_gb"
    #printf "Estimated processing time: %.1f hours\n" "$estimated_time_hours"
    # printf "Estimated output size: %.2f GB\n" "$estimated_output_gb"
    echo "============================"
}

# Function to setup output directory
setup_output_directory() {
    log_info "Setting up output directory: $OUTPUT_DIR"
    
    # Create output directory
    mkdir -p "$OUTPUT_DIR"
    
    # Create subdirectories for organization
    mkdir -p "$OUTPUT_DIR/logs"
    mkdir -p "$OUTPUT_DIR/parquet"
    
    # Check write permissions
    if [ ! -w "$OUTPUT_DIR" ]; then
        log_error "Output directory is not writable: $OUTPUT_DIR"
        exit 1
    fi
    
    log_success "Output directory setup complete"
}

# Function to run detokenization
run_detokenization() {
    log_info "Starting Megatron dataset detokenization..."
    
    start_time=$(date +%s)
    
    # Base Python command exactly like the indexing script style
    base_cmd="python3 /capstor/scratch/cscs/inesaltemir/scripts/detokenize/batch_detokenize.py \
        \"$INPUT_DIR\" \
        \"$OUTPUT_DIR\" \
        --tokenizer \"$TOKENIZER\" \
        --chunk-size \"$CHUNK_SIZE\" \
        --compression \"$COMPRESSION\""
    
    # Add optional parameters conditionally
    if [[ -n "$MAX_WORKERS" ]]; then
        base_cmd+=" --max-workers \"$MAX_WORKERS\""
    fi

    # Add file range arguments only if both are set and not empty
    if [[ -n "$FILE_RANGE_START" && -n "$FILE_RANGE_END" ]]; then
        base_cmd+=" --file-range-start \"$FILE_RANGE_START\" --file-range-end \"$FILE_RANGE_END\""
        log_info "Using file range: $FILE_RANGE_START to $FILE_RANGE_END"
    else
        log_info "Processing all files (no file range specified)"
    fi

    
    if [[ "$KEEP_SPECIAL_TOKENS" == "true" ]]; then
        base_cmd+=" --keep-special-tokens"
    fi
    
    if [[ "$DRY_RUN" == "true" ]]; then
        base_cmd+=" --dry-run"
    fi
    
    log_info "Executing command: $base_cmd"
    
    # Execute the command
    eval "$base_cmd" 2>&1

    detokenization_exit_code=${PIPESTATUS[0]}
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    if [ $detokenization_exit_code -eq 0 ]; then
        log_success "Detokenization completed successfully in ${duration} seconds"
        return 0
    else
        log_error "Detokenization failed with exit code $detokenization_exit_code after ${duration} seconds"
        return 1
    fi
}

# Function to show final output information
show_output_information() {
    log_info "=== Output Information ==="
    echo "Parquet files location: $OUTPUT_DIR/parquet"
    echo "Logs location: $OUTPUT_DIR/logs"
    
    # Count output files
    if [ -d "$OUTPUT_DIR/parquet" ]; then
        parquet_count=$(find "$OUTPUT_DIR/parquet" -name "*.parquet" | wc -l)
        total_output_size=$(du -sh "$OUTPUT_DIR/parquet" 2>/dev/null | cut -f1)
        echo "Generated parquet files: $parquet_count"
        echo "Total output size: $total_output_size"
        
        # Show first few files as examples
        echo "Sample output files:"
        find "$OUTPUT_DIR/parquet" -name "*.parquet" | head -5 | while read file; do
            echo "  - $(basename "$file")"
        done
    fi
    echo "=========================="
}

# Function to cleanup on exit
cleanup() {
    log_info "Cleaning up..."
    show_output_information
}

# Main execution function
main() {
    log_info "=== Megatron Dataset Detokenization Started ==="
    log_info "Timestamp: $(date)"
    log_info "Script: $0"
    log_info "Working directory: $(pwd)"
    
    # Set trap for cleanup
    trap cleanup EXIT
    
    # Show configuration
    show_configuration
    
    # System resource monitoring
    monitor_resources
    
    # Validate input directory
    validate_input_directory
    
    # Estimate processing requirements
    estimate_processing
    
    # Setup output directory
    setup_output_directory
    
    # Run detokenization
    if run_detokenization; then
        log_success "=== Detokenization process completed successfully ==="
        
        # Show where the output is stored
        show_output_information
        
        # Final system resource check
        log_info "Final system resources:"
        monitor_resources
        
        exit 0
    else
        log_error "=== Detokenization process failed ==="
        exit 1
    fi
}

# Run main function
main "$@"