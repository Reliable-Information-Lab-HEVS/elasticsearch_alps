#!/bin/bash
# Generate SLURM job submission commands for Elasticsearch index merging with directory splitting

# Configuration
# SOURCE_DATA_PATTERN can be either:
#   - A glob pattern (e.g., /path/to/es-data-*-part[1-8])
#   - A path to a .txt file containing directory paths (one per line)
# SOURCE_DATA_PATTERN="${SOURCE_DATA_PATTERN:-/iopsstor/scratch/cscs/inesaltemir/es-data-*-swissai-fineweb-*-deu_part[1-8]}"

#fw-edu-score-2: have indexed first two (920282 920283 in /capstor/scratch/cscs/inesaltemir/MERGE_logs/september_brouillon1/output/merge_001_923479.out)
# need to have even number of data dir, leave out the third 920284. Now 32 remaining data dir == 16 jobs
SOURCE_DATA_FILE="${SOURCE_DATA_FILE:-/capstor/scratch/cscs/inesaltemir/scripts/indexing/job_status_logs/fw-other-high/list_dir_fw_other_high.txt}"  # Alternative: provide a .txt file with directory paths
NUM_TARGET_INDEXES="${NUM_TARGET_INDEXES:-17}"
TARGET_INDEX_BASE="${TARGET_INDEX_BASE:-merged_swissai-fineweb-2-quality_33-filterrobots-merge_other-high-merge}"

# SOURCE_INDEX_PATTERNS="${SOURCE_INDEX_PATTERNS:-swissai-fineweb-edu-score-2-filterrobots-merge_part_001 swissai-fineweb-edu-score-2-filterrobots-merge_part_002}"
SOURCE_INDEX_PATTERNS="${SOURCE_INDEX_PATTERNS:-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part}"

# each have 40 ish shards - will do 70 target
# Merge script path 922129 
MERGE_SCRIPT="${MERGE_SCRIPT:-/capstor/scratch/cscs/inesaltemir/scripts/merge_indexes/new_merge_hardcoded_ports.sh}"

# Output directories for logs
OUTPUT_DIR="${OUTPUT_DIR:-/capstor/scratch/cscs/inesaltemir/MERGE_logs/${TARGET_INDEX_BASE}/output}"
ERROR_DIR="${ERROR_DIR:-/capstor/scratch/cscs/inesaltemir/MERGE_logs/${TARGET_INDEX_BASE}/err}"

# Performance parameters
BATCH_SIZE="${BATCH_SIZE:-10000}"
MERGE_CLUSTER_PORT_BASE="${MERGE_CLUSTER_PORT_BASE:-9200}"

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

echo "=== Elasticsearch Merge Job Generator ==="
log_info "Merge script path: $MERGE_SCRIPT"

# Determine source of directory list
if [ -n "$SOURCE_DATA_FILE" ] && [ -f "$SOURCE_DATA_FILE" ]; then
    log_info "Reading source directories from file: $SOURCE_DATA_FILE"
    SOURCE_MODE="file"
else
    log_info "Using glob pattern for source directories: $SOURCE_DATA_PATTERN"
    SOURCE_MODE="pattern"
fi

log_info "Number of target indexes: $NUM_TARGET_INDEXES"
log_info "Target index base name: $TARGET_INDEX_BASE"
log_info "Source index patterns: $SOURCE_INDEX_PATTERNS"
log_info "Output directory: $OUTPUT_DIR"
log_info "Error directory: $ERROR_DIR"
log_info "Batch size: $BATCH_SIZE"
log_info "Merge cluster port base: $MERGE_CLUSTER_PORT_BASE"
echo

# Validate merge script exists
if [ ! -f "$MERGE_SCRIPT" ]; then
    log_error "Merge script not found: $MERGE_SCRIPT"
    exit 1
fi

# Find all matching source data directories
log_info "Finding source data directories..."

if [ "$SOURCE_MODE" = "file" ]; then
    # Read directories from file
    mapfile -t matching_dirs < <(grep -v '^#' "$SOURCE_DATA_FILE" | grep -v '^[[:space:]]*$' | sed 's/[[:space:]]*$//')
    
    # Validate that directories exist
    valid_dirs=()
    for dir in "${matching_dirs[@]}"; do
        if [ -d "$dir" ]; then
            valid_dirs+=("$dir")
        else
            log_warn "Directory does not exist (skipping): $dir"
        fi
    done
    matching_dirs=("${valid_dirs[@]}")
else
    # Use glob pattern
    shopt -s nullglob
    matching_dirs=($SOURCE_DATA_PATTERN)
    shopt -u nullglob
fi

if [ ${#matching_dirs[@]} -eq 0 ]; then
    if [ "$SOURCE_MODE" = "file" ]; then
        log_error "No valid directories found in file: $SOURCE_DATA_FILE"
        log_info "Make sure the file contains one directory path per line"
        if [ -f "$SOURCE_DATA_FILE" ]; then
            log_info "File contents (first 10 lines):"
            head -10 "$SOURCE_DATA_FILE" | sed 's/^/  /'
        fi
    else
        log_error "No directories found matching pattern: $SOURCE_DATA_PATTERN"
        log_info "Searched in: $(dirname "$SOURCE_DATA_PATTERN")"
        
        # Show available directories for debugging
        base_dir=$(dirname "$SOURCE_DATA_PATTERN")
        if [ -d "$base_dir" ]; then
            log_info "Available es-data directories in $base_dir:"
            find "$base_dir" -maxdepth 1 -name "es-data-*" -type d 2>/dev/null | head -10 || log_warn "No es-data directories found"
        fi
    fi
    exit 1
fi

log_success "Found ${#matching_dirs[@]} source data directories"
for dir in "${matching_dirs[@]}"; do
    echo "  - $(basename "$dir")"
done
echo

# Validate number of target indexes
if [ $NUM_TARGET_INDEXES -lt 1 ]; then
    log_error "Number of target indexes must be at least 1"
    exit 1
fi

if [ $NUM_TARGET_INDEXES -gt ${#matching_dirs[@]} ]; then
    log_warn "Number of target indexes ($NUM_TARGET_INDEXES) exceeds number of source directories (${#matching_dirs[@]})"
    log_warn "Some target indexes will have fewer sources"
fi

# Calculate directories per job
TOTAL_DIRS=${#matching_dirs[@]}
DIRS_PER_JOB=$((TOTAL_DIRS / NUM_TARGET_INDEXES))
REMAINDER=$((TOTAL_DIRS % NUM_TARGET_INDEXES))

log_info "Total source directories: $TOTAL_DIRS"
log_info "Directories per merge job: ~$DIRS_PER_JOB"
echo

# Generate submission commands
log_success "=== Generated Job Submission Commands ==="
echo

for ((i=0; i<NUM_TARGET_INDEXES; i++)); do
    # Calculate start and end directory indices
    DIR_START=$((i * DIRS_PER_JOB))
    
    # Distribute remainder across first jobs
    if [ $i -lt $REMAINDER ]; then
        DIR_START=$((DIR_START + i))
        DIR_END=$((DIR_START + DIRS_PER_JOB + 1))
    else
        DIR_START=$((DIR_START + REMAINDER))
        DIR_END=$((DIR_START + DIRS_PER_JOB))
    fi
    
    # Ensure last job gets all remaining directories
    if [ $i -eq $((NUM_TARGET_INDEXES - 1)) ]; then
        DIR_END=$TOTAL_DIRS
    fi
    
    # Get the slice of directories for this job
    job_dirs=("${matching_dirs[@]:$DIR_START:$((DIR_END - DIR_START))}")
    
    # Format job number with leading zeros
    JOB_NUM=$(printf "%03d" $((i+1)))
    
    # Create unique target index name
    if [ $NUM_TARGET_INDEXES -eq 1 ]; then
        TARGET_INDEX_NAME="${TARGET_INDEX_BASE}"
    else
        TARGET_INDEX_NAME="${TARGET_INDEX_BASE}_part_${JOB_NUM}"
    fi
    
    # Assign unique port for each merge cluster to avoid conflicts
    MERGE_PORT=$((MERGE_CLUSTER_PORT_BASE + i))
    
    # Build space-separated list of source directories for this job
    SOURCE_DIRS_LIST="${job_dirs[@]}"
    
    # Generate job submission command
    cat << EOF
# ============================================================================
# Merge Job $JOB_NUM: $TARGET_INDEX_NAME
# Processing ${#job_dirs[@]} source directories (indices $DIR_START to $((DIR_END-1)))
# Merge cluster port: $MERGE_PORT
# ============================================================================
EOF
    
    # List the directories being processed
    echo "# Source directories for this job:"
    for dir in "${job_dirs[@]}"; do
        echo "#   - $(basename "$dir")"
    done
    echo "#"
    
    # Export environment variables and submit job
    cat << EOF
export SOURCE_DATA_DIRS="$SOURCE_DIRS_LIST"
export SOURCE_INDEX_PATTERNS="$SOURCE_INDEX_PATTERNS"
export TARGET_INDEX="$TARGET_INDEX_NAME"
export MERGE_CLUSTER_PORT="$MERGE_PORT"
export BATCH_SIZE="$BATCH_SIZE"

sbatch --job-name="merge_${JOB_NUM}_${TARGET_INDEX_NAME}" \\
       --output="${OUTPUT_DIR}/merge_${JOB_NUM}_%j.out" \\
       --error="${ERROR_DIR}/merge_${JOB_NUM}_%j.err" \\
       $MERGE_SCRIPT

EOF
    echo
done

# Generate summary
cat << 'EOF'
# ============================================================================
# SUMMARY AND INSTRUCTIONS
# ============================================================================
EOF

echo "# Total merge jobs to submit: $NUM_TARGET_INDEXES"
echo "# Total source directories: $TOTAL_DIRS"
echo "# Directories per job: ~$DIRS_PER_JOB"
echo "#"
echo "# Target indexes that will be created:"
for ((i=0; i<NUM_TARGET_INDEXES; i++)); do
    JOB_NUM=$(printf "%03d" $((i+1)))
    if [ $NUM_TARGET_INDEXES -eq 1 ]; then
        echo "#   - ${TARGET_INDEX_BASE}"
    else
        echo "#   - ${TARGET_INDEX_BASE}_part${JOB_NUM}"
    fi
done
echo "#"
echo "# Merge cluster ports: $MERGE_CLUSTER_PORT_BASE to $((MERGE_CLUSTER_PORT_BASE + NUM_TARGET_INDEXES - 1))"
echo "# Output logs: ${OUTPUT_DIR}/merge_XXX_%j.out"
echo "# Error logs: ${ERROR_DIR}/merge_XXX_%j.err"
echo "# Batch size: $BATCH_SIZE"
echo "# Source index patterns: $SOURCE_INDEX_PATTERNS"
echo "#"
echo "# ============================================================================"
echo "# PREPARATION STEPS:"
echo "# ============================================================================"
echo "# 1. Create log directories:"
echo "#    mkdir -p $OUTPUT_DIR"
echo "#    mkdir -p $ERROR_DIR"
echo "#"
echo "# 2. Verify merge script exists and is executable:"
echo "#    ls -la $MERGE_SCRIPT"
echo "#    chmod +x $MERGE_SCRIPT"
echo "#"
if [ "$SOURCE_MODE" = "file" ]; then
    echo "# 3. Verify source directories file:"
    echo "#    cat $SOURCE_DATA_FILE"
    echo "#"
fi
echo "# 4. Review the generated commands above"
echo "#"
echo "# ============================================================================"
echo "# SUBMISSION OPTIONS:"
echo "# ============================================================================"
if [ "$SOURCE_MODE" = "file" ]; then
    echo "# Using source directories from: $SOURCE_DATA_FILE"
    echo "#"
fi
echo "# Option 1: Submit all jobs at once (RECOMMENDED)"
echo "#    bash $0 | grep -E '^(export|sbatch)' | bash"
echo "#"
echo "# Option 2: Submit jobs one at a time"
echo "#    # Copy and paste each export/sbatch block manually"
echo "#"
echo "# Option 3: Save to file and review before submitting"
echo "#    bash $0 > merge_jobs.sh"
echo "#    # Review merge_jobs.sh"
echo "#    bash merge_jobs.sh"
echo "#"
echo "# ============================================================================"
echo "# POST-MERGE VERIFICATION:"
echo "# ============================================================================"
echo "# After jobs complete, verify the merged indexes:"
echo "# 1. Check SLURM job status:"
echo "#    squeue -u \$USER"
echo "#"
echo "# 2. Review log files for errors:"
echo "#    tail -n 50 $ERROR_DIR/merge_*_*.err"
echo "#    grep -i error $ERROR_DIR/merge_*_*.err"
echo "#"
echo "# 3. Check if target indexes were created (requires active ES cluster):"
echo "#    # Start a verification cluster on one of the target data directories"
echo "#    # then query: curl http://localhost:9200/_cat/indices?v"
echo "#"
echo "# 4. Verify document counts match expectations:"
echo "#    grep 'Final index count' $OUTPUT_DIR/merge_*_*.out"
echo "#"
echo "# ============================================================================"

# Final validation summary
echo
log_info "Configuration validation:"
if [ "$SOURCE_MODE" = "file" ]; then
    log_info "  ✓ Reading from file: $SOURCE_DATA_FILE"
fi
log_info "  ✓ Found $TOTAL_DIRS source directories"
log_info "  ✓ Will create $NUM_TARGET_INDEXES target index(es)"
log_info "  ✓ Merge script exists: $MERGE_SCRIPT"
log_info "  ✓ Job commands generated successfully"
echo
log_success "Job generator completed successfully!"
log_info "Review the commands above and submit when ready."