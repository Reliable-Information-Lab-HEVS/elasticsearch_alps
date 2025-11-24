#!/bin/bash
# Generate SLURM job submission commands for Elasticsearch indexing with file range splitting

# Configuration
DATA_DIR_PATTERN="${DATA_DIR_PATTERN:-/iopsstor/scratch/cscs/arthur/megamath-web/megamath-web/data/output}"
TOTAL_JOBS="${TOTAL_JOBS:-5}"
SCRIPT_PATH="/capstor/scratch/cscs/arthur/apertus-pretraining-data-indexing/scripts/index/index.sh"

# Extract index name from data directory pattern (last component before wildcard)
# Example: /path/to/dataset_part_* -> dataset
INDEX_BASE_NAME="${INDEX_BASE_NAME:=megamath-web-pro}"

# Output directories for logs
OUTPUT_DIR="${OUTPUT_DIR:-/iopsstor/scratch/cscs/arthur/${INDEX_BASE_NAME}/output}"
ERROR_DIR="${ERROR_DIR:-/iopsstor/scratch/cscs/arthur/${INDEX_BASE_NAME}/err}"

# Performance parameters
# batch_size: 5k is too small (too much http overhead, too many small bulk requests)
# batch_size: 10k or 15k
# thread_count: too much pressure on ES if high
# thread_count: try 4 or 6
# queue_size: 12 or 16
BATCH_SIZE="${BATCH_SIZE:-10000}"
THREAD_COUNT="${THREAD_COUNT:-16}"
QUEUE_SIZE="${QUEUE_SIZE:-32}"

echo "=== Elasticsearch Indexing Job Generator ==="
echo "Script path: $SCRIPT_PATH"
echo "Data directory pattern: $DATA_DIR_PATTERN"
echo "Index base name: $INDEX_BASE_NAME"
echo "Total jobs: $TOTAL_JOBS"
echo "Output directory: $OUTPUT_DIR"
echo "Error directory: $ERROR_DIR"
echo "Batch size: $BATCH_SIZE"
echo "Thread count: $THREAD_COUNT"
echo "Queue size: $QUEUE_SIZE"
echo

# First, count total files across all matching directories
echo "Counting total parquet files..."
shopt -s nullglob
matching_dirs=($DATA_DIR_PATTERN)
shopt -u nullglob

if [ ${#matching_dirs[@]} -eq 0 ]; then
    echo "ERROR: No directories found matching pattern: $DATA_DIR_PATTERN"
    exit 1
fi

echo "Found ${#matching_dirs[@]} directories matching pattern"

TOTAL_FILES=0
for dir in "${matching_dirs[@]}"; do
    file_count=$(find "$dir" -name "*.parquet" 2>/dev/null | wc -l)
    TOTAL_FILES=$((TOTAL_FILES + file_count))
    echo "  - $(basename "$dir"): $file_count files"
done

if [ $TOTAL_FILES -eq 0 ]; then
    echo "ERROR: No parquet files found in any matching directories"
    exit 1
fi

echo
echo "Total parquet files: $TOTAL_FILES"
echo "Files per job: ~$((TOTAL_FILES / TOTAL_JOBS))"
echo

# Calculate files per job
FILES_PER_JOB=$((TOTAL_FILES / TOTAL_JOBS))
REMAINDER=$((TOTAL_FILES % TOTAL_JOBS))

echo "=== Generated Job Submission Commands ==="
echo

for ((i=0; i<TOTAL_JOBS; i++)); do
    # Calculate start and end file indices
    FILE_START=$((i * FILES_PER_JOB))
    
    # Distribute remainder across first jobs
    if [ $i -lt $REMAINDER ]; then
        FILE_START=$((FILE_START + i))
        FILE_END=$((FILE_START + FILES_PER_JOB + 1))
    else
        FILE_START=$((FILE_START + REMAINDER))
        FILE_END=$((FILE_START + FILES_PER_JOB))
    fi
    
    # Ensure last job gets all remaining files
    if [ $i -eq $((TOTAL_JOBS - 1)) ]; then
        FILE_END=$TOTAL_FILES
    fi
    
    # Format job number with leading zeros
    JOB_NUM=$(printf "%03d" $((i+1)))
    
    # Create unique index name with part number
    INDEX_NAME="${INDEX_BASE_NAME}_part_${JOB_NUM}"
    
    # Generate job submission command
    cat << EOF
# Job $JOB_NUM: Processing files $FILE_START to $FILE_END ($(($FILE_END - $FILE_START)) files)
export DATA_DIR="$DATA_DIR_PATTERN"
export FILE_RANGE_START="$FILE_START"
export FILE_RANGE_END="$FILE_END"
export INDEX_NAME="$INDEX_NAME"
export JOB_PART_NUM="$JOB_NUM"
export BATCH_SIZE="$BATCH_SIZE"
export THREAD_COUNT="$THREAD_COUNT"
export QUEUE_SIZE="$QUEUE_SIZE"
sbatch --job-name="index_${JOB_NUM}" \\
       --output="${OUTPUT_DIR}/indexing_%j.out" \\
       --error="${ERROR_DIR}/indexing_%j.err" \\
       $SCRIPT_PATH

EOF
done

echo "# ==========================="
echo "# Summary:"
echo "# Total jobs: $TOTAL_JOBS"
echo "# Total files: $TOTAL_FILES"
echo "# Files per job: ~$FILES_PER_JOB"
echo "# Index pattern: ${INDEX_BASE_NAME}_part_XXX"
echo "# Output logs: ${OUTPUT_DIR}/indexing_XXX_%j.out"
echo "# Error logs: ${ERROR_DIR}/indexing_XXX_%j.err"
echo "# Batch size: $BATCH_SIZE"
echo "# Thread count: $THREAD_COUNT"
echo "# Queue size: $QUEUE_SIZE"
echo "# ES data dirs: /iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-${INDEX_BASE_NAME}_part_XXX-\${SLURM_JOB_ID}"
echo "# ES logs dirs: /iopsstor/scratch/cscs/inesaltemir/es-logs-septemberv1-${INDEX_BASE_NAME}_part_XXX-\${SLURM_JOB_ID}"
echo "#"
echo "# NOTE: Make sure these directories exist before submitting:"
echo "# mkdir -p $OUTPUT_DIR"
echo "# mkdir -p $ERROR_DIR"
echo "#"
echo "# To submit all jobs, run:"
echo "# bash $0 | grep -E '^(export|sbatch)' | bash"
echo "# ==========================="
