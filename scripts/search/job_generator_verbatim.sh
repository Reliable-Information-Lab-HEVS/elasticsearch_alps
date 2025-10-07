#!/bin/bash
# Generate SLURM job submission commands for Elasticsearch search queries

# Configuration
ES_DATA_DIRS_FILE="${ES_DATA_DIRS_FILE:-/capstor/scratch/cscs/inesaltemir/scripts/search/list_dir_verbatim.txt}" 

CSV_FILE="${CSV_FILE:-/capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2-single_index_920282_intervals.csv}"
# /capstor/scratch/cscs/inesaltemir/scripts/search_queries/WeaponizedWords/ww_en.csv
# /capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv
# /capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_en.csv

SCRIPT_PATH="${SCRIPT_PATH:-/capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh}"

# Dataset type for field extraction
DATASET="${DATASET:-pure_text}"  # Can be 'fineweb' or 'sft' or 'pure_text'

# Query execution configuration
EXECUTE_MATCH_QUERY="${EXECUTE_MATCH_QUERY:-false}"
EXECUTE_MATCH_PHRASE_QUERY="${EXECUTE_MATCH_PHRASE_QUERY:-true}"
EXECUTE_TERM_QUERY_EXACT="${EXECUTE_TERM_QUERY_EXACT:-false}"
EXECUTE_WILDCARD_QUERY="${EXECUTE_WILDCARD_QUERY:-false}"
EXECUTE_FUZZY_QUERY="${EXECUTE_FUZZY_QUERY:-false}"
EXECUTE_BOOL_MUST_QUERY="${EXECUTE_BOOL_MUST_QUERY:-false}"

# MATCH_QUERY_OPERATOR="${MATCH_QUERY_OPERATOR:-[\"or\"]}"
# MATCH_PHRASE_SLOP="${MATCH_PHRASE_SLOP:-[0]}"
BOOL_MUST_OPERATOR="${BOOL_MUST_OPERATOR:-or}"
BOOL_MUST_MAX_WORDS="${BOOL_MUST_MAX_WORDS:-3}"
BOOL_MUST_MINIMUM_SHOULD_MATCH="${BOOL_MUST_MINIMUM_SHOULD_MATCH:-50%}"

JOB_GROUP_ID=$(date +%Y%m%d_%H%M%S)

# Output directories for logs - now includes job group ID
OUTPUT_DIR_BASE="${OUTPUT_DIR_BASE:-/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/${JOB_GROUP_ID}}"

# Output directories for logs
# OUTPUT_DIR_BASE="${OUTPUT_DIR_BASE:-/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS}"
OUTPUT_LOGS="${OUTPUT_LOGS:-/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output}"
ERROR_LOGS="${ERROR_LOGS:-/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err}"

echo "=== Elasticsearch Search Job Generator ==="
echo "Script path: $SCRIPT_PATH"
echo "CSV file: $CSV_FILE"
echo "Dataset: $DATASET"
echo "Output results base: $OUTPUT_DIR_BASE"
echo "Output logs: $OUTPUT_LOGS"
echo "Error logs: $ERROR_LOGS"
echo

# Validate required parameters
if [ -z "$CSV_FILE" ]; then
    echo "ERROR: CSV_FILE is required"
    echo "Usage: CSV_FILE=queries.csv $0"
    echo "   Or: CSV_FILE=queries.csv ES_DATA_DIRS_FILE=dirs.txt $0"
    exit 1
fi

if [ ! -f "$CSV_FILE" ]; then
    echo "ERROR: CSV file not found: $CSV_FILE"
    exit 1
fi

CSV_BASENAME=$(basename "$CSV_FILE" .csv)

# Get list of ES data directories
if [ -n "$ES_DATA_DIRS_FILE" ] && [ -f "$ES_DATA_DIRS_FILE" ]; then
    echo "Reading ES data directories from: $ES_DATA_DIRS_FILE"
    mapfile -t es_dirs < "$ES_DATA_DIRS_FILE"
else
    echo "ERROR: ES_DATA_DIRS_FILE is required and must exist"
    echo "Create a file with ES data directories, one per line, e.g.:"
    echo "  /iopsstor/.../es-data-septemberv1-index_part_001-920282"
    echo "  /iopsstor/.../es-data-septemberv1-index_part_002-920283"
    exit 1
fi

TOTAL_JOBS=${#es_dirs[@]}

if [ $TOTAL_JOBS -eq 0 ]; then
    echo "ERROR: No ES data directories found"
    exit 1
fi

echo "Found $TOTAL_JOBS ES data directories"
echo

# swissai-fineweb-edu-score-2-filterrobots-merge_part_001-920282
# /iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_001-920282

extract_index_name_1() {
    local es_data_dir="$1"
    local basename=$(basename "$es_data_dir")
    # Remove prefix and trim any whitespace
    echo "$basename" | sed -E 's/^es-data-[^-]+-(.+)$/\1/' | xargs
}
extract_index_name() {
    local es_data_dir="$1"
    local basename=$(basename "$es_data_dir")
    
    # Extract index name from patterns like:
    # es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_035-920316
    # Should return: swissai-fineweb-edu-score-2-filterrobots-merge_part_035
    
    # Remove the 'es-data-{prefix}-' part and the trailing '-{6-digit-job-id}'
    local index_name=$(echo "$basename" | sed -E 's/^es-data-[^-]+-//; s/-[0-9]{6}$//')
    
    # Trim whitespace and return
    echo "$index_name" | xargs
}

echo "=== Generated Job Submission Commands ==="
echo

#export MATCH_QUERY_OPERATOR="$MATCH_QUERY_OPERATOR"
#export MATCH_PHRASE_SLOP="$MATCH_PHRASE_SLOP"


for ((i=0; i<TOTAL_JOBS; i++)); do
    es_data_dir="${es_dirs[$i]}"
    INDEX_NAME=$(extract_index_name "$es_data_dir")
    JOB_NUM=$(printf "%03d" $((i+1)))

    cat << EOF
# Job $JOB_NUM: $INDEX_NAME
export PATH_DATA="$es_data_dir"
export CSV_FILE="$CSV_FILE"
export INDEX_NAME="$INDEX_NAME"
export OUTPUT_DIR="${OUTPUT_DIR_BASE}/${INDEX_NAME}_${CSV_BASENAME}_"
export DATASET="$DATASET"
export EXECUTE_MATCH_QUERY="$EXECUTE_MATCH_QUERY"
export EXECUTE_MATCH_PHRASE_QUERY="$EXECUTE_MATCH_PHRASE_QUERY"
export EXECUTE_TERM_QUERY_EXACT="$EXECUTE_TERM_QUERY_EXACT"
export EXECUTE_WILDCARD_QUERY="$EXECUTE_WILDCARD_QUERY"
export EXECUTE_FUZZY_QUERY="$EXECUTE_FUZZY_QUERY"
export EXECUTE_BOOL_MUST_QUERY="$EXECUTE_BOOL_MUST_QUERY"
export BOOL_MUST_OPERATOR="$BOOL_MUST_OPERATOR"
export BOOL_MUST_MAX_WORDS="$BOOL_MUST_MAX_WORDS"
export BOOL_MUST_MINIMUM_SHOULD_MATCH="$BOOL_MUST_MINIMUM_SHOULD_MATCH"
sbatch --job-name="search_${JOB_NUM}" \\
       --output="${OUTPUT_LOGS}/search_%j.out" \\
       --error="${ERROR_LOGS}/search_%j.err" \\
       $SCRIPT_PATH

EOF
done

echo "# ==========================="
echo "# Summary:"
echo "# Total search jobs: $TOTAL_JOBS"
echo "# CSV file: $CSV_FILE (basename: $CSV_BASENAME)"
echo "# Dataset: $DATASET"
echo "# Results pattern: ${OUTPUT_DIR_BASE}/{INDEX_NAME}_${CSV_BASENAME}/"
echo "# Output logs: ${OUTPUT_LOGS}/search_%j.out"
echo "# Error logs: ${ERROR_LOGS}/search_%j.err"
echo "#"
echo "# NOTE: Make sure these directories exist before submitting:"
echo "# mkdir -p $OUTPUT_LOGS"
echo "# mkdir -p $ERROR_LOGS"
echo "#"
echo "# To submit all jobs, run:"
echo "# bash $0 | grep -E '^(export|sbatch)' | bash"
echo "# ==========================="