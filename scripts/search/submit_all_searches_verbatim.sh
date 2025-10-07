=== Elasticsearch Search Job Generator ===
Script path: /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh
CSV file: /capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2-single_index_920282_intervals.csv
Dataset: pure_text
Output results base: /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_202353
Output logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output
Error logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err

Reading ES data directories from: /capstor/scratch/cscs/inesaltemir/scripts/search/list_dir_verbatim.txt
Found 2 ES data directories

=== Generated Job Submission Commands ===

# Job 001: september_brouillon1
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-target-september_brouillon1-923479"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2-single_index_920282_intervals.csv"
export INDEX_NAME="september_brouillon1"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_202353/september_brouillon1_fw-edu-score-2-single_index_920282_intervals_"
export DATASET="pure_text"
export EXECUTE_MATCH_QUERY="false"
export EXECUTE_MATCH_PHRASE_QUERY="true"
export EXECUTE_TERM_QUERY_EXACT="false"
export EXECUTE_WILDCARD_QUERY="false"
export EXECUTE_FUZZY_QUERY="false"
export EXECUTE_BOOL_MUST_QUERY="false"
export BOOL_MUST_OPERATOR="or"
export BOOL_MUST_MAX_WORDS="3"
export BOOL_MUST_MINIMUM_SHOULD_MATCH="50%"
sbatch --job-name="search_001" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 002: swissai-fineweb-edu-score-2-filterrobots-merge_part_001
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_001-920282"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2-single_index_920282_intervals.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_001"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_202353/swissai-fineweb-edu-score-2-filterrobots-merge_part_001_fw-edu-score-2-single_index_920282_intervals_"
export DATASET="pure_text"
export EXECUTE_MATCH_QUERY="false"
export EXECUTE_MATCH_PHRASE_QUERY="true"
export EXECUTE_TERM_QUERY_EXACT="false"
export EXECUTE_WILDCARD_QUERY="false"
export EXECUTE_FUZZY_QUERY="false"
export EXECUTE_BOOL_MUST_QUERY="false"
export BOOL_MUST_OPERATOR="or"
export BOOL_MUST_MAX_WORDS="3"
export BOOL_MUST_MINIMUM_SHOULD_MATCH="50%"
sbatch --job-name="search_002" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# ===========================
# Summary:
# Total search jobs: 2
# CSV file: /capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2-single_index_920282_intervals.csv (basename: fw-edu-score-2-single_index_920282_intervals)
# Dataset: pure_text
# Results pattern: /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_202353/{INDEX_NAME}_fw-edu-score-2-single_index_920282_intervals/
# Output logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out
# Error logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err
#
# NOTE: Make sure these directories exist before submitting:
# mkdir -p /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output
# mkdir -p /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err
#
# To submit all jobs, run:
# bash ./job_generator_verbatim.sh | grep -E '^(export|sbatch)' | bash
# ===========================
