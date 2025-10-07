=== Elasticsearch Search Job Generator ===
Script path: /capstor/scratch/cscs/inesaltemir/scripts/search/search.sh
CSV file: /capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_en.csv
Dataset: pure_text
Output results base: /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251005_021616
Output logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output
Error logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err

Reading ES data directories from: /capstor/scratch/cscs/inesaltemir/scripts/search/list_dir_brouillon.txt
Found 1 ES data directories

=== Generated Job Submission Commands ===

# Job 001: september_brouillon1-923479 
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-target-september_brouillon1-923479"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2-september_brouillon1.csv"
export INDEX_NAME="september_brouillon1-923479 "
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/september_brouillon1-923479_verbatim"
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
       /capstor/scratch/cscs/inesaltemir/scripts/search/search.sh

# ===========================
# Summary:
# Total search jobs: 1
# CSV file: /capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_en.csv (basename: chemicals_en)
# Dataset: pure_text
# Results pattern: /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251005_021616/{INDEX_NAME}_chemicals_en/
# Output logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out
# Error logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err
#
# NOTE: Make sure these directories exist before submitting:
# mkdir -p /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output
# mkdir -p /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err
#
# To submit all jobs, run:
# bash ./job_generator_search.sh | grep -E '^(export|sbatch)' | bash
# ===========================
