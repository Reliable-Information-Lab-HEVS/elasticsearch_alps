=== Elasticsearch Search Job Generator ===
Script path: /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh
CSV file: /capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv
Dataset: pure_text
Output results base: /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010
Output logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output
Error logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err

Reading ES data directories from: /capstor/scratch/cscs/inesaltemir/scripts/indexing/job_status_logs/list_dir_fw_2_33.txt
Found 39 ES data directories

=== Generated Job Submission Commands ===

# Job 001: swissai-fineweb-2-quality_33-filterrobots-merge_euro-mid
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_euro-mid-926701"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_euro-mid"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_euro-mid_chemicals_ita_"
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

# Job 002: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_001
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_001-923585"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_001"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_001_chemicals_ita_"
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

# Job 003: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_002
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_002-921729"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_002"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_002_chemicals_ita_"
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
sbatch --job-name="search_003" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 004: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_003
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_003-921730"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_003"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_003_chemicals_ita_"
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
sbatch --job-name="search_004" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 005: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_004
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_004-921731"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_004"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_004_chemicals_ita_"
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
sbatch --job-name="search_005" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 006: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_005
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_005-921732"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_005"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_005_chemicals_ita_"
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
sbatch --job-name="search_006" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 007: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_006
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_006-921733"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_006"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_006_chemicals_ita_"
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
sbatch --job-name="search_007" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 008: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_007
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_007-921734"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_007"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_007_chemicals_ita_"
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
sbatch --job-name="search_008" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 009: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_008
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_008-923588"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_008"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_008_chemicals_ita_"
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
sbatch --job-name="search_009" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 010: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_009
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_009-921736"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_009"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_009_chemicals_ita_"
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
sbatch --job-name="search_010" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 011: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_010
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_010-921737"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_010"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_010_chemicals_ita_"
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
sbatch --job-name="search_011" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 012: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_011
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_011-921738"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_011"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_011_chemicals_ita_"
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
sbatch --job-name="search_012" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 013: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_012
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_012-921739"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_012"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_012_chemicals_ita_"
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
sbatch --job-name="search_013" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 014: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_013
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_013-921740"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_013"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_013_chemicals_ita_"
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
sbatch --job-name="search_014" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 015: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_014
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_014-921741"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_014"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_014_chemicals_ita_"
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
sbatch --job-name="search_015" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 016: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_015
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_015-921742"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_015"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_015_chemicals_ita_"
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
sbatch --job-name="search_016" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 017: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_016
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_016-921743"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_016"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_016_chemicals_ita_"
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
sbatch --job-name="search_017" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 018: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_017
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_017-921744"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_017"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_017_chemicals_ita_"
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
sbatch --job-name="search_018" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 019: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_018
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_018-921745"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_018"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_018_chemicals_ita_"
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
sbatch --job-name="search_019" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 020: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_019
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_019-921746"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_019"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_019_chemicals_ita_"
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
sbatch --job-name="search_020" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 021: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_020
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_020-921747"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_020"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_020_chemicals_ita_"
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
sbatch --job-name="search_021" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 022: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_021
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_021-921748"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_021"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_021_chemicals_ita_"
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
sbatch --job-name="search_022" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 023: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_022
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_022-921749"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_022"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_022_chemicals_ita_"
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
sbatch --job-name="search_023" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 024: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_023
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_023-921750"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_023"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_023_chemicals_ita_"
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
sbatch --job-name="search_024" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 025: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_024
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_024-921751"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_024"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_024_chemicals_ita_"
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
sbatch --job-name="search_025" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 026: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_025
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_025-921752"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_025"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_025_chemicals_ita_"
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
sbatch --job-name="search_026" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 027: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_026
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_026-921753"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_026"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_026_chemicals_ita_"
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
sbatch --job-name="search_027" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 028: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_027
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_027-921754"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_027"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_027_chemicals_ita_"
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
sbatch --job-name="search_028" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 029: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_028
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_028-921755"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_028"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_028_chemicals_ita_"
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
sbatch --job-name="search_029" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 030: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_029
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_029-921756"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_029"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_029_chemicals_ita_"
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
sbatch --job-name="search_030" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 031: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_030
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_030-921757"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_030"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_030_chemicals_ita_"
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
sbatch --job-name="search_031" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 032: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_031
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_031-921758"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_031"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_031_chemicals_ita_"
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
sbatch --job-name="search_032" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 033: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_032
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_032-921759"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_032"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_032_chemicals_ita_"
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
sbatch --job-name="search_033" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 034: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_033
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_033-921760"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_033"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_033_chemicals_ita_"
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
sbatch --job-name="search_034" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 035: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_034
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_034-921761"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_034"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_034_chemicals_ita_"
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
sbatch --job-name="search_035" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 036: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_035
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_035-921762"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_035"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_035_chemicals_ita_"
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
sbatch --job-name="search_036" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 037: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_036
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_036-925025"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_036"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_036_chemicals_ita_"
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
sbatch --job-name="search_037" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh

# Job 038: swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_037
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_037-925033"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv"
export INDEX_NAME="swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_037"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/swissai-fineweb-2-quality_33-filterrobots-merge_other-high_part_037_chemicals_ita_"
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
sbatch --job-name="search_038" \
       --output="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out" \
       --error="/capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err" \
       /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh


# ===========================
# Summary:
# Total search jobs: 39
# CSV file: /capstor/scratch/cscs/inesaltemir/scripts/search_queries/chemicals/chemicals_ita.csv (basename: chemicals_ita)
# Dataset: pure_text
# Results pattern: /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_165010/{INDEX_NAME}_chemicals_ita/
# Output logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out
# Error logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err
#
# NOTE: Make sure these directories exist before submitting:
# mkdir -p /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output
# mkdir -p /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err
#
# To submit all jobs, run:
# bash ./job_generator_search_fw2_33.sh | grep -E '^(export|sbatch)' | bash
# ===========================
