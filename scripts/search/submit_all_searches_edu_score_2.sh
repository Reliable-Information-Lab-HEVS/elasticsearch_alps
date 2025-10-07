=== Elasticsearch Search Job Generator ===
Script path: /capstor/scratch/cscs/inesaltemir/scripts/search/new_search.sh
CSV file: /capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv
Dataset: pure_text
Output results base: /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308
Output logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output
Error logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err

Reading ES data directories from: /capstor/scratch/cscs/inesaltemir/scripts/indexing/job_status_logs/edu-score-2/list_dir/list_dir_edu-score-2_920282_920316.txt
Found 35 ES data directories

=== Generated Job Submission Commands ===

# Job 001: swissai-fineweb-edu-score-2-filterrobots-merge_part_001
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_001-920282"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_001"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_001_obscene_en_"
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

# Job 002: swissai-fineweb-edu-score-2-filterrobots-merge_part_002
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_002-920283"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_002"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_002_obscene_en_"
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

# Job 003: swissai-fineweb-edu-score-2-filterrobots-merge_part_003
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_003-920284"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_003"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_003_obscene_en_"
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

# Job 004: swissai-fineweb-edu-score-2-filterrobots-merge_part_004
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_004-920285"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_004"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_004_obscene_en_"
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

# Job 005: swissai-fineweb-edu-score-2-filterrobots-merge_part_005
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_005-920286"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_005"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_005_obscene_en_"
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

# Job 006: swissai-fineweb-edu-score-2-filterrobots-merge_part_006
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_006-920287"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_006"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_006_obscene_en_"
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

# Job 007: swissai-fineweb-edu-score-2-filterrobots-merge_part_007
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_007-920288"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_007"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_007_obscene_en_"
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

# Job 008: swissai-fineweb-edu-score-2-filterrobots-merge_part_008
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_008-920289"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_008"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_008_obscene_en_"
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

# Job 009: swissai-fineweb-edu-score-2-filterrobots-merge_part_009
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_009-920290"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_009"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_009_obscene_en_"
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

# Job 010: swissai-fineweb-edu-score-2-filterrobots-merge_part_010
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_010-920291"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_010"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_010_obscene_en_"
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

# Job 011: swissai-fineweb-edu-score-2-filterrobots-merge_part_011
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_011-920292"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_011"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_011_obscene_en_"
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

# Job 012: swissai-fineweb-edu-score-2-filterrobots-merge_part_012
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_012-920293"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_012"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_012_obscene_en_"
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

# Job 013: swissai-fineweb-edu-score-2-filterrobots-merge_part_013
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_013-920294"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_013"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_013_obscene_en_"
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

# Job 014: swissai-fineweb-edu-score-2-filterrobots-merge_part_014
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_014-920295"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_014"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_014_obscene_en_"
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

# Job 015: swissai-fineweb-edu-score-2-filterrobots-merge_part_015
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_015-920296"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_015"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_015_obscene_en_"
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

# Job 016: swissai-fineweb-edu-score-2-filterrobots-merge_part_016
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_016-920297"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_016"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_016_obscene_en_"
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

# Job 017: swissai-fineweb-edu-score-2-filterrobots-merge_part_017
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_017-920298"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_017"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_017_obscene_en_"
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

# Job 018: swissai-fineweb-edu-score-2-filterrobots-merge_part_018
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_018-920299"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_018"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_018_obscene_en_"
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

# Job 019: swissai-fineweb-edu-score-2-filterrobots-merge_part_019
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_019-920300"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_019"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_019_obscene_en_"
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

# Job 020: swissai-fineweb-edu-score-2-filterrobots-merge_part_020
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_020-920301"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_020"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_020_obscene_en_"
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

# Job 021: swissai-fineweb-edu-score-2-filterrobots-merge_part_021
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_021-920302"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_021"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_021_obscene_en_"
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

# Job 022: swissai-fineweb-edu-score-2-filterrobots-merge_part_022
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_022-920303"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_022"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_022_obscene_en_"
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

# Job 023: swissai-fineweb-edu-score-2-filterrobots-merge_part_023
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_023-920304"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_023"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_023_obscene_en_"
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

# Job 024: swissai-fineweb-edu-score-2-filterrobots-merge_part_024
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_024-920305"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_024"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_024_obscene_en_"
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

# Job 025: swissai-fineweb-edu-score-2-filterrobots-merge_part_025
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_025-920306"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_025"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_025_obscene_en_"
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

# Job 026: swissai-fineweb-edu-score-2-filterrobots-merge_part_026
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_026-920307"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_026"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_026_obscene_en_"
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

# Job 027: swissai-fineweb-edu-score-2-filterrobots-merge_part_027
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_027-920308"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_027"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_027_obscene_en_"
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

# Job 028: swissai-fineweb-edu-score-2-filterrobots-merge_part_028
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_028-920309"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_028"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_028_obscene_en_"
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

# Job 029: swissai-fineweb-edu-score-2-filterrobots-merge_part_029
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_029-920310"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_029"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_029_obscene_en_"
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

# Job 030: swissai-fineweb-edu-score-2-filterrobots-merge_part_030
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_030-920311"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_030"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_030_obscene_en_"
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

# Job 031: swissai-fineweb-edu-score-2-filterrobots-merge_part_031
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_031-920312"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_031"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_031_obscene_en_"
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

# Job 032: swissai-fineweb-edu-score-2-filterrobots-merge_part_032
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_032-920313"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_032"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_032_obscene_en_"
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

# Job 033: swissai-fineweb-edu-score-2-filterrobots-merge_part_033
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_033-920314"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_033"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_033_obscene_en_"
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

# Job 034: swissai-fineweb-edu-score-2-filterrobots-merge_part_034
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_034-920315"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_034"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_034_obscene_en_"
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

# Job 035: swissai-fineweb-edu-score-2-filterrobots-merge_part_035
export PATH_DATA="/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_035-920316"
export CSV_FILE="/capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv"
export INDEX_NAME="swissai-fineweb-edu-score-2-filterrobots-merge_part_035"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/swissai-fineweb-edu-score-2-filterrobots-merge_part_035_obscene_en_"
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

# ===========================
# Summary:
# Total search jobs: 35
# CSV file: /capstor/scratch/cscs/inesaltemir/scripts/search_queries/Obscene/obscene_en.csv (basename: obscene_en)
# Dataset: pure_text
# Results pattern: /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/20251007_153308/{INDEX_NAME}_obscene_en/
# Output logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output/search_%j.out
# Error logs: /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err/search_%j.err
#
# NOTE: Make sure these directories exist before submitting:
# mkdir -p /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/output
# mkdir -p /capstor/scratch/cscs/inesaltemir/SEARCH_LOGS/err
#
# To submit all jobs, run:
# bash ./job_generator_search_edu_score_2.sh | grep -E '^(export|sbatch)' | bash
# ===========================
