#!/bin/bash
# Generate SLURM job submission commands for 19TB detokenization of fw-edu-score-2

TOTAL_JOBS=2  # Change to 80 for more conservative approach
SCRIPT_PATH="/capstor/scratch/cscs/inesaltemir/scripts/detokenize/detokenize.sh"

echo "Generating $TOTAL_JOBS jobs for 400GB detokenization..."
echo "Each job will process approximately $(echo "scale=2; 19000 / $TOTAL_JOBS" | bc) GB"
echo

# Calculate increment per job
INCREMENT=$(echo "scale=6; 100.0 / $TOTAL_JOBS" | bc)

for ((i=0; i<TOTAL_JOBS; i++)); do
    # Calculate start and end percentages
    START=$(echo "scale=6; $INCREMENT * $i" | bc)
    END=$(echo "scale=6; $INCREMENT * ($i + 1)" | bc)
    
    # Format job number with leading zeros
    JOB_NUM=$(printf "%03d" $((i+1)))
    
    # Create job-specific environment variables
    cat << EOF
# Job $JOB_NUM: Processing ${START}% to ${END}%
export FILE_RANGE_START="$START"
export FILE_RANGE_END="$END"
export OUTPUT_DIR="/capstor/scratch/cscs/inesaltemir/detokenized_output/swissai-fineweb-2-quality_33-filterrobots-merge_rest_part_$JOB_NUM"
sbatch --job-name="detok_${JOB_NUM}" $SCRIPT_PATH

EOF
done

echo "# Summary:"
echo "# Total jobs: $TOTAL_JOBS"
echo "# Range per job: $INCREMENT%"
echo "# Estimated time per job: ~7.5 hours"
echo "# Total estimated time: ~$TOTAL_JOBS jobs running in parallel"