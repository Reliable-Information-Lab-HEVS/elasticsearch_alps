#!/usr/bin/env python3
"""
Script to sample text segments of varying lengths from parquet files.
Optimized for large files with random row selection.
"""

import glob
import logging
from pathlib import Path
from typing import Optional, List
import random
import csv
import pyarrow.parquet as pq
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def sample_segments_from_file(
    parquet_file: Path,
    samples_per_length: int,
    text_column: str = 'text',
    batch_size: int = 10000
) -> dict:
    """
    Sample text segments from a single parquet file with random row selection.
    
    Args:
        parquet_file: Path to parquet file
        samples_per_length: Number of samples to collect for each length (1-300 words)
        text_column: Name of the text column in parquet
        batch_size: Number of rows to read at once
    
    Returns:
        Dictionary mapping length -> list of sampled segments
    """
    samples = {length: [] for length in range(1, 301)}
    
    try:
        # Open parquet file and get total row count
        parquet_file_obj = pq.ParquetFile(parquet_file)
        total_rows = parquet_file_obj.metadata.num_rows
        
        if total_rows == 0:
            logger.warning(f"File {parquet_file.name} has 0 rows")
            return samples
        
        # Generate random row indices to sample
        # We need enough rows to potentially get samples for all lengths
        # Oversample to account for short texts
        n_rows_to_sample = min(samples_per_length * 10, total_rows)
        sample_indices = sorted(random.sample(range(total_rows), n_rows_to_sample))
        
        logger.info(f"Sampling {n_rows_to_sample} random rows from {parquet_file.name} (total: {total_rows})")
        
        # Read sampled rows in batches
        sampled_texts = []
        current_batch_start = 0
        
        for batch in parquet_file_obj.iter_batches(batch_size=batch_size):
            batch_df = batch.to_pandas()
            current_batch_end = current_batch_start + len(batch_df)
            
            # Find which sample indices fall in this batch
            batch_indices = [
                idx - current_batch_start 
                for idx in sample_indices 
                if current_batch_start <= idx < current_batch_end
            ]
            
            if batch_indices:
                if text_column not in batch_df.columns:
                    logger.error(f"Column '{text_column}' not found in {parquet_file}")
                    return samples
                
                sampled_texts.extend(batch_df.iloc[batch_indices][text_column].tolist())
            
            current_batch_start = current_batch_end
            
            # Stop early if we've passed all sample indices
            if current_batch_start > sample_indices[-1]:
                break
        
        logger.info(f"Retrieved {len(sampled_texts)} sampled rows from {parquet_file.name}")
        
        # For each sampled text, extract segments of each length
        for text in sampled_texts:
            if not isinstance(text, str) or not text.strip():
                continue
            
            words = text.split()
            if len(words) == 0:
                continue
            
            # For each length, extract one random segment from this text
            for length in range(1, 301):
                if len(samples[length]) >= samples_per_length:
                    continue  # Already have enough samples for this length
                
                if len(words) >= length:
                    # Random starting position for the segment
                    max_start = len(words) - length
                    start_pos = random.randint(0, max_start)
                    segment = ' '.join(words[start_pos:start_pos + length])
                    samples[length].append(segment)
        
        # Log how many samples we got for each length range
        samples_1_50 = sum(len(samples[l]) for l in range(1, 51))
        samples_51_150 = sum(len(samples[l]) for l in range(51, 151))
        samples_151_300 = sum(len(samples[l]) for l in range(151, 301))
        logger.info(f"  Lengths 1-50: {samples_1_50} samples, 51-150: {samples_51_150}, 151-300: {samples_151_300}")
        
    except Exception as e:
        logger.error(f"Error processing {parquet_file}: {e}")
    
    return samples


def main(
    data_dir_pattern: str,
    output_csv: str,
    file_range_start: Optional[int] = None,
    file_range_end: Optional[int] = None,
    text_column: str = 'text',
    batch_size: int = 10000,
    seed: Optional[int] = None
):
    """
    Main function to sample segments from multiple parquet files.
    
    Args:
        data_dir_pattern: Glob pattern for directories (e.g., "/path/to/data_*")
        output_csv: Path to output CSV file
        file_range_start: Optional start index for file selection
        file_range_end: Optional end index for file selection
        text_column: Name of the text column in parquet files
        batch_size: Number of rows to read at once from parquet
        seed: Random seed for reproducibility
    """
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)
    
    # Find all directories matching the pattern
    matching_dirs = sorted(glob.glob(data_dir_pattern))
    
    if not matching_dirs:
        raise FileNotFoundError(f"No directories found matching pattern: {data_dir_pattern}")
    
    logger.info(f"Found {len(matching_dirs)} directories matching pattern")
    
    # Collect all parquet files from all matching directories
    all_parquet_files = []
    for dir_path in matching_dirs:
        dir_parquet_files = sorted(list(Path(dir_path).glob("*.parquet")))
        all_parquet_files.extend(dir_parquet_files)
    
    all_parquet_files = sorted(all_parquet_files)
    total_files = len(all_parquet_files)
    
    if total_files == 0:
        raise FileNotFoundError(f"No parquet files found in any matching directories")
    
    logger.info(f"Total parquet files across all directories: {total_files}")
    
    # Apply file range if specified
    if file_range_start is not None and file_range_end is not None:
        if file_range_start < 0 or file_range_start >= total_files:
            raise ValueError(f"file_range_start ({file_range_start}) is out of bounds [0, {total_files})")
        
        if file_range_end <= file_range_start or file_range_end > total_files:
            raise ValueError(f"file_range_end ({file_range_end}) must be > start ({file_range_start}) and <= {total_files}")
        
        selected_files = all_parquet_files[file_range_start:file_range_end]
        logger.info(f"Selected file range {file_range_start}:{file_range_end}")
    else:
        selected_files = all_parquet_files
        logger.info(f"Processing all {total_files} files")
    
    num_files = len(selected_files)
    
    # Calculate samples per file per length
    base_samples_per_file = 50 // num_files
    remainder = 50 % num_files
    
    logger.info(f"Target: 50 samples per length across {num_files} files")
    logger.info(f"Base samples per file: {base_samples_per_file}, with {remainder} files getting +1 extra")
    
    # Aggregate samples across all files
    all_samples = {length: [] for length in range(1, 301)}
    
    for file_idx, parquet_file in enumerate(selected_files):
        # Determine samples for this file
        samples_for_this_file = base_samples_per_file + (1 if file_idx < remainder else 0)
        
        if samples_for_this_file == 0:
            logger.warning(f"Skipping {parquet_file.name} - no samples allocated")
            continue
        
        logger.info(f"\nProcessing file {file_idx + 1}/{num_files}: {parquet_file.name}")
        logger.info(f"  Target: {samples_for_this_file} samples per length")
        
        # Sample from this file
        file_samples = sample_segments_from_file(
            parquet_file,
            samples_for_this_file,
            text_column=text_column,
            batch_size=batch_size
        )
        
        # Aggregate samples
        for length in range(1, 301):
            all_samples[length].extend(file_samples[length])
    
    # Ensure we have exactly 50 samples per length (trim or warn)
    logger.info("\n" + "="*60)
    logger.info("Final sample counts:")
    total_collected = 0
    insufficient_lengths = []
    
    for length in range(1, 301):
        count = len(all_samples[length])
        total_collected += min(count, 50)
        
        if count < 50:
            insufficient_lengths.append((length, count))
        elif count > 50:
            # Randomly select exactly 50
            all_samples[length] = random.sample(all_samples[length], 50)
    
    if insufficient_lengths:
        logger.warning(f"\nInsufficient samples for {len(insufficient_lengths)} lengths:")
        for length, count in insufficient_lengths[:10]:  # Show first 10
            logger.warning(f"  Length {length}: only {count}/50 samples")
        if len(insufficient_lengths) > 10:
            logger.warning(f"  ... and {len(insufficient_lengths) - 10} more")
    
    logger.info(f"\nTotal samples to write: {total_collected}")
    
    # Write to CSV
    logger.info(f"Writing samples to {output_csv}")
    
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write samples for each length (up to 50 per length)
        rows_written = 0
        for length in range(1, 301):
            samples = all_samples[length][:50]  # Take at most 50
            
            for sample in samples:
                writer.writerow([sample])
                rows_written += 1
    
    logger.info(f"Successfully wrote {rows_written} samples to {output_csv}")
    logger.info("Done!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Sample text segments of varying lengths from parquet files"
    )
    parser.add_argument(
        "data_dir_pattern",
        type=str,
        help="Glob pattern for data directories (e.g., '/path/to/data_*')"
    )
    parser.add_argument(
        "output_csv",
        type=str,
        help="Path to output CSV file"
    )
    parser.add_argument(
        "--file-range-start",
        type=int,
        default=None,
        help="Start index for file selection (inclusive)"
    )
    parser.add_argument(
        "--file-range-end",
        type=int,
        default=None,
        help="End index for file selection (exclusive)"
    )
    parser.add_argument(
        "--text-column",
        type=str,
        default="text",
        help="Name of the text column in parquet files (default: 'text')"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Number of rows to read at once from parquet (default: 10000)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    
    args = parser.parse_args()
    
    main(
        data_dir_pattern=args.data_dir_pattern,
        output_csv=args.output_csv,
        file_range_start=args.file_range_start,
        file_range_end=args.file_range_end,
        text_column=args.text_column,
        batch_size=args.batch_size,
        seed=args.seed
    )

    # /capstor/scratch/cscs/inesaltemir/detokenized_output/swissai-fineweb-edu-score-2-filterrobots-merge_part_*
    # to later query:  "/iopsstor/scratch/cscs/inesaltemir/es-data-septemberv1-swissai-fineweb-edu-score-2-filterrobots-merge_part_009-920290",
    # "file_start_range": 80,
    #    "file_end_range": 90,

    # python3 /capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/verbatim_sample_generator.py "/capstor/scratch/cscs/inesaltemir/detokenized_output/swissai-fineweb-edu-score-2-filterrobots-merge_part_*" /capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2-009-920290.csv  --file-range-start 80  --file-range-end 90

    # DO FIRST RUN ON SEPTEMBER BROUILLON 1 INDEX /capstor/scratch/cscs/inesaltemir/MERGE_logs/september_brouillon1/output/merge_001_923479.out
    

    # sample for verbatim check on 500GB index 
    # python3 /capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/verbatim_sample_generator.py "/capstor/scratch/cscs/inesaltemir/detokenized_output/swissai-fineweb-edu-score-2-filterrobots-merge_part_*" /capstor/scratch/cscs/inesaltemir/scripts/search_queries/verbatim_check/fw-edu-score-2_single_index-920282.csv  --file-range-start 0  --file-range-end 10
    