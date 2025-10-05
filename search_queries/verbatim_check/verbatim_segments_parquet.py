import pandas as pd
import pyarrow.parquet as pq
import random
import csv
from pathlib import Path

def sample_segments_from_parquet(
    input_path,
    output_path="sampled_segments.csv",
    n_samples=1000,
    segment_lengths=[3, 10, 20],
    text_column="text",
    seed=42
):
    """
    Sample segments of different lengths from a large Parquet file.
    Each sampled row produces multiple segments (one per length).
    Output CSV has one segment per line with no header.
    
    Args:
        input_path: Path to the input Parquet file
        output_path: Path to the output CSV file
        n_samples: Number of rows to sample
        segment_lengths: List of segment lengths to extract
        text_column: Name of the column containing text
        seed: Random seed for reproducibility
    """
    random.seed(seed)
    
    # Read Parquet file metadata to get total row count
    parquet_file = pq.ParquetFile(input_path)
    total_rows = parquet_file.metadata.num_rows
    
    print(f"Total rows in Parquet file: {total_rows}")
    
    # Generate random row indices to sample
    if n_samples >= total_rows:
        sample_indices = list(range(total_rows))
        print(f"Warning: Requested {n_samples} samples but file only has {total_rows} rows")
    else:
        sample_indices = sorted(random.sample(range(total_rows), n_samples))
    
    print(f"Sampling {len(sample_indices)} rows...")
    
    # Read only the sampled rows efficiently using row groups
    # This reads in batches to minimize memory usage
    batch_size = 10000
    sampled_data = []
    current_batch_start = 0
    
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        batch_df = batch.to_pandas()
        current_batch_end = current_batch_start + len(batch_df)
        
        # Find which sample indices fall in this batch
        batch_indices = [
            idx - current_batch_start 
            for idx in sample_indices 
            if current_batch_start <= idx < current_batch_end
        ]
        
        if batch_indices:
            sampled_data.append(batch_df.iloc[batch_indices])
        
        current_batch_start = current_batch_end
        
        if current_batch_start > sample_indices[-1]:
            break
    
    # Combine all sampled batches
    df_sampled = pd.concat(sampled_data, ignore_index=True)
    print(f"Successfully sampled {len(df_sampled)} rows")
    
    # Extract segments of different lengths
    all_segments = []
    
    for idx, row in df_sampled.iterrows():
        text = str(row[text_column])
        words = text.split()
        
        if len(words) < max(segment_lengths):
            print(f"Warning: Row {idx} has only {len(words)} words, skipping")
            continue
        
        # Extract one segment for each length
        for length in segment_lengths:
            if len(words) >= length:
                # Random starting position for the segment
                max_start = len(words) - length
                start_pos = random.randint(0, max_start)
                segment = " ".join(words[start_pos:start_pos + length])
                all_segments.append(segment)
    
    # Write to CSV with no header, one segment per line
    if all_segments:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for segment in all_segments:
                writer.writerow([segment])
        
        print(f"\nSuccessfully wrote {len(all_segments)} segments to {output_path}")
        print(f"({len(all_segments) // len(segment_lengths)} rows × {len(segment_lengths)} segments each)")
    else:
        print("No valid segments were generated!")

if __name__ == "__main__":
    input_path = "/capstor/scratch/cscs/inesaltemir/detokenized_output/gutenberg/00000_tokens.parquet"
    output_path = "/capstor/scratch/cscs/inesaltemir/scripts/search_WORDS/verbatim_check/gutenberg_verbatim_small.csv"
    
    sample_segments_from_parquet(
        input_path=input_path,
        output_path=output_path,
        n_samples=100,
        segment_lengths=[3, 10, 20],
        text_column="text",  # Change this if your text column has a different name
        seed=42
    )
    
    print("\nDone!")