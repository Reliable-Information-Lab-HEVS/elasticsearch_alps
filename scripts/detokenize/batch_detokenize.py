#!/usr/bin/env python3
"""
Batch processor for multiple Megatron datasets.
Handles all dump-*-merged.bin/idx files in a directory.
"""

import os
import argparse
import multiprocessing as mp
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
from typing import List, Tuple

from megatron_detokenizer import process_dataset, load_tokenizer


def find_dataset_pairs(input_dir: str) -> List[Tuple[str, str, str]]:
    """Find all .bin/.idx pairs in the input directory."""
    input_path = Path(input_dir)
    pairs = []
    
    for bin_file in input_path.glob("*.bin"):
        idx_file = bin_file.with_suffix(".idx")
        if idx_file.exists():
            # Generate output filename
            output_name = bin_file.stem + ".parquet"
            pairs.append((str(bin_file), str(idx_file), output_name))
    
    return sorted(pairs)


def process_single_dataset(args: Tuple[str, str, str, str, int, bool, str, str]) -> Tuple[str, bool, str]:
    """Process a single dataset - wrapper for multiprocessing."""
    bin_path, idx_path, output_name, tokenizer_path, chunk_size, keep_special, compression, output_dir = args
    
    output_path = os.path.join(output_dir, output_name)
    
    try:
        # Load tokenizer (each process needs its own)
        tokenizer = load_tokenizer(tokenizer_path)
        
        start_time = time.time()
        process_dataset(
            bin_path=bin_path,
            idx_path=idx_path,
            output_path=output_path,
            tokenizer=tokenizer,
            chunk_size=chunk_size,
            remove_special_tokens=not keep_special,
            compression=compression
        )
        
        elapsed = time.time() - start_time
        file_size_gb = os.path.getsize(bin_path) / (1024**3)
        
        return output_name, True, f"Success ({elapsed:.1f}s, {file_size_gb:.2f}GB)"
        
    except Exception as e:
        return output_name, False, f"Error: {str(e)}"


def estimate_processing_time(pairs: List[Tuple[str, str, str]], sample_size: int = 3) -> None:
    """Estimate total processing time based on file sizes."""
    total_size_gb = sum(os.path.getsize(pair[0]) for pair in pairs) / (1024**3)
    
    print(f"\nDataset Statistics:")
    print(f"  Total files: {len(pairs)}")
    print(f"  Total size: {total_size_gb:.2f} GB")
    print(f"  Estimated processing time: {total_size_gb * 2:.1f} - {total_size_gb * 5:.1f} minutes")
    print(f"  (assuming 0.5-2 GB/min processing rate)")


def main():
    parser = argparse.ArgumentParser(description="Batch detokenize Megatron datasets")
    parser.add_argument("input_dir", help="Directory containing .bin/.idx files")
    parser.add_argument("output_dir", help="Output directory for .parquet files")
    parser.add_argument("--tokenizer", required=True, help="Tokenizer name or path")
    parser.add_argument("--chunk-size", type=int, default=1000,
                        help="Sequences per batch")
    parser.add_argument("--keep-special-tokens", action="store_true",
                        help="Keep special tokens in output")
    parser.add_argument("--compression", default="snappy",
                        choices=["snappy", "gzip", "brotli", "lz4"],
                        help="Parquet compression")
    parser.add_argument("--max-workers", type=int, default=None,
                        help="Max parallel processes (default: CPU count // 2)")
    parser.add_argument("--file-range-start", type=float, default=None,
                    help="Starting file index for processing subset")
    parser.add_argument("--file-range-end", type=float, default=None,
                    help="Ending file index for processing subset")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be processed without running")
    
    args = parser.parse_args()
    
    # Validate directories
    if not os.path.isdir(args.input_dir):
        raise NotADirectoryError(f"Input directory not found: {args.input_dir}")
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Find all dataset pairs
    pairs = find_dataset_pairs(args.input_dir)
    
    if not pairs:
        print(f"No .bin/.idx pairs found in {args.input_dir}")
        return
    
    # Apply file range filtering if specified
    if args.file_range_start is not None and args.file_range_end is not None:
        total_files = len(pairs)
        start_idx = int((args.file_range_start / 100.0) * total_files)
        end_idx = int((args.file_range_end / 100.0) * total_files)
        
        # Ensure valid indices
        start_idx = max(0, min(start_idx, total_files - 1))
        end_idx = max(start_idx + 1, min(end_idx, total_files))
        
        original_pairs = pairs
        pairs = pairs[start_idx:end_idx]
        
        print(f"File range filter applied:")
        print(f"  Range: {args.file_range_start}% - {args.file_range_end}%")
        print(f"  Total files: {len(original_pairs)}")
        print(f"  Selected files: {len(pairs)} (indices {start_idx}-{end_idx-1})")
    
    print(f"Found {len(pairs)} dataset pairs:")
    for i, (bin_path, idx_path, output_name) in enumerate(pairs, 1):
        size_gb = os.path.getsize(bin_path) / (1024**3)
        print(f"  {i:2d}. {output_name:<25} ({size_gb:6.2f} GB)")
    
    estimate_processing_time(pairs)
    
    if args.dry_run:
        print("\nDry run - no processing performed.")
        return
    
    # Determine number of workers
    max_workers = args.max_workers
    if max_workers is None:
        max_workers = max(1, mp.cpu_count() // 2)  # Conservative for memory usage
    
    print(f"\nUsing {max_workers} parallel workers")
    
    # Prepare arguments for multiprocessing
    process_args = []
    for bin_path, idx_path, output_name in pairs:
        process_args.append((
            bin_path, idx_path, output_name, args.tokenizer,
            args.chunk_size, args.keep_special_tokens, 
            args.compression, args.output_dir
        ))
    
    # Process datasets in parallel
    start_time = time.time()
    completed = 0
    failed = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_name = {
            executor.submit(process_single_dataset, arg): arg[2] 
            for arg in process_args
        }
        
        # Process results as they complete
        for future in as_completed(future_to_name):
            output_name = future_to_name[future]
            try:
                name, success, message = future.result()
                if success:
                    completed += 1
                    print(f"✓ {name}: {message}")
                else:
                    failed += 1
                    print(f"✗ {name}: {message}")
            except Exception as e:
                failed += 1
                print(f"✗ {output_name}: Unexpected error: {e}")
    
    # Summary
    total_time = time.time() - start_time
    print(f"\n" + "="*60)
    print(f"Processing completed in {total_time/60:.1f} minutes")
    print(f"  Successful: {completed}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(pairs)}")
    
    if completed > 0:
        total_output_size = sum(
            os.path.getsize(os.path.join(args.output_dir, f"{Path(pair[0]).stem}.parquet"))
            for pair in pairs
            if os.path.exists(os.path.join(args.output_dir, f"{Path(pair[0]).stem}.parquet"))
        ) / (1024**3)
        print(f"  Total output size: {total_output_size:.2f} GB")


if __name__ == "__main__":
    main()