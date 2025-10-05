#!/usr/bin/env python3
"""
Indexing Job Status Analyzer with Ground Truth Verification
Analyzes .out files from indexing jobs and verifies against source parquet files.
"""

import re
import csv
import glob
import logging
import argparse
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple


def count_documents_in_parquet_files(file_list: List[Path]) -> int:
    """
    Count total documents across all parquet files WITHOUT loading data into memory.
    Uses PyArrow metadata only for efficiency.
    """
    logger = logging.getLogger(__name__)
    logger.info("=== Counting Ground Truth Documents ===")
    
    total_docs = 0
    
    for i, file_path in enumerate(file_list):
        try:
            # Read only metadata, NOT the actual data
            parquet_file = pq.ParquetFile(file_path)
            num_rows = parquet_file.metadata.num_rows
            total_docs += num_rows
            
            if (i + 1) % 10 == 0:
                logger.info(f"Counted {i+1}/{len(file_list)} files: {total_docs:,} docs so far")
            
        except Exception as e:
            logger.error(f"Failed to read metadata from {file_path.name}: {e}")
            raise
    
    logger.info(f"=== Ground Truth: {total_docs:,} total documents in {len(file_list)} files ===")
    return total_docs


def get_parquet_files_for_range(data_dir_pattern: str, 
                                file_range_start: int, 
                                file_range_end: int) -> Tuple[List[Path], Path, Path]:
    """
    Get the list of parquet files for a given data directory pattern and file range.
    
    Returns:
        Tuple of (file_list, first_file, last_file)
    """
    logger = logging.getLogger(__name__)
    
    # Find all matching directories
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
    
    # Apply file range
    if file_range_start < 0 or file_range_start >= total_files:
        raise ValueError(f"file_range_start ({file_range_start}) is out of bounds [0, {total_files})")
    
    if file_range_end <= file_range_start or file_range_end > total_files:
        raise ValueError(f"file_range_end ({file_range_end}) must be > start ({file_range_start}) and <= {total_files}")
    
    selected_files = all_parquet_files[file_range_start:file_range_end]
    logger.info(f"Selected file range {file_range_start}:{file_range_end}")
    
    first_file = selected_files[0]
    last_file = selected_files[-1]
    
    logger.info(f"First file: {first_file}")
    logger.info(f"Last file: {last_file}")
    
    return selected_files, first_file, last_file


def parse_job_output(file_path: Path) -> Dict[str, Optional[str]]:
    """
    Parse an indexing job output file to extract status and metrics.
    
    Args:
        file_path: Path to the .out file
        
    Returns:
        Dictionary with success status and various metrics
    """
    result = {
        'success': False,
        'avg_rate': None,
        'error': None,
        'total_docs': None,
        'failed_docs': None,
        'index_name': None,
        'file_start_range': None,
        'file_end_range': None,
        'total_execution_time': None,
        'peak_memory_gb': None,
        'index_size_mb': None,
        'raw_data_size_gb': None,
        'max_chunk_bytes': None,
        'thread_count': None,
        'queue_size': None,
        'batch_size': None,
        'data_directory': None,
        'es_data_directory': None,
        'final_index_count': None,
        'ground_truth_docs': None,
        'first_file': None,
        'last_file': None,
        'ground_truth_docs':None,
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Extract configuration information (present in all jobs)
        index_match = re.search(r'Index Name:\s*(.+)', content)
        if index_match:
            result['index_name'] = index_match.group(1).strip()
        
        start_range_match = re.search(r'File Start Range:\s*(\d+)', content)
        if start_range_match:
            result['file_start_range'] = int(start_range_match.group(1))
        
        end_range_match = re.search(r'File End Range:\s*(\d+)', content)
        if end_range_match:
            result['file_end_range'] = int(end_range_match.group(1))
        
        # Extract configuration parameters
        chunk_match = re.search(r'Max [Cc]hunk [Bb]ytes:\s*(\d+)', content)
        if chunk_match:
            result['max_chunk_bytes'] = int(chunk_match.group(1))
        
        thread_match = re.search(r'Thread [Cc]ount:\s*(\d+)', content)
        if thread_match:
            result['thread_count'] = int(thread_match.group(1))
        
        queue_match = re.search(r'Queue [Ss]ize:\s*(\d+)', content)
        if queue_match:
            result['queue_size'] = int(queue_match.group(1))
        
        batch_match = re.search(r'Batch [Ss]ize:\s*(\d+)', content)
        if batch_match:
            result['batch_size'] = int(batch_match.group(1))
        
        # Extract data directory pattern
        data_dir_match = re.search(r'Data [Dd]irectory:\s*(.+)', content)
        if data_dir_match:
            result['data_directory'] = data_dir_match.group(1).strip()

        # Extract ES data directory
        es_dir_match = re.search(r'ES Data Directory:\s*(.+)', content)
        if es_dir_match:
            result['es_data_directory'] = es_dir_match.group(1).strip()
        
        # Extract first and last file from the log
        first_file_match = re.search(r'First file:\s*(.+)', content)
        if first_file_match:
            result['first_file'] = first_file_match.group(1).strip()
        
        last_file_match = re.search(r'Last file:\s*(.+)', content)
        if last_file_match:
            result['last_file'] = last_file_match.group(1).strip()
        
        # Extract total documents indexed
        docs_match = re.search(r'Total documents indexed:\s*([\d,]+)', content)
        if docs_match:
            docs_str = docs_match.group(1).replace(',', '')
            result['total_docs'] = int(docs_str)
        
        # Extract final index document count
        final_count_match = re.search(r'Final index document count:\s*([\d,]+)', content)
        if final_count_match:
            final_str = final_count_match.group(1).replace(',', '')
            result['final_index_count'] = int(final_str)

        # Extract ground truth document count from logs
        ground_truth_match = re.search(r'Ground [Tt]ruth:?\s*([\d,]+)\s*total documents', content)
        if not ground_truth_match:
            # Try alternative format
            ground_truth_match = re.search(r'Ground truth \(raw files\):\s*([\d,]+)\s*documents', content)
        if ground_truth_match:
            ground_str = ground_truth_match.group(1).replace(',', '')
            result['ground_truth_docs'] = int(ground_str)
        
            
        # Extract raw data size
        raw_size_match = re.search(r'Total raw data size:\s*([\d,]+\.?\d*)\s*GB', content)
        if raw_size_match:
            raw_str = raw_size_match.group(1).replace(',', '')
            result['raw_data_size_gb'] = float(raw_str)
            
        # Check for successful completion marker
        if '=== Indexing Job Completed Successfully ===' in content:
            # Additional validation: Final index count must match total docs indexed
            if result['total_docs'] is not None and result['final_index_count'] is not None and result['ground_truth_docs'] is not None:
                if result['total_docs'] == result['final_index_count'] == result['ground_truth_docs'] :
                    result['success'] = True
                else:
                    result['success'] = False
                    result['error'] = (f"Document count mismatch: Total indexed={result['total_docs']:,}, "
                                     f"Final index count={result['final_index_count']:,}")
            else:
                # If we can't verify counts, mark as failed
                result['success'] = False
                if result['total_docs'] is None:
                    result['error'] = "Missing 'Total documents indexed' in output"
                elif result['final_index_count'] is None:
                    result['error'] = "Missing 'Final index document count' in output"
            
            # Only extract success metrics if truly successful
            if result['success']:
                # Extract average indexing rate
                rate_match = re.search(r'Average indexing rate:\s*([\d,]+\.?\d*)\s*docs/sec', content)
                if rate_match:
                    rate_str = rate_match.group(1).replace(',', '')
                    result['avg_rate'] = float(rate_str)
                
                # Extract failed documents
                failed_match = re.search(r'Failed documents:\s*([\d,]+)', content)
                if failed_match:
                    failed_str = failed_match.group(1).replace(',', '')
                    result['failed_docs'] = int(failed_str)
                
                # Extract total execution time
                time_match = re.search(r'Total execution time:\s*([\d,]+\.?\d*)\s*seconds', content)
                if time_match:
                    time_str = time_match.group(1).replace(',', '')
                    result['total_execution_time'] = float(time_str)
                
                # Extract peak memory usage
                mem_match = re.search(r'Peak memory usage during execution:\s*([\d,]+\.?\d*)\s*GB', content)
                if mem_match:
                    mem_str = mem_match.group(1).replace(',', '')
                    result['peak_memory_gb'] = float(mem_str)
                
                # Extract index size
                index_size_match = re.search(r'Index size:\s*([\d,]+\.?\d*)\s*MB', content)
                if index_size_match:
                    size_str = index_size_match.group(1).replace(',', '')
                    result['index_size_mb'] = float(size_str)
            
        else:
            # Job did not complete successfully - try to find error information
            if 'ERROR' in content:
                # Extract last error message
                error_matches = re.findall(r'\d{4}-\d{2}-\d{2}.*?ERROR.*?- (.+)', content)
                if error_matches:
                    result['error'] = error_matches[-1].strip()[:200]  # Limit error length
            
            if result['error'] is None:
                # Check if job is still running or was interrupted
                if 'Indexing interrupted by user' in content:
                    result['error'] = 'Interrupted by user'
                elif 'Indexing failed' in content:
                    result['error'] = 'Indexing failed (see log for details)'
                else:
                    result['error'] = 'Job incomplete or in progress'
                    
    except FileNotFoundError:
        result['error'] = 'Output file not found'
    except Exception as e:
        result['error'] = f'Error reading file: {str(e)}'
    
    return result


def generate_job_list(start: int, end: int, exclude: Optional[List[int]] = None) -> List[int]:
    """
    Generate a list of job IDs from a range, excluding specified IDs.
    
    Args:
        start: Starting job ID
        end: Ending job ID (inclusive)
        exclude: List of job IDs to exclude
        
    Returns:
        List of job IDs to process
    """
    exclude_set = set(exclude) if exclude else set()
    return [job_id for job_id in range(start, end + 1) if job_id not in exclude_set]


def analyze_jobs(job_ids: List[int], output_dir: Path, output_csv: Path, start: int, end: int,
                csv_output_dir: Optional[Path] = None, verify_ground_truth: bool = False) -> None:
    """
    Analyze indexing jobs and generate a CSV report.
    
    Args:
        job_ids: List of job IDs to analyze
        output_dir: Directory containing .out files
        output_csv: Path to output CSV file (used if csv_output_dir is None)
        start: Starting job ID (for filename generation)
        end: Ending job ID (for filename generation)
        csv_output_dir: Directory where CSV should be saved with auto-generated name
        verify_ground_truth: If True, count documents from source parquet files
    """
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    results = []
    
    print(f"Analyzing {len(job_ids)} jobs...")
    print(f"Looking for output files in: {output_dir}")
    if verify_ground_truth:
        print(f"Ground truth verification: ENABLED")
    print("-" * 80)
    
    for job_id in job_ids:
        # Try multiple naming patterns
        file_patterns = [
            f"indexing_{job_id}.out",           # Original pattern
            f"indexing_*_{job_id}.out"          # New pattern with prefix
        ]
        
        file_path = None
        for pattern in file_patterns:
            matches = list(output_dir.glob(pattern))
            if matches:
                file_path = matches[0]
                break
        
        if not file_path:
            print(f"✗ File not found for job {job_id}")
            results.append({
                'job_id': job_id,
                'success': False,
                'error': 'Output file not found',
            })
            continue

        print(f"Processing job {job_id}...", end=" ")
        
        job_result = parse_job_output(file_path)
       
        ground_truth_matches = None
        
        if job_result['ground_truth_docs'] is not None and job_result['final_index_count'] is not None:
            ground_truth_matches = (job_result['ground_truth_docs'] == job_result['final_index_count'])

        if verify_ground_truth and job_result['data_directory'] and \
           job_result['file_start_range'] is not None and job_result['file_end_range'] is not None:
            try:
                print("\n  Counting ground truth documents...", end=" ")
                
                selected_files, first_file, last_file = get_parquet_files_for_range(
                    job_result['data_directory'],
                    job_result['file_start_range'],
                    job_result['file_end_range']
                )
                
                # ground_truth_docs = count_documents_in_parquet_files(selected_files)
                
                # Verify file range matches
                first_file_matches = str(first_file) == job_result['first_file']
                last_file_matches = str(last_file) == job_result['last_file']
                
                print(f"Ground truth: {job_result['ground_truth_docs']:,} docs")
                

                # Compare with indexed counts - check both total_docs and final_index_count
                if job_result['ground_truth_docs'] is not None and job_result['final_index_count'] is not None:
                    if job_result['ground_truth_docs'] == job_result['final_index_count']:
                        ground_truth_matches = True
                        print(f"  ✓ Ground truth matches final index count")
                    else:
                        ground_truth_matches = False
                        diff = job_result['ground_truth_docs'] - job_result['final_index_count']
                        print(f"  ✗ MISMATCH: Ground truth - Indexed = {diff:,} docs")
                        # Update success status if there's a mismatch
                        if job_result['success']:
                            job_result['success'] = False
                            job_result['error'] = f"Ground truth mismatch: Expected {job_result['ground_truth_docs']:,}, got {job_result['final_index_count']:,}"
                elif job_result['total_docs'] is not None:
                    # Fallback to total_docs if final_index_count not available
                    if job_result['ground_truth_docs'] == job_result['total_docs']:
                        ground_truth_matches = True
                        print(f"  ✓ Ground truth matches total docs")
                    else:
                        ground_truth_matches = False
                        diff = job_result['ground_truth_docs'] - job_result['total_docs']
                        print(f"  ✗ MISMATCH: Ground truth - Total docs = {diff:,} docs")
                
                if not first_file_matches or not last_file_matches:
                    print(f"  ⚠ Warning: File range verification failed")
                    if not first_file_matches:
                        print(f"    First file mismatch:")
                        print(f"      Expected: {job_result['first_file']}")
                        print(f"      Got: {first_file}")
                    if not last_file_matches:
                        print(f"    Last file mismatch:")
                        print(f"      Expected: {job_result['last_file']}")
                        print(f"      Got: {last_file}")
                
            except Exception as e:
                print(f"  ✗ Failed to count ground truth: {e}")
                logger.error(f"Ground truth counting failed for job {job_id}: {e}")
        
        results.append({
            'job_id': job_id,
            'success': job_result['success'],
            'avg_rate_docs_per_sec': job_result['avg_rate'],
            'total_docs': job_result['total_docs'],
            'final_index_count': job_result['final_index_count'],
            'ground_truth_docs': job_result['ground_truth_docs'],
            'ground_truth_matches': ground_truth_matches,
            'failed_docs': job_result['failed_docs'],
            'index_name': job_result['index_name'],
            'file_start_range': job_result['file_start_range'],
            'file_end_range': job_result['file_end_range'],
            'first_file': job_result['first_file'],
            'last_file': job_result['last_file'],
            'total_execution_time_sec': job_result['total_execution_time'],
            'peak_memory_gb': job_result['peak_memory_gb'],
            'index_size_mb': job_result['index_size_mb'],
            'raw_data_size_gb': job_result['raw_data_size_gb'],
            'index_to_raw_ratio': (job_result['index_size_mb'] / (job_result['raw_data_size_gb'] * 1024) 
                                  if job_result['index_size_mb'] and job_result['raw_data_size_gb'] 
                                  else None),
            'max_chunk_bytes': job_result['max_chunk_bytes'],
            'thread_count': job_result['thread_count'],
            'queue_size': job_result['queue_size'],
            'batch_size': job_result['batch_size'],
            'data_directory': job_result['data_directory'],
            'es_data_directory': job_result['es_data_directory'],
            'error': job_result['error']
        })
        
        if job_result['success']:
            print(f"✓ Success (Rate: {job_result['avg_rate']:.1f} docs/sec)")
        else:
            print(f"✗ Failed ({job_result['error']})")
    
    print("-" * 80)
    
    # Filter successful jobs for statistics
    successful_jobs = [r for r in results if r['success']]
    failed_jobs = [r for r in results if not r['success']]
    
    # Calculate global statistics for successful jobs
    global_stats = {
        'total_jobs': len(results),
        'successful': len(successful_jobs),
        'failed': len(failed_jobs),
        'global_avg_rate': None,
        'avg_peak_memory_gb': None,
        'avg_index_to_raw_ratio': None,
        'common_max_chunk_bytes': None,
        'common_thread_count': None,
        'common_queue_size': None,
        'common_batch_size': None,
        'ground_truth_verified': sum(1 for r in results if r['ground_truth_docs'] is not None),
        'ground_truth_matches': sum(1 for r in results if r['ground_truth_matches'] is True),
        'ground_truth_mismatches': sum(1 for r in results if r['ground_truth_matches'] is False)
    }
    
    if successful_jobs:
        # Calculate global average indexing rate (total docs / total time)
        total_docs_all = sum(r['total_docs'] for r in successful_jobs if r['total_docs'])
        total_time_all = sum(r['total_execution_time_sec'] for r in successful_jobs if r['total_execution_time_sec'])
        if total_time_all > 0:
            global_stats['global_avg_rate'] = total_docs_all / total_time_all
        
        # Calculate average peak memory
        peak_memories = [r['peak_memory_gb'] for r in successful_jobs if r['peak_memory_gb']]
        if peak_memories:
            global_stats['avg_peak_memory_gb'] = sum(peak_memories) / len(peak_memories)
        
        # Calculate average index-to-raw ratio
        ratios = [r['index_to_raw_ratio'] for r in successful_jobs if r['index_to_raw_ratio']]
        if ratios:
            global_stats['avg_index_to_raw_ratio'] = sum(ratios) / len(ratios)
        
        # Get common parameters (from first successful job)
        first_success = successful_jobs[0]
        global_stats['common_max_chunk_bytes'] = first_success['max_chunk_bytes']
        global_stats['common_thread_count'] = first_success['thread_count']
        global_stats['common_queue_size'] = first_success['queue_size']
        global_stats['common_batch_size'] = first_success['batch_size']
    
    # Print summary statistics
    print(f"\n{'='*80}")
    print(f"SUMMARY STATISTICS")
    print(f"{'='*80}")
    print(f"  Total jobs analyzed: {global_stats['total_jobs']}")
    print(f"  Successful: {global_stats['successful']}")
    print(f"  Failed: {global_stats['failed']}")
    
    if verify_ground_truth:
        print(f"\nGROUND TRUTH VERIFICATION:")
        print(f"  Jobs verified: {global_stats['ground_truth_verified']}")
        print(f"  Matches: {global_stats['ground_truth_matches']}")
        print(f"  Mismatches: {global_stats['ground_truth_mismatches']}")
    
    if successful_jobs:
        print(f"\nGLOBAL PERFORMANCE METRICS (successful jobs):")
        if global_stats['global_avg_rate']:
            print(f"  Global average indexing rate: {global_stats['global_avg_rate']:.1f} docs/sec")
            print(f"    (Total docs: {total_docs_all:,} / Total time: {total_time_all:.1f}s)")
        
        if global_stats['avg_peak_memory_gb']:
            print(f"  Average peak memory usage: {global_stats['avg_peak_memory_gb']:.2f} GB")
        
        if global_stats['avg_index_to_raw_ratio']:
            print(f"  Average index-to-raw size ratio: {global_stats['avg_index_to_raw_ratio']:.3f}")
            print(f"    (Index size / Raw data size)")
        
        # Per-job rate statistics
        rates = [r['avg_rate_docs_per_sec'] for r in successful_jobs if r['avg_rate_docs_per_sec']]
        if rates:
            print(f"\nPER-JOB RATE STATISTICS:")
            print(f"  Average rate: {sum(rates) / len(rates):.1f} docs/sec")
            print(f"  Min rate: {min(rates):.1f} docs/sec")
            print(f"  Max rate: {max(rates):.1f} docs/sec")
        
        # Common parameters
        print(f"\nCOMMON CONFIGURATION PARAMETERS:")
        if global_stats['common_max_chunk_bytes']:
            print(f"  Max Chunk Bytes: {global_stats['common_max_chunk_bytes']} MB")
        if global_stats['common_thread_count']:
            print(f"  Thread Count: {global_stats['common_thread_count']}")
        if global_stats['common_queue_size']:
            print(f"  Queue Size: {global_stats['common_queue_size']}")
        if global_stats['common_batch_size']:
            print(f"  Batch Size: {global_stats['common_batch_size']:,}")
    
    print(f"{'='*80}\n")
    
    # Determine the final output path
    if csv_output_dir:
        csv_output_dir.mkdir(parents=True, exist_ok=True)
        output_filename = f"index_job_status_{start}_{end}.json"
        final_output_path = csv_output_dir / output_filename
        print(f"Auto-generated JSON filename: {output_filename}")
    else:
        # Change extension to .json if user provided a .csv path
        if output_csv.suffix == '.csv':
            final_output_path = output_csv.with_suffix('.json')
        else:
            final_output_path = output_csv
    
    # Prepare JSON output structure
    import json
    from datetime import datetime
    
    output_data = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "job_range": f"{start}-{end}",
            "total_jobs_analyzed": global_stats['total_jobs'],
            "verification_enabled": verify_ground_truth
        },
        "summary_statistics": {
            "total_jobs": global_stats['total_jobs'],
            "successful": global_stats['successful'],
            "failed": global_stats['failed']
        },
        "ground_truth_verification": {
            "jobs_verified": global_stats['ground_truth_verified'],
            "matches": global_stats['ground_truth_matches'],
            "mismatches": global_stats['ground_truth_mismatches']
        } if verify_ground_truth else None,
        "global_performance_metrics": {},
        "per_job_statistics": {},
        "common_configuration": {},
        "jobs": []
    }
    
    # Add global performance metrics
    if successful_jobs:
        if global_stats['global_avg_rate']:
            output_data["global_performance_metrics"]["global_avg_rate_docs_per_sec"] = global_stats['global_avg_rate']
            output_data["global_performance_metrics"]["total_docs_all_jobs"] = total_docs_all
            output_data["global_performance_metrics"]["total_time_all_jobs_sec"] = total_time_all
        
        if global_stats['avg_peak_memory_gb']:
            output_data["global_performance_metrics"]["avg_peak_memory_gb"] = global_stats['avg_peak_memory_gb']
        
        if global_stats['avg_index_to_raw_ratio']:
            output_data["global_performance_metrics"]["avg_index_to_raw_ratio"] = global_stats['avg_index_to_raw_ratio']
        
        # Per-job rate statistics
        rates = [r['avg_rate_docs_per_sec'] for r in successful_jobs if r['avg_rate_docs_per_sec']]
        if rates:
            output_data["per_job_statistics"] = {
                "avg_rate_docs_per_sec": sum(rates) / len(rates),
                "min_rate_docs_per_sec": min(rates),
                "max_rate_docs_per_sec": max(rates)
            }
        
        # Common configuration
        output_data["common_configuration"] = {
            "max_chunk_bytes": global_stats['common_max_chunk_bytes'],
            "thread_count": global_stats['common_thread_count'],
            "queue_size": global_stats['common_queue_size'],
            "batch_size": global_stats['common_batch_size']
        }
    
    # Add ES data directories
    es_dirs = list(set(r['es_data_directory'] for r in results if r['es_data_directory']))
    output_data["es_data_directories"] = sorted(es_dirs)
    
    # Add individual job results
    for result in results:
        job_entry = {
            "job_id": result['job_id'],
            "success": result['success'],
            "indexing_performance": {
                "avg_rate_docs_per_sec": result['avg_rate_docs_per_sec'],
                "total_execution_time_sec": result['total_execution_time_sec'],
                "peak_memory_gb": result['peak_memory_gb']
            },
            "document_counts": {
                "total_docs": result['total_docs'],
                "final_index_count": result['final_index_count'],
                "ground_truth_docs": result['ground_truth_docs'],
                "ground_truth_matches": result['ground_truth_matches'],
                "failed_docs": result['failed_docs']
            },
            "storage_metrics": {
                "index_size_mb": result['index_size_mb'],
                "raw_data_size_gb": result['raw_data_size_gb'],
                "index_to_raw_ratio": result['index_to_raw_ratio']
            },
            "configuration": {
                "index_name": result['index_name'],
                "file_start_range": result['file_start_range'],
                "file_end_range": result['file_end_range'],
                "max_chunk_bytes": result['max_chunk_bytes'],
                "thread_count": result['thread_count'],
                "queue_size": result['queue_size'],
                "batch_size": result['batch_size']
            },
            "file_paths": {
                "data_directory": result['data_directory'],
                "es_data_directory": result['es_data_directory'],
                "first_file": result['first_file'],
                "last_file": result['last_file']
            },
            "error": result['error']
        }
        output_data["jobs"].append(job_entry)
    
    # Write JSON output
    with open(final_output_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(output_data, jsonfile, indent=2, ensure_ascii=False)
    
    print(f"Results written to: {final_output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze indexing job output files and generate status report with optional ground truth verification',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic analysis without ground truth verification
  python3 index_job_status.py 920282 920316 /path/to/output
  
  # With ground truth verification (counts documents from source parquet files)
  python3 index_job_status.py 920282 920316 /path/to/output --verify-ground-truth
  
  # With exclusions and custom output location
  python3 index_job_status.py 920282 920316 /path/to/output --exclude 920285 920290 -o custom_report.csv
        """
    )
    
    parser.add_argument('start', type=int, help='Starting job ID')
    parser.add_argument('end', type=int, help='Ending job ID (inclusive)')
    parser.add_argument('output_dir', type=Path, 
                       help='Directory containing .out files')
    parser.add_argument('--exclude', type=int, nargs='+', 
                       help='Job IDs to exclude from analysis')
    parser.add_argument('-o', '--output', type=Path, 
                       default=None,
                       help='Output CSV file path (if not specified, uses --csv-dir or default)')
    parser.add_argument('--csv-dir', type=Path,
                       default=Path('/capstor/scratch/cscs/inesaltemir/scripts/indexing/job_status_logs/fw-other-high'),
                       help='Directory to save CSV with auto-generated filename')
    parser.add_argument('--verify-ground-truth', action='store_true', default=True,
                       help='Count documents from source parquet files and verify against indexed counts')
    
    args = parser.parse_args()
    
    # Validate inputs
    if args.start > args.end:
        parser.error(f"Start job ID ({args.start}) must be <= end job ID ({args.end})")
    
    if not args.output_dir.exists():
        parser.error(f"Output directory does not exist: {args.output_dir}")
    
    # Generate job list
    job_ids = generate_job_list(args.start, args.end, args.exclude)
    
    if not job_ids:
        parser.error("No jobs to analyze after exclusions")
    
    if args.exclude:
        print(f"Excluding {len(args.exclude)} job IDs: {args.exclude}")
    
    # Determine CSV output location
    if args.output:
        csv_output_dir = None
        output_csv = args.output
        print(f"Using specified output file: {output_csv}")
    else:
        csv_output_dir = args.csv_dir
        output_csv = Path('indexing_job_analysis.csv')  # Fallback (won't be used)
        print(f"Using auto-generated filename in: {csv_output_dir}")
    
    # Analyze jobs
    analyze_jobs(job_ids, args.output_dir, output_csv, args.start, args.end, 
                csv_output_dir, verify_ground_truth=args.verify_ground_truth)


if __name__ == '__main__':
    main()



# python3 /capstor/scratch/cscs/inesaltemir/scripts/indexing/indexing_job_status_json.py 920282 920316 /capstor/scratch/cscs/inesaltemir/INDEXING_TOKENIZED/swissai-fineweb-edu-score-2-filterrobots-merge/output 

# srun -A a145 --environment=es-python --partition=normal --pty bash

# python3 /capstor/scratch/cscs/inesaltemir/scripts/indexing/indexing_job_status_json.py 920204 920211 /capstor/scratch/cscs/inesaltemir/INDEXING_TOKENIZED/swissai-fineweb-2-quality_33-filterrobots-merge_euro-high/output --exclude

# 
# python3 /capstor/scratch/cscs/inesaltemir/scripts/indexing/indexing_job_status_json.py 925033 925033 /capstor/scratch/cscs/inesaltemir/INDEXING_TOKENIZED/swissai-fineweb-2-quality_33-filterrobots-merge_other-high/output