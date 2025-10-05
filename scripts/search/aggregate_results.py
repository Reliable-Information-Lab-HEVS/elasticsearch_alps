#!/usr/bin/env python3
"""
Aggregate search results from multiple JSON files
Counts occurrences of each unique query (segment_text) across all files
"""

import json
import os
import glob
from collections import defaultdict
import argparse
import csv


def find_result_files(base_dir):
    """Find all search_results_detailed_*.json files in directory"""
    pattern = os.path.join(base_dir, "**/search_results_detailed_*.json")
    files = glob.glob(pattern, recursive=True)
    print(f"Found {len(files)} result files")
    return files


def aggregate_results(files):
    """
    Aggregate results from all files
    Returns a dictionary with query statistics
    """
    # Track counts per query
    query_stats = defaultdict(lambda: {
        'total_occurrences': 0,
        'files_found_in': set(),
        'query_types': defaultdict(int),
        'total_hits_sum': 0,
        'max_score_max': 0,
        'avg_query_time_ms': [],
        'errors': 0
    })
    
    for file_path in files:
        print(f"Processing: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                results = json.load(f)
            
            for result in results:
                segment_text = result.get('segment_text', '')
                if not segment_text:
                    continue
                
                stats = query_stats[segment_text]
                stats['total_occurrences'] += 1
                stats['files_found_in'].add(os.path.basename(file_path))
                stats['query_types'][result.get('query_type', 'unknown')] += 1
                stats['total_hits_sum'] += result.get('total_hits', 0)
                stats['max_score_max'] = max(
                    stats['max_score_max'], 
                    result.get('max_score', 0) or 0
                )
                stats['avg_query_time_ms'].append(result.get('query_time_ms', 0))
                
                if result.get('error'):
                    stats['errors'] += 1
                    
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    return query_stats


def save_aggregated_results(query_stats, output_file):
    """Save aggregated statistics to JSON"""
    
    # Convert to list of dictionaries
    rows = []
    for query, stats in query_stats.items():
        avg_query_time = (
            sum(stats['avg_query_time_ms']) / len(stats['avg_query_time_ms'])
            if stats['avg_query_time_ms'] else 0
        )
        
        row = {
            'segment_text': query,
            'total_occurrences': stats['total_occurrences'],
            'num_files': len(stats['files_found_in']),
            'total_hits_sum': stats['total_hits_sum'],
            'max_score_overall': round(stats['max_score_max'], 3),
            'avg_query_time_ms': round(avg_query_time, 2),
            'errors': stats['errors'],
            'query_types': dict(stats['query_types']),
            'files': sorted(stats['files_found_in'])
        }
        rows.append(row)
    
    # Sort by total occurrences (descending)
    rows.sort(key=lambda x: x['total_occurrences'], reverse=True)
    
    # Create summary statistics
    output_data = {
        'summary': {
            'total_unique_queries': len(rows),
            'total_query_executions': sum(r['total_occurrences'] for r in rows),
            'total_files_processed': len(set(f for r in rows for f in r['files']))
        },
        'queries': rows
    }
    
    # Write to JSON with nice formatting
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\nAggregated results saved to: {output_file}")
    return rows


def print_summary(rows):
    """Print summary statistics"""
    print("\n" + "=" * 60)
    print("AGGREGATION SUMMARY")
    print("=" * 60)
    print(f"Total unique queries: {len(rows)}")
    print(f"Total query executions: {sum(r['total_occurrences'] for r in rows)}")
    print()
    
    print("Top 10 Most Frequent Queries:")
    print("-" * 60)
    for i, row in enumerate(rows[:10], 1):
        print(f"{i}. '{row['segment_text'][:50]}{'...' if len(row['segment_text']) > 50 else ''}'")
        print(f"   Occurrences: {row['total_occurrences']}, "
              f"Total Hits: {row['total_hits_sum']}, "
              f"Max Score: {row['max_score_overall']}")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate search results from multiple JSON files"
    )
    parser.add_argument(
        "--base-dir",
        nargs='+',
        required=True,
        help="Base directory(ies) containing search result files (can specify multiple)"
    )
    parser.add_argument(
        "--output",
        default="aggregated_search_results.json",
        help="Output JSON file (default: aggregated_search_results.json)"
    )
    
    args = parser.parse_args()
    
    # Normalize to list
    base_dirs = args.base_dir if isinstance(args.base_dir, list) else [args.base_dir]
    
    # Validate directories
    valid_dirs = []
    for base_dir in base_dirs:
        if os.path.isdir(base_dir):
            valid_dirs.append(base_dir)
        else:
            print(f"Warning: Directory not found, skipping: {base_dir}")
    
    if not valid_dirs:
        print("Error: No valid directories found!")
        return 1
    
    print(f"Searching for result files in {len(valid_dirs)} directories:")
    for d in valid_dirs:
        print(f"  - {d}")
    print("=" * 60)
    
    # Find all result files from all directories
    all_files = []
    for base_dir in valid_dirs:
        files = find_result_files(base_dir)
        all_files.extend(files)
    
    files = all_files
    
    if not files:
        print("No result files found!")
        return 1
    
    # Aggregate results
    print("\nAggregating results...")
    query_stats = aggregate_results(files)
    
    # Save results
    rows = save_aggregated_results(query_stats, args.output)
    
    # Print summary
    print_summary(rows)
    
    print("\nDone!")
    return 0


if __name__ == "__main__":
    exit(main())

# python3 /capstor/scratch/cscs/inesaltemir/scripts/search/aggregate_results.py --base-dir /capstor/scratch/cscs/inesaltemir/SEARCH_RESULTS/ --output /capstor/scratch/cscs/inesaltemir/scripts/search/aggregated_results.json