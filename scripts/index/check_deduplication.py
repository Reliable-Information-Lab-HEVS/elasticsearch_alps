import pandas as pd
import hashlib
import sys
import pyarrow.parquet as pq
from collections import defaultdict

def create_doc_id(text: str) -> str:
    """Create content-based document ID using SHA256 hash."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def analyze_duplicates(parquet_path: str, column_name: str, top_n: int = 5):
    parquet_file = pq.ParquetFile(parquet_path)
    hash_counts = defaultdict(int)
    hash_examples = {}

    total_docs = 0

    print(f"🔍 Scanning '{parquet_path}' for duplicates...")

    # Pass 1: Build hash count map
    for i in range(parquet_file.num_row_groups):
        table = parquet_file.read_row_group(i, columns=[column_name])
        series = table[column_name].to_pandas()

        for text in series:
            if text is None:
                continue
            total_docs += 1
            text_str = str(text).strip()
            doc_hash = create_doc_id(text_str)

            hash_counts[doc_hash] += 1
            if doc_hash not in hash_examples:
                hash_examples[doc_hash] = text_str

        print(f"Processed row group {i+1}/{parquet_file.num_row_groups}...")

    # Compute stats
    unique_docs = len(hash_counts)
    duplicated_docs = sum(count - 1 for count in hash_counts.values() if count > 1)

    print("\n📊 Duplication statistics:")
    print(f"  Total docs:      {total_docs}")
    print(f"  Unique docs:     {unique_docs}")
    print(f"  Duplicated docs: {duplicated_docs}")
    print(f"  % Duplicates:    {duplicated_docs / total_docs * 100:.2f}%")

    # Sort hashes by occurrence (descending)
    sorted_hashes = sorted(hash_counts.items(), key=lambda x: x[1], reverse=True)

    print(f"\n🔥 Top {top_n} most frequent hashes:")
    for h, c in sorted_hashes[:top_n]:
        print(f"  {h} → {c} occurrences")
        print(f"    Example text: {repr(hash_examples[h])}")  # show truncated text

    # Optional: Check for collisions (same hash, different texts)
    print("\n🧩 Checking for potential collisions...")
    seen = {}
    collisions = []

    for i in range(parquet_file.num_row_groups):
        table = parquet_file.read_row_group(i, columns=[column_name])
        series = table[column_name].to_pandas()

        for text in series:
            if text is None:
                continue
            text_str = str(text).strip()
            doc_hash = create_doc_id(text_str)

            if doc_hash in seen and seen[doc_hash] != text_str:
                collisions.append((doc_hash, seen[doc_hash], text_str))
            else:
                seen[doc_hash] = text_str

    if collisions:
        print(f"\n🚨 Found {len(collisions)} hash collisions! SHA256 should not collide — check your data.")
        for h, t1, t2 in collisions[:3]:
            print(f"\nHash: {h}")
            print(f"  Text 1: {repr(t1)}")
            print(f"  Text 2: {repr(t2)}")
    else:
        print("✅ No collisions found — all identical hashes map to identical text.")

def main():
    if len(sys.argv) != 3:
        print("Usage: python check_duplicates_hashmap.py <path_to_parquet_file> <column_name>")
        sys.exit(1)

    parquet_path = sys.argv[1]
    column_name = sys.argv[2]

    analyze_duplicates(parquet_path, column_name)

if __name__ == "__main__":
    main()


# /capstor/scratch/cscs/inesaltemir/detokenized_output/swissai-fineweb-2-quality_33-filterrobots-merge_euro-high_part_001/dump-0-merged.parquet

# /capstor/scratch/cscs/inesaltemir/detokenized_output/swissai-fineweb-2-quality_33-filterrobots-merge_euro-high_part_001/dump-1-merged.parquet