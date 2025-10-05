#!/usr/bin/env python3
"""
Efficient Megatron dataset detokenizer for large-scale datasets (up to 19TB).
Reads .bin/.idx files and outputs detokenized text to Parquet format.
"""

import struct
import argparse
import os
from pathlib import Path
from typing import Iterator, Tuple
import mmap

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import psutil
from transformers import AutoTokenizer

# Constants
_INDEX_HEADER = b"MMIDIDX\x00\x00"


#def load_tokenizer(name_or_path: str):
#    """Load tokenizer from file or HuggingFace hub."""
#    
#    if os.path.isfile(name_or_path):
#        # For local tokenizer files
#        return AutoTokenizer.from_pretrained(name_or_path, local_files_only=True)
#    else:
#        # For HuggingFace hub models
#        return AutoTokenizer.from_pretrained(name_or_path)

def load_tokenizer(name_or_path: str):
    
    # Get HF token from environment
    hf_token = os.environ.get('HF_TOKEN')
    
    if os.path.isfile(name_or_path):
        # For local tokenizer files
        return AutoTokenizer.from_pretrained(name_or_path, local_files_only=True)
    else:
        # For HuggingFace hub models
        try:
            if hf_token:
                # Use token for authentication
                return AutoTokenizer.from_pretrained(
                    name_or_path, 
                    token=hf_token  # Updated parameter name for newer transformers
                )
            else:
                # Try without token first
                return AutoTokenizer.from_pretrained(name_or_path)
        except Exception as e:
            if "gated repo" in str(e).lower() or "401" in str(e):
                raise Exception(f"Authentication required for {name_or_path}. Please set HF_TOKEN environment variable.")
            else:
                raise e
            

class MegatronDatasetReader:
    """Efficient reader for Megatron-format tokenized datasets."""
    
    def __init__(self, bin_path: str, idx_path: str):
        self.bin_path = bin_path
        self.idx_path = idx_path
        self._parse_index()
        
    def _parse_index(self):
        """Parse the .idx file to extract metadata and pointers."""
        with open(self.idx_path, 'rb') as f:
            # Verify header
            header = f.read(9)
            if header != _INDEX_HEADER:
                raise ValueError(f"Invalid index header in {self.idx_path}")
            
            # Read version
            version = struct.unpack('<Q', f.read(8))[0]
            if version != 1:
                raise ValueError(f"Unsupported version {version}")
            
            # Read dtype code
            dtype_code = struct.unpack('<B', f.read(1))[0]
            self.token_size = 4 if dtype_code == 4 else 2
            self.token_dtype = np.int32 if self.token_size == 4 else np.uint16
            
            # Read counts
            self.sequence_count = struct.unpack('<Q', f.read(8))[0]
            self.document_count = struct.unpack('<Q', f.read(8))[0]
            
            # Read sequence lengths
            sequence_lengths_bytes = f.read(self.sequence_count * 4)
            self.sequence_lengths = np.frombuffer(sequence_lengths_bytes, dtype=np.int32)
            
            # Read sequence pointers
            sequence_pointers_bytes = f.read(self.sequence_count * 8)
            self.sequence_pointers = np.frombuffer(sequence_pointers_bytes, dtype=np.int64)
            
            # Read document indices
            document_indices_bytes = f.read(self.document_count * 8)
            self.document_indices = np.frombuffer(document_indices_bytes, dtype=np.int64)
    
    def get_sequence_tokens(self, sequence_idx: int, bin_file) -> np.ndarray:
        """Get tokens for a specific sequence using memory mapping."""
        if sequence_idx >= self.sequence_count:
            raise IndexError(f"Sequence index {sequence_idx} out of range")
        
        start_byte = self.sequence_pointers[sequence_idx]
        length = self.sequence_lengths[sequence_idx]
        
        # Seek to position and read tokens
        bin_file.seek(start_byte)
        token_bytes = bin_file.read(length * self.token_size)
        tokens = np.frombuffer(token_bytes, dtype=self.token_dtype)
        
        return tokens
    
    def iter_sequences(self, chunk_size: int = 1000) -> Iterator[Tuple[int, np.ndarray]]:
        """Iterate through sequences in chunks for memory efficiency."""
        with open(self.bin_path, 'rb') as bin_file:
            for i in range(0, self.sequence_count, chunk_size):
                end_idx = min(i + chunk_size, self.sequence_count)
                sequences = []
                
                for seq_idx in range(i, end_idx):
                    tokens = self.get_sequence_tokens(seq_idx, bin_file)
                    sequences.append((seq_idx, tokens))
                
                yield sequences


def get_memory_limit() -> int:
    """Get recommended memory limit based on available RAM."""
    available_gb = psutil.virtual_memory().available / (1024**3)
    # Use 60% of available memory, minimum 2GB
    recommended_gb = max(2, available_gb * 0.6)
    return int(recommended_gb * 1024**3)  # Convert to bytes


def estimate_batch_size(avg_seq_length: int, memory_limit: int) -> int:
    """Estimate optimal batch size based on memory constraints."""
    # Rough estimate: each character ~4 bytes in memory + overhead
    bytes_per_sequence = avg_seq_length * 4 * 2  # 2x for safety margin
    batch_size = max(100, memory_limit // bytes_per_sequence)
    return min(batch_size, 10000)  # Cap at 10k sequences per batch


def detokenize_batch(tokenizer, sequences: list, remove_special_tokens: bool = True) -> pd.DataFrame:
    """Detokenize a batch of token sequences using batch_decode for efficiency."""
    
    # Prepare batch data
    token_lists = []
    seq_ids = []
    
    for seq_id, tokens in sequences:
        try:
            # Convert numpy array to list if needed
            if isinstance(tokens, np.ndarray):
                tokens = tokens.tolist()
            
            # Ensure tokens are integers (AutoTokenizer expects this)
            tokens = [int(token) for token in tokens]
            
            token_lists.append(tokens)
            seq_ids.append(seq_id)
            
        except Exception as e:
            print(f"Warning: Failed to prepare sequence {seq_id}: {e}")
            continue
    
    if not token_lists:
        return pd.DataFrame({'sequence_id': [], 'text': []})
    
    try:
        # Use batch_decode for much better performance
        texts = tokenizer.batch_decode(token_lists, skip_special_tokens=remove_special_tokens)
        
        # Clean up texts and filter empty ones
        cleaned_texts = []
        cleaned_seq_ids = []
        
        for seq_id, text in zip(seq_ids, texts):
            text = text.strip()
            if text:  # Only include non-empty texts
                cleaned_texts.append(text)
                cleaned_seq_ids.append(seq_id)
        
        return pd.DataFrame({
            'sequence_id': cleaned_seq_ids,
            'text': cleaned_texts
        })
        
    except Exception as e:
        print(f"Warning: batch_decode failed, falling back to individual decode: {e}")
        # Fallback to individual decode if batch fails
        texts = []
        valid_seq_ids = []
        
        for seq_id, tokens in zip(seq_ids, token_lists):
            try:
                text = tokenizer.decode(tokens, skip_special_tokens=remove_special_tokens)
                text = text.strip()
                if text:
                    texts.append(text)
                    valid_seq_ids.append(seq_id)
            except Exception as e2:
                print(f"Warning: Failed to detokenize sequence {seq_id}: {e2}")
                continue
        
        return pd.DataFrame({
            'sequence_id': valid_seq_ids,
            'text': texts
        })





def process_dataset(
    bin_path: str,
    idx_path: str,
    output_path: str,
    tokenizer,  # Changed type hint to be more generic
    chunk_size: int = 1000,
    remove_special_tokens: bool = True,
    compression: str = 'snappy'
):
    """Process the entire dataset with progress tracking."""
    
    # Initialize reader
    reader = MegatronDatasetReader(bin_path, idx_path)
    print(f"Dataset info:")
    print(f"  Sequences: {reader.sequence_count:,}")
    print(f"  Documents: {reader.document_count:,}")
    print(f"  Token size: {reader.token_size} bytes")
    print(f"  Average sequence length: {reader.sequence_lengths.mean():.1f} tokens")
    
    # Estimate optimal batch size
    memory_limit = get_memory_limit()
    optimal_chunk = estimate_batch_size(int(reader.sequence_lengths.mean()), memory_limit)
    chunk_size = min(chunk_size, optimal_chunk)
    print(f"  Using chunk size: {chunk_size}")
    
    # Setup Parquet writer
    schema = pa.schema([
        ('sequence_id', pa.int64()),
        ('text', pa.string())
    ])
    
    # Create output directory if needed
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Process in chunks
    total_chunks = (reader.sequence_count + chunk_size - 1) // chunk_size
    processed_sequences = 0
    
    with pq.ParquetWriter(output_path, schema, compression=compression) as writer:
        with tqdm(total=reader.sequence_count, desc="Detokenizing", unit="seq") as pbar:
            
            for chunk_sequences in reader.iter_sequences(chunk_size):
                # Detokenize batch
                df_batch = detokenize_batch(tokenizer, chunk_sequences, remove_special_tokens)
                
                if not df_batch.empty:
                    # Convert to PyArrow table and write
                    table = pa.Table.from_pandas(df_batch, schema=schema)
                    writer.write_table(table)
                
                processed_sequences += len(chunk_sequences)
                pbar.update(len(chunk_sequences))
                
                # Memory cleanup
                del df_batch, chunk_sequences
    
    print(f"Successfully processed {processed_sequences:,} sequences")
    print(f"Output saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Detokenize Megatron-format datasets")
    parser.add_argument("bin_path", help="Path to .bin file")
    parser.add_argument("idx_path", help="Path to .idx file") 
    parser.add_argument("output_path", help="Output .parquet file path")
    parser.add_argument("--tokenizer", required=True, help="Tokenizer name or path")
    parser.add_argument("--chunk-size", type=int, default=1000, 
                        help="Number of sequences to process per batch")
    parser.add_argument("--keep-special-tokens", action="store_true",
                        help="Keep special tokens in output")
    parser.add_argument("--compression", default="snappy", 
                        choices=["snappy", "gzip", "brotli", "lz4"],
                        help="Compression algorithm for Parquet")
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.exists(args.bin_path):
        raise FileNotFoundError(f"Binary file not found: {args.bin_path}")
    if not os.path.exists(args.idx_path):
        raise FileNotFoundError(f"Index file not found: {args.idx_path}")
    
    # Load tokenizer
    print(f"Loading tokenizer: {args.tokenizer}")
    tokenizer = load_tokenizer(args.tokenizer)
    
    # Process dataset
    process_dataset(
        bin_path=args.bin_path,
        idx_path=args.idx_path,
        output_path=args.output_path,
        tokenizer=tokenizer,
        chunk_size=args.chunk_size,
        remove_special_tokens=not args.keep_special_tokens,
        compression=args.compression
    )


if __name__ == "__main__":
    main()