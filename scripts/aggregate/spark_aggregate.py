#!/usr/bin/env python3
"""
Phase 1: Spark-based global deduplication and source aggregation.

Reads all parquet files from a FineWeb-style dataset, deduplicates
globally by SHA256(text), and collapses all per-occurrence metadata
into a sources array.

Output parquet schema (fixed, read by index_aggregated.py):
  sha256:        string   -- SHA256(text), becomes ES _id
  text:          string
  language:      string   -- top-level; assumed same across occurrences
  source_count:  integer  -- number of occurrences before dedup
  sources:       array of struct {id, url, date, file_path, folder_path}

Handles two dataset layouts:
  per-crawl    FineWeb 1: <dataset_dir>/CC-MAIN-XXXX-XX/*.parquet
  per-language FineWeb 2: <dataset_dir>/<lang_code>/*.parquet

Usage examples
--------------
# FineWeb 1 - all crawls at once (large, needs big node)
spark-submit spark_aggregate.py \\
    --dataset-dir .../fineweb-1_3_0-quality_33-filterrobots/data/output \\
    --output-dir /iopsstor/scratch/cscs/$USER/fineweb-1_3_0-quality_33-filterrobots-aggregated \\
    --layout per-crawl

# FineWeb 2 - one language at a time
spark-submit spark_aggregate.py \\
    --dataset-dir .../fineweb-2_0_1-quality_33-filterrobots/data/output \\
    --output-dir /iopsstor/scratch/cscs/$USER/fineweb-2_0_1-quality_33-filterrobots-aggregated/fin_Latn \\
    --layout per-language \\
    --only-subdir fin_Latn
"""

import argparse
import os
import sys
from pathlib import Path

from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, StructType


# ============================================================
# Spark setup
# ============================================================

def build_spark(app_name: str, driver_memory: str, shuffle_partitions: int) -> SparkSession:
    # Use SPARK_LOCAL_DIR env var if set (should point to large scratch), else /tmp
    local_dir = os.environ.get("SPARK_LOCAL_DIR", "/tmp")
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.local.dir", local_dir)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.mergeSchema", "true")
        # Avoid NPE in parquet codec on single-partition writes (small datasets)
        .config("spark.sql.parquet.compression.codec", "gzip")
        # Avoid OOM on large collect_list
        .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "128")
        .getOrCreate()
    )


# ============================================================
# Schema normalisation
# ============================================================

def _sha256(col):
    """SHA-256 of a text column using Spark's built-in sha2 (runs in JVM, no Python UDF overhead)."""
    return F.sha2(col.cast("binary"), 256)


def _col_or_null(cols: set, *candidates) -> "Column":
    """Return the first existing column from candidates, or NULL."""
    for c in candidates:
        if c in cols:
            return F.col(c).cast(StringType())
    return F.lit(None).cast(StringType())


def _from_metadata(df, field: str, metadata_type: str) -> "Column":
    """Extract a field from the metadata column, handling struct vs map."""
    if metadata_type == "struct":
        # Check if the sub-field actually exists in the struct
        subfields = {f.name for f in df.schema["metadata"].dataType.fields}
        if field in subfields:
            return F.col(f"metadata.{field}").cast(StringType())
    elif metadata_type == "map":
        return F.col("metadata")[field].cast(StringType())
    return F.lit(None).cast(StringType())


def normalize(df, language_override: Optional[str] = None):
    """
    Flatten any parquet schema to:
      text, doc_id, url, date, language, file_path, folder_path
    """
    cols = set(df.columns)

    # Detect metadata column type (struct or map)
    metadata_type = None
    if "metadata" in cols:
        dtype = df.schema["metadata"].dataType
        if isinstance(dtype, StructType):
            metadata_type = "struct"
        elif isinstance(dtype, MapType):
            metadata_type = "map"

    def get(top_candidates, metadata_field, fallback_candidates=()):
        # 1. Try top-level columns
        for c in top_candidates:
            if c in cols:
                return F.col(c).cast(StringType())
        # 2. Try metadata sub-field
        if metadata_type and metadata_field:
            v = _from_metadata(df, metadata_field, metadata_type)
            if v is not None:
                return v
        # 3. Fallback top-level
        for c in fallback_candidates:
            if c in cols:
                return F.col(c).cast(StringType())
        return F.lit(None).cast(StringType())

    select_exprs = [
        F.col("text"),
        F.input_file_name().alias("file_path"),
        get(["id"],                "id").alias("doc_id"),
        get(["url"],               "url").alias("url"),
        get(["date"],              "date",
            fallback_candidates=["date_download", "crawl_timestamp"]).alias("date"),
        get(["language", "lang"],  "language").alias("language"),
    ]

    df = df.select(select_exprs)

    # Override language if caller knows it (e.g. from directory name)
    if language_override:
        df = df.withColumn("language", F.lit(language_override))

    # Derive folder_path from file_path
    df = df.withColumn(
        "folder_path",
        F.regexp_extract(F.col("file_path"), r"^(.*)/[^/]+$", 1),
    )

    return df


# ============================================================
# Aggregation
# ============================================================

def aggregate(df):
    """Group by SHA256(text), collect all source occurrences."""
    df = df.withColumn("sha256", _sha256(F.col("text")))

    df = df.withColumn("source", F.struct(
        F.col("doc_id").alias("id"),
        F.col("url"),
        F.col("date"),
        F.col("file_path"),
        F.col("folder_path"),
    ))

    result = (
        df.groupBy("sha256")
        .agg(
            F.first("text").alias("text"),
            F.first("language").alias("language"),
            F.count("*").cast("integer").alias("source_count"),
            F.collect_list("source").alias("sources"),
        )
    )

    return result


def merge(df):
    """
    Merge mode: input is already-aggregated parquet (per-crawl outputs).
    Schema: sha256, text, language, source_count, sources (array of structs).
    Combines per-crawl outputs into a global dedup with merged sources arrays.
    """
    result = (
        df.groupBy("sha256")
        .agg(
            F.first("text").alias("text"),
            F.first("language").alias("language"),
            F.sum("source_count").cast("integer").alias("source_count"),
            F.flatten(F.collect_list("sources")).alias("sources"),
        )
    )
    return result


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("--mode", choices=["aggregate", "merge"],
                        default="aggregate",
                        help="aggregate: raw parquet → dedup+sources; "
                             "merge: combine per-crawl aggregated outputs globally")
    parser.add_argument("--batch-start", type=int, default=None,
                        help="(merge mode) 0-based index of first crawl dir to process (inclusive)")
    parser.add_argument("--batch-end", type=int, default=None,
                        help="(merge mode) 0-based index of last crawl dir to process (exclusive)")
    parser.add_argument("--dataset-dir", required=True,
                        help="Root directory containing per-crawl or per-language subdirs "
                             "(aggregate mode), or directory of per-crawl agg outputs (merge mode)")
    parser.add_argument("--output-dir", required=True,
                        help="Where to write aggregated parquet (scratch path)")
    parser.add_argument("--layout", choices=["per-crawl", "per-language"],
                        default="per-crawl",
                        help="Directory layout of the dataset (aggregate mode only)")
    parser.add_argument("--only-subdir", default=None,
                        help="Process only this single subdirectory (crawl ID or lang code). "
                             "Useful for testing or per-language FineWeb 2 jobs.")
    parser.add_argument("--language", default=None,
                        help="Override the language field for all documents. "
                             "Use when language is not stored in the parquet (e.g. FineWeb 1).")
    parser.add_argument("--driver-memory", default="200g",
                        help="Spark driver (= executor in local mode) memory")
    parser.add_argument("--shuffle-partitions", type=int, default=4000,
                        help="spark.sql.shuffle.partitions (default 4000)")
    parser.add_argument("--output-partitions", type=int, default=200,
                        help="Number of output parquet files")
    args = parser.parse_args()

    dataset_dir = Path(args.dataset_dir)
    output_dir = Path(args.output_dir)

    if not dataset_dir.is_dir():
        print(f"[ERROR] dataset-dir not found: {dataset_dir}", file=sys.stderr)
        sys.exit(1)

    # ---- Merge mode: read per-crawl aggregated parquet and global-dedup ----
    if args.mode == "merge":
        # dataset_dir contains per-crawl subdirs, each with aggregated parquet
        all_crawl_dirs = sorted(d for d in dataset_dir.iterdir() if d.is_dir())
        if not all_crawl_dirs:
            print(f"[ERROR] No subdirectories found in {dataset_dir}", file=sys.stderr)
            sys.exit(1)
        # Optional batch slice
        start = args.batch_start if args.batch_start is not None else 0
        end   = args.batch_end   if args.batch_end   is not None else len(all_crawl_dirs)
        crawl_dirs = all_crawl_dirs[start:end]
        if not crawl_dirs:
            print(f"[ERROR] No crawl dirs in slice [{start}:{end}]", file=sys.stderr)
            sys.exit(1)
        # Build explicit glob from selected dirs (not wildcard, to respect batch slice)
        input_paths = [str(d / "*.parquet") for d in crawl_dirs]
        print(f"[INFO] Mode:         merge")
        print(f"[INFO] Dataset dir:  {dataset_dir}")
        print(f"[INFO] Crawl dirs:   {len(crawl_dirs)} of {len(all_crawl_dirs)} (slice [{start}:{end}])")
        print(f"[INFO] First crawl:  {crawl_dirs[0].name}")
        print(f"[INFO] Last crawl:   {crawl_dirs[-1].name}")
        print(f"[INFO] Output dir:   {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

        spark = build_spark(
            app_name=f"merge-{dataset_dir.name}-{start}-{end}",
            driver_memory=args.driver_memory,
            shuffle_partitions=args.shuffle_partitions,
        )
        print(f"[INFO] Spark UI: {spark.sparkContext.uiWebUrl}")
        print(f"[INFO] Reading per-crawl aggregated parquets ...")
        df = spark.read.option("mergeSchema", "true").parquet(*input_paths)
        print(f"[INFO] Schema: {df.columns}")
        merged_df = merge(df)
        print(f"[INFO] Writing globally merged output ({args.output_partitions} partitions) ...")
        (
            merged_df
            .repartition(args.output_partitions)
            .write
            .mode("overwrite")
            .parquet(str(output_dir))
        )
        out_count = spark.read.parquet(str(output_dir)).count()
        print(f"[INFO] ✓ Globally unique documents written: {out_count:,}")
        spark.stop()
        print("[INFO] Done.")
        return

    # ---- Aggregate mode (default): raw parquet → dedup + sources ----

    # Determine glob pattern for input files
    if args.only_subdir:
        subdir = dataset_dir / args.only_subdir
        if not subdir.is_dir():
            print(f"[ERROR] Subdir not found: {subdir}", file=sys.stderr)
            sys.exit(1)
        input_glob = str(subdir / "*.parquet")
        n_subdirs = 1
    else:
        subdirs = sorted(d for d in dataset_dir.iterdir() if d.is_dir())
        n_subdirs = len(subdirs)
        input_glob = str(dataset_dir / "*" / "*.parquet")

    if n_subdirs == 0:
        print(f"[ERROR] No subdirectories found in {dataset_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Mode:         aggregate")
    print(f"[INFO] Dataset dir:  {dataset_dir}")
    print(f"[INFO] Subdirs:      {'1 (' + args.only_subdir + ')' if args.only_subdir else n_subdirs}")
    print(f"[INFO] Input glob:   {input_glob}")
    print(f"[INFO] Output dir:   {output_dir}")
    print(f"[INFO] Language:     {args.language or '(from parquet)'}")

    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark(
        app_name=f"aggregate-{dataset_dir.parent.parent.name}",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )

    print(f"[INFO] Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"[INFO] Reading parquets ...")

    df = (
        spark.read
        .option("mergeSchema", "true")
        .option("recursiveFileLookup", "false")
        .parquet(input_glob)
    )

    print(f"[INFO] Schema: {df.columns}")

    df = normalize(df, language_override=args.language)
    agg_df = aggregate(df)

    print(f"[INFO] Writing aggregated output ({args.output_partitions} partitions) ...")
    (
        agg_df
        .repartition(args.output_partitions)
        .write
        .mode("overwrite")
        .parquet(str(output_dir))
    )

    # Quick count to confirm output
    out_count = spark.read.parquet(str(output_dir)).count()
    print(f"[INFO] ✓ Unique documents written: {out_count:,}")

    spark.stop()
    print("[INFO] Done.")


if __name__ == "__main__":
    main()
