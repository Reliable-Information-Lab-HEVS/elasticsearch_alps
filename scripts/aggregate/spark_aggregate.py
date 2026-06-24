#!/usr/bin/env python3
"""
Phase 1: Spark-based global deduplication and source aggregation.

Reads all parquet files from a dataset, deduplicates globally by SHA256(text),
and collapses all per-occurrence metadata into a sources array.

Output parquet schema (fixed; read by index_aggregated.py and the ES indexer):
  sha256:        string          -- SHA256(text), becomes ES _id
  text:          string
  language:      array<string>   -- unique languages across all sources
  date:          array<string>   -- unique crawl dates across all sources
  source_count:  integer         -- total occurrences before dedup
  sources:       array of struct {
                   dataset,        -- e.g. "fineweb-1", "fineweb-edu", "dclm"
                   id,             -- original document id
                   language,       -- per-source language
                   date,           -- per-source crawl date
                   url,            -- per-source URL
                   language_score, -- fasttext language confidence
                   folder_path,    -- parent directory of source file (for retrieval)
                 }

Handles dataset layouts:
  per-crawl    FineWeb 1/edu: <dataset_dir>/CC-MAIN-XXXX-XX/*.parquet
  per-language FineWeb 2:     <dataset_dir>/<lang_code>/*.parquet
  flat         DCLM, others:  <dataset_dir>/*.parquet  (no subdirs)

Usage examples
--------------
# Aggregate FW-edu one crawl at a time (95 crawls → submit as SLURM array)
spark-submit spark_aggregate.py \\
    --mode aggregate --dataset fineweb-edu \\
    --dataset-dir .../fineweb-edu-score-2-filterrobots/data/output \\
    --layout per-crawl --only-subdir CC-MAIN-2020-05 \\
    --output-dir $SCRATCH/fw-edu-per-crawl/CC-MAIN-2020-05

# Merge per-crawl outputs into single deduped dataset
spark-submit spark_aggregate.py \\
    --mode merge --dataset fineweb-edu \\
    --dataset-dir $SCRATCH/fw-edu-per-crawl \\
    --output-dir $SCRATCH/fw-edu-merged

# Aggregate DCLM (flat layout, all parquets in one directory)
spark-submit spark_aggregate.py \\
    --mode aggregate --dataset dclm \\
    --dataset-dir .../dclm-edu-filterrobots_fine/data/output \\
    --layout flat \\
    --output-dir $SCRATCH/dclm-aggregated

# Cross-dataset global dedup: FW1 (already merged) + FW-edu + DCLM
# Format: path:dataset_name  (path must contain *.parquet files directly)
spark-submit spark_aggregate.py \\
    --mode global-merge \\
    --output-dir $SCRATCH/fw-english-merged \\
    --shuffle-partitions 8000 --output-partitions 500 \\
    --input-datasets \\
        $SCRATCH/fw1-merged:fineweb-1 \\
        $SCRATCH/fw-edu-merged:fineweb-edu \\
        $SCRATCH/dclm-aggregated:dclm

# FineWeb 2 - one language at a time (unchanged workflow)
spark-submit spark_aggregate.py \\
    --mode aggregate --dataset fineweb-2 \\
    --dataset-dir .../fineweb-2_0_1-quality_33-filterrobots/data/output \\
    --layout per-language --only-subdir fin_Latn \\
    --output-dir $SCRATCH/fw2-aggregated/fin_Latn
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, MapType, StringType, StructType
)


# ============================================================
# Spark setup
# ============================================================

def build_spark(app_name: str, driver_memory: str, shuffle_partitions: int) -> SparkSession:
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
        .config("spark.sql.parquet.compression.codec", "gzip")
        .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "128")
        .getOrCreate()
    )


# ============================================================
# Schema normalisation
# ============================================================

def _sha256(col):
    return F.sha2(col.cast("binary"), 256)


def normalize(df, language_override: Optional[str] = None) -> DataFrame:
    """
    Flatten any supported parquet schema to the canonical intermediate form:
      text, doc_id, url, date, language, file_path, folder_path,
      cc_dump, language_score, pii_count, edu_score, edu_int_score,
      quality_score, toxic_score, token_count, fasttext_score

    Fields not present in a given dataset are null.
    """
    cols = set(df.columns)

    # Inspect metadata column type
    metadata_type = None
    meta_subfields: set = set()
    if "metadata" in cols:
        dtype = df.schema["metadata"].dataType
        if isinstance(dtype, StructType):
            metadata_type = "struct"
            meta_subfields = {f.name for f in dtype.fields}
        elif isinstance(dtype, MapType):
            metadata_type = "map"

    def meta(*fields):
        """
        Try metadata sub-fields in order; return first available, else null.
        For struct metadata: skips fields not in the schema.
        For map metadata: tries each key (null if absent).
        """
        exprs = []
        for field in fields:
            if metadata_type == "struct":
                if field in meta_subfields:
                    exprs.append(F.col(f"metadata.{field}"))
            elif metadata_type == "map":
                exprs.append(F.col("metadata")[field])
        if not exprs:
            return F.lit(None)
        return F.coalesce(*exprs) if len(exprs) > 1 else exprs[0]

    def top_or_meta(top_col, *meta_fields):
        """Use top-level column if present, otherwise fall back to metadata."""
        if top_col in cols:
            return F.col(top_col)
        return meta(*meta_fields)

    select_exprs = [
        F.col("text"),
        F.input_file_name().alias("file_path"),
        top_or_meta("id",       "id")               .cast(StringType()) .alias("doc_id"),
        top_or_meta("url",      "url")              .cast(StringType()) .alias("url"),
        top_or_meta("date",     "date", "date_download",
                    "crawl_timestamp")              .cast(StringType()) .alias("date"),
        top_or_meta("language", "language")         .cast(StringType()) .alias("language"),
        meta("language_score")                      .cast(DoubleType()) .alias("language_score"),
    ]

    df = df.select(select_exprs)

    if language_override:
        df = df.withColumn("language", F.lit(language_override))

    df = df.withColumn(
        "folder_path",
        F.regexp_extract(F.col("file_path"), r"^(.*)/[^/]+$", 1),
    )

    return df


# ============================================================
# Aggregation
# ============================================================

def aggregate(df: DataFrame, dataset_name: str) -> DataFrame:
    """
    Deduplicate by SHA256(text), building the sources array.
    Produces the canonical output schema.
    """
    df = df.withColumn("sha256", _sha256(F.col("text")))

    df = df.withColumn("source", F.struct(
        F.lit(dataset_name)       .alias("dataset"),
        F.col("doc_id")           .alias("id"),
        F.col("language"),
        F.col("date"),
        F.col("url"),
        F.col("language_score"),
        F.col("folder_path"),
    ))

    result = (
        df.groupBy("sha256")
        .agg(
            F.first("text")                                    .alias("text"),
            F.collect_set("language")                          .alias("language"),   # unique values
            F.collect_set("date")                              .alias("date"),       # unique values
            F.count("*").cast(IntegerType())                   .alias("source_count"),
            F.collect_list("source")                           .alias("sources"),
        )
    )

    return result


def reduce_merge(dfs: List[DataFrame]) -> DataFrame:
    """
    Hierarchical reduce-merge: union multiple already-aggregated DataFrames and
    deduplicate by sha256.  Does NOT re-stamp sources[].dataset — sources arrays
    already carry correct dataset names from the original aggregate step.
    Used for every round of the hierarchical global merge.
    """
    normalized = []
    for df in dfs:
        df = _normalize_sources_schema(df)
        df = _ensure_array_columns(df)
        normalized.append(df)

    combined = normalized[0]
    for df in normalized[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    return (
        combined.groupBy("sha256")
        .agg(
            F.first("text")                                            .alias("text"),
            F.array_distinct(F.flatten(F.collect_list("language")))   .alias("language"),
            F.array_distinct(F.flatten(F.collect_list("date")))       .alias("date"),
            F.sum("source_count").cast(IntegerType())                 .alias("source_count"),
            F.flatten(F.collect_list("sources"))                      .alias("sources"),
        )
    )


def merge(df: DataFrame, dataset_name: str) -> DataFrame:
    """
    Within-dataset merge: combine per-crawl aggregated outputs for one dataset.
    Stamps dataset_name into each source entry (handles outputs written before
    the dataset field was introduced).
    """
    df = _stamp_dataset(df, dataset_name)
    df = _ensure_array_columns(df)

    result = (
        df.groupBy("sha256")
        .agg(
            F.first("text")                                            .alias("text"),
            F.array_distinct(F.flatten(F.collect_list("language")))   .alias("language"),
            F.array_distinct(F.flatten(F.collect_list("date")))       .alias("date"),
            F.sum("source_count").cast(IntegerType())                 .alias("source_count"),
            F.flatten(F.collect_list("sources"))                      .alias("sources"),
        )
    )
    return result


def global_merge(dfs_with_names: List[Tuple[DataFrame, str]]) -> DataFrame:
    """
    Cross-dataset merge: deduplicate across multiple already-aggregated datasets.
    Each input DataFrame is stamped with its dataset name, missing columns are
    filled with null, then all are unioned and grouped by sha256.

    Documents present in more than one dataset will have multiple entries in
    their sources array, one per occurrence, with the dataset field identifying
    the origin.
    """
    stamped = []
    for df, name in dfs_with_names:
        df = _stamp_dataset(df, name)
        df = _normalize_sources_schema(df)
        df = _ensure_array_columns(df)
        stamped.append(df)

    combined = stamped[0]
    for df in stamped[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    result = (
        combined.groupBy("sha256")
        .agg(
            F.first("text")                                            .alias("text"),
            F.array_distinct(F.flatten(F.collect_list("language")))   .alias("language"),
            F.array_distinct(F.flatten(F.collect_list("date")))       .alias("date"),
            F.sum("source_count").cast(IntegerType())                 .alias("source_count"),
            F.flatten(F.collect_list("sources"))                      .alias("sources"),
        )
    )
    return result


# ============================================================
# Helpers
# ============================================================

def _stamp_dataset(df: DataFrame, dataset_name: str) -> DataFrame:
    """
    Set sources[*].dataset = dataset_name for every element in the sources array.
    Handles both old outputs (no dataset field) and new ones (already set).
    Requires Spark 3.1+ for withField on nested structs.
    """
    return df.withColumn(
        "sources",
        F.transform(
            F.col("sources"),
            lambda s: s.withField("dataset", F.lit(dataset_name)),
        ),
    )


_CANONICAL_SOURCE_FIELDS = [
    ("dataset",        StringType()),
    ("id",             StringType()),
    ("language",       StringType()),
    ("date",           StringType()),
    ("url",            StringType()),
    ("language_score", DoubleType()),
    ("folder_path",    StringType()),
]


def _normalize_sources_schema(df: DataFrame) -> DataFrame:
    """
    Project sources[*] to the canonical field set, adding null columns for any
    fields absent in the current struct (e.g. FW1's old output lacks language,
    language_score, and url inside source entries).
    Required so unionByName works cleanly across datasets with different source schemas.
    """
    from pyspark.sql.types import ArrayType
    sources_type = df.schema["sources"].dataType
    if not isinstance(sources_type, ArrayType):
        return df
    existing = {f.name for f in sources_type.elementType.fields}

    return df.withColumn(
        "sources",
        F.transform(
            F.col("sources"),
            lambda s: F.struct(*[
                s[name].cast(dtype).alias(name) if name in existing
                else F.lit(None).cast(dtype).alias(name)
                for name, dtype in _CANONICAL_SOURCE_FIELDS
            ]),
        ),
    )


def _ensure_array_columns(df: DataFrame) -> DataFrame:
    """
    Ensure language and date are array columns. Old aggregated outputs (pre-schema
    update, e.g. the already-merged FW1) have them as scalar strings; coerce to
    single-element arrays so merge/global-merge aggs work uniformly.
    Also ensures a top-level url column exists.
    """
    from pyspark.sql.types import ArrayType

    schema = df.schema

    for col_name in ("language", "date"):
        if col_name in df.columns:
            if not isinstance(schema[col_name].dataType, ArrayType):
                df = df.withColumn(col_name, F.array(F.col(col_name)))
        else:
            # Derive from sources array field
            df = df.withColumn(col_name, F.array_distinct(F.col(f"sources.{col_name}")))

    return df


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )
    parser.add_argument(
        "--mode",
        choices=["aggregate", "merge", "global-merge", "reduce-merge"],
        default="aggregate",
        help=(
            "aggregate: raw parquet → per-dataset dedup+sources; "
            "merge: combine per-crawl aggregated outputs for one dataset; "
            "global-merge: cross-dataset dedup from multiple aggregated dirs"
        ),
    )
    parser.add_argument(
        "--dataset", default=None,
        help="Dataset name stamped into each source entry "
             "(required for aggregate and merge; for global-merge use --input-datasets)",
    )
    # aggregate / merge
    parser.add_argument("--dataset-dir", default=None,
                        help="Root directory (aggregate / merge modes)")
    parser.add_argument(
        "--layout",
        choices=["per-crawl", "per-language", "flat"],
        default="per-crawl",
        help=(
            "Directory layout for aggregate mode. "
            "per-crawl: subdirs named CC-MAIN-XXXX-XX/; "
            "per-language: subdirs named by language code; "
            "flat: all parquets directly in dataset-dir"
        ),
    )
    parser.add_argument("--only-subdir", default=None,
                        help="Process only this single subdirectory (crawl or lang code)")
    parser.add_argument("--language", default=None,
                        help="Override the language field for all documents")
    parser.add_argument("--batch-start", type=int, default=None,
                        help="(merge) 0-based index of first crawl dir (inclusive)")
    parser.add_argument("--batch-end", type=int, default=None,
                        help="(merge) 0-based index of last crawl dir (exclusive)")
    # global-merge
    parser.add_argument(
        "--input-datasets", nargs="+", default=None,
        help="(global-merge) 'path:dataset_name' pairs. path must contain *.parquet files.",
    )
    # reduce-merge
    parser.add_argument(
        "--input-dirs", nargs="+", default=None,
        help="(reduce-merge) directories whose *.parquet files are merged together.",
    )
    parser.add_argument(
        "--input-manifest", default=None,
        help="(reduce-merge) text file with one parquet path per line.",
    )
    # common
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--driver-memory", default="200g")
    parser.add_argument("--shuffle-partitions", type=int, default=4000)
    parser.add_argument("--output-partitions", type=int, default=200)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    # ------------------------------------------------------------------
    # Global-merge mode
    # ------------------------------------------------------------------
    if args.mode == "global-merge":
        if not args.input_datasets:
            print("[ERROR] --input-datasets required for global-merge mode", file=sys.stderr)
            sys.exit(1)

        specs: List[Tuple[str, str]] = []
        for spec in args.input_datasets:
            if ":" not in spec:
                print(f"[ERROR] --input-datasets must be 'path:dataset_name', got: {spec}",
                      file=sys.stderr)
                sys.exit(1)
            path, ds_name = spec.rsplit(":", 1)
            specs.append((path, ds_name))

        print(f"[INFO] Mode:         global-merge")
        print(f"[INFO] Output dir:   {output_dir}")
        for path, name in specs:
            print(f"[INFO] Input:        {path}  (dataset={name})")

        output_dir.mkdir(parents=True, exist_ok=True)

        spark = build_spark(
            app_name="global-merge-" + "-".join(n for _, n in specs),
            driver_memory=args.driver_memory,
            shuffle_partitions=args.shuffle_partitions,
        )
        print(f"[INFO] Spark UI: {spark.sparkContext.uiWebUrl}")

        dfs_with_names = []
        for path, ds_name in specs:
            pq_path = str(Path(path) / "*.parquet")
            df = spark.read.option("mergeSchema", "true").parquet(pq_path)
            print(f"[INFO] Loaded {ds_name}: schema={df.columns}")
            dfs_with_names.append((df, ds_name))

        merged_df = global_merge(dfs_with_names)

        print(f"[INFO] Writing globally merged output ({args.output_partitions} partitions) ...")
        (
            merged_df
            .repartition(args.output_partitions)
            .write.mode("overwrite")
            .parquet(str(output_dir))
        )
        out_count = spark.read.parquet(str(output_dir)).count()
        print(f"[INFO] ✓ Globally unique documents written: {out_count:,}")
        spark.stop()
        print("[INFO] Done.")
        return

    # ------------------------------------------------------------------
    # Reduce-merge mode (hierarchical global dedup, no dataset re-stamping)
    # ------------------------------------------------------------------
    if args.mode == "reduce-merge":
        if not args.input_dirs and not args.input_manifest:
            print("[ERROR] --input-dirs or --input-manifest required for reduce-merge",
                  file=sys.stderr)
            sys.exit(1)

        input_paths: List[str] = []
        if args.input_manifest:
            manifest = Path(args.input_manifest)
            if not manifest.exists():
                print(f"[ERROR] Manifest not found: {manifest}", file=sys.stderr)
                sys.exit(1)
            input_paths = [p.strip() for p in manifest.read_text().splitlines() if p.strip()]
            print(f"[INFO] Mode:          reduce-merge (manifest: {manifest}, {len(input_paths)} files)")
        else:
            for d in args.input_dirs:
                files = sorted(Path(d).glob("*.parquet"))
                if not files:
                    print(f"[WARN] No parquet files in {d}", file=sys.stderr)
                input_paths.extend(str(f) for f in files)
            print(f"[INFO] Mode:          reduce-merge (dirs: {args.input_dirs}, {len(input_paths)} files)")

        if not input_paths:
            print("[ERROR] No parquet files found", file=sys.stderr)
            sys.exit(1)

        print(f"[INFO] Output dir:    {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

        spark = build_spark(
            app_name="reduce-merge",
            driver_memory=args.driver_memory,
            shuffle_partitions=args.shuffle_partitions,
        )
        print(f"[INFO] Spark UI: {spark.sparkContext.uiWebUrl}")

        # Group files by parent directory so each read call sees a uniform schema.
        # This is required when mixing sources with different schemas (e.g. FW1 has
        # scalar language/date strings while FW-edu/DCLM have array types).
        from collections import defaultdict
        by_dir = defaultdict(list)
        for p in input_paths:
            by_dir[str(Path(p).parent)].append(p)

        dfs = []
        for parent_dir, paths in sorted(by_dir.items()):
            print(f"[INFO] Reading {len(paths)} files from {parent_dir}")
            df = spark.read.option("mergeSchema", "true").parquet(*paths)
            df = _normalize_sources_schema(df)
            df = _ensure_array_columns(df)
            dfs.append(df)

        merged_df = reduce_merge(dfs)

        print(f"[INFO] Writing reduced output ({args.output_partitions} partitions) ...")
        (
            merged_df
            .repartition(args.output_partitions)
            .write.mode("overwrite")
            .parquet(str(output_dir))
        )
        out_count = spark.read.parquet(str(output_dir)).count()
        print(f"[INFO] ✓ Unique documents written: {out_count:,}")
        spark.stop()
        print("[INFO] Done.")
        return

    # ------------------------------------------------------------------
    # Merge mode (within one dataset, across crawl batches)
    # ------------------------------------------------------------------
    if args.mode == "merge":
        if not args.dataset:
            print("[ERROR] --dataset required for merge mode", file=sys.stderr)
            sys.exit(1)
        if not args.dataset_dir:
            print("[ERROR] --dataset-dir required for merge mode", file=sys.stderr)
            sys.exit(1)

        dataset_dir = Path(args.dataset_dir)
        all_crawl_dirs = sorted(d for d in dataset_dir.iterdir() if d.is_dir())
        if not all_crawl_dirs:
            print(f"[ERROR] No subdirectories found in {dataset_dir}", file=sys.stderr)
            sys.exit(1)

        start = args.batch_start if args.batch_start is not None else 0
        end   = args.batch_end   if args.batch_end   is not None else len(all_crawl_dirs)
        crawl_dirs = all_crawl_dirs[start:end]
        if not crawl_dirs:
            print(f"[ERROR] No crawl dirs in slice [{start}:{end}]", file=sys.stderr)
            sys.exit(1)

        input_paths = [str(d / "*.parquet") for d in crawl_dirs]

        print(f"[INFO] Mode:         merge  (dataset={args.dataset})")
        print(f"[INFO] Dataset dir:  {dataset_dir}")
        print(f"[INFO] Crawl dirs:   {len(crawl_dirs)} of {len(all_crawl_dirs)} [{start}:{end}]")
        print(f"[INFO] Output dir:   {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

        spark = build_spark(
            app_name=f"merge-{args.dataset}-{start}-{end}",
            driver_memory=args.driver_memory,
            shuffle_partitions=args.shuffle_partitions,
        )
        print(f"[INFO] Spark UI: {spark.sparkContext.uiWebUrl}")
        df = spark.read.option("mergeSchema", "true").parquet(*input_paths)
        merged_df = merge(df, dataset_name=args.dataset)

        print(f"[INFO] Writing merged output ({args.output_partitions} partitions) ...")
        (
            merged_df
            .repartition(args.output_partitions)
            .write.mode("overwrite")
            .parquet(str(output_dir))
        )
        out_count = spark.read.parquet(str(output_dir)).count()
        print(f"[INFO] ✓ Unique documents written: {out_count:,}")
        spark.stop()
        print("[INFO] Done.")
        return

    # ------------------------------------------------------------------
    # Aggregate mode (raw parquet → per-dataset dedup)
    # ------------------------------------------------------------------
    if not args.dataset:
        print("[ERROR] --dataset required for aggregate mode", file=sys.stderr)
        sys.exit(1)
    if not args.dataset_dir:
        print("[ERROR] --dataset-dir required for aggregate mode", file=sys.stderr)
        sys.exit(1)

    dataset_dir = Path(args.dataset_dir)

    if args.layout == "flat":
        input_glob = str(dataset_dir / "*.parquet")
        subdir_desc = "(flat)"
    elif args.only_subdir:
        subdir = dataset_dir / args.only_subdir
        if not subdir.is_dir():
            print(f"[ERROR] Subdir not found: {subdir}", file=sys.stderr)
            sys.exit(1)
        input_glob = str(subdir / "*.parquet")
        subdir_desc = args.only_subdir
    else:
        subdirs = sorted(d for d in dataset_dir.iterdir() if d.is_dir())
        if not subdirs:
            print(f"[ERROR] No subdirectories found in {dataset_dir}", file=sys.stderr)
            sys.exit(1)
        input_glob = str(dataset_dir / "*" / "*.parquet")
        subdir_desc = f"{len(subdirs)} subdirs"

    print(f"[INFO] Mode:         aggregate  (dataset={args.dataset})")
    print(f"[INFO] Layout:       {args.layout}  ({subdir_desc})")
    print(f"[INFO] Input glob:   {input_glob}")
    print(f"[INFO] Output dir:   {output_dir}")
    print(f"[INFO] Language:     {args.language or '(from parquet)'}")
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark(
        app_name=f"aggregate-{args.dataset}",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )
    print(f"[INFO] Spark UI: {spark.sparkContext.uiWebUrl}")

    df = (
        spark.read
        .option("mergeSchema", "true")
        .option("recursiveFileLookup", "false")
        .parquet(input_glob)
    )
    print(f"[INFO] Input schema: {df.columns}")

    df = normalize(df, language_override=args.language)
    agg_df = aggregate(df, dataset_name=args.dataset)

    print(f"[INFO] Writing aggregated output ({args.output_partitions} partitions) ...")
    (
        agg_df
        .repartition(args.output_partitions)
        .write.mode("overwrite")
        .parquet(str(output_dir))
    )

    out_count = spark.read.parquet(str(output_dir)).count()
    print(f"[INFO] ✓ Unique documents written: {out_count:,}")
    spark.stop()
    print("[INFO] Done.")


if __name__ == "__main__":
    main()
