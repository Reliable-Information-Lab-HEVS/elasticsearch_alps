# Deduplication pipeline

This folder contains the Spark-based deduplication pipeline for Swiss-AI pretraining datasets.
The goal is to produce globally deduplicated parquet outputs that can be indexed into Elasticsearch
without any document appearing twice.

---

## Which datasets are deduplicated here?

### English web data — joint cross-dataset dedup

FineWeb-1, FineWeb-edu, and DCLM all draw from Common Crawl and are known to overlap significantly.
They are deduplicated **together** so that a document present in multiple datasets appears only once
in the final index.

| Dataset | Capstor source path |
|---------|---------------------|
| FineWeb-1 | (pre-merged on scratch, see below) |
| FineWeb-edu | `.../fineweb-edu-score-2-filterrobots/data/output/` |
| DCLM | `.../dclm-edu-filterrobots_fine/data/output/` |

All capstor paths are under `/capstor/store/cscs/swissai/infra01/datasets/swiss-ai/`.

### FineWeb-2 — per-language dedup

FineWeb-2 covers ~100 languages. Each language is deduplicated independently
(one `aggregate_job.sh` per language), then stored to capstor. There is no cross-language
dedup since different-language documents are disjoint by definition.

### All other datasets — no Spark dedup

EuroParl, ParaDocs, FLAN, EuroBlocks, Institutional Books, Gutenberg, and Canaries are
**not deduplicated with Spark**. They are either internally clean (curated datasets) or small
enough that even if duplicates exist, their impact on total index size is negligible.
Instead, they are indexed directly using SHA256(text) as the Elasticsearch `_id` — any
exact-duplicate documents are silently dropped by ES on write (first write wins).

---

## Storage paths

| Phase | Filesystem | Path |
|-------|-----------|------|
| Processing (Spark scratch) | iopsstor | `/iopsstor/scratch/cscs/alexandersternfeld/` |
| Long-term dedup storage | capstor | `/capstor/store/cscs/swissai/a145/es_indices_2026/<dataset>/deduplication/` |
| ES index data | capstor | `/capstor/store/cscs/swissai/a145/es_indices_2026/<dataset>/indices/` |

iopsstor has a 14-day retention policy. After deduplication completes, slim parquets
(text column dropped) are copied to capstor using the scripts in `../store/`.
The full-text parquets on iopsstor are only needed during indexing; they can be deleted
once the ES index is archived to capstor.

---

## English web dedup pipeline

The pipeline has five phases, all orchestrated by `submit_dedup_pipeline.sh`:

```
Phase A  FW-edu per-crawl aggregate   95 × aggregate_job.sh (SLURM array)
           ↓
Phase B  FW-edu batch merges          10 × aggregate_job.sh --mode merge
           ↓
Phase C  FW-edu final merge           1  × spark_aggregate.py --mode global-merge
                                           (combines the 10 batch outputs)
Phase D  DCLM flat aggregate          1  × aggregate_job.sh (runs in parallel with A-C)
           ↓ (after C and D)
Phase E  Hierarchical global merge    100 → 50 → 25 → 10 → 5 → 1 jobs
           FW1 + FW-edu + DCLM            submit_global_merge_hierarchical.py
           → english-web-dedup/
```

FineWeb-1 enters at Phase E already merged (previous pipeline run).

### Why hierarchical for Phase E?

The combined input (FW1 ~2.7TB + FW-edu ~3TB + DCLM ~1.4TB ≈ 7TB) is too large for a
single Spark groupBy without timing out or running out of memory during the shuffle.
The hierarchical approach distributes files across 100 round-1 jobs, then reduces through
successive rounds. Each round's jobs depend on the previous round completing.

A lightweight launcher job (submitted with `afterok:C:D`) runs
`submit_global_merge_hierarchical.py` once the input directories exist, which in turn
submits all hierarchical rounds with the correct dependency chains.

Output: `$SCRATCH/english-web-dedup/` (~2-3TB, globally unique documents)

---

## FineWeb-2 per-language pipeline

FW2 is handled separately from the orchestrator above. Each language is submitted
individually via `aggregate_job.sh` with `--layout per-language --only-subdir <lang>`.

There is no merge step — the per-language aggregate output is the final dedup output
for FW2. It is indexed directly from `$SCRATCH/fineweb-2_0_1-quality_33-filterrobots-aggregated/`.

---

## Scripts

| Script | Purpose |
|--------|---------|
| `spark_aggregate.py` | Core Spark pipeline. Modes: `aggregate`, `merge`, `global-merge`, `reduce-merge`. |
| `aggregate_job.sh` | SLURM wrapper for `aggregate` and `merge` modes. |
| `submit_dedup_pipeline.sh` | Orchestrates the full English web dedup pipeline (Phases A–E). Run with `--dry-run` to preview. |
| `submit_global_merge_hierarchical.py` | Submits the Phase E hierarchical merge rounds with dependency chains. Run from the login node after Phase C and D complete (or automatically via the launcher job). |
| `reduce_merge_job.sh` | SLURM wrapper for a single `reduce-merge` round (used by the hierarchical pipeline). |
| `fw_edu_crawls.txt` | Auto-generated list of FW-edu CC-MAIN-XXXX-XX crawl names. |

### `spark_aggregate.py` modes

- **`aggregate`** — reads raw parquet (per-crawl or flat), computes SHA256, groups by SHA256 into a `sources` array. One job per crawl/language.
- **`merge`** — combines multiple per-crawl `aggregate` outputs for the same dataset. Used for FW-edu batch merges.
- **`global-merge`** — unions multiple already-aggregated datasets, handles schema differences, re-stamps `sources[].dataset`. Used for FW-edu's final merge across its own batches.
- **`reduce-merge`** — same reduce logic as `global-merge` but does **not** re-stamp dataset names (sources already correctly labelled from the original aggregate step). Used for all hierarchical Phase E rounds.

---

## After deduplication

Once dedup outputs are on iopsstor scratch, run the store scripts to copy slim parquets
(text column dropped) to capstor for long-term retention:

```bash
# See ../store/README.md or scripts in ../store/
bash ../store/store_results.sh   # requires DATASET_NAME and AGG_DIR env vars
```

Then index everything into Elasticsearch using `../index/index_all.sh`.
