# ElasticSearch Deployment on Clariden ALPS CSCS Cluster

# Welcome

## Container Creation
The container_image folder contains the Dockerfile for the Container Image. Exact instructions on how to proceed are:
1. There is a created Dockerfile with desired packages, in “container_image/" folder 
2. Ensure you're in the directory containing the `Dockerfile` and run:
    podman build -t image:tag (⇒ you choose the desired image:tag)
3. enroot import -x mount -o <image_name.sqsh> podman://image:tag
4. Create .toml file in home directory /.edf path. 
    '''
    image = "/container_image/<image_name.sqsh>"
    workdir = "/capstor/scratch/cscs/<username>"
    writable = true
    mounts = [
        "/iopsstor/scratch/cscs/<username>:/iopsstor/scratch/cscs/<username>",
        "/capstor/scratch/cscs/<username>:/capstor/scratch/cscs/<username>",
        "/iopsstor/scratch/cscs/<username>/es-data:/usr/share/elasticsearch/data",
        "/iopsstor/scratch/cscs/<username>/es-logs:/usr/share/elasticsearch/logs"
    ]
    [annotations.com.hooks.ssh]
    enabled = "true"
    '''
    Explain that environment variables do not seem to be ingested at this point.
    Mount every directory you wish to be able to work from with this container.
    Absolutely need to connect to the elasticsearch data and logs folder
        
5. Open the container with: srun -A a-<account_number> --environment=<image_name> --pty bash
    1. Give instructions to verify versions

link to cscs documentation

## Index Configuration Settings for ElasticSearch

In the <index_config> folder, we store the index configuration for our desired index mappings. These may or not include the <keyword> field (potentially for the "url" field), have a different number of shards (depending on the size of dataset we're indexing), and on the refresh interval. ((Can parametrize these values, to have a single index config))

# Scripts

## Index

Explain code, how to run it, basic parameters to set (remove your specific names and paths)

## Search (provide examples?)

We explain the different types of search queries implemented in thid Elasticsearch search queries pipeline. Each query type serves different search scenarios and has specific use cases, limitations, and performance characteristics.

### Query Types Overview

The pipeline implements **6 different query types** that can be selectively enabled/disabled through configuration flags:

### 1. Match Query (`match_query`)
**Purpose**: the standard query for performing a (general) full-text search

**How it works**:
- Performs an OR query for all individual terms in the search phrase by default
- Analyzes the input text and searches for each term separately
- Terms can appear in any order within the document
- Can be configured to use AND operator (all terms must be present, set <operator> parameter)
- Finding documents that contain most/all search terms (higher score - explain in detail how computed)
- Handling synonyms and stemming through analyzers (depends on analyzer used - can parametrize this too to have +- harsh analyzer)

talk about <minimum_should_match> (Minimum number of clauses that must match for a document to be returned)

### 2. Match Phrase Query (`match_phrase_query`)
**Purpose**: Exact phrase matching with word order preservation

**How it works**:
- Searches for the exact phrase where word order matters
- Functions as an AND clause between all individual terms with positional information
- Supports slop parameter for allowing word gaps
- Uses the `text.exact` field for precise matching in our precise case


### 3. Term Query Exact (`term_query_exact`)
**Purpose**: Exact token (keyword) matching without analysis

**How it works**:
- Looks for exact matches in the inverted index
- Searches for a single token that exactly matches the entire input string
- **Limitation**: Only works with single words - automatically skips multi-word phrases (no fallback)
- Uses lowercase conversion for matching

### 4. Wildcard Query (`wildcard_query`) -- MAY DELETE, too costly timewise
**Purpose**: Pattern matching with wildcards on single tokens

**How it works**:
- Performs single token matching with wildcard patterns (`*text*`)
- Works on individual tokens, not across multiple tokens
- **Limitation**: Only executes on single words - skips multi-word phrases
- More expensive than other query types - slower due to pattern matching complexity
- Performs partial word matching, finding variations of a root word (same as match_phrase with stemmer?)

### 5. Fuzzy Query (`fuzzy_query`)
**Purpose**: Handling typos and spelling variations

**How it works**:
- Designed for single words with typos or spelling variations
- Uses edit distance (Levenshtein distance) with "AUTO" fuzziness
- **Fallback**: For multi-word phrases, uses `multi_match` with fuzziness and AND operator
- Tries to match against single tokens

### 6. Boolean Must Query (`bool_must_query`)
**Purpose**: Complex boolean combinations with multiple conditions

**How it works**:
- Combines multiple match conditions using boolean logic
- All conditions must be satisfied (AND logic)
- If single word provided, duplicates it for the boolean structure
- Can construct advanced filtering scenarios for complex search requirements


**Output and Analysis**

Each query execution provides:
- **Response time**: Query execution time in milliseconds
- **Hit count**: Number of matching documents
- **Score information**: Relevance scoring details
- **Hit snippets**: Top 5 results with highlighted matches

**Configuration Options**

```python
# Query execution flags - set to False to skip query types
execute_match_query = True
execute_match_phrase_query = True
execute_term_query_exact = True
execute_wildcard_query = True
execute_fuzzy_query = True
execute_bool_must_query = False  
```


