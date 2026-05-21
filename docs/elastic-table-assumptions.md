# Elastic Table Assumptions

Powdrr's current Elastic-compatible serving path does not accept an arbitrary
Iceberg table shape. It builds one `_search_index.parquet` sidecar per data
file and joins that sidecar back to the base file at query time. This document
captures the assumptions that contract currently depends on.

## Validated Assumptions

These checks now run in both places that build Elastic sidecars:

- `powdrr-cli elastic validate`
- local cache builds through `powdrr-cli elastic build`
- server-side Elastic sidecar generation in `query_runtime/src/elastic_search_index.rs`

The current validated assumptions are:

1. Every data file must be readable as Parquet.
2. Every data file must contain the document id field.
3. The document id field must use the same scalar type in every file in the
   table.
   Supported types today are `string`, `integer`, `float`, and `boolean`.
4. Every data file must expose at least one additional top-level string column
   besides the document id field.
5. If the document id field is overridden from the CLI, its field name must be
   a simple SQL identifier:
   ASCII letters, numbers, and underscores, starting with a letter or
   underscore.

If any of those assumptions fail, Elastic sidecar generation now returns an
explicit error instead of silently publishing invalid metadata.

## Serving Behavior

Even when validation passes, the current Elastic sidecar path still makes
behavioral assumptions worth calling out:

- Only top-level string columns are indexed for text search.
- Nested objects, arrays, and non-string columns are not tokenized.
- Tokenization is whitespace-based via `string_to_array(field_value, ' ')`.
- Powdrr generates one `_search_index.parquet` companion file per data file.
- Query execution joins `si.doc_id` back to the base table's document id field.

## Optional Performance Recommendations

These are not required for correctness, but they can improve query latency and
reduce scan cost for serving workloads.

### Bloom filters

Parquet bloom filters can help, but only in the parts of the serving path that
actually consult Parquet pruning metadata.

- Powdrr enables DataFusion's Parquet pruning, bloom-filter pruning, and
  page-index pruning in the execution layer.
- That means bloom filters are most likely to help selective equality-style
  predicates on exact-match fields such as tenant ids, document ids, enum-like
  dimensions, or exact term columns.
- Bloom filters are less likely to help broad scans or low-selectivity text
  queries.
- Bloom filters are optional today. Powdrr does not require them, and the table
  validator does not check for them.

If you can control the writer, bloom filters are a reasonable optimization for:

- the base table's document id field
- high-selectivity equality filter fields
- the sidecar's term-oriented columns when you expect many exact-term lookups

### Page indexes and row-group metadata

Page indexes and accurate row-group statistics are also useful because Powdrr's
serving planner already tracks row-group metadata and can benefit from tighter
pruning.

Recommendations:

- write Parquet page indexes when your writer supports them
- preserve accurate min/max statistics for commonly filtered and sorted columns
- avoid extremely large row groups for selective serving workloads

### Layout recommendations

Good file layout matters as much as optional indexes.

Recommendations:

- partition or cluster by the highest-value serving dimensions, such as tenant
  and time
- compact away excessive tiny files, because too many files increase planning
  and file-open cost
- keep row groups small enough for selective pruning to matter, but not so
  small that metadata overhead dominates
- if one or two fields dominate exact-match traffic, consider sorting or
  clustering by them before writing Parquet

## Current Scope Limits

Some constraints are not just validation rules. They are current implementation
limits:

- The clustered/server-side serving path still assumes `_id_seq_no` as the
  document id field. The local CLI can override the field with
  `--doc-id-field`, but the engine's default Elastic extension path still uses
  `_id_seq_no`.
- Delete-file handling in some search paths still assumes `_id_seq_no` on the
  delete side as well.
- This validation does not prove semantic quality, such as global uniqueness of
  document ids or whether the chosen string fields are actually useful for
  search. It only validates the structural assumptions required by the current
  sidecar/indexing implementation.
