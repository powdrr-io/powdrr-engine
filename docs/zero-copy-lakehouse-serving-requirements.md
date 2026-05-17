# Zero-Copy Lakehouse Serving Requirements

You are describing something closer to a **serving database whose base storage is an open lakehouse table**, not a conventional query engine and not a replicated warehouse.

The key requirement is this:

> The system must turn a table-format snapshot plus Parquet files into a low-latency, high-concurrency serving surface by using metadata, indexes, caching, pruning, and strict query-shape control — without copying the full table into another storage engine.

That is feasible, but only if “selective query” is tightly defined.

---

## 1. Define “zero-copy” honestly

The system should not promise “no auxiliary data.” It should promise:

```text
No full base-table copy.
No customer-operated load/reverse-ETL pipeline.
No second system that must own the source of truth.

Allowed:
  - catalog metadata
  - file/row-group/page statistics
  - pruning indexes
  - key/secondary indexes
  - text/vector indexes for selected fields
  - aggregate summaries
  - hot data caches
  - query-result caches
```

A good product contract would be:

```text
Base data remains in Iceberg / Delta / Hudi on object storage.
The serving system maintains only bounded, purpose-built acceleration state.
Every acceleration artifact is snapshot-aware.
Queries are served from a consistent table snapshot.
```

That distinction matters because object-storage Parquet alone will not give database-like latency for arbitrary point lookups or full-text search. The product needs indexes and caches; it just should not need a **full warehouse/search/KV copy**.

---

## 2. Support a narrow query contract first

The system should not try to support arbitrary SQL at the beginning. It should support query shapes where pruning or indexing can make the read set tiny.

A good initial contract:

| Query shape | Example | Should support? |
|---|---|---:|
| Primary-key lookup | `WHERE order_id = ?` | Yes |
| Tenant-scoped key lookup | `WHERE tenant_id = ? AND user_id = ?` | Yes |
| Time-window filter | `WHERE tenant_id = ? AND ts BETWEEN ? AND ?` | Yes |
| Small `IN` lookup | `WHERE id IN (...)` | Yes |
| Top-N over indexed order | `WHERE tenant_id = ? ORDER BY ts DESC LIMIT 100` | Yes |
| Simple aggregate | `COUNT`, `SUM`, `AVG`, maybe approximate percentiles | Yes |
| Group-by low/medium cardinality | `GROUP BY status`, `GROUP BY country` | Yes |
| Faceted search | text filter plus counts by facet | Maybe |
| Relevance-ranked text search | `MATCH(text, '...')` | Only with a search index |
| Arbitrary joins | customer table join event table | No, or route elsewhere |
| Unbounded table scan | `SELECT * FROM huge_table` | No, or slow path |
| UDF-heavy predicates | arbitrary Python/regex/function filters | No |
| High-cardinality group-by over broad scan | `GROUP BY user_id` over billions of rows | No, unless precomputed |

The most important product feature may be **query classification**:

```text
Fast path:
  query can be answered by index/cache/pruned lake read

Slow path:
  query is valid but will scan too much

Rejected/routed path:
  query should go to Trino/Spark/warehouse instead
```

Without this, users will send warehouse-style queries and then judge the system by impossible SLOs.

---

## 3. Snapshot consistency is non-negotiable

If the system serves Iceberg/Delta/Hudi tables, it must understand table versions, not just files.

For Iceberg, table state is maintained in metadata files; changes create a new metadata file and atomically swap the table metadata pointer. A snapshot represents the table state at a point in time and tracks the complete set of data files for that state. Readers use the snapshot current when they load metadata and are not affected by later changes until refresh.

That implies every serving query needs a version model:

```text
query_start_snapshot = current_snapshot(table)

planner uses:
  table_id
  snapshot_id
  schema_id
  partition_spec_id
  data files in snapshot
  delete files / deletion vectors / commit timeline if relevant

index lookup must be valid for query_start_snapshot
```

The system also needs an atomic promotion model:

```text
snapshot N committed in lake
↓
serving system discovers snapshot N
↓
builds/updates indexes and cache metadata for N
↓
validates completeness
↓
atomically promotes N as serveable
```

That lets you offer clear freshness guarantees:

```text
read-after-batch-publish: yes, after index promotion
maximum serving lag: e.g. 30s / 5m / next batch
consistent snapshot reads: yes
mixed old/new file reads: never
```

---

## 4. The catalog layer has to be first-class

This system needs to integrate with the customer’s catalog, not scrape object storage folders.

Minimum catalog support:

```text
Iceberg:
  REST catalog
  Hive Metastore
  AWS Glue
  Nessie / Polaris / Snowflake Open Catalog-like catalogs where relevant

Delta:
  Delta transaction log
  Unity Catalog / external Delta readers where relevant

Hudi:
  Hudi timeline and metadata table
  Hive/Glue catalog integration
```

For Iceberg specifically, engines such as Trino rely on catalog access plus object-storage access. The Iceberg connector documentation lists requirements such as network access to distributed object storage, access to a supported catalog, and data files in Parquet, ORC, or Avro.

The serving system should avoid object-store listing as much as possible. Iceberg tracks data-file paths in metadata, so a reader can avoid listing every partition folder. Trino’s docs contrast this with Hive-style discovery where a query often has to call the metastore, list partition locations, and read per-file metadata.

---

## 5. The planner must exploit table metadata aggressively

The system’s first “index” is the table format metadata itself.

Iceberg uses manifest lists and manifest files for planning. Manifest files track data files along with partition data and column-level stats; manifest lists track snapshot manifests and partition-value ranges. Iceberg uses this metadata to filter manifests first, then eliminate data files using stats such as value counts, null counts, lower bounds, and upper bounds.

That means a serious serving planner needs:

```text
metadata cache:
  current snapshots
  manifest lists
  manifests
  file-level stats
  partition transforms
  schema and partition evolution history

predicate compiler:
  SQL/API predicate → table-format predicate
  logical column predicate → partition transform predicate
  file-level pruning predicate
  row-group/page-level pruning predicate

planning budget:
  do not spend 500ms planning a 50ms query
```

For Delta, the analogous requirement is to use data-skipping metadata and clustering. Databricks documents that data skipping records minimum values, maximum values, null counts, and total records per file, then uses that information at query time. It also recommends liquid clustering for new tables and notes Z-ordering can colocate related values for data-skipping benefits.

For Hudi, the metadata table is central. Hudi tracks file listings to avoid expensive cloud-storage list operations and can expose column statistics for query planning and data skipping. Its docs explicitly call out the cost of reading footers from all files on large cloud-storage tables.

---

## 6. You need multiple index types, not one magic index

For this workload, think in terms of layered indexes.

```text
Level 0: table-format metadata
  snapshot → manifest → file pruning

Level 1: file / partition / column stats
  min/max/null-count/record-count zone maps

Level 2: Parquet-native indexes
  row-group stats
  page index
  bloom filters

Level 3: serving indexes
  primary-key index
  secondary-key index
  bitmap index
  inverted text index
  pre-aggregate index
  vector index, if needed

Level 4: cache
  manifest cache
  decoded-column cache
  hot-row/object cache
  aggregate-result cache
```

Parquet Bloom filters are useful for high-cardinality membership predicates because they are compact and can answer “definitely no” or “probably yes” without false negatives. Parquet page indexes add page-level navigation: a `ColumnIndex` can locate pages containing matching values, and an `OffsetIndex` helps navigate to corresponding pages/rows.

For Hudi specifically, the index subsystem already points in this direction: Hudi’s docs describe metadata-backed indexes such as `bloom_filters`, `column_stats`, `partition_stats`, `record_index`, `secondary_index`, and `expression_index`, with asynchronous indexing support.

A zero-copy serving product would likely need its own equivalent index layer for Iceberg/Delta, or integrate with existing table-format indexes where available.

---

## 7. Point lookup is harder than it sounds

A point lookup over Parquet is not the same as a point lookup in RocksDB, DynamoDB, Postgres, or Redis.

For a query like:

```sql
SELECT * FROM events WHERE event_id = 'abc';
```

the system needs to avoid:

```text
listing files
reading lots of footers
opening many Parquet files
scanning row groups
decoding irrelevant columns
decompressing large chunks
materializing entire rows
```

The fast path should look more like:

```text
event_id
  → primary-key index
  → candidate file(s)
  → candidate row group/page
  → range-read needed Parquet bytes
  → decode only requested columns
  → apply final predicate
  → return row
```

But there are several complications:

| Problem | Requirement |
|---|---|
| Parquet is columnar, not row-addressable | Index should point to file + row group/page, not merely file |
| Compression granularity can be large | Tune row group/page size for serving use cases |
| `SELECT *` may require many column chunks | Encourage narrow projections or hot-row cache |
| Multiple rows can share key unless uniqueness is known | Require declared primary key or uniqueness semantics |
| Deletes/updates complicate lookup | Index must understand equality deletes, position deletes, deletion vectors, or Hudi commit timeline |
| Object storage range reads have nontrivial latency | Cache hot files/pages/decoded columns |

So for point lookup, a practical product may need a **small row/object projection cache**:

```text
key → compact encoded hot row / selected serving columns
```

That is not a full warehouse copy. It is a serving projection for hot keys or declared lookup columns.

---

## 8. Aggregates need precomputation or tight pruning

Simple aggregates can be served zero-copy if the filtered range is narrow:

```sql
SELECT count(*)
FROM events
WHERE tenant_id = ? AND event_date = ? AND status = 'failed';
```

But broad aggregates over large data still require either scanning or pre-aggregation.

The product needs aggregate acceleration types:

```text
count/sum/min/max per:
  table snapshot
  partition
  file
  bucket
  tenant
  time window
  low-cardinality dimension

bitmap/roaring indexes for:
  status
  country
  event_type
  boolean flags

approximate sketches for:
  count distinct
  percentiles
  top-k
```

The important requirement is **snapshot-aware aggregate validity**:

```text
aggregate_index(table, snapshot_id, dimensions, measures)
```

When new files arrive, the system should incrementally add their aggregate contribution. When files are removed or deleted, it must subtract or invalidate appropriately. For append-only batch tables, this is much easier. For upsert/delete-heavy tables, it is much harder.

---

## 9. Free-text search needs a real inverted index

Free-text search is the area where “zero-copy” most often becomes misleading.

For this:

```sql
WHERE MATCH(description, 'red running shoes')
```

you need an inverted index:

```text
token → postings list → doc_ids / row refs
```

That index may be much smaller than a full copy if it stores only:

```text
doc_id
tokens/postings
selected facets
optional small stored fields
pointer to lake row
snapshot/version
```

But it is still a copy of the searchable representation.

A good requirement would be:

```text
Search index is a projection, not a full data copy.
Only declared searchable fields are indexed.
Stored fields are optional and bounded.
The canonical row remains in the lakehouse table.
```

The query path could be:

```text
text query
  → inverted index returns doc_ids / candidate row refs
  → optional facet counts from search index
  → optional row materialization from Parquet/cache
```

Avoid a design where the search layer returns millions of IDs and the lake reader tries to fetch them one by one. For text-matched aggregate queries, either the search index must support the aggregation, or the lakehouse serving layer needs a compatible bitmap/postings representation.

---

## 10. The system needs a “serving-aware” table layout contract

You can avoid a full load step, but you cannot avoid physical layout. The lake table must be written in a way that makes serving possible.

Minimum writer/table requirements:

```text
Files:
  avoid tiny files
  target predictable file sizes
  write useful column statistics
  use Parquet page indexes / bloom filters where engines support them
  avoid huge row groups for point-serving workloads

Partitioning:
  partition by lifecycle/coarse filters, not raw high-cardinality IDs
  use tenant/date/time/version when common
  support partition evolution

Clustering/sorting:
  cluster by tenant, time, lookup key, or common filter columns
  sort within files where point/range lookup matters
  bucket/hash high-cardinality keys when useful

Schema:
  stable column IDs where table format supports them
  avoid uncontrolled nested/huge text columns in hot paths
  declare primary keys / lookup keys / searchable fields
```

Iceberg’s hidden partitioning is helpful here because it can transform logical columns into partition values and use them for pruning without forcing users to query physical partition columns directly. Iceberg also allows partition schemes to evolve over time as volume changes.

The serving product should probably include a **layout advisor**:

```text
This query is slow because 8,400 files overlap user_id = ?
Recommended: cluster by tenant_id, bucket user_id, sort by ts.
Estimated index/cache savings: ...
```

Ideally, it also includes an optional maintenance service. Iceberg’s own maintenance docs call out that many small files increase metadata and file-open cost, and that compacting data files can reduce metadata overhead and runtime file-open cost. They also describe manifest rewrite as a way to improve planning and pruning because the metadata tree functions as an index over table data.

---

## 11. High concurrency requires admission control and caching

Object storage is excellent for durability and throughput. It is not automatically excellent for thousands of concurrent tiny random reads.

To serve high concurrency, the system needs:

```text
metadata cache:
  avoid reloading manifests/logs for every query

object/file cache:
  cache hot Parquet byte ranges or whole small files

decoded column cache:
  cache decoded vectors for hot columns/segments

row/object cache:
  cache hot lookup results

result cache:
  cache common aggregate results by snapshot ID and query fingerprint

admission control:
  protect p99 by rejecting/routing broad scans

tenant isolation:
  per-tenant QPS, memory, cache, and scan budgets

request coalescing:
  collapse identical concurrent lookups or aggregate queries

warmup:
  prewarm newest snapshot, hot partitions, hot tenants
```

The serving system should expose different latency classes:

```text
index-only hit:          very fast
cache hit + small decode: fast
cold selective lake read: moderate
broad scan:              not a serving query
```

That is how you prevent a few accidental broad scans from destroying p99 latency for point lookups.

---

## 12. Query planning must be cost-based and conservative

For each query, the planner should estimate:

```text
candidate manifests
candidate files
candidate row groups/pages
object-store GET/range-read count
bytes to read
columns to decode
expected output rows
index/cache availability
freshness constraints
```

Then decide:

```text
serve immediately
serve with warning
route to analytical engine
reject as outside serving contract
require index creation
```

A good product behavior:

```text
Query rejected:
  Reason: estimated 1.8 TB scan and 42,000 files.
  Fast path unavailable because no index exists on customer_email.
  Suggested action: create secondary index on customer_email, or add tenant_id/date predicate.
```

This is much better than silently running a 60-second lake scan behind an API that users expect to be 100 ms.

---

## 13. Index maintenance must be incremental

The indexer should not “reload the table.” It should diff snapshots.

For append-only Iceberg-style batches:

```text
old_snapshot → new_snapshot
  added files
  removed files
  changed manifests
```

Then:

```text
for added files:
  read metadata
  optionally read selected columns
  build index fragments
  compute aggregate fragments
  publish index fragment for snapshot N

for removed files:
  mark old fragments inactive for snapshot N
```

For Iceberg, this maps naturally to snapshot and manifest metadata. The spec says data files in snapshots are tracked by manifests, and manifest lists store metadata about manifests including partition stats and file counts, which are used to avoid reading unnecessary manifests.

For Delta, the indexer needs to follow the transaction log and table protocol. Delta Lake’s docs describe ACID transactions, scalable metadata handling, and serializable isolation, with readers never seeing inconsistent data.

For Hudi, the system needs to understand the timeline, metadata table, and table services. Hudi’s docs warn that metadata-table concurrency and table services require proper configuration, and that enabling metadata inconsistently across writers can be unsafe.

---

## 14. Delete/update semantics are a major requirement

An MVP can be append-only. A mature product needs to handle:

```text
append
overwrite partition
copy-on-write updates
merge-on-read updates
position deletes
equality deletes
deletion vectors
late-arriving corrections
schema evolution
partition evolution
time travel
vacuum / snapshot expiration
```

Deletes are especially important for point lookup and search. If the index says a row exists, but the table’s current snapshot has deleted it, the serving system must not return stale data.

Safe index entries need metadata like:

```text
table_id
snapshot_id or sequence range
schema_id
data_file_path
file content id / file sequence number
row group/page/position if available
delete applicability
```

A conservative rule:

```text
If the index cannot prove validity for the query snapshot, it cannot serve as authoritative.
```

It can still be used for candidate generation, followed by validation against table metadata and delete information.

---

## 15. Security and governance must match the lake

Enterprise buyers will expect this system to honor existing governance.

Requirements:

```text
catalog-level authorization
object-storage IAM integration
row-level policies
column-level masking
PII controls
audit logs
query history
lineage: table snapshot → index snapshot → serving response
encryption in transit and at rest
private networking / VPC deployment
no unauthorized data egress
```

This is especially important because the system maintains auxiliary state. If the base table has masked columns but the index stores raw searchable text, you have created a governance bypass.

Every index needs a policy model:

```text
Can this column be indexed?
Can this value be stored in cleartext?
Can this index be shared across tenants?
Must this index be encrypted with customer-managed keys?
Can this cached result be reused for another principal?
```

---

## 16. Observability is part of the product, not an add-on

Users need to know why a query was fast or slow.

Expose:

```text
query latency:
  p50 / p95 / p99

planning:
  manifests considered
  files considered
  files pruned
  row groups/pages pruned

I/O:
  object-store GETs
  range reads
  bytes read
  columns decoded

index:
  index hit/miss
  index freshness
  index size
  index build lag
  index coverage

cache:
  cache hit rate
  hot partitions
  evictions

freshness:
  latest table snapshot
  latest serveable snapshot
  lag in commits/time

cost:
  estimated cost per query
  bytes scanned avoided
```

A killer feature would be:

```sql
EXPLAIN SERVING SELECT ...
```

Example output:

```text
Fast path: yes
Snapshot: 982314
Primary index: hit
Candidate files: 1
Candidate row groups: 1
Projected columns: 6 of 84
Object bytes read: 1.7 MB
Cache: decoded-column miss, object-range hit
Estimated latency class: 50-100 ms
```

---

## 17. The product needs clear SLO tiers

Do not sell one latency number for all queries. Sell SLOs by path.

| Path | Example | Possible SLO class |
|---|---|---|
| Index-only | `COUNT WHERE tenant_id=? AND status=?` from aggregate index | tens of ms |
| Point lookup hot cache | `GET /order/{id}` | single/tens of ms |
| Point lookup cold but indexed | key → file/page → Parquet read | tens/hundreds of ms |
| Selective aggregate | few files/row groups | hundreds of ms |
| Broad lake scan | many files | seconds+; not serving path |

The product should make these classes explicit during onboarding:

```text
This table has a serving SLO for:
  - order_id lookup
  - tenant_id + time range
  - status aggregates by hour

This table does not have a serving SLO for:
  - arbitrary text search
  - broad unfiltered scans
  - joins
```

---

## 18. Customers should declare access patterns

To avoid manual ETL, you still need an access-pattern declaration.

Something like:

```yaml
table: prod.orders
format: iceberg
primary_key: order_id

serving_patterns:
  - name: order_lookup
    predicate: order_id = ?
    projection: [order_id, customer_id, status, total, updated_at]
    latency_slo_ms_p95: 100

  - name: tenant_recent_orders
    predicate: tenant_id = ? AND created_at BETWEEN ? AND ?
    order_by: created_at DESC
    limit: 100
    latency_slo_ms_p95: 250

  - name: status_counts
    predicate: tenant_id = ? AND created_at BETWEEN ? AND ?
    aggregate:
      group_by: [status]
      measures: [count, sum(total)]
    latency_slo_ms_p95: 200

  - name: product_search
    text_fields: [title, description]
    facets: [brand, category, availability]
    stored_fields: [product_id, title, price]
```

The system can then decide:

```text
required index:
  primary key index on order_id
  clustering recommendation on tenant_id, created_at
  aggregate index for status counts
  inverted index for title/description
  hot projection cache for lookup fields
```

This is how you avoid customers hand-building pipelines while still giving the system enough information to serve fast.

---

## 19. A realistic MVP

The first version should probably be narrower than the full vision.

I would start with:

```text
Format:
  Iceberg only

Storage:
  S3 first, then GCS/ADLS

Catalog:
  REST catalog + Glue + Hive Metastore

File format:
  Parquet only

Tables:
  append-only or mostly append-only
  no complex deletes at first

Queries:
  single table
  no joins
  projection
  equality predicates
  range predicates
  IN predicates
  LIMIT
  ORDER BY on declared sort/time key
  COUNT/SUM/AVG/GROUP BY on declared dimensions

Indexes:
  metadata/file stats
  primary-key index
  secondary-key index
  low-cardinality bitmap index
  optional aggregate index

API:
  REST/gRPC serving API first
  limited SQL second

Consistency:
  snapshot-consistent reads
  atomic index promotion
  explicit freshness lag

Operations:
  automatic index build
  automatic index compaction
  cache management
  query explain
```

Why Iceberg first? Its metadata/snapshot model is very friendly to this kind of product: snapshots, manifests, manifest lists, file-level stats, partition evolution, and hidden partitioning are all useful primitives for serving. That does not mean Delta or Hudi are bad; it means multi-format support multiplies complexity early.

---

## 20. What would make it compelling

The product becomes compelling if it can reliably say:

```text
Point us at your lakehouse table.
Declare the API/query patterns you care about.
We build and maintain the minimal acceleration state.
You keep one canonical copy of the data.
Your API gets predictable p95/p99.
Your analysts can still use existing lakehouse engines.
```

The strongest buyer-facing requirements are:

```text
No full data copy
No reverse ETL job
No separate warehouse as source of truth
Snapshot-consistent serving
Low p95/p99 for declared query patterns
Automatic index/cache maintenance
Clear cost and freshness controls
Governance-compatible auxiliary state
Honest rejection/routing for non-serving queries
```

The hardest technical requirements are:

```text
1. Snapshot-aware index correctness.
2. Cold selective reads from object storage without terrible p99.
3. Handling deletes, schema evolution, and compaction safely.
4. Avoiding query patterns that devolve into broad scans.
5. Making indexing/caching automatic enough that customers do not feel they are operating another database.
```

In other words, the opportunity is not “query Parquet faster.” It is **turn declared lakehouse access patterns into managed serving indexes and caches, while preserving the lakehouse table as the canonical data store.**

---

## Source links referenced

- Apache Iceberg specification: https://iceberg.apache.org/spec/
- Apache Iceberg performance: https://iceberg.apache.org/docs/latest/performance/
- Apache Iceberg partitioning: https://iceberg.apache.org/docs/latest/partitioning/
- Apache Iceberg maintenance: https://iceberg.apache.org/docs/latest/maintenance/
- Trino Iceberg connector: https://trino.io/docs/current/connector/iceberg.html
- Databricks Delta data skipping: https://docs.databricks.com/aws/en/delta/data-skipping
- Delta Lake documentation: https://docs.delta.io/
- Apache Hudi metadata table: https://hudi.apache.org/docs/metadata/
- Apache Hudi metadata indexing: https://hudi.apache.org/docs/metadata_indexing/
- Apache Parquet Bloom filter: https://parquet.apache.org/docs/file-format/bloomfilter/
- Apache Parquet page index: https://parquet.apache.org/docs/file-format/pageindex/
