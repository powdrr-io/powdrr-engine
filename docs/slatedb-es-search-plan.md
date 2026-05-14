# SlateDB Elasticsearch Search Migration Plan

## Status

Rejected.

This plan is kept for historical analysis only. It was superseded once the
storage requirement became Iceberg-managed immutable table storage rather than
SlateDB SST storage. The active direction is
`docs/iceberg-es-roadmap.md`.

## Goal

Expose the existing Elasticsearch-compatible HTTP API while using SlateDB as the
real ingest, storage, and shard-local search engine.

The priority is search functionality, not metrics or OLAP. The design should
support scaling out read instances for more cache and more query compute.

## Scope

In scope for the first major cut:

- `PUT /:index`
- `POST|PUT /_bulk`
- `GET /:index/_doc/:id`
- `POST /_search`
- `POST /:index/_search`
- Aliases and simple templates only where needed for compatibility
- Query types we already mostly support: `term`, `bool`, `range`, `match`,
  `simple_query_string`

Out of scope for the first cut:

- Metrics and analytical queries
- Rich aggregations
- Full Elasticsearch mapping semantics
- ILM, monitoring, and broad xpack compatibility
- A drop-in Lucene-equivalent feature set

## Current Architecture Summary

The current codebase is not an abstract ES API layer over pluggable storage. It
is a specific execution model built around:

- HTTP handlers in `main_lib/src/elastic_search_endpoints.rs`
- A parser that turns ES-like DSL into SQL/DataFusion-oriented plans in
  `main_lib/src/elastic_search_parser.rs`
- Object-store files for primary data via the speedboat and iceberg paths in
  `main_lib/src/elastic_search_ingest.rs` and `main_lib/src/private_api.rs`
- A sidecar parquet full-text index generated in
  `main_lib/src/elastic_search_index.rs`
- Query fan-out by file subset across peers in `main_lib/src/data_contract.rs`
  and `main_lib/src/private_api.rs`

### Important Current Assumptions

1. Full-text search is driven by a `_search_index.parquet` sidecar per data
   file.
2. Search queries rely on joining the base table with the sidecar on
   `si.doc_id = t._id_seq_no`.
3. The parser emits filters against `si.field_name` and `si.field_term`, so the
   logical query model is already coupled to the sidecar schema.
4. Bulk ingest writes files first and updates cluster metadata around those
   files.
5. Peer distribution is based on hashing file paths, not assigning owned shards.

### Code Paths That Matter

- Search entrypoints:
  `main_lib/src/elastic_search_endpoints.rs`
- Search parsing:
  `main_lib/src/elastic_search_parser.rs`
- Query SQL assembly:
  `main_lib/src/schema_massager.rs`
- Shard-local execution today:
  `main_lib/src/private_api.rs`
- Index sidecar generation:
  `main_lib/src/elastic_search_index.rs`
- Ingest and seq/version handling:
  `main_lib/src/elastic_search_ingest.rs`,
  `main_lib/src/elastic_search_storage_schema.rs`
- Peer/file distribution:
  `main_lib/src/data_contract.rs`

## Why The Current Parquet Shortcut Does Not Map To SlateDB

Today the implementation simplifies search because the data path expects a
physical companion file layout:

- base data file
- companion `_search_index.parquet` file

That works because the query engine loads both files together and performs a
join against a stable `doc_id` field. The important invariant is not row-for-row
identity; it is deterministic companion-file identity.

SlateDB does not preserve that style of physical invariant. SSTs are internal
LSM artifacts. Flush and compaction can rewrite data independently, and the unit
of organization is sorted keys in LSM state, not "primary file plus paired index
file".

So the migration cannot be "replace parquet files with SlateDB files and keep
the join".

The replacement invariant has to be logical:

- each shard owns a SlateDB database
- each document write atomically updates document storage plus search index
  entries in that shard
- search becomes prefix scans plus set operations over keyspaces, not SQL joins
  over paired files

## Target Architecture

### High-Level Shape

Keep:

- The ES-compatible HTTP surface
- Response shaping and much of the existing request routing
- Alias/template metadata handling, at least initially

Replace the search hot path with:

- A storage-agnostic `SearchPlan`
- A shard router and broker
- A node-local merge layer
- A SlateDB-backed shard engine for writes, reads, and postings access
- A hierarchical top-k merge layer for distributed search

### Proposed Request Flow

1. HTTP handler parses the request body and query parameters.
2. Parser produces a `SearchPlan`, not SQL.
3. Broker resolves indices and aliases to concrete shards.
4. Broker groups target shards by node.
5. Each node executes the plan across its assigned shards.
6. The node merges shard-local top-k results into one node-local result set.
7. The broker merges node-local results and fetches final `_source` documents.
8. Existing response code shapes the ES-compatible payload.

## SlateDB Storage Model

Use one SlateDB database per shard.

That aligns with SlateDB's strengths:

- simple ownership boundaries
- one writer leader per shard
- many read-only `DbReader` instances per shard
- isolated compaction and cache behavior per shard

### Keyspaces

Minimum shard-local keyspaces:

1. `doc/{doc_id}`
   Stores the canonical document record and metadata needed to serve `_source`
   and versioning.

2. `post/{field}/{term}/{doc_id}`
   Stores postings entries for full-text search. Value should include term
   frequency and optionally positions later.

3. `fwd/{doc_id}`
   Stores the forward index needed to remove stale postings on update and delete.
   This is the critical replacement for the current rebuild-from-file behavior.

4. `stats/{field}`
   Stores field-level totals needed for scoring, such as total docs, total field
   lengths, or other analyzer-specific counters.

5. `termstats/{field}/{term}`
   Stores document frequency and other per-term statistics if we want BM25-like
   scoring without rescanning postings.

Optional later:

- `sort/{field}/{encoded_value}/{doc_id}`
- `range/{field}/{encoded_value}/{doc_id}`
- `stored_field/{doc_id}/{field}`

### Document Value Shape

Each `doc/{doc_id}` value should include:

- raw `_source`
- `_id`
- `_seq_no`
- `_version`
- `_primary_term`
- routing if present
- analyzer or mapping metadata only if needed by the shard engine

### Posting Value Shape

Each `post/{field}/{term}/{doc_id}` value should include:

- term frequency
- field length for the document, or a reference to it
- optional future position payloads

Keep the first cut simple. The current engine already uses a very lightweight
term model, so we do not need positional queries or phrase queries to make
progress.

## Write Path

### Bulk and Single-Document Ingest

The first cut should replace the speedboat commit path with a shard-local write
pipeline:

1. Resolve target index and shard for the document.
2. Load the current forward index for the existing doc, if any.
3. Normalize the incoming document.
4. Tokenize indexed string fields.
5. Begin a SlateDB transaction.
6. Upsert `doc/{doc_id}`.
7. Remove old `post/...` entries using `fwd/{doc_id}`.
8. Insert new `post/...` entries.
9. Upsert `fwd/{doc_id}`.
10. Update `stats/...` and `termstats/...`.
11. Commit.

This replaces:

- speedboat file emission
- async sidecar generation
- eventual extension coverage checks

### Sequence Numbers and Versions

The current code allocates sequence numbers through `distributed_cache` and
builds file commits around them. In the SlateDB design, seq/version handling
should move into the shard write path.

Recommended rule:

- maintain monotonic per-shard `_seq_no`
- maintain per-document `_version`
- assign both within the shard writer before commit

This is operationally simpler than keeping Redis in the write-critical path.

## Query Execution Model

### Replace SQL With `SearchPlan`

Add a new internal logical plan that captures:

- target indices and resolved shards
- filter clauses
- scoring clauses
- requested fields and `_source` handling
- pagination
- sort requirements

Example variants:

- `Term { field, value }`
- `Match { field, query }`
- `SimpleQueryString { fields, query }`
- `Range { field, op, value }`
- `Bool { must, should, filter, must_not }`

The current parser can be migrated incrementally: keep the request parsing code
but change the output from SQL-oriented builder mutations to `SearchPlan`
construction.

### How Each Query Type Maps

- `term`
  Use direct lookup or a small prefix scan over a field-specific keyspace.

- `match`
  Tokenize the query with the same analyzer as ingest, then read postings from
  `post/{field}/{term}/...`.

- `simple_query_string`
  Expand into term-level subplans and apply boolean composition in the executor.

- `bool`
  Intersect, union, and subtract sorted `doc_id` streams.

- `range`
  First cut can filter on stored doc values after candidate generation for
  smaller result sets. If needed for performance, add dedicated range/sort
  keyspaces.

### Scoring

The current BM25-ish scoring logic is approximate and depends on DataFusion
tables plus Redis metadata. We should not preserve that implementation detail.

Recommended first cut:

- shard-local scoring based on `termstats/{field}/{term}` and per-doc term
  frequency
- broker merges top-k by score

This is enough to preserve the current user-facing behavior more honestly than
the current fixed `avgdl` shortcut.

## Sharding, Compute, and Cache Scaling

### Shard Ownership

Move from file hashing to explicit shard ownership.

Recommended model:

- index metadata defines shard count
- each shard has exactly one writer leader
- any number of read replicas can serve that shard

### What A Shard Is

For this design, a shard is an independently addressable SlateDB database path
for one partition of one index.

Example:

- `logs-v1/shard-000`
- `logs-v1/shard-001`
- `logs-v1/shard-002`

Each shard owns a disjoint subset of documents. Routing is deterministic:

- if the request provides an explicit routing key, hash that
- otherwise hash `_id`
- map the hash to `shard_id = hash % number_of_shards`

That means:

- writes for one document always go to one shard
- point lookups go to one shard
- index-wide search fans out to all shards for that index

### Read And Write Roles Per Shard

Each shard should have:

- one writable owner
- zero or more read replicas

The writable owner opens the shard as a `Db` and handles:

- ingest
- version and sequence number assignment
- posting maintenance
- stats maintenance

Read replicas open the same shard path as `DbReader` instances and handle:

- query execution
- local top-k scoring
- `_source` fetches for winning docs

This matches SlateDB's model well:

- SlateDB is designed for a single writer
- SlateDB supports many readers
- stale writers are fenced

### Query Fan-Out

For `POST /:index/_search`, the broker should use a two-level merge tree.

The controller broker does:

1. resolve the target index or alias
2. enumerate all shard IDs for that index
3. group shards by node placement
4. choose one healthy node-level search worker per node group
5. send the `SearchPlan` plus the shard list for that node
6. merge one partial result set per node

Each node-level worker does:

1. execute the `SearchPlan` against each local shard
2. compute shard-local scores and top-k candidates
3. merge those shard-local results into one node-local top-k result
4. return the node-local result set to the controller

If the query includes routing, the broker can target a smaller shard set.

### Planner Metadata And Shard Pruning

Yes, we should expose planner metadata so the controller and each node can avoid
hitting every shard for every query.

The key rule is:

- exact metadata may be used for hard shard elimination
- approximate metadata may be used only for safe pruning with false positives,
  never false negatives

Recommended metadata objects:

1. `IndexCatalog`
   Global index metadata:
   - index name and generation
   - shard count
   - routing mode
   - alias bindings
   - analyzer version

2. `ShardPlacement`
   Cluster placement metadata:
   - shard ID
   - writer owner
   - reader replica nodes
   - health
   - writer epoch or lease version

3. `ShardSummary`
   Planner-visible shard stats:
   - doc count
   - deleted count
   - last committed seq/version
   - freshness timestamp
   - exact min/max for selected range-prunable fields such as `@timestamp`
   - exact partition values for fields intentionally used for routing or tenancy
   - field existence flags or counts where useful
   - text-term bloom/sketch metadata for search pruning

4. `NodeShardSummary`
   Node-local cached view of the shards currently assigned to that node, plus
   local health and cache-warmth hints.

Recommended pruning signals:

- routing key:
  exact, highest-value pruning signal
- time bounds:
  exact min/max per shard for time-based indices
- tenant or partition key:
  exact if the field is part of routing or maintained as a shard summary value
- field existence:
  exact if maintained as a summary counter or flag
- text term presence:
  approximate via bloom/sketch, safe only if there are no false negatives

For text queries, bloom-style shard summaries are useful because they let the
planner skip shards that definitely do not contain a queried term. False
positives are fine. False negatives are not.

### Who Uses The Metadata

The controller should use planner metadata first to build the candidate shard
set for the whole query.

Each node-level worker should then use the same metadata snapshot to do a second
pruning pass across only its local shard set before opening local executors.

That gives us two layers of pruning:

- global pruning at the controller
- local pruning at the node

This works especially well with oversharding because the controller does not
need to ship work for obviously irrelevant shards, and the node does not need to
open every local shard just because it was assigned there.

### Where The Metadata Should Live

Do not fetch shard stats live from shard databases on the query path.

Instead:

- shard writers maintain local shard summary state
- shard writers publish summary updates into a replicated metadata plane
- controller and nodes subscribe to or periodically refresh that metadata into
  an in-memory planner cache

In the current codebase, this likely means extending the state-provider layer
beyond today's minimal `TableDescription`, which currently only holds `name` and
`tags`.

### What Must Be Exact Versus Approximate

Use exact metadata for:

- shard ownership and placement
- routing partitions
- time min/max bounds
- seq/version freshness

Use approximate metadata for:

- text-term presence
- maybe cache-warmth or selectivity hints

If we are ever uncertain whether a summary can produce false negatives, we
should treat it as an advisory ranking hint, not a pruning filter.

### Why Node-Local Merge Matters

This should be the default design, not a later optimization.

Benefits:

- less network fan-out from the controller
- less cross-node transfer volume
- less final-merge work on the controller
- better cache locality because one node can reuse data across many local shards
- a cleaner execution model if one node owns many shards

The practical rule is:

- in the exact case, shard-local and node-local executors can each return top
  `K`
- in deferred or approximate cases, shard-local executors should overproduce a
  bounded candidate set
- node-local workers should merge and trim according to the needs of the next
  stage
- the controller should perform only the final global merge

### Top-K Merge Contract

For the exact case, overfetch is not inherently required.

If all of the following are true:

- every shard can apply the full filter locally
- every shard computes the exact same final score and sort key the controller
  will use
- there is no later dedup, collapse, rescore, or rerank stage
- the request is for top `K` rather than `from + size > K`

then:

- each shard returning its top `K` is sufficient
- each node merging local shards down to top `K` is also sufficient
- the controller can merge node-local top `K` results exactly

This works because any document below rank `K` within a shard already has at
least `K` documents in that shard above it, so it cannot enter the global top
`K`. The same argument holds again at the node-local merge layer.

Overfetch is needed only when some part of the final ranking or eligibility is
deferred or approximate.

Examples:

- global rescoring or reranking after retrieval
- grouping, field collapsing, or dedup across shards
- approximate local scoring that is corrected later
- filters that cannot be fully applied at the shard
- pagination where the true target is top `from + size`

Recommended rule:

- exact fully local execution: use `K`
- deferred or approximate execution: use bounded overfetch at the shard and/or
  node layer sized to the later-stage needs

### Scaling Up

There are two different scale-up operations.

#### 1. Add More Read Compute And Cache

This is the cheap one.

To scale search throughput up:

- add more nodes
- open more `DbReader` replicas for existing shards on those nodes
- start routing shard searches to those readers

No document movement is required because all readers point at the same
object-store-backed shard path.

This increases:

- aggregate CPU available for queries
- aggregate block cache
- aggregate object-store cache capacity across nodes

This does not increase:

- write parallelism for an existing shard

#### 2. Add More Write Parallelism

This requires more shards.

Because each shard has one writer, write throughput for one index scales by
increasing the shard count. That should be treated as an index-layout decision,
not an autoscaling action on running pods.

For a new index:

- create the index with more shards

For an existing index:

- create a new index generation with a higher shard count
- reindex from old to new
- switch aliases

Do not assume shard count can be changed in place cheaply. Once routing is
`hash % shard_count`, changing `shard_count` remaps most documents.

### Scaling Down

There are also two different scale-down operations.

#### 1. Remove Read Replicas

This is also cheap.

To reduce search capacity:

- stop assigning new queries to selected reader replicas
- drain in-flight requests
- close those `DbReader` instances

No data movement is required.

#### 2. Remove Or Drain Nodes That Own Writers

This is a controlled shard handoff:

1. pick a new node for each affected shard writer
2. mark the old writer as draining
3. stop sending writes to the old owner
4. bring up a new writer for the same shard path
5. rely on fencing so the old writer cannot continue writing
6. resume writes through the new owner

Reader replicas can remain available during the handoff.

### Rebalancing Shard Placement

Rebalancing means moving responsibility, not rewriting the shard.

For read rebalancing:

- start readers on new nodes
- wait for health
- shift query routing
- remove readers from old nodes

For write rebalancing:

- move the writer lease for the shard
- reopen the same shard DB path on the new node
- keep the shard ID and data path unchanged

This is why one-SlateDB-DB-per-shard is the right unit. A shard can move between
nodes without changing its identity.

### Reducing Shard Count

Reducing shard count is not the same as scaling down pods.

If you want fewer shards for an existing index, that is a reshard:

- create a new index with fewer shards
- read from a consistent snapshot of the old index
- rewrite documents into the new shard layout
- rebuild postings and stats in the new shards
- cut traffic over

SlateDB checkpoints and clones can help create a consistent source snapshot for
that migration, but they do not eliminate the rewrite. The target shard mapping
is different, so the documents and their postings still have to be repartitioned.

### Read Scaling

SlateDB's `DbReader` is a good fit for shard replicas:

- readers follow checkpointed manifest state
- they can replay durable WAL state
- they do not own the write path

That matches the requirement to add instances for more cache and more compute.

### Cache Scaling

Use both:

- SlateDB block cache for hot decoded SST blocks
- SlateDB object-store cache for cross-instance cache reuse on the same machine

Operationally:

- more read instances increase aggregate compute
- co-locating multiple readers on a node can reuse object-store cache contents
- sticky routing by shard will matter for cache locality

### Segment Extractor

This should be treated as optional for v1.

Possible later use:

- segment by key family such as `doc/`, `post/`, `stats/`
- isolate compaction pressure between large postings scans and point document
  reads

Do not make segmenting a prerequisite for the first implementation. A clean
single-shard key layout matters more.

## Module-Level Migration Plan

### Keep With Moderate Changes

- `main_lib/src/router.rs`
- `main_lib/src/elastic_search_endpoints.rs`
- alias/template parts of `main_lib/src/elastic_search_ingest.rs`
- parts of `main_lib/src/state_provider.rs`

### Replace Or Heavily Rewrite

- `main_lib/src/elastic_search_parser.rs`
- `main_lib/src/schema_massager.rs`
- `main_lib/src/private_api.rs`
- `main_lib/src/elastic_search_index.rs`
- `main_lib/src/elastic_search_storage_schema.rs`
- the search-critical parts of `main_lib/src/elastic_search_ingest.rs`
- `main_lib/src/elastic_search_commands.rs`
- search-related pieces of `main_lib/src/data_access.rs`

### Retire From The Search Hot Path

- DataFusion SQL execution for search
- companion `_search_index.parquet` generation
- file-path hashing for query distribution
- extension work used to build search sidecars

## Phased Implementation Plan

### Phase 0: Freeze Behavior and Add Test Coverage

Goal:
capture the search behavior we need to preserve before changing internals.

Work:

- Add golden tests for `_bulk`, `_doc`, `term`, `bool`, `match`,
  `simple_query_string`, `range`
- Add fixtures that cover update, delete, and mixed-field queries
- Record any currently accepted quirks that we will intentionally preserve or
  intentionally break

### Phase 1: Introduce Storage-Agnostic Search Planning

Goal:
separate ES request parsing from SQL/DataFusion execution.

Work:

- Add a new `SearchPlan` module
- Refactor parser output to emit `SearchPlan`
- Keep the existing SQL engine behind an adapter temporarily so behavior can be
  compared during the transition

### Phase 2: Build A Shard-Local SlateDB Engine

Goal:
introduce a new internal engine that can read and write shard state without the
existing file pipeline.

Work:

- Add a `slatedb_search` module or crate
- Define key encodings and value schemas
- Implement shard writer
- Implement shard reader
- Implement tokenizer and field extraction rules

### Phase 3: Replace Ingest

Goal:
make `_bulk` and single-document CRUD write directly into SlateDB shards.

Work:

- Replace `SpeedboatCommitBuilder` usage on the search path
- Assign shard ownership and routing
- Move seq/version assignment into shard-local logic
- Preserve ES-compatible responses

### Phase 4: Replace Query Execution

Goal:
serve `_search` from SlateDB rather than DataFusion.

Work:

- Implement local executor for `term`, `bool`, `match`,
  `simple_query_string`, `range`
- Add shard-local scoring
- Add top-k broker merge
- Add `_source` fetch path

### Phase 5: Add Read Replicas, Cache Tuning, and Operational Controls

Goal:
support scaling out for cache and compute in a way that is operationally
predictable.

Work:

- Add shard-aware routing
- Add writer and reader roles
- Tune block cache and object-store cache settings
- Add shard health and lag visibility

### Phase 6: Cut Over And Remove Legacy Search Dependencies

Goal:
eliminate the search code paths that depend on DataFusion sidecars and file
scatter/gather.

Work:

- Remove sidecar index generation from the search path
- Remove SQL join assumptions from request execution
- Retire search-specific file distribution logic

## First Concrete Implementation Slices

These are the next changes worth making in code.

1. Add `docs/slatedb-es-search-plan.md` and keep it current.
2. Add `main_lib/src/search_plan.rs` with query enums and request-independent
   plan types.
3. Refactor `elastic_search_parser.rs` to emit `SearchPlan`.
4. Add `main_lib/src/slatedb_search/` with:
   - `keys.rs`
   - `analyzer.rs`
   - `shard_writer.rs`
   - `shard_reader.rs`
   - `executor.rs`
5. Add a feature-flagged execution branch in
   `elastic_search_endpoints.rs` and `private_api.rs` so both engines can be
   compared while migrating.
6. Replace `_bulk` for one test index path first, before attempting full
   compatibility.

## Risks And Open Questions

1. Analyzer behavior is currently very simple. We need to decide whether v1
   keeps whitespace tokenization or introduces a slightly better normalizer.
2. Range and sort performance will be limited unless we add dedicated keyspaces.
3. Updates and deletes require a correct forward index. If that logic is wrong,
   postings drift immediately.
4. PIT and checkpoint semantics should probably map to SlateDB checkpoints, but
   that should stay out of the first search cut.
5. Alias, template, and mapping metadata can likely remain in the current state
   provider initially, but long-term ownership should be clarified.

## Recommended Near-Term Milestone

The first meaningful milestone is:

"Serve `_bulk`, `_doc`, and `/_search` for one index through a single-shard
SlateDB path with `term`, `bool`, `match`, and `simple_query_string`, while the
existing HTTP API remains unchanged."

Once that works, the rest of the plan becomes operational scaling and
compatibility work rather than architecture uncertainty.
