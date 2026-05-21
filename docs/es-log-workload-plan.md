# Elasticsearch Logs Workload Plan

This document defines the next Elasticsearch compatibility milestone after the
current route, fixture, and JavaScript client smoke coverage.

It is intentionally narrower than "be Elasticsearch." The next goal is to make
one important real workload solid:

- read-only log search over `logs-*`
- existing Elasticsearch clients
- single-node correctness first
- multi-index correctness before broader DSL expansion
- clear unsupported behavior for everything outside the target slice

This plan builds on:

- [README.md](../README.md)
- [docs/es-compatibility-matrix.md](./es-compatibility-matrix.md)
- [docs/iceberg-es-roadmap.md](./iceberg-es-roadmap.md)
- [docs/cross-protocol-serving-optimization-plan.md](./cross-protocol-serving-optimization-plan.md)

## Goal

Make Powdrr good enough to serve a real Elasticsearch-style logs workload,
not just to satisfy connection probes and isolated endpoint checks.

The target user experience is:

- an existing Elasticsearch JavaScript client can connect directly
- multi-index `logs-*` searches work
- time-range filtering works
- common log search text queries work
- sorted pagination with PIT plus `search_after` works
- basic log dashboard aggregations work
- unsupported features fail with explicit, stable error payloads

## Non-Goals

Not part of this milestone:

- write compatibility beyond what already exists
- Kibana parity as a product claim
- scroll
- async search
- nested queries
- highlighting
- regexp and wildcard text search
- geo search
- script scoring
- full xpack behavior

## Target Workload

The first-class workload is a log-search pattern like:

- indices: `logs-*`
- filters:
  - `@timestamp` range
  - tenant / org / environment / service exact filters
  - small `IN` lists on exact fields
- text:
  - `match`
  - `multi_match`
  - a narrow `query_string` subset
- sort:
  - `@timestamp desc`
  - secondary tiebreak field when needed
- pagination:
  - PIT
  - `search_after`
- aggregations:
  - `date_histogram`
  - `terms`
  - `cardinality`
  - filter + metric sub-aggregations

Representative request shapes:

1. "Show the last 100 auth failures across `logs-*` for tenant X."
2. "Filter to service=A, env=prod, time range=last hour, sorted newest first."
3. "Paginate the result set with PIT plus `search_after`."
4. "Facet by service and status code."
5. "Show a histogram of hits over time."

## Why This Is The Right Next Slice

This is the best next step because it moves the project from endpoint
compatibility to workload compatibility.

Current `main` already has:

- broad read-only Elasticsearch route coverage
- fixture-driven differential tests against real Elasticsearch
- official JavaScript client smoke coverage for the supported subset

The biggest remaining risk is not "will the client connect?" It is "will a
normal Elasticsearch read workload run without falling into missing semantics?"

## Current State

The current system is already strong enough for:

- root and product checks
- index metadata probes
- aliases and field caps
- `_search`, `_count`, `_msearch`, `_mget`
- point `GET` and `HEAD`
- PIT
- `search_after`
- route-level unsupported behavior for several non-goal APIs

The main remaining gaps for a realistic logs workload are:

- richer DSL coverage
- broader multi-index search correctness
- more aggregation parity
- workload-level differential and client-driven tests
- performance measurement for the exact supported workload

## Scope

This milestone should add and certify the following Elasticsearch behavior.

### Queries

- `multi_match`
- `terms`
- `ids`
- restricted `query_string`
- better multi-index bool, range, and sort handling across aliases and wildcards

### Aggregations

- `date_histogram`
- `cardinality`
- `terms` with sub-aggregations
- `filter` with nested metric sub-aggregations

### Multi-Index Semantics

- wildcard resolution for `logs-*`
- alias-backed multi-index reads
- sorted multi-index top-k merge
- PIT plus `search_after` on multi-index reads
- clear error contracts where true parity is not yet available

### Client Certification

- official JavaScript client workload tests
- request shapes that resemble real app and dashboard traffic, not just probes

### Performance

- benchmark Powdrr versus Elasticsearch on the same workload
- separate correctness from latency, but keep both in the same harness

## Out Of Scope For This Milestone

These should stay explicitly unsupported unless they become required by the
target workload:

- scroll
- async search
- search templates
- cat APIs
- nested
- highlight
- wildcard / regexp text search
- geo queries
- script scoring
- collapse

Every unsupported route or query shape should either:

- fail with a stable JSON error payload, or
- be rejected in the fixture matrix as a local-only unsupported contract

## Implementation Plan

### Phase 1: Freeze The Logs Workload Contract

Add a workload-specific compatibility section and fixture corpus for:

- `logs-*` multi-index routing
- `@timestamp` range queries
- exact-field filters on service/env/tenant/status
- sorted `@timestamp desc` search
- PIT plus `search_after`
- `multi_match`
- restricted `query_string`
- `date_histogram`
- `terms` plus sub-aggregations
- `cardinality`

Deliverables:

- new workload fixtures in `testdata/es_compat_cases.json`
- matrix doc updates in `docs/es-compatibility-matrix.md`
- route or feature manifest expansion if new handlers are involved

Acceptance:

- every target workload shape is represented in the compatibility suite
- every non-goal route or shape has an explicit unsupported contract

### Phase 2: Make Multi-Index Execution First-Class

Move all remaining important multi-index read semantics onto the shared search
execution path instead of endpoint-local shortcuts.

Focus:

- `logs-*` wildcard resolution
- alias expansion
- sorted multi-index top-k merge
- PIT plus `search_after` across multiple indices
- aggregation merge correctness across multiple indices

Acceptance:

- the workload fixtures above pass against Powdrr and Elasticsearch
- no route-level special casing remains for the common sorted multi-index path

### Phase 3: Expand Richer DSL For Logs

Implement the minimum richer DSL needed for the workload.

Priority order:

1. `multi_match`
2. `terms`
3. `ids`
4. restricted `query_string`

`query_string` should be deliberately narrow. It should support only the
operators and field targeting needed for the workload and fail clearly for the
rest.

Acceptance:

- workload fixtures pass differentially
- the JavaScript client workload suite exercises each supported query type
- unsupported `query_string` constructs fail with explicit errors

### Phase 4: Expand Aggregation Parity For Logs

Implement the aggregation subset that real log dashboards need.

Priority order:

1. `date_histogram`
2. `cardinality`
3. `terms` with metric sub-aggregations
4. `filter` plus nested metric sub-aggregations

Acceptance:

- date-bucket results match Elasticsearch on controlled fixtures
- cardinality behavior is documented if approximate
- sub-aggregation response shape is validated differentially

### Phase 5: Add Workload-Level JavaScript Client Tests

Extend the official JS client suite beyond smoke probes.

Add workload scenarios that use the real client API for:

- `search` over `logs-*`
- `openPointInTime`
- `search_after`
- `msearch`
- `count`
- workload aggregations

Also add explicit client-verified unsupported cases for:

- scroll
- search template
- unsupported `query_string` constructs

Acceptance:

- the JS client can run the whole target workload against both Powdrr and
  Elasticsearch
- normalized summaries match across both systems for the supported slice

### Phase 6: Benchmark The Same Workload

Do not benchmark arbitrary synthetic queries. Benchmark the exact workload we
are claiming to support.

Measure at minimum:

- single-index versus multi-index
- cold versus warm cache
- top-N sorted search
- PIT plus `search_after`
- `_msearch`
- aggregation-heavy log queries

Compare:

- Powdrr
- single-node Elasticsearch

Record:

- p50
- p95
- p99
- throughput under moderate concurrency
- correctness notes for any known semantic compromises

Acceptance:

- benchmark harness can run the workload on both engines
- reported results are tied to the same fixture dataset and same query corpus

## Testing Plan

Use four layers of coverage.

### 1. Parser And Plan Tests

Validate:

- `multi_match` lowering
- restricted `query_string` parsing
- multi-index target resolution
- PIT plus `search_after` validation

### 2. Differential Fixture Tests

Expand `query_server/tests/es_compatibility_matrix.rs` so every supported workload
shape is compared against real Elasticsearch.

### 3. Official Client Tests

Extend `query_server/tests/elasticsearch_js_client_compat.rs` and
`tests/es_js_client/smoke.mjs` into workload-level checks.

### 4. Benchmark Harness

Use the same dataset and query corpus in `benchmark/` so correctness and
performance stay tied to the same target workload.

## Performance And Execution Notes

This milestone should not fork the serving core around Elasticsearch-specific
logic.

The preferred direction remains:

- protocol adapter normalizes request
- shared planner chooses the execution path
- multi-index merge happens in the shared executor
- benchmarkable physical paths are reusable by other protocols later

For the logs workload, the main execution wins should come from:

- strong multi-index top-k merge
- narrow projections
- better sort-aware execution
- better aggregation partials
- eventually file and row-group pruning on `@timestamp` and exact fields

## Clear Unsupported Policy

A route or query shape should never "sort of work" by accident.

For this milestone, anything outside the supported logs workload should do one
of two things:

1. succeed and match Elasticsearch closely enough to be in the differential
   suite
2. fail with a clear, stable, explicit JSON error payload

That policy is as important as adding features. It keeps clients predictable.

## Exit Criteria

This milestone is done when all of the following are true:

1. an official JavaScript Elasticsearch client can run the target logs
   workload against Powdrr
2. the same workload passes differential checks against real Elasticsearch
3. unsupported shapes fail clearly and are covered by fixtures
4. the benchmark harness can compare Powdrr and Elasticsearch on that workload
5. we can describe the supported Elasticsearch slice in workload terms, not
   just endpoint terms

## Immediate Next Step

Start with Phase 1 and Phase 2 together:

- freeze the `logs-*` workload in fixtures
- finish pushing multi-index sorted search and aggregation behavior onto the
  shared execution path

That gives the fastest path from "broad API coverage" to "real Elasticsearch
workload compatibility."
