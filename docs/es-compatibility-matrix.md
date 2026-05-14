# ES Compatibility Matrix

## Purpose

This file is the tracked Phase 0 compatibility contract for the current engine.

It has two jobs:

1. document which Elasticsearch-compatible behaviors we rely on today
2. point to the fixture-driven tests that must continue to pass during the
   Iceberg-engine migration

The backing test artifacts are:

- fixture corpus: `main_lib/tests/data/es_compat_cases.json`
- local and differential harness: `main_lib/tests/es_compatibility_matrix.rs`

## How To Read This Matrix

- `Automated Local` means the fixture runs against the current in-repo engine.
- Local runs require the existing test dependencies used by the router suite:
  LocalStack/DynamoDB on `127.0.0.1:4566` and Redis on `127.0.0.1:6379`.
- `Differential Ready` means the same fixture can also run against a real
  Elasticsearch instance when `POWDRR_ES_COMPAT_URL` is set.
- `Planned` means the behavior is in scope but not yet fixture-backed.

## Automated Now

| Area | Behavior | Fixture ID | Automated Local | Differential Ready | Notes |
|---|---|---|---|---|---|
| Index lifecycle | `PUT /:index` returns acknowledged create response | `create_index_acknowledged` | Yes | Yes | Baseline index creation contract |
| Search | `_bulk` ingest followed by `match` search returns expected hits | `bulk_match_search_returns_expected_hits` | Yes | Yes | Freezes current basic full-text behavior |
| Document lifecycle | `POST /:index/_create/:id` conflicts after refresh | `create_with_id_conflict_after_refresh` | Yes | Yes | Captures current create-vs-existing semantics |
| Document lifecycle | `GET /:index/_doc/:id` returns stored source | `get_existing_doc_returns_source` | Yes | Yes | Freezes current `_doc` retrieval behavior |
| Document lifecycle | `DELETE /:index/_doc/:id` succeeds for existing doc | `delete_existing_doc_returns_200` | Yes | Yes | Status-only for first cut |
| Document lifecycle | `GET /:index/_doc/:id` returns `404` after delete and refresh | `get_deleted_doc_returns_404` | Yes | Yes | Captures delete visibility |

## Planned Next

| Area | Behavior | Status | Existing Source Reference | Notes |
|---|---|---|---|---|
| Search | `term` query semantics | Planned | `main_lib/src/elastic_search_parser.rs` | Should become first-class fixture coverage |
| Search | `bool.must` / `bool.should` / `bool.filter` / `bool.must_not` | Planned | `main_lib/src/elastic_search_parser.rs` | High-value migration gate |
| Search | `range` query semantics | Planned | `main_lib/src/elastic_search_parser.rs` | Important for shard pruning and doc-values work |
| Search | `simple_query_string` behavior | Planned | `main_lib/src/elastic_search_parser.rs` | Needed before parser swap |
| Search | zero-hit queries on existing indices | Planned | `main_lib/src/router.rs` | Current behavior already exercised inline |
| Mutations | `_update_by_query` subset | Planned | `main_lib/src/router.rs` | Needed for Kibana compatibility surface |
| Index metadata | aliases update/read | Planned | `main_lib/src/router.rs` | Must preserve if currently relied on |
| Templates | `_index_template` and `_component_template` flows | Planned | `main_lib/src/router.rs` | Good fit for fixture backing |
| Aggregations | current supported aggregation subset | Planned | `main_lib/src/router.rs` | Should be separated from search-core tests |
| Topology | multi-shard single-node search | Planned | `benchmark/src/main.rs` | Needed before engine swap |
| Topology | node-local merge plus controller merge | Planned | New engine work | Migration-critical |
| Performance | single-node ingest/query baseline | Planned | `benchmark/src/main.rs` | Baseline before replacing execution path |

## Differential Test Mode

To run the same automated fixtures against a real Elasticsearch node, set:

```bash
POWDRR_ES_COMPAT_URL=http://localhost:9200 rtk cargo test -p powdrr_lib --test es_compatibility_matrix -- --nocapture
```

Without `POWDRR_ES_COMPAT_URL`, only the local fixture run executes.

## Immediate Expansion Order

The next fixtures to add should be:

1. `term` queries
2. `bool` queries
3. `range` queries
4. `simple_query_string`
5. `_update_by_query`
6. alias and template flows

That order matches the highest-risk parts of the search-engine migration.
