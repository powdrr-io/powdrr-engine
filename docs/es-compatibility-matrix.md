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
  LocalStack/DynamoDB on `127.0.0.1:4566`, Redis on `127.0.0.1:6379`,
  MinIO on `127.0.0.1:9000`, and the Iceberg REST catalog on `127.0.0.1:8181`.
- `Differential Ready` means the same fixture can also run against a real
  Elasticsearch instance when `POWDRR_ES_COMPAT_URL` is set, and the harness
  compares the normalized assertion-defined result projection between Powdrr
  and Elasticsearch.
- `Planned` means the behavior is in scope but not yet fixture-backed.

## Automated Now

| Area | Behavior | Fixture ID | Automated Local | Differential Ready | Notes |
|---|---|---|---|---|---|
| Index lifecycle | `PUT /:index` returns acknowledged create response | `create_index_acknowledged` | Yes | Yes | Baseline index creation contract |
| Search | `_bulk` ingest followed by `match` search returns expected hits | `bulk_match_search_returns_expected_hits` | Yes | Yes | Freezes current basic full-text behavior |
| Search | `term` query on a numeric field returns the exact hit | `term_query_on_numeric_field_returns_exact_hit` | Yes | Yes | Freezes exact-match term behavior without text-analysis ambiguity |
| Search | `bool.must` plus `bool.must_not` narrows the hit set | `bool_must_and_must_not_returns_single_hit` | Yes | Yes | Captures basic bool composition |
| Search | `bool.filter` plus `should` and `minimum_should_match` returns filtered hits | `bool_filter_should_minimum_should_match_returns_filtered_hits` | Yes | Yes | Freezes a Kibana-like bool pattern |
| Search | `range` query on `@timestamp` returns expected hits | `range_query_on_timestamp_returns_expected_hits` | Yes | Yes | Important for future shard-pruning and doc-values work |
| Search | `simple_query_string` with a single term returns the expected hits | `simple_query_string_with_and_operator_returns_expected_hit` | Yes | Yes | Freezes the currently working parser path before engine swap |
| Search | zero-hit query on an existing index returns zero total hits | `zero_hit_query_on_existing_index_returns_zero_total` | Yes | Yes | Guards empty-result behavior |
| Document lifecycle | `POST /:index/_create/:id` conflicts after refresh | `create_with_id_conflict_after_refresh` | Yes | Yes | Captures current create-vs-existing semantics |
| Document lifecycle | `GET /:index/_doc/:id` returns stored source | `get_existing_doc_returns_source` | Yes | Yes | Freezes current `_doc` retrieval behavior |
| Document lifecycle | `DELETE /:index/_doc/:id` succeeds for existing doc | `delete_existing_doc_returns_200` | Yes | Yes | Status-only for first cut |
| Document lifecycle | `GET /:index/_doc/:id` returns `404` after delete and refresh | `get_deleted_doc_returns_404` | Yes | Yes | Captures delete visibility |
| Mutations | `_update_by_query` scripted field becomes searchable after refresh | `update_by_query_scripted_field_becomes_searchable_after_refresh` | Yes | Yes | Freezes the supported update-by-query subset |
| Index metadata | `PUT /_aliases` allows subsequent search via alias name | `alias_update_allows_search_via_alias_name` | Yes | Yes | Covers alias routing even though `GET /_alias` is still stubbed |
| Templates | `HEAD /_index_template/:name` returns `200` after create | `index_template_head_returns_200_after_create` | Yes | Yes | Captures template existence checks |
| Templates | `GET /_index_template/:name` returns the stored body | `index_template_get_returns_stored_body` | Yes | No | Current local shape differs from Elasticsearch's wrapped response |
| Aggregations | `terms` aggregation over string fields returns expected bucket keys and doc counts | `terms_aggregation_returns_expected_bucket_keys_and_counts` | Yes | No | Current local behavior depends on aggregating over analyzed text fields, which Elasticsearch rejects without exact-field mapping or fielddata |
| Aggregations | `avg` plus filtered sub-aggregation returns expected metric values | `avg_and_filter_subaggregation_return_expected_values` | Yes | Yes | Uses exact alphanumeric string terms to avoid analyzed-text drift between Powdrr and Elasticsearch |

## Planned Next

| Area | Behavior | Status | Existing Source Reference | Notes |
|---|---|---|---|---|
| Index metadata | `GET /:name/_alias` response shape | Planned | `main_lib/src/elastic_search_endpoints.rs` | Endpoint currently returns `{}` and needs real compatibility work |
| Templates | wrapped `GET /_index_template/:name` Elasticsearch response shape | Planned | `main_lib/src/elastic_search_endpoints.rs` | Local GET is intentionally tracked as current behavior, not full ES compatibility |
| Templates | `_component_template` flows | Planned | `main_lib/src/router.rs` | Good next metadata-surface expansion |
| Aggregations | `missing`, `cardinality`, `date_histogram`, and range-bucket responses | Planned | `main_lib/src/elastic_search_parser.rs` | Next aggregation slice after terms/filter/avg |
| Search | multi-term `simple_query_string` semantics | Planned | `main_lib/src/elastic_search_parser.rs` | Current engine does not yet match Elasticsearch for richer query-string semantics |
| Topology | multi-shard single-node search | Planned | `benchmark/src/main.rs` | Needed before engine swap |
| Topology | node-local merge plus controller merge | Planned | New engine work | Migration-critical |
| Performance | single-node ingest/query baseline | Planned | `benchmark/src/main.rs` | Baseline before replacing execution path |

## Differential Test Mode

For a full local differential run, use the dedicated compatibility stack:

```bash
bash scripts/run_es_compat_local.sh
```

That script starts Redis, MinIO, the Iceberg REST catalog, LocalStack, and a
single-node Elasticsearch instance, then runs the fixture parser check, the
local-only compatibility suite, and the external differential suite as separate
`cargo test` invocations. The local and differential suites intentionally run
in separate processes because Powdrr's test harness keeps process-global state.

If you already have the dependencies running and only want to point the harness
at Elasticsearch directly, set:

```bash
POWDRR_ES_COMPAT_URL=http://localhost:9200 cargo test -p powdrr_lib --test es_compatibility_matrix -- --nocapture
```

Without `POWDRR_ES_COMPAT_URL`, only the local fixture run executes.

## Immediate Expansion Order

The next fixtures to add should be:

1. remaining aggregation response coverage (`missing`, `cardinality`, `date_histogram`, range buckets)
2. `_component_template` flows
3. alias readback response shape
4. multi-shard topology cases
5. node-local merge cases
6. single-node performance baselines

That order matches the remaining high-risk parts of the search-engine migration.
