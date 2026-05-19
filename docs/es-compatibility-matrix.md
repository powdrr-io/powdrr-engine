# ES Compatibility Matrix

## Purpose

This file is the tracked Phase 0 compatibility contract for the current engine.

It has two jobs:

1. document which Elasticsearch-compatible behaviors we rely on today
2. point to the fixture-driven tests that must continue to pass during the
   Iceberg-engine migration

The backing test artifacts are:

- fixture corpus: `main_lib/tests/data/es_compat_cases.json`
- route coverage manifest: `main_lib/tests/data/es_api_coverage_manifest.json`
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
- `Local Only Contract` means the fixture is intentionally pinned to the
  current Powdrr behavior because the route is useful but not yet
  Elasticsearch-identical.
- `Unsupported Contract` means the route is intentionally present only to fail
  with a checked, explicit error payload.

The manifest file turns this into an enforceable surface contract:

- every routed `es_*` handler in `main_lib/src/router.rs` must appear in the
  manifest
- every manifest entry must reference at least one fixture id
- differential handlers may only reference `differential_enabled: true` cases
- local-only and unsupported handlers may only reference local-only fixtures

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
| Search | `multi_match` over `logs-*` returns the expected multi-index hits | `logs_wildcard_multi_match_returns_expected_hits` | Yes | Yes | First workload-level multi-index text search contract |
| Search | field sort returns hits in descending numeric order | `field_sort_returns_expected_descending_hit_order` | Yes | Yes | Covers the typed node-local/controller merge path for explicit sorts |
| Search | wildcard multi-index sorted pagination with `search_after` returns the expected next page | `logs_wildcard_search_after_returns_expected_hits` | Yes | Yes | Freezes the current typed wildcard merge path |
| Search | zero-hit query on an existing index returns zero total hits | `zero_hit_query_on_existing_index_returns_zero_total` | Yes | Yes | Guards empty-result behavior |
| Document lifecycle | `POST /:index/_create/:id` conflicts after refresh | `create_with_id_conflict_after_refresh` | Yes | Yes | Captures current create-vs-existing semantics |
| Document lifecycle | `GET /:index/_doc/:id` returns stored source | `get_existing_doc_returns_source` | Yes | Yes | Freezes current `_doc` retrieval behavior |
| Document lifecycle | `DELETE /:index/_doc/:id` succeeds for existing doc | `delete_existing_doc_returns_200` | Yes | Yes | Status-only for first cut |
| Document lifecycle | `GET /:index/_doc/:id` returns `404` after delete and refresh | `get_deleted_doc_returns_404` | Yes | Yes | Captures delete visibility |
| Mutations | `_update_by_query` scripted field becomes searchable after refresh | `update_by_query_scripted_field_becomes_searchable_after_refresh` | Yes | Yes | Freezes the supported update-by-query subset |
| Index metadata | `PUT /_aliases` allows subsequent search via alias name | `alias_update_allows_search_via_alias_name` | Yes | Yes | Covers alias routing even though `GET /_alias` is still stubbed |
| Templates | `HEAD /_index_template/:name` returns `200` after create | `index_template_head_returns_200_after_create` | Yes | Yes | Captures template existence checks |
| Templates | `GET /_index_template/:name` returns the stored body | `index_template_get_returns_stored_body` | Yes | No | Current local shape differs from Elasticsearch's wrapped response |
| Probes | `GET /` and `HEAD /` return Elasticsearch-style probe responses | `root_get_returns_basic_server_info`, `root_head_returns_200` | Yes | Yes | Covers direct client product checks |
| Cluster | `GET /_cluster/settings` and `GET /_cluster/health/:name` return compatibility payloads | `cluster_settings_include_defaults_returns_defaults`, `cluster_health_named_route_returns_green_status` | Yes | Yes | Also guards against regressions like the old `flat_settings` panic |
| Metadata | index, alias, mapping, settings, resolve-index, and field-caps routes are fixture-backed | `head_index_returns_200_after_create` and related ids | Yes | Yes | The route manifest points each handler to its exact fixture ids |
| Batch reads | `_mget` and `_msearch` are covered on both global and index-scoped routes | `table_mget_returns_found_and_missing_docs` and related ids | Yes | Yes | Keeps batched client read flows stable |
| Document lifecycle | `HEAD /:index/_doc/:id` returns `200` for existing docs | `head_existing_doc_returns_200` | Yes | Yes | Complements existing GET and DELETE coverage |
| Local-only admin | nodes, license, xpack, PIT, pipeline simulation, ILM, monitoring bulk | manifest-backed local-only fixtures | Yes | No | Useful for clients, but not yet frozen against real Elasticsearch |
| Unsupported | scroll, search template, async search, cat APIs, GET pipeline, `_update/:id` | manifest-backed unsupported fixtures | Yes | No | Every such route must now fail with a clear checked error payload |
| Aggregations | `terms` aggregation over string fields returns expected bucket keys and doc counts | `terms_aggregation_returns_expected_bucket_keys_and_counts` | Yes | No | Current local behavior depends on aggregating over analyzed text fields, which Elasticsearch rejects without exact-field mapping or fielddata |
| Aggregations | `avg` plus filtered sub-aggregation returns expected metric values | `avg_and_filter_subaggregation_return_expected_values` | Yes | Yes | Uses exact alphanumeric string terms to avoid analyzed-text drift between Powdrr and Elasticsearch |
| Aggregations | `date_histogram` over `logs-*` returns expected per-day buckets | `logs_wildcard_date_histogram_returns_expected_buckets` | Yes | Yes | First differential workload histogram contract |
| Aggregations | `cardinality` over `logs-*` returns the expected distinct value count | `logs_wildcard_cardinality_returns_expected_value` | Yes | Yes | Uses exact merge semantics in the typed path |
| Aggregations | `terms` with per-bucket `avg` sub-aggregations over `logs-*` returns the expected merged buckets | `logs_wildcard_terms_subaggregation_returns_expected_buckets` | Yes | Yes | First differential contract for typed bucket sub-aggregation merge |
| Aggregations | `date_histogram` with nested per-bucket `avg` metrics over `logs-*` returns the expected bucket metrics | `logs_wildcard_date_histogram_metric_subaggregation_returns_expected_bucket_metrics` | Yes | Yes | Freezes typed histogram bucket metric merge |
| Aggregations | `date_histogram` with nested `terms` buckets over `logs-*` returns the expected nested bucket tree | `logs_wildcard_date_histogram_terms_subaggregation_returns_expected_nested_buckets` | Yes | Yes | Covers histogram-to-terms dashboard drilldown |
| Aggregations | `terms` with nested `date_histogram` buckets over `logs-*` returns the expected nested bucket tree | `logs_wildcard_terms_date_histogram_subaggregation_returns_expected_nested_buckets` | Yes | Yes | Covers terms-to-histogram dashboard drilldown |

## Surface Rules

Every routed ES handler must fit one of three buckets:

1. exact enough to run differentially against real Elasticsearch
2. intentionally local-only, with the current response contract pinned
3. intentionally unsupported, with a clear error payload

If a new route lands without being added to the manifest and fixture corpus,
the manifest coverage test fails.

## Planned Next

The next compatibility additions should focus on remaining differential drift:

The broader workload milestone for that next phase is documented in
`docs/es-log-workload-plan.md`.

1. narrower `query_string` support for the logs workload
2. more aggregation parity: `missing`, range buckets, and bucket-level sub-aggregations
3. official client smoke tests for Python and Go
4. broader multi-index differential coverage beyond the first `logs-*` workload
5. explicit unsupported contracts for any remaining ambiguous write/admin routes

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
