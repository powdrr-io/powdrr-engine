# powdrr-query-runtime

Shared runtime and orchestration layer behind the engine and CLI.

## Owns

- serving runtime and query execution orchestration
- ingest, mutation, and index build flows
- compaction
- state providers and metadata-store integration
- peer fanout and prefetch
- local CLI execution

## Does Not Own

- HTTP and wire routing details
- control-plane service APIs
- pure query-plan model types that can live in `query_core`

## Main Entry Points

- [src/state_provider.rs](./src/state_provider.rs)
- [src/lakehouse_serving.rs](./src/lakehouse_serving.rs)
- [src/serving_protocol.rs](./src/serving_protocol.rs)
- [src/elastic_search_ingest.rs](./src/elastic_search_ingest.rs)
- [src/local_cli.rs](./src/local_cli.rs)

## Tests

- runtime-focused integration tests: [tests/](./tests)

## Dependency Rule

This crate is where shared runtime behavior belongs. It should call into
`query_lib` and `query_core`, and it should be called by `query_server`, not
the other way around.
