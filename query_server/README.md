# powdrr-query-server

Protocol adapters and request-routing surface for the engine.

## Owns

- top-level router
- Elasticsearch-compatible handlers
- DynamoDB-compatible handlers
- Mongo and Redis protocol shims
- test-only HTTP endpoints

## Does Not Own

- a separate serving engine
- checkpoint publication semantics
- low-level parquet/object-store logic

## Main Entry Points

- [src/router.rs](./src/router.rs)
- [src/elastic_search_endpoints.rs](./src/elastic_search_endpoints.rs)
- [src/dynamodb_protocol.rs](./src/dynamodb_protocol.rs)
- [src/mongodb_protocol.rs](./src/mongodb_protocol.rs)

## Tests

- compatibility and wire suites: [tests/](./tests)

## Dependency Rule

Translate protocol requests at the edge and call the shared runtime. If code is
useful to more than one protocol surface, it probably belongs in `query_runtime`
or lower.
