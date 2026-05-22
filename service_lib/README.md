# powdrr-service-lib

Control-plane service implementation and metadata backends.

## Owns

- metadata-store interfaces
- service-side DynamoDB / ephemeral / Raft implementations
- service-side peers and coordination helpers
- control-plane state transitions used by `powdrr-io-service`

## Does Not Own

- protocol routing for the query server
- low-level parquet serving execution
- search/query runtime orchestration for the engine

## Main Entry Points

- [src/metadata_store.rs](./src/metadata_store.rs)
- [src/dynamodb_service_impl.rs](./src/dynamodb_service_impl.rs)
- [src/ephemeral_service_impl.rs](./src/ephemeral_service_impl.rs)
- [src/raft_service_impl.rs](./src/raft_service_impl.rs)

## Dependency Rule

This crate should stay focused on the control-plane service and metadata
backends. If a change is about serving a query or executing a protocol read,
that belongs in the query-side crates instead.
