# powdrr-query-lib

Low-level execution and storage helpers used by the runtime layer.

## Owns

- parquet/object-store reads
- low-level query execution helpers
- speedboat buffer helpers

## Does Not Own

- protocol routing
- state-provider orchestration
- checkpoint publication logic
- peer/runtime coordination

## Main Entry Points

- [src/data_access.rs](./src/data_access.rs)
- [src/query_execution.rs](./src/query_execution.rs)
- [src/speedboat_buffer.rs](./src/speedboat_buffer.rs)

## Dependency Rule

Keep this crate below runtime orchestration. If code needs to know about
publication frontiers, metadata-store backends, or HTTP request handling, it
does not belong here.
