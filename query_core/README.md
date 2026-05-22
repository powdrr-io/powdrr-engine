# powdrr-query-core

Pure shared query and serving-plan types.

## Owns

- read/search/serving plan types
- schema massaging and validation
- shared ES query DTOs used by multiple layers
- query-path classification helpers

## Does Not Own

- HTTP or wire protocols
- state providers or metadata backends
- peer fanout
- object-store mutation or compaction orchestration

## Main Entry Points

- [src/serving_plan.rs](./src/serving_plan.rs)
- [src/search_plan.rs](./src/search_plan.rs)
- [src/read_plan.rs](./src/read_plan.rs)
- [src/schema_massager.rs](./src/schema_massager.rs)

## Dependency Rule

If a change can be expressed as a pure data model or planning concern, it
probably belongs here. If it needs network, protocol, or state-provider
integration, it belongs higher in the stack.
