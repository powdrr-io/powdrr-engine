# powdrr-control-plane

Shared control-plane contracts used by both the runtime side and the service
side.

## Owns

- table metadata and checkpoint contracts
- shared schema-related helpers
- test API contracts shared across packages

## Does Not Own

- protocol routing
- query execution
- state-provider orchestration
- service implementation logic

## Main Entry Points

- [src/data_contract.rs](./src/data_contract.rs)
- [src/test_api.rs](./src/test_api.rs)
- [src/checkpoint_descriptor.rs](./src/checkpoint_descriptor.rs)

## Dependency Rule

Keep this crate low in the dependency graph. New code here should be reusable by
both `query_runtime` and `service_lib` without pulling in protocol or runtime
machinery.
