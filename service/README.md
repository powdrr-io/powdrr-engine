# powdrr-io-service

The control-plane service binary.

## Owns

- process entrypoint and configuration wiring for `service_lib`
- exposing the control-plane HTTP/API surface

## Does Not Own

- query-serving runtime behavior
- protocol adapter logic for Elasticsearch/DynamoDB/Mongo serving requests

## Start Here

- [src/main.rs](./src/main.rs)
- [src/router.rs](./src/router.rs)
- [src/service_impl_provider.rs](./src/service_impl_provider.rs)

Most metadata behavior changes belong in `service_lib`, not here.
