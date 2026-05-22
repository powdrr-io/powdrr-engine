# powdrr-io-engine

The main query and serving server binary.

## Owns

- process entrypoint and configuration wiring for the query server
- assembly of `query_server` and `query_runtime`

## Does Not Own

- protocol handler implementation details
- shared serving runtime logic
- control-plane metadata service behavior

## Start Here

- [src/main.rs](./src/main.rs)
- [src/configuration.rs](./src/configuration.rs)

Most behavioral changes belong in `query_server` or `query_runtime`, not here.
