# powdrr-cli

Local CLI for building and querying data through the shared Powdrr runtime
without starting the HTTP server.

## Owns

- command-line flags and subcommand wiring
- local entrypoint into `query_runtime::local_cli`

## Does Not Own

- the serving runtime itself
- metadata backend implementations
- HTTP or wire protocol behavior

## Start Here

- [src/main.rs](./src/main.rs)

Most functional behavior changes belong in `query_runtime`, not here.
