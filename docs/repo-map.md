# Repo Map

This is the quickest directory-to-package map for the current workspace.

## Workspace Packages

| Directory | Cargo package | Role |
|---|---|---|
| [benchmark/](../benchmark) | `powdrr-benchmark` | End-to-end serving benchmark and workload harness |
| [cli/](../cli) | `powdrr-cli` | Local CLI for indexing/querying without the HTTP server |
| [control_plane/](../control_plane) | `powdrr-control-plane` | Shared control-plane contracts and schema helpers |
| [engine/](../engine) | `powdrr-io-engine` | Query and serving server binary |
| [query_core/](../query_core) | `powdrr-query-core` | Pure shared query/plan/types layer |
| [query_lib/](../query_lib) | `powdrr-query-lib` | Low-level execution and storage helpers |
| [query_runtime/](../query_runtime) | `powdrr-query-runtime` | Shared runtime, serving engine, ingest, state providers |
| [query_server/](../query_server) | `powdrr-query-server` | Protocol adapters and HTTP/wire routing |
| [service/](../service) | `powdrr-io-service` | Control-plane service binary |
| [service_lib/](../service_lib) | `powdrr-service-lib` | Control-plane service implementation and metadata backends |

## Non-Package Top-Level Directories

| Directory | Purpose |
|---|---|
| [clients/](../clients) | Client-side experiments or examples |
| [dev_stack/](../dev_stack) | Local development stack support files such as compose and cert material |
| [docs/](../docs) | Architecture notes, contribution guides, and roadmap/design docs |
| [iceberg_lib/](../iceberg_lib) | External or experimental code not currently in the workspace |
| [kubernetes/](../kubernetes) | Kubernetes manifests or deployment assets |
| [testdata/](../testdata) | Shared parquet, JSON, and compatibility fixtures |
| [tests/](../tests) | Docker Compose harnesses and external test support assets |
| [scripts/](../scripts) | Repo automation, worktree helpers, and local harness entrypoints |

## Typical Ownership Questions

### “Where do I add a new protocol handler?”

Start in [query_server/](../query_server).

### “Where do I change checkpoint/publication behavior?”

Start in [query_runtime/](../query_runtime) and [service_lib/](../service_lib).

### “Where do I change pure query/plan/data model types?”

Start in [query_core/](../query_core).

### “Where do I change low-level object-store or parquet access?”

Start in [query_lib/](../query_lib).

### “Where do I change the CLI?”

Start in [cli/](../cli) for flags/entrypoint and [query_runtime/](../query_runtime)
for local execution behavior.

### “Where do integration tests live?”

- protocol compatibility and wire tests: [query_server/tests/](../query_server/tests)
- runtime/local CLI tests: [query_runtime/tests/](../query_runtime/tests)
- shared fixtures: [testdata/](../testdata) and [tests/](../tests)
