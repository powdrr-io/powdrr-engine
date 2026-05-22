# Protocol Compatibility Contract

## Purpose

This document is the top-level contract for Powdrr's compatibility surfaces.

It defines:

- which Elasticsearch, DynamoDB, and Redis APIs Powdrr currently supports
- which requests are intentionally unsupported and must fail explicitly
- what `readonly` mode means for those surfaces
- which external clients or frameworks we have actually verified

This is the summary contract. The detailed route- and fixture-level matrices
remain in:

- `docs/es-compatibility-matrix.md`
- `docs/dynamodb-compatibility-matrix.md`

## Contract Rules

Powdrr only claims compatibility for the explicitly documented subset.

- Supported means the route or command is intentionally implemented and covered
  by tests.
- Unsupported means the route or command must fail explicitly. Silent success,
  partial no-op behavior, and generic internal failures are contract bugs.
- `readonly` means write APIs hard error even if the same surface is supported
  in normal read-write mode.

`readonly` mode is explicit:

- engine/runtime config: `API_MODE=readonly`
- testing config: `TestProcessingMode { api_mode: "readonly", ... }`

In `readonly` mode, Powdrr rejects:

- Elasticsearch write APIs
- DynamoDB mutation APIs
- Redis write commands
- compatibility-surface config writes such as `PUT /:table/_serve/config`,
  `PUT /:table/_dynamodb/config`, `PUT /:table/_mongo/config`, and
  `PUT /:table/_redis/config`

## Elasticsearch-Compatible HTTP API

Detailed matrix: `docs/es-compatibility-matrix.md`

### Supported Read and Metadata Routes

- `GET /`
- `HEAD /`
- `GET /_nodes`
- `GET /_license`
- `GET /_xpack`
- `GET /_cluster/settings`
- `GET /_cluster/health`
- `GET /_cluster/health/:name`
- `GET /_alias`
- `GET /_alias/:alias`
- `GET /_resolve/index/:name`
- `GET /:index`
- `HEAD /:index`
- `GET /:index/_alias`
- `GET /:index/_alias/:alias`
- `HEAD /:index/_alias/:alias`
- `GET /:index/_settings`
- `GET /:index/_mapping`
- `GET /_index_template/:name`
- `GET /_component_template/:name`
- `HEAD /_template/:name`
- `HEAD /_index_template/:name`
- `GET /_template/:name`
- `GET /_search`
- `POST /_search`
- `GET /:index/_search`
- `POST /:index/_search`
- `GET /_count`
- `POST /_count`
- `GET /:index/_count`
- `POST /:index/_count`
- `POST /_msearch`
- `POST /:index/_msearch`
- `POST /_mget`
- `POST /:index/_mget`
- `GET /_field_caps`
- `POST /_field_caps`
- `GET /:index/_field_caps`
- `POST /:index/_field_caps`
- `GET /:index/_doc/:id`
- `HEAD /:index/_doc/:id`
- `POST /:index/_pit`
- `DELETE /_pit`

### Supported Write Routes In Read-Write Mode

- `PUT /:index`
- `POST /:index`
- `POST /:index/_create/:id`
- `PUT /:index/_create/:id`
- `POST /:index/_doc`
- `POST /:index/_doc/:id`
- `PUT /:index/_doc/:id`
- `DELETE /:index/_doc/:id`
- `POST /:index/_update/:id`
- `POST /:index/_update_by_query`
- `POST /_bulk`
- `PUT /_bulk`
- `PUT /_aliases`
- `POST /_aliases`
- `PUT /_index_template/:name`
- `POST /_index_template/:name`
- `PUT /_component_template/:name`
- `POST /_component_template/:name`
- `POST /_ingest/pipeline/_simulate`
- `POST /_ilm/policy/:name`
- `PUT /_ilm/policy/:name`
- `POST /_monitoring/bulk`
- `PUT /_monitoring/bulk`

### Explicitly Unsupported Routes

- `GET /_ingest/pipeline/:name`
- `PUT /_ingest/pipeline/:name`
- `POST /_ingest/pipeline/:name`
- `GET /_ingest/pipeline/_simulate`
- `GET /_ingest/pipeline/:name/_simulate`
- `POST /_ingest/pipeline/:name/_simulate`
- `POST /_search/scroll`
- `DELETE /_search/scroll`
- `POST /_search/template`
- `POST /:index/_search/template`
- `POST /_async_search`
- `GET /_cat/indices`
- `GET /_cat/aliases`

Those must fail with explicit checked Elasticsearch-style error payloads.

### Read-Only Mode Behavior

In `API_MODE=readonly`, the supported Elasticsearch write routes above hard
error with:

- HTTP `403`
- error type `cluster_block_exception`

### Verified Clients and Harnesses

- fixture matrix in `query_server/tests/es_compatibility_matrix.rs`
- differential run against a real Elasticsearch instance via
  `POWDRR_ES_COMPAT_URL`
- official JavaScript client smoke test in
  `query_server/tests/elasticsearch_js_client_compat.rs`
  using `@elastic/elasticsearch`

Not yet verified:

- official Python Elasticsearch client
- official Go Elasticsearch client

## DynamoDB-Compatible HTTP API

Detailed matrix: `docs/dynamodb-compatibility-matrix.md`

### Supported Operations

Powdrr currently routes `POST /` with `X-Amz-Target: DynamoDB_20120810.*` and
supports:

- `ListTables`
- `DescribeTable`
- `GetItem`
- `BatchGetItem`
- `PutItem`
- `UpdateItem`
- `DeleteItem`
- `BatchWriteItem`
- `Query`
- `Scan`

### Explicitly Unsupported Operations

Everything outside the supported list is intentionally unsupported and must
fail explicitly, including:

- transactional APIs such as `TransactGetItems` and `TransactWriteItems`
- control-plane APIs such as `CreateTable`, `DeleteTable`, and `UpdateTable`
- backup, import, export, TTL, tagging, autoscaling, global-table, and
  streaming APIs

The matrix harness also freezes explicit failures for unsupported request
members and unsupported expression shapes on otherwise supported operations.

### Read-Only Mode Behavior

In `API_MODE=readonly`, the DynamoDB mutation operations hard error:

- `PutItem`
- `UpdateItem`
- `DeleteItem`
- `BatchWriteItem`

The current contract is:

- HTTP `400`
- `__type: "ValidationException"`
- a message that clearly says the operation is disabled in read-only mode

Read operations such as `ListTables`, `DescribeTable`, `GetItem`,
`BatchGetItem`, `Query`, and `Scan` remain available.

### Verified Clients and Harnesses

- operation matrix in `query_server/tests/dynamodb_compatibility_matrix.rs`
- SDK differential smoke test in `query_server/tests/dynamodb_sdk_compat.rs`
- real Rust AWS SDK client: `aws-sdk-dynamodb`
- baseline differential target: LocalStack DynamoDB

Not yet verified:

- AWS JavaScript SDK DynamoDB client
- AWS Python `boto3` DynamoDB client
- AWS Go SDK DynamoDB client

## Redis-Compatible RESP API

Powdrr's Redis surface is exposed on `REDIS_FRONTEND_PORT` and is intentionally
read-oriented.

### Supported Commands

- `PING`
- `ECHO`
- `HELLO`
- `CLIENT`
- `COMMAND`
- `READONLY`
- `SELECT`
- `GET`
- `MGET`
- `EXISTS`
- `QUIT`

Powdrr also exposes Redis table metadata over HTTP:

- `GET /:table/_redis/config`
- `PUT /:table/_redis/config`

### Explicitly Unsupported Commands

Commands outside the supported list are not silently approximated. They fail
explicitly with Redis `ERR` responses such as:

- `ERR unsupported Redis command <NAME>`

Unsupported `CLIENT` subcommands and unsupported `HELLO` protocol versions are
also rejected explicitly.

### Read-Only Mode Behavior

Redis is already a read-only protocol surface in terms of supported commands,
but `readonly` mode strengthens the contract for known write commands.

In `API_MODE=readonly`, known Redis write commands return:

- RESP error prefix `READONLY`
- message `This Powdrr Redis frontend is running in read-only mode`

That currently covers commands such as:

- `SET`
- `MSET`
- `DEL`
- `INCR`
- `DECR`
- `HSET`
- `LPUSH`
- `RPUSH`
- `SADD`
- `ZADD`
- `EXPIRE`
- `FLUSHDB`
- `FLUSHALL`

### Verified Clients and Harnesses

- RESP wire smoke test in `query_server/tests/redis_wire_compat.rs`
- real Rust Redis client crate: `redis`

Not yet verified:

- `redis-py`
- `node-redis`
- `go-redis`

## Config and Support Endpoints

These endpoints are part of the compatibility contract because they expose the
compatibility metadata that the wire layers depend on:

- `GET /:table/_serve/config`
- `PUT /:table/_serve/config`
- `GET /:table/_dynamodb/config`
- `PUT /:table/_dynamodb/config`
- `GET /:table/_redis/config`
- `PUT /:table/_redis/config`

In `API_MODE=readonly`, the `PUT` forms must hard error explicitly. They are
not allowed to succeed as partial no-ops.

Mongo config routes are also blocked in read-only mode, but the Mongo command
surface is tracked separately in `docs/mongodb-client-api-plan.md`.
