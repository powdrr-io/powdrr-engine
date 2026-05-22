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

## How Compatibility Surfaces Target Tables

Powdrr compatibility surfaces do not all identify a table the same way. The
selection rules are:

- Elasticsearch-compatible HTTP:
  `:index` path segments resolve against Powdrr table names and configured
  Elasticsearch aliases. Root routes such as `/_search` and wildcard or
  comma-separated index expressions can target more than one table.
- DynamoDB-compatible HTTP:
  the DynamoDB `TableName` request member is the Powdrr table name. Each table
  also needs its own `/:table/_dynamodb/config`.
- Mongo-shaped HTTP:
  each Powdrr table can be exposed as one configured `(database, collection)`
  pair through `/:table/_mongo/config`. Mongo commands then target that
  database and collection.
- Redis-compatible RESP:
  each Powdrr table can be exposed as one configured Redis database number
  through `/:table/_redis/config`. The Redis client chooses the table with
  `SELECT <db>`, then issues `GET`, `MGET`, or `EXISTS` inside that selected
  database.

The Redis case is intentionally one-table-per-database:

- Powdrr rejects duplicate enabled `database` assignments across tables
- `SELECT` fails if the chosen database is not configured
- `GET`/`MGET`/`EXISTS` only operate against the currently selected table
- a single Redis command does not span multiple Powdrr tables

For read traffic, the selected table also needs a published servable
checkpoint; protocol routing alone is not enough.

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

### How Table Selection Works

- `GET /:index/...` and `POST /:index/...` routes target the Powdrr table named
  by `:index`
- the same `:index` slot also accepts configured Elasticsearch aliases
- comma-separated and wildcard index expressions are accepted on the routes
  that already take `:index`
- root routes like `GET /_search` and `POST /_search` operate across all
  matching tables

Examples:

- `POST /logs/_search` targets the `logs` table
- `POST /logs_alias/_search` targets the table bound to alias `logs_alias`
- `POST /logs,events/_search` targets both `logs` and `events`
- `GET /_resolve/index/logs*` resolves matching table names and aliases

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

### How Table Selection Works

The DynamoDB-compatible HTTP surface always targets a Powdrr table through the
AWS `TableName` member in the request body.

Examples:

- `DescribeTable` with `TableName: "events"` targets the Powdrr table `events`
- `GetItem`, `Query`, and `Scan` use that same `TableName`
- `BatchGetItem` and `BatchWriteItem` can reference multiple Powdrr tables in
  their request-items map, because DynamoDB itself is multi-table at that API
  shape

Per-table DynamoDB compatibility config is managed through:

- `GET /:table/_dynamodb/config`
- `PUT /:table/_dynamodb/config`

That config attaches the DynamoDB key model to one Powdrr table; it does not
create a second independent storage copy.

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

### How Table Selection Works

Redis does not carry a table name on each `GET`. Powdrr therefore maps one
Powdrr table to one Redis database number:

- configure table binding with `PUT /:table/_redis/config`
- client issues `SELECT <db>`
- Powdrr resolves that database number to the configured table
- `GET`, `MGET`, and `EXISTS` run against that table's published checkpoint

The binding is defined by:

```json
{
  "enabled": true,
  "database": 0,
  "key_field": "user_id",
  "value_field": "payload"
}
```

Important limits:

- only one enabled Powdrr table may claim a given Redis database number
- `SELECT` to an unconfigured database returns an explicit error
- Redis commands do not target multiple Powdrr tables at once
- this is currently an exact-lookup surface, not a general Redis data model

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
