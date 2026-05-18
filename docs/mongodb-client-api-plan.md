# MongoDB Client API Plan

This document describes what "MongoDB client compatible API" means for this
repo, what already exists, what is still missing, and what the next practical
implementation slices should be.

## Goal

Support a read-only MongoDB-compatible query surface on top of Powdrr's lake
serving path without forking the optimizer or duplicating the read engine.

The important distinction is:

- **Mongo-shaped serving API** means Powdrr can accept a subset of Mongo query
  semantics and compile them into the existing serving planner.
- **MongoDB client compatibility** means off-the-shelf MongoDB drivers can talk
  to Powdrr over the Mongo wire protocol and receive valid Mongo command and
  cursor responses.

Those are related, but they are not the same amount of work.

## What Already Exists

The repo already has the right logical seams for a read-only Mongo frontend:

- protocol-neutral request planning in
  [main_lib/src/serving_plan.rs](../main_lib/src/serving_plan.rs)
- a shared serving executor in
  [main_lib/src/lakehouse_serving.rs](../main_lib/src/lakehouse_serving.rs)
- an outbound Mongo renderer used by the benchmark in
  [main_lib/src/serving_protocol.rs](../main_lib/src/serving_protocol.rs)
- a concrete example of a protocol frontend translating into
  `ServingRequestPlan` in
  [main_lib/src/dynamodb_protocol.rs](../main_lib/src/dynamodb_protocol.rs)

This branch also adds the inverse Mongo read-path adapter:

- `from_mongodb_find(...)` in
  [main_lib/src/serving_protocol.rs](../main_lib/src/serving_protocol.rs)
- a temporary HTTP bridge at
  [main_lib/src/mongodb_protocol.rs](../main_lib/src/mongodb_protocol.rs)
  exposed as:
  - `POST /:table/_mongo/find`
  - `POST /_mongo/:database/_command`
- table metadata for Mongo exposure and `_id` mapping via
  `MongoDbTableConfig` in
  [main_lib/src/data_contract.rs](../main_lib/src/data_contract.rs)
- HTTP config endpoints at `GET` / `PUT /:table/_mongo/config`
- an experimental Mongo wire listener at
  [main_lib/src/mongodb_wire_protocol.rs](../main_lib/src/mongodb_wire_protocol.rs)
  with runtime wiring in
  [engine/src/main.rs](../engine/src/main.rs) behind `MONGO_PORT`

That means we now have:

- ES-style outbound rendering
- Mongo-style outbound rendering
- Dynamo-style inbound translation
- Mongo-style inbound translation for a small `find` subset
- an HTTP seam that can execute that subset against the shared serving engine
- explicit database / collection exposure config
- explicit `_id` backing-field config for Mongo-facing documents
- a database-scoped command dispatcher for `hello`, `ping`,
  `buildInfo`, `listCollections`, `listDatabases`, `listIndexes`,
  `collStats`, `dbStats`, `find`, `getMore`, and `killCursors`
- collection-to-table lookup through Mongo config instead of internal table
  names on the command path
- an in-memory read-only cursor registry with inactivity timeout and cleanup
  for `batchSize`-driven pagination on the HTTP debug surface
- metadata-backed stats on the HTTP debug surface when the latest checkpoint
  exposes file counts and sizes
- a minimal `OP_MSG` transport that can pass BSON command documents into the
  existing Mongo command executor

## What The New Translator Supports

The new inbound Mongo adapter is intentionally small and read-only.

Supported:

- `find`
- equality predicates
- `$eq`
- `$in`
- `$gt`, `$gte`, `$lt`, `$lte`
- top-level `$and`
- inclusion projection
- `_id: 0`
- single-field or multi-field sort expressed as `1` / `-1`
- positive `limit`
- `batchSize`
- `singleBatch`
- `noCursorTimeout`

Rejected on purpose:

- `skip`
- exclusion projection other than `_id: 0`
- `$or`, `$nor`, `$regex`, `$text`, `$elemMatch`, and other richer operators
- session behavior
- writes
- aggregation pipeline

That is enough to make concrete progress on the logical frontend without
pretending the server is already Mongo-driver compatible.

## What Is Still Missing For True MongoDB Driver Compatibility

### 1. A Wire-Protocol Server

The current main server is Gotham HTTP. MongoDB drivers do not speak HTTP.

True compatibility requires:

- a raw TCP listener
- Mongo message framing
- `OP_MSG` request/response handling
- likely `OP_COMPRESSED` handling for common driver configurations

Status:

- **partially implemented**
- `OP_MSG` request/response framing now exists in
  [main_lib/src/mongodb_wire_protocol.rs](../main_lib/src/mongodb_wire_protocol.rs)
- the listener can be started by setting `MONGO_PORT`
- current transport is intentionally narrow:
  - `OP_MSG` only
  - no `OP_COMPRESSED`
  - no auth / SASL
  - BSON framing at the transport edge, but command execution still bridges
    through the existing JSON-based command path

This should keep living in a dedicated server module or binary rather than
being forced into the current HTTP router.

### 2. Handshake and Topology Commands

Drivers will not begin with `find`. They first expect command responses such as:

- `hello`
- often `ping`
- likely session-related envelope handling

Status:

- available on both the HTTP debug surface and the new wire listener for the
  current read-only command subset
- still incomplete for broader driver expectations and future auth/session work

The handshake response must advertise coherent values like:

- `maxBsonObjectSize`
- `maxMessageSizeBytes`
- `maxWireVersion`
- `minWireVersion`
- `logicalSessionTimeoutMinutes`

Even a read-only single-node implementation needs a stable handshake contract.

### 3. BSON-Native Request and Response Handling

The current serving path mostly works in JSON values. That is insufficient for
full Mongo compatibility.

We need BSON-native handling for:

- field-order-preserving command documents
- typed values like `ObjectId`, `Date`, and binary payloads
- command response envelopes
- cursor batch documents

This matters especially because BSON document field order is meaningful in some
places, while JSON-object handling in the current repo is not a good transport
substitute.

Status:

- wire transport now accepts and returns BSON documents
- command execution still converts BSON documents into `serde_json::Value`
  before planning and response shaping
- typed BSON fidelity is therefore still incomplete

### 4. Cursor Lifecycle

Real drivers expect cursor semantics, not just one-shot arrays.

Minimum read-path cursor support means:

- `find`
- `getMore`
- `killCursors`
- `batchSize`
- `singleBatch`
- stable cursor IDs and server-side cursor state

The current `ServingRequestPlan` has `limit`, but no cursor or offset model.

Status:

- read-only cursor paging already exists for `find`, `getMore`, and
  `killCursors`
- cursor state is still process-local and in-memory
- `skip` and richer cursor semantics are still unsupported

### 5. Collection / Database Mapping

Mongo commands are scoped by database and collection.

Powdrr currently reasons more directly in terms of tables and serving configs.
We need an explicit mapping model, likely:

- Mongo database -> Powdrr namespace or org/table grouping
- Mongo collection -> Powdrr table

That mapping also needs:

- metadata discovery
- `listDatabases`
- `listCollections`
- `dbStats` / `collStats` decisions, even if partially stubbed at first

Status:

- explicit `database.collection -> Powdrr table` config now exists
- `listCollections` and `listDatabases` now exist on the HTTP debug surface
- `listIndexes`, `collStats`, and `dbStats` now exist on the HTTP debug
  surface
- uniqueness is now enforced for enabled `database.collection` bindings
- metadata-backed stats are best-effort and may return `null` counts when the
  latest checkpoint does not carry file-level record counts

### 6. `_id` Semantics

Mongo clients assume `_id` means something real.

Powdrr currently has:

- serving rows
- table-configured keys for Dynamo
- internal `_id_seq_no` style metadata in some paths

We need a consistent `_id` story:

- explicit configured field mapped to Mongo `_id`, or
- synthetic `_id` generation with clear stability guarantees

Without that, a Mongo-compatible read API will feel incorrect quickly.

### 7. Capability and Rejection Model

The serving engine should reject unsupported Mongo shapes early and clearly.

We should model a Mongo frontend capability profile similar to the way Dynamo
already narrows its accepted request surface.

Examples to reject initially:

- aggregation pipeline
- regex search
- text search
- collation
- skip-based pagination
- writes and transactions

## Recommended Implementation Order

### Phase 1: Read-Only Logical Frontend

Target:

- support a strict subset of `find`
- compile it into `ServingRequestPlan`
- keep reusing `execute_serving_query(...)`

Status:

- now partially done in
  [main_lib/src/serving_protocol.rs](../main_lib/src/serving_protocol.rs)

Next work inside this phase:

- extend the translator with a few more safe operators only when the serving IR
  can preserve semantics
- decide whether count-like read commands should compile into the same serving
  path or stay out of scope until the wire server exists

### Phase 2: HTTP Debug Surface

This is optional, but useful for iteration.

Goal:

- expose a Mongo-shaped request body over HTTP for tests and benchmarks
- validate translation and response shaping before building the wire server

Status:

- now implemented as `POST /:table/_mongo/find`
- now also implemented as `POST /_mongo/:database/_command`
- a table must opt in through `PUT /:table/_mongo/config`
- the command `find` field must match the configured Mongo collection name
- the database command path resolves `find` by configured
  `database.collection`, not by internal table name
- only fast-path serving queries are accepted; slow-path or rejected plans are
  returned as Mongo-shaped command errors
- responses return `{ cursor: { id, ns, firstBatch }, ok: 1.0 }`
- namespaces now come from configured `database.collection`, not the internal
  Powdrr table name
- `_id` is now backed by an explicit configured source field
- `hello`, `ping`, `listCollections`, and `listDatabases` now return
  Mongo-shaped command responses over HTTP
- `buildInfo` now returns a stable bridge identity response over HTTP
- `listIndexes` now returns the Mongo-facing `_id_` index for configured
  collections
- `listCollections` now supports `nameOnly: true` and simple `filter.name`
- `collStats` and `dbStats` now return metadata-derived storage and document
  stats when the latest checkpoint includes file statistics
- `find` now supports `batchSize`, `singleBatch`, and `noCursorTimeout`
- `getMore` and `killCursors` now exist on the HTTP debug surface with
  process-local in-memory cursor state
- inactive cursors now expire automatically and return `CursorNotFound`

Example request:

```json
{
  "find": "serve_flights",
  "filter": { "title": { "$gte": "A" } },
  "projection": { "title": 1, "_id": 0 },
  "sort": { "title": 1 },
  "limit": 5
}
```

Example response shape:

```json
{
  "cursor": {
    "id": 0,
    "ns": "analytics.serve_flights",
    "firstBatch": [
      { "title": "..." }
    ]
  },
  "ok": 1.0
}
```

Example error shape:

```json
{
  "ok": 0.0,
  "errmsg": "Path table serve_flights_internal is exposed as Mongo collection serve_flights but request targeted other_collection",
  "code": 2,
  "codeName": "BadValue"
}
```

Current bridge constraints:

- only tables with explicit Mongo config are visible
- only enabled configs are visible
- `_id` must be backed by a configured source field in the table schema
- only fast-path serving queries are currently executed

Important:

- this is **not** driver compatibility
- it is only a development seam
- the new `/_mongo/:database/_command` route is the closer approximation to
  the eventual wire-protocol command model
- cursor state is currently process-local and will not survive a restart or
  failover, even though it now has inactivity timeout / cleanup behavior

### Phase 3: Wire-Protocol Gateway

Build a dedicated Mongo-facing server crate or module that:

- accepts TCP connections
- parses `OP_MSG`
- handles `hello`
- dispatches `find` into the Phase 1 translator
- returns BSON command and cursor responses

This should initially be read-only and single-node.

Status:

- now partially implemented in
  [main_lib/src/mongodb_wire_protocol.rs](../main_lib/src/mongodb_wire_protocol.rs)
- the remaining gaps are compression, richer command coverage, and a
  BSON-native execution path instead of the current BSON -> JSON bridge

### Phase 4: Durable Cursor and Session Support

Add:

- durable cursor registry
- session envelope handling
- batch splitting beyond the current in-memory debug implementation

### Phase 5: Expanded Surface

Only after read-only `find` is solid:

- projection edge cases
- more filter operators
- `count`-like commands
- `aggregate` for bounded patterns
- writes, if we actually want them

## Recommended Near-Term Scope

Do **not** start with a full Mongo clone.

The right first product slice is:

- read-only
- one-node
- `hello` + `find` + `getMore` + `killCursors`
- configured `_id` mapping
- no writes
- no aggregation pipeline
- no transactions
- explicit rejection for unsupported operators

That gives us a usable story for analytics-style or app-read traffic while
keeping the optimizer and storage model aligned with the rest of the repo.

The current wire listener is aimed at exactly that scope, with one extra
constraint: use direct-connection no-auth clients only until the handshake and
session story is more complete.

## Concrete Next Code Changes

1. Run a stock Mongo driver smoke test against the new wire listener and patch
   whatever handshake/session envelope gaps it exposes first.
2. Harden the cursor layer so it is no longer process-local:
   move the registry into storage that can survive restarts or leader changes.
3. Add count-like read commands only if they can map cleanly into the same
   serving plan without creating a second execution path.
4. Fill in only the remaining discovery commands that clearly unblock real
   clients, such as `count` or `countDocuments` equivalents and lightweight
   session/metadata envelopes.
5. Replace the current BSON -> JSON bridge with a more BSON-native execution
   path once the initial driver smoke test is green.
