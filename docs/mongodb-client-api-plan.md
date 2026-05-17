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
  exposed as `POST /:table/_mongo/find`

That means we now have:

- ES-style outbound rendering
- Mongo-style outbound rendering
- Dynamo-style inbound translation
- Mongo-style inbound translation for a small `find` subset
- an HTTP seam that can execute that subset against the shared serving engine

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

Rejected on purpose:

- `skip`
- exclusion projection other than `_id: 0`
- `$or`, `$nor`, `$regex`, `$text`, `$elemMatch`, and other richer operators
- cursor/session behavior
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

This should live in a dedicated server module or binary rather than being
forced into the current HTTP router.

### 2. Handshake and Topology Commands

Drivers will not begin with `find`. They first expect command responses such as:

- `hello`
- often `ping`
- likely session-related envelope handling

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

- add table-level Mongo exposure config if needed
- extend the translator with a few more safe operators only when the serving IR
  can preserve semantics

### Phase 2: HTTP Debug Surface

This is optional, but useful for iteration.

Goal:

- expose a Mongo-shaped request body over HTTP for tests and benchmarks
- validate translation and response shaping before building the wire server

Status:

- now implemented as `POST /:table/_mongo/find`
- command body still includes `find`, and the path name must match it
- only fast-path serving queries are accepted; slow-path or rejected plans are
  returned as Mongo-shaped command errors
- responses return `{ cursor: { id, ns, firstBatch }, ok: 1.0 }`

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
    "ns": "powdrr.serve_flights",
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
  "errmsg": "Path table serve_flights does not match Mongo find collection other_collection",
  "code": 2,
  "codeName": "BadValue"
}
```

Known limitation in the current bridge:

- it does not synthesize or remap Mongo `_id` values yet
- if the underlying serving result has no natural `_id`, the bridge currently
  returns documents without inventing one
- fixing that cleanly requires a table-level `_id` mapping contract, not just
  HTTP response reshaping

Important:

- this is **not** driver compatibility
- it is only a development seam

### Phase 3: Wire-Protocol Gateway

Build a dedicated Mongo-facing server crate or module that:

- accepts TCP connections
- parses `OP_MSG`
- handles `hello`
- dispatches `find` into the Phase 1 translator
- returns BSON command and cursor responses

This should initially be read-only and single-node.

### Phase 4: Cursor and Session Support

Add:

- cursor registry
- `getMore`
- `killCursors`
- batch splitting
- minimal session envelope handling

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

## Concrete Next Code Changes

1. Keep expanding the read-only Mongo frontend around
   [main_lib/src/serving_protocol.rs](../main_lib/src/serving_protocol.rs).
2. If we want an integration seam before the TCP gateway, add a temporary
   Mongo-shaped HTTP route on top of
   [main_lib/src/lakehouse_serving.rs](../main_lib/src/lakehouse_serving.rs).
3. Add explicit table metadata for Mongo exposure and `_id` mapping, likely in
   [main_lib/src/data_contract.rs](../main_lib/src/data_contract.rs).
4. Create a dedicated Mongo wire server module or crate rather than extending
   the current HTTP router directly.
