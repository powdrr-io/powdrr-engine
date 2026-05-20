# DynamoDB Compatibility Matrix

## Purpose

This file is the tracked compatibility contract for the current DynamoDB wire
surface.

It has two jobs:

1. enumerate the full DynamoDB SDK operation surface we are claiming against
2. point to the test artifacts that prove each operation either matches the
   supported DynamoDB subset or fails explicitly

The backing artifacts are:

- operation coverage corpus:
  `main_lib/tests/data/dynamodb_compat_cases.json`
- compatibility harness:
  `main_lib/tests/dynamodb_compatibility_matrix.rs`
- complementary read-path SDK smoke test:
  `main_lib/tests/dynamodb_sdk_compat.rs`

## Contract

- `differential_supported` means the operation is implemented and compared
  against LocalStack through a real DynamoDB client.
- `explicit_error` means the operation is not supported, and the Powdrr wire
  endpoint must reject it explicitly with a `ValidationException` instead of
  silently ignoring it or partially approximating it.

## Supported Now

| Operation | Coverage | Notes |
|---|---|---|
| `ListTables` | Differential | Must only surface tables explicitly exposed through DynamoDB config |
| `DescribeTable` | Differential | Includes primary key schema, billing mode, and declared GSIs and LSIs |
| `GetItem` | Differential | Projection behavior and `ReturnConsumedCapacity` response shape compared against LocalStack |
| `BatchGetItem` | Differential | Response shape and item projection compared against LocalStack |
| `PutItem` | Differential | Covers key replacement, `ConditionExpression`, and `ReturnValues=ALL_OLD` / `NONE` |
| `DeleteItem` | Differential | Covers key deletes, conditional failures, and `ReturnValues=ALL_OLD` / `NONE` |
| `Query` | Differential | Covers primary-key range queries, `begins_with`, richer filter behavior, pagination, GSI queries, LSI queries, and `ReturnConsumedCapacity` response shape |
| `Scan` | Differential | Covers ordered pagination, filtered counts, continuation keys, and `ReturnConsumedCapacity` response shape |

## Auth Contract

- The DynamoDB wire endpoint now requires SigV4-style `Authorization` headers.
- Requests without SigV4 must fail explicitly with
  `UnrecognizedClientException`.
- The matrix harness checks required-auth rejection directly on the raw wire
  path, while the differential suites use real SDK clients with static test
  credentials.

## Explicit Error Surface

Every remaining DynamoDB operation in the current SDK surface is intentionally
tracked as `explicit_error` in `dynamodb_compat_cases.json`. The harness
verifies that Powdrr rejects each of them through the wire endpoint with an
explicit unsupported-operation error.

That includes:

- unsupported write operations such as `UpdateItem` and `BatchWriteItem`
- transactional operations such as `TransactGetItems` and
  `TransactWriteItems`
- control-plane APIs such as `CreateTable`, `DeleteTable`, `UpdateTable`, and
  backup/import/export flows
- policy, tagging, global table, TTL, autoscaling, and Kinesis streaming APIs

## Negative Contract

Supported operations must also fail explicitly for unsupported request members
or unsupported expression forms. The matrix harness currently checks:

- unknown top-level request fields on supported operations
- required SigV4 auth for raw wire requests
- unsupported `Select` / `ProjectionExpression` combinations
- unsupported `ReturnConsumedCapacity` values
- unsupported `KeyConditionExpression` shapes

Supported read operations are also expected to accept and exercise the current
read-contract subset:

- `ReturnConsumedCapacity` on `GetItem`, `BatchGetItem`, `Query`, and `Scan`
- `ConsistentRead` on table and LSI reads, with explicit rejection on GSIs
- `FilterExpression` support for `AND`, `OR`, `NOT`, `contains`,
  `attribute_exists`, `attribute_not_exists`, `attribute_type`, `size`,
  `IN`, `BETWEEN`, and `begins_with`

The first supported write milestone currently adds:

- `PutItem` with full-item replacement semantics on the primary key
- `DeleteItem` with idempotent key deletes
- `ConditionExpression` on `PutItem` and `DeleteItem` using the current
  expression subset
- `ReturnValues` support for `NONE` and `ALL_OLD` on `PutItem` and
  `DeleteItem`
- `PutItem` only for attributes already present in the published table
  schema; introducing new top-level attributes is an explicit
  `ValidationException`

## Local Runner

To run the full local DynamoDB compatibility stack:

```bash
bash scripts/run_dynamodb_compat_local.sh
```

That script starts the existing local dependencies, runs the compatibility
matrix harness, and is the same path used by CI.

To run the older narrower SDK smoke test separately:

```bash
bash scripts/run_dynamodb_sdk_compat_local.sh
```
