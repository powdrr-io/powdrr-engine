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
| `DescribeTable` | Differential | Includes primary key schema, billing mode, and declared GSIs |
| `GetItem` | Differential | Projection behavior compared against LocalStack |
| `BatchGetItem` | Differential | Response shape and item projection compared against LocalStack |
| `Query` | Differential | Covers primary-key range queries, `begins_with`, filter subset, pagination, and GSI queries |

## Explicit Error Surface

Every remaining DynamoDB operation in the current SDK surface is intentionally
tracked as `explicit_error` in `dynamodb_compat_cases.json`. The harness
verifies that Powdrr rejects each of them through the wire endpoint with an
explicit unsupported-operation error.

That includes:

- write operations such as `PutItem`, `UpdateItem`, `DeleteItem`, and
  `BatchWriteItem`
- scan and transactional operations such as `Scan`, `TransactGetItems`, and
  `TransactWriteItems`
- control-plane APIs such as `CreateTable`, `DeleteTable`, `UpdateTable`, and
  backup/import/export flows
- policy, tagging, global table, TTL, autoscaling, and Kinesis streaming APIs

## Negative Contract

Supported operations must also fail explicitly for unsupported request members
or unsupported expression forms. The matrix harness currently checks:

- unknown top-level request fields on supported operations
- unsupported `FilterExpression` clauses
- unsupported `KeyConditionExpression` shapes

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
