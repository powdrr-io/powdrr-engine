# Iceberg Support Surface Config

Powdrr now has a table-level support config for declaring how an Iceberg table
should be exposed through the shared compatibility surfaces.

The goal is to stop treating each frontend as an isolated one-off mapping.
Instead, a table can declare:

- whether it should be exposed through compatibility surfaces at all
- which field acts as the primary key
- whether there is a range key
- how that key schema should drive DynamoDB, Redis, and exact/range
  Elasticsearch-style serving

## Route

The config lives behind:

- `GET /:table/_support/config`
- `PUT /:table/_support/config`

`PUT` stores the declared support contract and derives the existing
surface-specific table metadata from it.

## Request Shape

```json
{
  "key_schema": {
    "primary_key": "customer_id",
    "range_key": "event_ts"
  },
  "elasticsearch": {},
  "dynamodb": {
    "local_secondary_indexes": [
      {
        "name": "by_status",
        "sort_key": "status"
      }
    ],
    "global_secondary_indexes": [
      {
        "name": "by_region",
        "partition_key": "region",
        "sort_key": "event_ts"
      }
    ]
  },
  "redis": null
}
```

For a Redis-oriented table:

```json
{
  "key_schema": {
    "primary_key": "session_id"
  },
  "redis": {
    "database": 3,
    "value_field": "payload_json"
  }
}
```

## What Powdrr Derives

### Shared Serving Patterns

The support config derives serving patterns from the declared key schema so the
compatibility surfaces all land on the same exact/range-serving path.

With only a primary key:

- `get_item`

With a primary key and range key:

- `get_item`
- `exact_query`
- `partition_query_asc`
- `partition_query_desc`
- `range_query_asc`
- `range_query_desc`

These patterns are written into the table's serving config with internal
support-specific names so Powdrr can distinguish them from manually-authored
patterns.

### DynamoDB Config

If `dynamodb` is present, Powdrr derives:

- `partition_key` from `key_schema.primary_key`
- `sort_key` from `key_schema.range_key`
- local/global secondary indexes from the provided `dynamodb` block

The support config does not duplicate the key names inside the DynamoDB block.
There is one shared key schema for the table, and DynamoDB inherits it.

### Redis Config

If `redis` is present, Powdrr derives:

- `enabled = true`
- `database` from `redis.database`
- `key_field` from `key_schema.primary_key`
- `value_field` from `redis.value_field`

Redis support is intentionally narrower than DynamoDB support today:

- no range key
- one configured table per Redis database number

### Elasticsearch-Style Support

The `elasticsearch` block currently means:

- derive exact/range serving patterns suitable for key-oriented Elasticsearch
  compatibility paths

It does **not** currently mean:

- automatic text indexing sidecars
- automatic `_search` full-text mapping
- alias or template provisioning

This slice is about exact/range key exposure, not the full search contract.

## Validation Rules

`PUT /:table/_support/config` fails fast when the contract does not match the
table shape.

Current enforced rules:

- `primary_key` must be present and non-empty
- `range_key`, when set, must be non-empty and different from `primary_key`
- at least one support surface must be enabled:
  `elasticsearch`, `dynamodb`, or `redis`
- `primary_key`, `range_key`, and `redis.value_field` must exist in the table
  schema
- Redis support rejects `range_key`
- Redis database numbers must be unique across configured tables
- DynamoDB index names must be unique across local and global secondary indexes
- DynamoDB key fields must use scalar types Powdrr maps cleanly to DynamoDB
  keys today: `String`, `Integer`, or `Float`

## Schema Source

Powdrr validates the support config against the table schema from the active
checkpoint. That means:

- the table must already exist
- the table must already have a usable active checkpoint
- that checkpoint must carry either Iceberg schema metadata, speedboat file
  schemas, or the merged checkpoint schema

If there is no usable active checkpoint schema yet, support config writes fail
with a checked error instead of guessing.

## Current Scope And Limits

This support config is the preferred unified declaration for exposing Iceberg
tables through the compatibility surfaces, but a few limits still matter:

- legacy per-surface config routes still exist:
  `/_serve/config`, `/_dynamodb/config`, `/_redis/config`, `/_mongo/config`
- those legacy routes are preserved for now and can still be edited directly
- Mongo is not yet included in the unified support config
- Elasticsearch here means key-oriented serving patterns, not full search
  lifecycle automation

So the practical contract is:

- use `/_support/config` when you want one declared source of truth for table
  exposure and key mapping
- use the older per-surface routes only when you intentionally want to manage a
  frontend mapping by hand
