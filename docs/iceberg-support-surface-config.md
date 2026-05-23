# Iceberg Support Surface Config

Powdrr now supports a service-owned bootstrap config for declaring how Iceberg
tables should be exposed through the shared compatibility surfaces.

The goal is to stop treating each frontend as an isolated one-off mapping and
to avoid mutating that mapping through a public HTTP config API. Instead, the
service loads one YAML file at startup, then writes the derived table metadata
into the metadata plane before serving requests.

## Startup Contract

Set:

- `SUPPORT_SURFACES_CONFIG_PATH=/path/to/support-surfaces.yaml`

on `powdrr-io-service`.

At startup, the service:

1. parses the YAML file
2. ensures the configured org exists
3. derives serving, DynamoDB, and Redis metadata from each table's support
   mapping
4. upserts that metadata into the service state

There is intentionally no public `/_support/config` route in this design.

## YAML Shape

```yaml
org:
  org_id: default
  license_type: Free
  creds:
    - access_key_id: access
      secret_access_key: secret

tables:
  - name: events
    tags:
      team: serving
    support:
      key_schema:
        primary_key: tenant
        range_key: event_id
      elasticsearch: {}
      dynamodb:
        global_secondary_indexes:
          - name: by_event
            partition_key: event_id

  - name: sessions
    support:
      key_schema:
        primary_key: session_id
      redis:
        database: 3
        value_field: payload_json
```

## What Powdrr Derives

Each table entry writes a normal `CreateTable`-style metadata record containing:

- `support`
  The original declarative support mapping.
- `serving`
  Derived exact/range serving patterns.
- `dynamodb`
  Derived DynamoDB key and secondary-index mapping.
- `redis`
  Derived Redis key/value mapping.

Existing `mongodb` config is preserved if it already exists in metadata. Any
manual serving patterns that do not belong to the support-derived or
DynamoDB-derived families are also preserved.

## Shared Serving Patterns

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

These are stored with internal support-prefixed names so Powdrr can distinguish
them from manually-authored patterns.

## DynamoDB Mapping

If `support.dynamodb` is present, Powdrr derives:

- `partition_key` from `key_schema.primary_key`
- `sort_key` from `key_schema.range_key`
- local/global secondary indexes from `support.dynamodb`

The DynamoDB block does not repeat the primary key names. There is one shared
key schema for the table, and DynamoDB inherits it.

## Redis Mapping

If `support.redis` is present, Powdrr derives:

- `enabled = true`
- `database` from `support.redis.database`
- `key_field` from `key_schema.primary_key`
- `value_field` from `support.redis.value_field`

Current Redis limits:

- no range key
- one configured table per Redis database number

## Elasticsearch-Style Mapping

The `support.elasticsearch` block currently means:

- derive exact/range serving patterns suitable for key-oriented
  Elasticsearch-compatible serving

It does **not** currently mean:

- automatic text indexing sidecars
- automatic `_search` full-text provisioning
- alias or template lifecycle management

This slice is about key-oriented exposure, not the full search contract.

## Validation Rules

The startup loader currently enforces structural config rules:

- `org.org_id` must be non-empty
- `org.creds` must contain at least one credential
- `primary_key` must be present and non-empty
- `range_key`, when set, must be non-empty and different from `primary_key`
- at least one support surface must be enabled:
  `elasticsearch`, `dynamodb`, or `redis`
- Redis support rejects `range_key`
- `support.redis.value_field` must be non-empty
- DynamoDB index names must be unique across local and global secondary indexes

## What Is Not Validated Yet

The bootstrap loader does **not** currently validate the support mapping
against the live checkpoint schema at service startup.

That means:

- it does not check whether `primary_key`, `range_key`, or
  `redis.value_field` exist in the current Iceberg schema
- it does not check whether DynamoDB key fields map to scalar types that the
  runtime will accept later

Those are still real constraints, but they are not enforced by the service
bootstrap file yet. This is the main gap between the earlier API-driven
prototype and the file-driven design.

## Current Scope And Limits

- the support mapping is owned by service bootstrap, not a public config API
- legacy per-surface config routes still exist:
  `/_serve/config`, `/_dynamodb/config`, `/_redis/config`, `/_mongo/config`
- those legacy routes can still be edited directly, but the YAML file is now
  the preferred source of truth for table exposure
- Mongo is not yet included in the unified support config
- Elasticsearch here means key-oriented serving patterns, not full search
  lifecycle automation
