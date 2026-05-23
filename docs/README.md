# Docs Index

This folder mixes current architecture notes, contribution guides, and longer
range design plans. Start here instead of guessing which document is current.

## Current State

- [architecture.md](./architecture.md)
  Current request flows, write flows, metadata flows, and test ownership.
- [repo-map.md](./repo-map.md)
  Directory-to-package map for the current workspace and support folders.
- [why-powdrr.md](./why-powdrr.md)
  Product motivation and problem framing.
- [speedboat-vs-iceberg-architecture.md](./speedboat-vs-iceberg-architecture.md)
  Current storage-role model and the intended relationship between mutable
  deltas and canonical Iceberg state.
- [dynamodb-compatibility-matrix.md](./dynamodb-compatibility-matrix.md)
  Current DynamoDB compatibility scope and local harness.
- [es-compatibility-matrix.md](./es-compatibility-matrix.md)
  Current Elasticsearch compatibility scope and local harness.

## Contribution Playbooks

- [playbooks/protocol-change.md](./playbooks/protocol-change.md)
  Adding or changing a protocol surface in `query_server`.
- [playbooks/serving-engine-change.md](./playbooks/serving-engine-change.md)
  Changing the shared serving path in `query_runtime`, `query_lib`, or
  `query_core`.
- [playbooks/metadata-state-provider-change.md](./playbooks/metadata-state-provider-change.md)
  Changing checkpoint, frontier, or state-provider behavior.
- [playbooks/compatibility-test-change.md](./playbooks/compatibility-test-change.md)
  Updating compatibility fixtures, matrix tests, or protocol harnesses.
- [playbooks/benchmark-change.md](./playbooks/benchmark-change.md)
  Changing the serving benchmark or adding new benchmark cases.

## Active Design and Roadmap Docs

- [lakehouse-serving-roadmap.md](./lakehouse-serving-roadmap.md)
- [iceberg-es-roadmap.md](./iceberg-es-roadmap.md)
- [object-store-readonly-state-provider-design.md](./object-store-readonly-state-provider-design.md)
- [raft-metadata-coherence-design.md](./raft-metadata-coherence-design.md)
- [redis-dependency-removal-plan.md](./redis-dependency-removal-plan.md)
- [mongodb-client-api-plan.md](./mongodb-client-api-plan.md)
- [cross-protocol-serving-optimization-plan.md](./cross-protocol-serving-optimization-plan.md)
- [guaranteed-feature-computation-platform.md](./guaranteed-feature-computation-platform.md)
- [feast-metadata-service-extension-proposal.md](./feast-metadata-service-extension-proposal.md)
- [feature-metadata-control-plane-contract.md](./feature-metadata-control-plane-contract.md)

These are important, but many of them describe target architecture or staged
plans rather than the exact structure of the current `main` branch. Check
`architecture.md` first if you need to understand the code as it exists today.

## Exploratory or Workload-Specific Notes

- [es-log-workload-plan.md](./es-log-workload-plan.md)
- [exact-lookup-performance.md](./exact-lookup-performance.md)
- [slatedb-es-search-plan.md](./slatedb-es-search-plan.md)
- [slatedb-performance-review.md](./slatedb-performance-review.md)
- [strict-cutover-consensus-design.md](./strict-cutover-consensus-design.md)
- [zero-copy-lakehouse-serving-requirements.md](./zero-copy-lakehouse-serving-requirements.md)

Use these when you are working on those specific areas. They are not intended
to be the default starting point for everyday contributions.
