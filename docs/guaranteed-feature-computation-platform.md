# Guaranteed Feature Computation Platform

This document proposes a full system for online and offline feature
computation built around:

- Feast-like feature registry and control-plane concepts
- Arroyo-like bounded and unbounded computation
- Powdrr-backed canonical storage, publication, and serving

The target product is not "three separate systems glued together." The target
is a single platform that you hand raw events to and that can:

1. land those events durably in object storage and Iceberg
2. compute historical features for experimentation and training
3. publish created features for online serving
4. process new events continuously into production feature state

The most important goal is not connector breadth. It is a credible guarantee
that online and offline feature values come from the same definitions, the same
raw events, and the same time/correction semantics.

## Product Goal

The product contract is:

- one canonical raw event log
- one feature definition system
- one canonical computation engine
- one canonical publication and serving plane
- explicit revision and provenance metadata
- continuous online/offline equivalence validation

If any one of those is weak, feature skew comes back.

## Minimal System Shape

To keep the operational shape simple, aim for:

- one control-plane service
- one data-plane service
- one object store
- one metadata plane

Optional adapters such as Kafka protocol ingest or Debezium-style CDC can feed
the system, but they should not be required as separate always-on
dependencies.

## Overall Architecture

```text
                           +-----------------------------------+
                           |         Control Plane             |
                           |  Feast-like registry + compiler   |
                           |-----------------------------------|
                           | entities                          |
                           | feature views                     |
                           | feature services                  |
                           | model -> feature revision         |
                           | schema/time/lateness policy       |
                           | validation & skew policy          |
                           +----------------+------------------+
                                            |
                                            v
                           +-----------------------------------+
                           |       Feature IR / Planner        |
                           |-----------------------------------|
                           | validate safe subset              |
                           | compile to one execution plan     |
                           | emit storage / serving contracts  |
                           +----------------+------------------+
                                            |
                                            v
+--------------------+      +-----------------------------------+      +----------------------+
| Ingest adapters    |----->|            Data Plane             |----->|    Online Serving    |
|--------------------|      |   Arroyo-like engine + Powdrr     |      |----------------------|
| HTTP / gRPC        |      |-----------------------------------|      | exact lookup         |
| Kafka protocol     |      | raw event landing                 |      | feature vectors      |
| Debezium / CDC     |      | bounded replay                    |      | Redis/hash API       |
| file drops         |      | real-time incremental compute     |      | native feature API   |
+--------------------+      | mutable frontier                  |      +----------------------+
                            | checkpoint publication            |
                            | compaction to Iceberg             |
                            +----------------+------------------+
                                             |
                                             v
                                  +---------------------------+
                                  |      Object Store         |
                                  |---------------------------|
                                  | raw event Iceberg tables  |
                                  | feature Iceberg tables    |
                                  | snapshots/checkpoints     |
                                  | replay/backfill artifacts |
                                  +---------------------------+
                                             |
                                             v
                                  +---------------------------+
                                  | Offline Retrieval / Train |
                                  |---------------------------|
                                  | point-in-time datasets    |
                                  | experiment snapshots      |
                                  | model training revisions  |
                                  +---------------------------+
```

## System Responsibilities

### Control Plane

The control plane should own:

- entities
- feature views
- feature services
- feature-definition metadata
- feature ownership and tagging
- revision metadata
- model-to-feature bindings
- validation policy

This is where Feast concepts fit best.

### Data Plane

The data plane should own:

- event ingest
- canonical event landing
- bounded historical replay
- live incremental computation
- feature publication
- online serving

This is where Arroyo-style execution and Powdrr-style publication converge.

### Object Store

The object store should hold:

- canonical raw event tables
- canonical feature tables
- snapshots and checkpoints
- replay and backfill artifacts

## Data Model

### Raw Event Tables

These are append-oriented canonical source tables.

Required columns:

- `event_id`
- `entity_key`
- `event_time`
- `ingest_time`
- `source`
- `payload`

Recommended optional columns:

- correction type
- source offset or sequence metadata
- schema revision

These tables are the replay source for both bounded and live computation.

### Feature Tables

These are the canonical outputs.

Required columns:

- entity key columns
- effective or feature timestamp
- feature values
- feature revision metadata
- publication finality metadata

These tables must be readable both offline and online.

### Revision Metadata

The system needs explicit metadata for:

- feature definition revision
- computation plan revision
- source event range or offsets
- execution checkpoint
- Powdrr checkpoint
- Iceberg snapshot
- final vs provisional status
- model training revision

Without this, the system cannot prove parity or support reliable audits.

## The Core Guarantee

The guarantee must be narrower than "all possible features work perfectly."

A feature should only be marked as guaranteed if it belongs to a constrained
class with:

- explicit entity key
- explicit event timestamp
- deterministic logic
- replayable source events
- explicit lateness policy
- explicit correction policy
- stable output schema
- revision lineage

Unsupported feature shapes should be rejected up front instead of being
silently accepted and then treated as guaranteed.

## The Online/Offline Strategy

This is the most important section.

### 1. One Feature Definition

Every guaranteed feature must be defined once.

The system must not allow:

- one Spark job for offline
- one streaming job for online
- one application-side implementation for serving

That is the main source of online/offline skew.

Instead:

- feature definitions live in the control plane
- the control plane compiles them into a canonical feature IR
- the same compiled plan drives bounded replay and live incremental execution

### 2. One Canonical Raw Log

Every feature value must be derived from the same raw event log:

- historical experiments replay the raw event log
- live computation tails the raw event log
- backfills replay the raw event log

This avoids a second skew source where offline and online use different raw
inputs even if the feature logic is nominally the same.

### 3. Explicit Time Semantics

The weakest point in most "guaranteed" systems is time behavior.

Each feature view should declare:

- event time column
- watermark policy
- allowed lateness
- correction horizon
- retention horizon
- finalization rule

These declarations must affect both live computation and bounded replay.

### 4. Separate Provisional From Final

Freshness and finality are different.

Recommended model:

- provisional revisions: low-latency online updates
- final revisions: stable outputs after the lateness/correction horizon

Rules:

- online serving may use provisional revisions
- training defaults to final revisions
- every response and dataset must carry finality metadata
- validation compares like with like

### 5. Treat Corrections As First-Class

The system must have an explicit correction model for:

- late events
- duplicate events
- CDC corrections
- replay after logic fixes

Acceptable strategies include:

- bounded lateness with finalization after the bound
- correction events that create new provisional revisions
- explicit replay and republish of affected revision ranges

What matters is that the behavior is declared, versioned, and testable.

### 6. Publish Revisions, Not Just Values

Every materialized feature result needs provenance.

The serving plane should know:

- which feature definition revision produced the data
- which source event range fed the computation
- which execution checkpoint produced it
- which Powdrr checkpoint published it
- which Iceberg snapshot finalized it

This is what lets the system say:

- model `M` trained on revision `R`
- online request `Q` was served from revision `R`

or prove when they differed.

### 7. Validate Continuously

The system is only guaranteed if it is continuously validated.

The validation stack should include:

#### Compile-Time Validation

- reject unsupported operators or UDFs
- reject missing entity keys or timestamps
- reject unbounded semantics outside the guaranteed class

#### Golden Replay Tests

- fixed input log
- fixed feature definition revision
- known expected outputs

#### Stream-vs-Batch Equivalence Tests

- run the same plan in bounded replay mode
- run the same plan in live/incremental mode
- compare outputs by entity, timestamp, feature, and revision

#### Production Shadow Validation

- sample online feature requests
- reconstruct the expected offline values from the same revision
- compare vectors
- alert on mismatches

#### Backfill Reconciliation

- periodically recompute a historical slice
- compare it with previously published final snapshots
- alert on drift or silent corruption

This validation system is part of the product, not an optional add-on.

## Why Use Feast Concepts

Feast contributes the right control-plane ideas:

- entities
- feature views
- feature services
- model-to-feature grouping
- user-facing registry concepts

That does not mean Feast must remain a separate product at runtime. It means
those concepts are a good fit for the control-plane layer.

## Why Use Arroyo Concepts

Arroyo contributes the right computation ideas:

- one engine that can run bounded and unbounded inputs
- event-time and watermark concepts
- stateful operators
- replayable execution

The likely path is to fork or incorporate the relevant functionality so the
system can enforce a stricter guaranteed-feature contract than a general
purpose streaming engine normally does.

## Why Use Powdrr

Powdrr is the right place to own:

- the mutable write frontier
- checkpoint publication
- Iceberg snapshot promotion
- revision-aware online serving
- exact feature vector lookup

The key idea is that the "online store" should not be a separate ad hoc KV
copy. It should be the currently published frontier of the same canonical
feature tables used offline.

## Product Modes

The system should support four user-visible flows:

### 1. Land Raw Events

Path:

- event arrives through native ingest or a compatibility adapter
- event is normalized into a canonical raw-event envelope
- event is durably acknowledged only after object-store and metadata durability
- raw event tables are published as replayable source state

### 2. Compute Offline Features

Path:

- user requests an experiment or training slice
- control plane resolves the feature-service and revision contract
- bounded replay executes the same canonical feature plan against historical
  raw events
- output lands in feature tables or experiment-scoped snapshots

### 3. Publish Features Online

Path:

- feature outputs are published through Powdrr checkpoints and revisions
- provisional or final revisions become serveable
- online fetches are tagged with the revision used

### 4. Process Events In Real Time

Path:

- live engine consumes new raw events
- feature state is updated incrementally
- Powdrr publishes the new provisional frontier
- serving reads the same feature definitions from the same canonical tables

## Minimal Moving Parts Strategy

To keep the system easy to run:

- do not require Kafka as core infrastructure
- do not require a second batch engine like Spark for correctness
- do not require a separate online KV store as the canonical serving source

Instead:

- support HTTP, file, CDC, and Kafka-protocol adapters at the ingest edge
- keep one canonical event landing path
- keep one canonical compute engine
- keep one canonical publication and serving plane

Optional protocol compatibility is fine. Required separate data paths are not.

## Fork Strategy

### Feast

Keep or absorb:

- feature registry concepts
- feature-service grouping
- governance and tagging concepts

Replace or extend:

- online-store adapter with a Powdrr-backed serving adapter
- offline retrieval path with revision-aware Powdrr/Iceberg integration
- uncontrolled push/materialization paths that bypass the canonical revision
  model

### Arroyo

Keep or absorb:

- bounded and unbounded execution model
- stateful operators
- watermarks and event-time model

Extend or fork:

- stronger guaranteed-feature subset enforcement
- stronger correction semantics
- more explicit finalization semantics
- better large-state and long-retention handling
- revision-aware outputs

## Major Risks

The biggest risks are:

- allowing too many feature shapes into the guaranteed class too early
- treating provisional latest values as equivalent to final training values
- allowing uncontrolled push or side-write paths
- postponing validation until after the platform is already in use
- assuming checkpoint durability is enough without explicit correction and
  replay semantics

## Milestone Plan

### Phase 0: Contract

Deliver:

- guaranteed feature class spec
- time, lateness, and finality spec
- correction and replay spec
- revision and provenance spec

Exit:

- every guaranteed feature has explicit semantics

### Phase 1: Registry and IR

Deliver:

- Feast-compatible registry or embedded equivalent
- canonical feature IR
- feature-definition validation

Exit:

- one source of truth for feature definitions

### Phase 2: Canonical Event Ingest

Deliver:

- native ingest API
- optional Kafka protocol adapter
- CDC ingestion contract
- raw event landing into object store and Iceberg

Exit:

- all feature computation replays the same raw events it ingests live

### Phase 3: Guaranteed Compute Engine

Deliver:

- bounded replay runner
- live incremental runner
- shared feature plan execution
- explicit lateness and correction support

Exit:

- one feature definition runs in both bounded and live modes

### Phase 4: Powdrr Publication and Serving

Deliver:

- canonical feature table conventions
- checkpoint and revision publication
- online feature vector API
- exact lookup serving paths

Exit:

- online serving is revision-aware and powered by canonical feature tables

### Phase 5: Offline Retrieval and Experimentation

Deliver:

- revision-pinned dataset generation
- point-in-time retrieval
- experiment snapshots
- model training bindings

Exit:

- experiments and training have stable reproducible provenance

### Phase 6: Validation Platform

Deliver:

- stream-vs-batch diff service
- online shadow reconstruction
- backfill reconciliation jobs
- skew alerting

Exit:

- the guarantee is continuously measured in production

### Phase 7: Operational Hardening

Deliver:

- autoscaling
- state retention controls
- rollback by revision
- operator debugging tools

Exit:

- the platform can be run as a primary production system

## First 90 Days

### Days 1-30

- freeze the guarantee contract
- implement feature IR
- add canonical event ingest
- land raw events into object store / Iceberg
- support a tiny guaranteed subset:
  - entity keys
  - deterministic projections
  - deterministic filters
  - simple aggregations

### Days 31-60

- implement bounded replay
- implement live incremental execution
- build equivalence tests for same-plan replay
- publish features through Powdrr checkpoints
- add native feature-vector serving

### Days 61-90

- add registry integration
- add model-to-revision tagging
- add point-in-time training retrieval
- add online/offline diff auditing
- stand up the first end-to-end guaranteed feature service

## Initial PR-Sized Work

1. Add feature revision and model-binding types to `control_plane`.
2. Add canonical raw event table conventions and ingest contracts.
3. Add native feature-vector serving and revision metadata in Powdrr.
4. Add a bounded replay interface for canonical feature plans.
5. Add an online/offline differential validation harness.

## Summary

The system can be exceptionally easy to run only if it is opinionated:

- one raw event log
- one feature definition system
- one computation engine
- one publication plane
- one serving plane
- one validation loop

That is the path to a real online/offline guaranteed computation platform,
not just a pile of individually useful data tools.
