# Why Powdrr

Powdrr exists for a very specific problem:

you already have valuable data in your warehouse or lake, but your production
systems need to read that data with relatively low latency.

That data is often produced by offline computation:

- ML feature generation over historical data
- recommendations or ranking inputs
- fraud or risk signals
- eligibility or policy decisions
- denormalized customer or account views
- large joins, aggregations, or enrichment jobs that are too expensive to do on
  the request path

In a lot of companies, the hard part is not producing these datasets. The hard
part is getting them into production serving safely.

## The Usual Offline-To-Online Mess

The common pattern looks like this:

1. Build the data in Spark, Flink, dbt, SQL, or some batch ML pipeline.
2. Materialize it into the lake or warehouse.
3. Copy it again into an online system such as Elasticsearch, Redis,
   Cassandra, DynamoDB, or a bespoke serving cache.
4. Build a publication process to decide when that second system is safe to
   serve.
5. Add warmup logic so the first requests after a rollout do not destroy p99.

That second half is where teams burn time.

You do not just need to move bytes. You need to answer hard correctness
questions:

- Which version is production serving right now?
- Are all derived indexes and caches aligned with the same base snapshot?
- What happens if one server has loaded the new data and another has not?
- When can old files be deleted?
- How do you avoid serving mixed old/new results during cutover?

And you need to answer hard performance questions:

- How do you avoid cold object-store reads on the first request?
- How do you avoid broad scans for point lookups or selective filters?
- How do you keep tail latency stable when a new batch lands?
- How do you expose familiar APIs without building a new execution path for each
  protocol?

This is the offline-to-online problem in practice. The data pipeline is only
half the job. The rest is coherent publication, warmup, and low-latency
serving.

## Why This Shows Up So Often In ML Systems

ML systems run into this problem constantly because the useful online state is
often generated from large historical context:

- a feature table is recomputed nightly or hourly
- an embedding table is refreshed from a heavyweight training or batch inference
  job
- a ranking candidate set is generated offline
- fraud features depend on long historical joins
- a recommendation model writes precomputed outputs for downstream services

By the time the data is ready, it is already sitting in the lake in a format
the data platform understands well. But the application team still has to
figure out how to serve it.

That usually means inventing a second storage pipeline:

- export from the lake
- transform into the online store's schema
- bulk load it
- wait for indexes to catch up
- push cache warmups
- flip traffic when it feels safe

The result is operational drag and a constant correctness risk around partial
cutovers.

## What Powdrr Changes

Powdrr is built around a simpler contract:

- keep the canonical data in Iceberg
- add only bounded serving-specific acceleration state
- make that acceleration state snapshot-aware
- publish one coherent serveable version at a time
- serve through familiar client-facing APIs

The point is not "query Parquet directly and hope for the best."

The point is:

- you point Powdrr at your Iceberg table
- Powdrr tracks which snapshot is safe to serve
- Powdrr maintains the minimal extra metadata, indexes, and caches needed for
  selective low-latency reads
- Powdrr shifts traffic only when the new snapshot is coherent and ready

That means you do not need a separate full online copy just to expose the data
to production systems.

## The Primary Product Modes

Powdrr is converging on two primary operating modes and one secondary one:

- single-node read-only
- clustered read-only
- compatibility and mutation flows

The important distinction is that the first two are the product center.

Single-node read-only means:

- Iceberg and object storage are the durable truth
- Powdrr warms local state and serves one active snapshot
- no second serving database is required

Clustered read-only means:

- the same storage model
- cluster coordination only for target/active cutover and work ownership
- no need for Redis or DynamoDB on the read path

Compatibility and mutation surfaces still matter, but they are not the main
thing Powdrr is trying to be.

## Exact Lookup Is The First Hard Contract

The first serving class Powdrr needs to be unambiguously good at is exact
lookup over coherent snapshots:

- key/value lookups
- batch key lookups
- exact document lookup
- bounded key-range reads

That is why the mmap-backed exact-lookup path matters so much. It is not just
an optimization. It is the first place where the lakehouse-serving story has to
feel operationally real.

## Coherent Snapshots Matter More Than People Expect

Serving from lake-managed data is only useful if results are coherent.

If the base table has advanced to snapshot `N+1` but the derived pruning data,
secondary indexes, or warmed caches still reflect snapshot `N`, you get a bad
system:

- some requests see old results
- some requests see new results
- some point lookups miss
- some range queries include deleted rows
- tail latency spikes while nodes scramble to load different versions

Powdrr is designed around avoiding that class of failure.

The serving boundary is not "whatever files a node happened to notice first."
The serving boundary is a published, coherent snapshot frontier. Derived state
is tied to that frontier, and new data should not become active until the
system is ready to serve it as one version.

That is the part many offline-to-online stacks leave to ad hoc scripts,
playbooks, and luck.

## Why P99 Usually Gets Ugly

Even when correctness is acceptable, the first production rollout of a new
batch often wrecks latency:

- caches are cold
- object-store reads are cold
- metadata has not been loaded yet
- indexes are partially ready
- a previously selective query suddenly scans more files than expected

So teams add more machinery:

- prefetch jobs
- warmup endpoints
- blue/green data copies
- staged index builds
- rollout pauses

Powdrr is meant to make this a built-in concern instead of a custom side
project. The system should know that a new snapshot exists, prepare the
necessary serving state, and avoid promoting it to active service until the
warm path is ready.

## The Real Value Proposition

Powdrr is valuable when you want all of these at once:

- the warehouse or lake remains the canonical store
- production systems can read that data through low-latency APIs
- new offline output can become available without a bespoke reload pipeline
- serving stays snapshot-coherent
- cutovers do not blow up p99 latency

In short:

Powdrr is for teams that want to serve warehouse-generated data in production
without building and babysitting a second data-loading architecture.

## A Concrete Example

Imagine a team building a recommendation or fraud system.

Every hour, an offline pipeline writes a fresh Iceberg table containing:

- entity ID
- a few hundred derived features
- model score
- eligibility flags
- updated timestamps

Without Powdrr, that team often has to:

- export the table
- reshape it for a key-value or document store
- bulk load it into the online system
- wait for the load and indexes to finish
- warm caches
- flip traffic carefully
- debug mismatches between the warehouse version and the online version

With Powdrr, the goal is much simpler:

- write the new data to Iceberg
- let Powdrr discover the new snapshot
- let Powdrr prepare the serving state for that snapshot
- let Powdrr activate it only when the snapshot is coherent and warm
- let clients keep using the same serving API or compatibility surface

That is the product promise.

## What "Zero-Copy" Means Here

Powdrr does not claim that no auxiliary state exists.

It claims something more useful:

- there is one canonical copy of the table
- Powdrr does not require a second full serving database copy
- the extra state is bounded and in service of serving
- that extra state is versioned against the underlying table snapshot

So "zero-copy" here means "no second full system of record," not "no serving
indexes, no caches, and no metadata."

## Why Iceberg

Iceberg is a strong fit for this problem because its snapshot and metadata
model gives Powdrr a clean publication boundary:

- immutable snapshots
- explicit file membership
- manifest metadata
- partition evolution
- file-level and row-group-level pruning opportunities

That makes it much easier to answer the most important serving question:

what exact version of the table is safe to expose right now?

Powdrr builds on that instead of forcing teams to reconstruct versioning and
publication semantics outside the lake.

## Bottom Line

If your production-serving data is already produced in the warehouse or lake,
the usual next step is painful: copy it again, index it again, warm it again,
and somehow cut it over coherently.

Powdrr is meant to remove that burden.

Point it at the Iceberg table. Let the lake stay canonical. Let Powdrr handle
the serveable snapshot, the bounded derived state, and the warm coherent
cutover into low-latency production reads.
