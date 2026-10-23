---
title: Deephaven's design
class: test
---

<div className="comment-title">

An in-depth look at what we've done, why we've done it, and why you should care.

</div>

> [!NOTE]
> New to Deephaven? Start with [How Deephaven works: A mental model](./deephaven-mental-model.md) for an approachable introduction before reading this technical deep-dive.

We built Deephaven to be an incredible tool for working with tabular data — full stop. To us, tables are dynamic, powerful constructs, but we certainly care about static, batch ones too. In this piece, we explore some of the underlying technical and architectural decisions that, taken together, deliver Deephaven's value proposition.

This guide assumes familiarity with distributed systems, JVM architecture, and modern data platforms. For hands-on, task-focused guidance, see our [how-to guides](../intro.md#how-to-guides).

Deephaven's architecture is built on several key innovations. This page starts with two overviews and then explains each innovation:

- **[The live data stack](#the-live-data-stack)**: The engine, API, and UI layers that carry live data from its source to code, dashboards, and applications.
- **[How Deephaven compares](#how-deephaven-compares)**: A side-by-side look at Deephaven and traditional batch-plus-streaming architectures.
- **[Table update model](#table-update-model)**: Real-time and static data flow through the same [directed acyclic graph](./dag.md) (DAG) with automatic dependency tracking and incremental updates.
- **[Distributing DAGs](#distributing-dags-and-global-consistency)**: Snapshot-plus-delta replication extends the update model across processes.
- **[Unified batch and streaming](#unified-batch-and-streaming)**: Batch and real-time data coexist behind a single, consistent API — no separate systems or complex coordination required.
- **[Tables designed for sharing](#tables-designed-for-sharing-and-updating)**: [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s and [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s enable zero-copy operations and efficient memory usage across query operations. [Walkthroughs](#how-operations-work) of [`where`](../reference/table-operations/filter/where.md) and [`update`](../reference/table-operations/select/update.md) show the sharing in practice, and [listeners](#how-operations-stay-live) keep each result live.
- **[Mechanical sympathy](#mechanical-sympathy)**: Chunk-oriented processing, columnar layouts, and careful JVM optimization make the engine work with the hardware and JVM rather than against them.
- **[Formula and condition evaluation](#formula-and-condition-evaluation)**: Most user expressions compile to Java classes that read data in chunks.
- **[Language and client integration](#language-and-client-integration)**: Groovy and Java run directly on the JVM engine, Python reaches it through [jpy](https://github.com/jpy-consortium/jpy), and clients in other languages connect through [gRPC](./deephaven-core-api.md). For [custom UIs](#building-uis), [`deephaven.ui`](https://deephaven.io/core/ui/docs/) builds reactive web applications entirely in Python, and [`web-client-ui`](https://github.com/deephaven/web-client-ui) provides JavaScript/TypeScript components for custom front ends.

## The live data stack

Deephaven is a full-stack data system that unifies live and historical data in a single, composable environment. Unlike traditional architectures that force trade-offs between batch and streaming, Deephaven delivers both through a unified platform built on three integrated layers:

| Layer               | Components                                                                                                                                                  | What it provides                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| **Engine**          | [update graph](#update-graph-ug-cycles), [`ColumnSource`s and `RowSet`s](#tables-designed-for-sharing-and-updating), [chunks](#chunk-oriented-architecture) | Incremental computation, columnar storage, zero-copy operations          |
| **APIs**            | gRPC/[Barrage](./what-is-barrage.md), Python, Java, C++, C#, JS, Go, and R clients, [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md)     | Cross-language access, network-transparent DAGs, real-time subscriptions |
| **UI/Applications** | [`deephaven.ui`](https://deephaven.io/core/ui/docs/), [`web-client-ui`](https://github.com/deephaven/web-client-ui), Jupyter widgets                        | Live dashboards, reactive components, real-time visualization            |

<iframe src="../assets/conceptual/architecture/deephaven-architecture-overview.html" title="Diagram of the Deephaven architecture, from the Java query engine through language integration and network protocols to the client APIs" loading="lazy" style={{width: '100%', aspectRatio: '1280 / 1259', border: 'none'}} />

At the core of this stack are **Live Dataframes**, Deephaven's signature abstraction. A live dataframe is a table that updates continuously as its sources change, so its data flows naturally through code, dashboards, and applications. When a source table changes, updates propagate through the entire stack:

1. **Engine**: The [update graph](#update-graph-ug-cycles) detects changes and propagates them through the [DAG](./dag.md).
2. **APIs**: The [Barrage](./what-is-barrage.md) protocol streams incremental updates to connected clients.
3. **UI**: Components automatically refresh to reflect the latest data.

This architecture means the same table can simultaneously serve a Python script, a Java application, a web dashboard, and a remote client. Remote clients receive the table as a snapshot followed by incremental deltas. [Distributing DAGs and global consistency](#distributing-dags-and-global-consistency) explains how that works and what consistency it gives.

**Why this matters**: Traditional systems require separate pipelines for batch and streaming, with different APIs, different mental models, and complex coordination. Deephaven's live data stack removes that complexity. Whether you're analyzing historical [Parquet](../how-to-guides/data-import-export/parquet-import.md) files or streaming [Kafka](../how-to-guides/data-import-export/kafka-stream.md) data, you use the same code, the same operations, and the same UI, and subscribed consumers stay up to date as the data changes. [Unified batch and streaming](#unified-batch-and-streaming) explains how.

## How Deephaven compares

| Capability            | Traditional approach              | Deephaven                                                                                                                                                                |
| --------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Batch + real-time** | Separate systems for each         | Unified table API for both                                                                                                                                               |
| **Update model**      | Recompute full datasets           | Incremental (only affected rows)                                                                                                                                         |
| **Memory efficiency** | Each derived view copies its data | Shared [`RowSet`s and `ColumnSource`s](#tables-designed-for-sharing-and-updating)                                                                                        |
| **Query consistency** | Manual coordination required      | Automatic within a server via [DAG and logical clock](#how-the-dag-works); [consistent snapshots and deltas](#distributing-dags-and-global-consistency) across processes |
| **UI development**    | Separate front-end team/codebase  | Pure Python ([`deephaven.ui`](#building-uis)) or JS ([`web-client-ui`](#building-uis))                                                                                   |
| **Data movement**     | Row-at-a-time or full scans       | [Chunk-oriented](#chunk-oriented-architecture) (thousands of values at once)                                                                                             |
| **Performance**       | Tuned per system                  | Engine designed around hardware and JVM behavior ([mechanical sympathy](#mechanical-sympathy))                                                                           |

The sections below explain how Deephaven delivers each of these capabilities.

## Table update model

Deephaven's real-time engine uses a [directed acyclic graph](./dag.md) (DAG) to propagate updates efficiently through query operations. This architecture provides significant advantages over traditional batch processing.

### How the DAG works

Queries automatically form a [DAG](./dag.md) where:

- **Vertices** represent tables or data operations.
- **Edges** represent dependencies and data flow.
- **Updates** propagate incrementally — only the affected data recomputes.
- **Consistency** comes from a logical clock that coordinates update cycles.

An _update cycle_ is one pass in which the engine propagates a batch of changes through the [DAG](./dag.md). [Update graph (UG) cycles](#update-graph-ug-cycles) explains the batching.

The _logical clock_ tracks two values for each cycle:

- **Step**: a counter that increases once per cycle.
- **Phase**: updating or idle.

Tables check the clock to tell whether their inputs have finished updating in the current cycle, so every table sees the same set of changes. The [table update model](./table-update-model.md) describes the clock in more detail.

For example, consider this simple query:

```python ticking-table order=null
from deephaven import time_table, agg

# Source table updates every second
source = time_table("PT1S").update("Value = ii * 2")

# Derived table (depends on source)
filtered = source.where("Value > 10")

# Another derived table (depends on source)
aggregated = source.agg_by([agg.sum_("Value")])
```

When `source` receives a new row, the [DAG](./dag.md) ensures that `filtered` and `aggregated` update automatically and consistently. The engine processes only what changed.

**Performance impact**: Incremental updates mean a 1-row change to a million-row table recomputes only what that change affects, not the entire dataset. How far a change reaches depends on the operation:

- A [filter](../how-to-guides/use-filters.md) or [`update`](../reference/table-operations/select/update.md) touches the changed rows.
- A [join](../how-to-guides/joins-exact-relational.md) or [aggregation](../how-to-guides/combined-aggregations.md) updates the matching output rows or groups.

In a trading scenario with 1,000 updates per second to a 10-million-row table, each one-second cycle handles about 1,000 changed rows, and for most operations the work tracks those rows. A full-recompute system would reprocess all 10 million rows every time it refreshes.

### Update graph (UG) cycles

The update graph (UG) is the engine component that drives the [DAG](./dag.md). It batches updates at a configurable target interval and propagates each batch through the [DAG](./dag.md) in topological order, one batch per cycle. A cycle takes longer than the interval when its work requires it. The [Periodic Update Graph](./periodic-update-graph-configuration.md) guide describes how to set the interval.

Batching updates into cycles:

- **Improves efficiency**: Processes multiple changes together instead of one at a time.
- **Ensures consistency**: All tables see the same snapshot of changes.
- **Reduces overhead**: Minimizes per-update costs.

For comprehensive coverage of DAGs, UG cycles, garbage collection, custom listeners, and performance optimization, see our [DAG concept guide](./dag.md) and [table update model](./table-update-model.md).

## Distributing DAGs and global consistency

At Deephaven, we believe that our approach to propagating static and updating tabular data will revolutionize distributed data systems development.

As [How the DAG works](#how-the-dag-works) describes, the Deephaven query engine propagates updates via a [DAG](./dag.md), relying on a logical clock to mark phase and step changes for internal consistency. While this sort of coordination is suitable within a single process, the coordination overhead becomes prohibitive when extending such a DAG across multiple processes.

Based on this observation, we've implemented a design for multi-process data-driven applications. It replicates tables consistently by sending an initial snapshot and then incremental deltas. This design:

- Extends the [DAG](./dag.md) across process and network boundaries, so you can build distributed data-driven applications.
- Gives each process its own logical clock, independent of the others, so processes propagate updates in parallel.
- Allows bidirectional data flows. A process that publishes one table can consume other tables.

Decoupling the logical clocks intentionally trades away "global consistency" for increased throughput and scalability. In practice, we think such a global view is either illusory or better built from end-to-end sequence numbers:

- **Often illusory**: Input sources often publish asynchronously with respect to each other. True consistency is then limited to something narrower, such as "mutual consistency based on the inputs observed at a given point in time."
- **Better built from sequence numbers**: For data sources that contain correlatable sequence numbers, Deephaven offers tools for [synchronizing tables](../how-to-guides/synchronizing-tables.md) that reconstruct a consistent state within the query engine.

Remote clients use the same model. A client with an active [Barrage](./what-is-barrage.md) subscription receives an initial snapshot and then consistent, incremental deltas as the table updates. Each remote client receives updates through its own subscription stream, so different clients may see an update at slightly different times.

A client can instead take a one-time snapshot, for example with [`pydeephaven.Table.to_arrow`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.to_arrow), which uses [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md)'s `DoGet`. That client gets a static copy of the table at that moment, not a live stream.

## Unified batch and streaming

<iframe src="../assets/conceptual/architecture/unified-batch-streaming.html" title="Diagram comparing a traditional multi-system batch and streaming stack with Deephaven's unified single-system model" loading="lazy" style={{width: '100%', aspectRatio: '1280 / 1024', border: 'none'}} />

Many use cases require analyzing and commingling static and dynamic data. Capital markets trading systems are one classic example. They manage previous days' historical data, which is static, separately from intraday data, which is still growing. Models are often bootstrapped over the historical data and then extended with intraday data as it arrives.

Infrastructure monitoring systems that aggregate data hierarchically across many nodes are another example. They batch-process older data and aggregate it at coarse granularity, while keeping fresh data in raw form for minimum latency.

We don't think users should have to use different tools or APIs for these two types of sources, for two reasons:

1. **Learning curve**: Learning two APIs can take twice as long as learning one, and users must keep remembering how each API performs the same operation.
2. **Ease of data integration**: Merging or joining data accessible via different systems often requires a third tool for data federation or a custom tool, which is one more system to build and maintain.

At Deephaven, we have designed and implemented a unified table API that offers the same operations and semantics for both static and dynamic data sources. A few specialized table types restrict some operations. For example, [blink tables](./table-types.md#unsupported-operations) don't support [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md).

A Deephaven table backed by real-time data refreshes in every [update graph cycle](#update-graph-ug-cycles) in which its inputs change. Each update presents a **consistent** view of the data to downstream tables in the [update graph](#update-graph-ug-cycles). External listeners and data extraction tools, whether in-process or remote, see the same consistent view.

**Example**: The same code works for both static and live data:

```python syntax
from deephaven.parquet import read
from deephaven.stream.kafka.consumer import consume as consume_kafka
from deephaven.stream.kafka.consumer import json_spec, KeyValueSpec, TableType
from deephaven import dtypes as dht
from deephaven import agg

# Works with static Parquet data
static_trades = read("/data/historical_trades.parquet")
result1 = static_trades.where("Price > 100").agg_by([agg.avg("Price")], by=["Symbol"])

# The same query works with a live Kafka stream
live_trades = consume_kafka(
    {"bootstrap.servers": "localhost:9092"},
    "trades",
    key_spec=KeyValueSpec.IGNORE,
    value_spec=json_spec({"Symbol": dht.string, "Price": dht.double}),
    table_type=TableType.append(),
)
result2 = live_trades.where("Price > 100").agg_by([agg.avg("Price")], by=["Symbol"])

# result2 updates in real-time as new trades arrive
```

Traditional [lambda architecture](https://en.wikipedia.org/wiki/Lambda_architecture) requires separate systems for batch and real-time data — a batch layer, a speed layer, and a serving layer — with complex coordination between them. Deephaven needs none of that. As the example shows, only the data source changes.

The [gRPC API](./deephaven-core-api.md) links Deephaven query engines with one another and with client applications. Together, the update model, the unified API, and the gRPC API let Deephaven stand in for the lambda architecture. End users learn one API for data manipulation and access, and it works the same on every layer.

Deephaven Community Core gives users a comprehensive, extensible query engine that can interact with and commingle data accessed or ingested directly from disparate sources, including custom applications, [Apache Parquet](../how-to-guides/data-import-export/parquet-import.md), [Apache Kafka](../how-to-guides/data-import-export/kafka-stream.md), [Apache Iceberg](../how-to-guides/data-import-export/iceberg.md), [CSV](../how-to-guides/data-import-export/csv-import.md), JSON, [Arrow](../how-to-guides/data-import-export/arrow-flight.md), JDBC, [ODBC and ADBC](../how-to-guides/data-import-export/execute-sql-queries.md), and CDC streams.

Deephaven [Enterprise](/enterprise/docs/enterprise-overview) builds the unified batch-and-streaming model into the product. It provides tools to:

- Publish persistent, append-only, immutable data streams as tables.
- Distribute that data to many query engines hosted in remote applications.
- Manage the data lifecycle.
- Orchestrate data-driven applications.

Deephaven Enterprise post-processes, stores, and serves historical data in a form that maximizes throughput for common queries. It serves raw, real-time data with minimum latency. Its distributed data service layer integrates both.

## Tables designed for sharing and updating

Deephaven builds tables from data structures that lend themselves to efficient sharing and incremental updating. At its most basic, a table consists of two components:

1. A **[`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)** that enumerates the possibly sparse sequence of _row keys_ (non-negative 64-bit integers) in the table.
2. A **map** associating names with [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s that allow data retrieval for individual row keys, either element-by-element or in larger batches.

A [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) represents a column of data, which may update dynamically and which multiple tables can share.

A [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) selects elements of that [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html). It might cover all the data in the [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) or only a subset of it.

A [`RowRedirection`](/core/javadoc/io/deephaven/engine/table/impl/util/RowRedirection.html) is a separate structure that maps row keys from one key space to another. Deephaven applies it to a [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html), not to the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html). This lets a table present its parent's column data under a different key space. For example, a table can appear reordered without copying the underlying column data.

<iframe src="../assets/conceptual/architecture/table-structure.html" title="Diagram showing table structure with RowSets and ColumnSources" loading="lazy" style={{width: '100%', aspectRatio: '840 / 888', border: 'none'}} />

Filtering with [`where`](../reference/table-operations/filter/where.md) creates a new [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) that is a subset of the parent's [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html).

[Sorting](../reference/table-operations/sort/sort.md) usually creates a new [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) for the result. When a static source is already in the requested order, the result keeps the parent's [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html). The shape of a new sorted [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) depends on the source:

- For a static or [blink](./table-types.md#specialization-3-blink) source, it is flat. Its row keys run contiguously from 0 to the row count minus 1.
- For any other [refreshing](./table-types.md) source, it starts as one contiguous range of row keys placed in the middle of the row key space, leaving room on both sides. As updates crowd a region, the engine opens gaps between rows and places later rows in those gaps in sorted order, so it rarely has to renumber many existing rows.

When it creates a new [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html), [sorting](../reference/table-operations/sort/sort.md) applies a [`RowRedirection`](/core/javadoc/io/deephaven/engine/table/impl/util/RowRedirection.html) to the parent's [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s so the rows appear in the new order.

Allowing a [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)'s row keys to be sparse greatly reduces data movement within the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) and the [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s it addresses. This is essential for the performance of Deephaven's incremental [sort](../reference/table-operations/sort/sort.md) operation. It also matters when source tables publish changes that are more complex than simple append-only growth. Examples include multiple independently growing partitions and tabular representations of key-value store state.

A table may share its [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) with any other table in its [update graph](#update-graph-ug-cycles) that contains the same row keys. A single parent table maintains the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) for itself and every descendant that inherited it. These operations inherit their parent's [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html):

- Column projection and derivation operations, such as [`view`](../reference/table-operations/select/view.md) and [`update`](../reference/table-operations/select/update.md).
- Column selection with [`select`](../reference/table-operations/select/select.md), except on a static table whose [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) is not flat. There, [`select`](../reference/table-operations/select/select.md) usually creates a new, flat [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html).
- Some [exact and relational joins](../how-to-guides/joins-exact-relational.md) and [time-series joins](../how-to-guides/joins-timeseries-range.md), such as [`natural_join`](../reference/table-operations/join/natural-join.md), [`exact_join`](../reference/table-operations/join/exact-join.md), and [`aj`](../reference/table-operations/join/aj.md). These inherit the left input table's [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html).

A table may share any of its [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s with any other table in its [update graph](#update-graph-ug-cycles) that uses the same row key space or whose row key space can be _redirected_ onto the same row key space. A single parent table is responsible for updating the contents of a given [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) for itself and any descendants that inherited that [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html).

**Example of sharing**:

```python ticking-table order=null
from deephaven import time_table

# Original table
source = time_table("PT1S").update(["A = ii", "B = ii * 2"])

# These operations share the ColumnSource for column A
filtered = source.where("B > 10")  # Shares A's ColumnSource
renamed = source.view(["A", "C = B"])  # Shares A's ColumnSource

# Only one copy of column A exists in memory, shared by all three tables
```

By itself, this sharing capability represents an important optimization that avoids some data processing or copying work. In an _updating_ query engine, this avoidance repeats in every update cycle, so the savings last for the lifetime of a query.

**Performance impact**: Shared [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s save memory for queries with multiple filtered or joined views of the same source data. For example, five different [`where`](../reference/table-operations/filter/where.md) filters on a 10-column table share all 10 [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s, storing only five different [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s instead of duplicating 50 columns.

### Operation pattern summary

Each operation decides separately whether to share or create its [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) and each of its [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s. This table shows what each common operation does:

| Operation type                                                                                                                                                                                         | [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) | [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| **Filtering** ([`where`](../reference/table-operations/filter/where.md))                                                                                                                               | New (subset)                                                     | Shared                                                                       |
| **Column derivation** ([`update`](../reference/table-operations/select/update.md), [`view`](../reference/table-operations/select/view.md))                                                             | Shared                                                           | Mixed (shared + new)                                                         |
| **Sorting** ([`sort`](../reference/table-operations/sort/sort.md))                                                                                                                                     | Usually new (flat, or centered with room to grow)                | Shared (via redirection)                                                     |
| **Joining** ([`natural_join`](../reference/table-operations/join/natural-join.md), [`exact_join`](../reference/table-operations/join/exact-join.md), [`aj`](../reference/table-operations/join/aj.md)) | Shared (left table's)                                            | Left: shared; right: shared via redirection                                  |
| **Aggregation** ([`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md), etc.)                                                                                                         | New                                                              | New                                                                          |

This sharing model, combined with [incremental updates](./table-update-model.md) through the [DAG](./dag.md), enables Deephaven to handle complex queries on large, rapidly changing datasets efficiently.

## How operations work

Understanding how table operations execute helps explain why Deephaven is so efficient. This section traces through two common operations to show how [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) and [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) sharing work in practice.

### What happens when you call `where`

The [`where`](../reference/table-operations/filter/where.md) operation filters rows based on a condition. Here's what happens internally:

1. **Expression parsing**: The engine parses the filter condition, such as `"Price > 100"`, and converts it into a [`WhereFilter`](/core/javadoc/io/deephaven/engine/table/impl/select/WhereFilter.html) object that evaluates the condition efficiently.

2. **Consistent view**: The operation reads a consistent view of the parent table's current [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html). For [refreshing](./table-types.md) tables, this ensures the filter sees a stable view of the data.

3. **Evaluation**: The [`WhereFilter`](/core/javadoc/io/deephaven/engine/table/impl/select/WhereFilter.html) evaluates the condition against the parent's [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) data, processing rows in [chunks](#chunk-oriented-architecture) for efficiency. The filter collects only the row keys that satisfy the condition.

4. **Result table creation**: The engine creates a new table with:
   - **New** [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) containing only the matching row keys, a subset of the parent's [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)
   - **Shared** [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s, so the result points to the parent's column data with no copying

5. **Listener attachment**: If the parent table is [refreshing](./table-types.md), or if the filter depends on refreshing data, the engine attaches a [listener](#how-operations-stay-live). The listener receives each of the parent's updates, so the filtered table updates automatically. On each update cycle, it typically re-evaluates only the changed rows, though some filter conditions may trigger broader re-evaluation.

**Why this is efficient**: The result table doesn't copy any column data. It maintains a smaller [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) that selects which rows from the shared [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s are visible. This is the sharing that the [five-filter example](#tables-designed-for-sharing-and-updating) relies on.

### What happens when you call `update`

The [`update`](../reference/table-operations/select/update.md) operation adds or replaces columns with computed values. Here's the execution flow:

1. **Formula parsing**: A direct column reference, such as `"X"` or `"Y = X"`, is recognized first and skips compilation. [JavaParser](https://javaparser.org/) parses every other [formula](../how-to-guides/formulas.md), such as `"Total = Price * Quantity"`, and the engine compiles it into executable code.

2. **Column source creation**: Each computed column gets a new [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html).
   - [`update`](../reference/table-operations/select/update.md) computes the values once and stores them.
   - [`update_view`](../reference/table-operations/select/update-view.md) stores only the formula and computes each value when it is read.
   - A direct column reference (an alias like `"Y = X"`) reuses the parent's [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) when that column is already held in memory. Otherwise, [`update`](../reference/table-operations/select/update.md) copies the data into a new one.

3. **Formula evaluation**: For [`update`](../reference/table-operations/select/update.md), the engine evaluates the formula for each row in the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) and stores the results in the new [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html). It processes data in [chunks](#chunk-oriented-architecture) for efficiency.

4. **Result table creation**: The engine creates a new table with:
   - **Shared** [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html), so the result has exactly the same rows as the parent
   - **Shared** [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s for pass-through columns
   - **New** [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s for the computed columns

5. **Listener attachment**: For [refreshing](./table-types.md) tables, a [listener](#how-operations-stay-live) computes derived columns for added rows and recomputes them for modified rows whose input columns changed.

**Why this is efficient**: Only the new computed columns require storage. All other columns are shared with the parent, and the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) is inherited directly.

### How operations stay live

The listeners that attach to [refreshing](./table-types.md) tables are what make Deephaven tables "live." When a parent table updates:

1. The parent table enqueues an update notification for each child listener.
2. Each listener receives a [`TableUpdate`](/core/javadoc/io/deephaven/engine/table/TableUpdate.html) describing which rows were added, removed, modified, or shifted, along with information about which columns changed.
3. The listener processes the update and propagates its own update downstream. Most operations process only the changed rows. Some [filters](../how-to-guides/use-filters.md) may need to re-examine additional rows.

This is how the [DAG propagation](#how-the-dag-works) described earlier happens at the level of individual operations. A single source change cascades through [filters](../how-to-guides/use-filters.md), [joins](../how-to-guides/joins-exact-relational.md), and [aggregations](../how-to-guides/combined-aggregations.md). The engine processes this DAG within a single [update graph cycle](#update-graph-ug-cycles). The results then flow asynchronously through the [API layer](#the-live-data-stack) via [Barrage](./what-is-barrage.md) to connected clients and UI components.

This is the technical foundation of [Live Dataframes](#the-live-data-stack). A table whose source never changes is static. A table connected to streaming data is live. Most downstream operations preserve that behavior. Some operations, such as [`snapshot`](../reference/table-operations/snapshot/snapshot.md), intentionally produce static results from live sources.

## Mechanical sympathy

> "You don't have to be an engineer to be a racing driver, but you do have to have Mechanical Sympathy." <br/> _— Jackie Stewart, racing driver_

Mechanical sympathy means writing software that works with the hardware and runtime underneath it rather than against them. One of our favorite blogs explains the term and its rationale in [Why Mechanical Sympathy?](https://mechanical-sympathy.blogspot.com/2011/07/why-mechanical-sympathy.html). As systems programmers, we know it's best to use our tools the way their designers intended. These tools include the JVM, the host OS, I/O subsystems, and the CPU. We've optimized our query engine with this principle in mind, and we continue to refine it.

### Key observations

A few key observations shape Deephaven's approach to mechanical sympathy:

- **Large-scale [aggregations](../how-to-guides/combined-aggregations.md) and other operations** are often best served by columnar storage. Deephaven's column orientation provides flexibility and performance.
- **Data movement** can have high fixed costs. It's often best to amortize those costs over many rows, cells, or bytes to achieve higher throughput. Larger reads have limits, though, so Deephaven uses block-oriented data reads and caches, with block sizes chosen to balance that saving against two costs of larger reads:
  - Fetching more data than an operation uses has its own cost, called "read amplification."
  - Working with overly large blocks of data can hurt locality of reference and cache performance.
- **Predictable code** is code that is unlikely to mislead the branch predictor, and the CPU can optimize it more easily. Deephaven's engine code often has relatively few branches, especially in the inner code of a loop. Our generated code typically prunes unreachable cases.
- **JIT compilation** in modern JVMs can work wonders for performance if you let it. We try to help it along by:
  - Avoiding virtual method invocations that can't trivially be inlined, especially in inner loops.
  - Operating on dense regions of memory to allow for vectorization when the CPU offers SIMD instructions.
- **Garbage collection** can seriously degrade performance, both by consuming CPU resources that might otherwise be available for application throughput and by triggering unpredictable slowdowns or pauses at performance-critical times. Deephaven uses preallocation and object pooling to lower overall young generation usage. To the extent possible, and particularly within our inner loops, we try to use reusable objects rather than allocating many temporaries.
- **Concurrency** can be a double-edged sword. Poorly designed concurrent systems can be slower than single-threaded ones because of resource contention, false sharing, and thrashing, so Deephaven avoids shared state wherever possible.
- **Engine parallelism**: The engine parallelizes _stateless_ [`select`](../reference/table-operations/select/select.md), [`update`](../reference/table-operations/select/update.md), and [`where`](../reference/table-operations/filter/where.md) expressions, and it processes independent parts of the [update graph](#update-graph-ug-cycles) in parallel. A stateless expression doesn't depend on mutable external inputs or on the order in which rows are evaluated. [Parallelizing queries](./query-engine/parallelization.md) covers both, including when expressions that use Python objects run in parallel.
- **User parallelism**: [Partitioned tables](../how-to-guides/partitioned-tables.md) let users parallelize their own workloads along natural partition boundaries.

### Chunk-oriented architecture

The Deephaven query engine moves data around using a data structure called a [`Chunk`](/core/javadoc/io/deephaven/chunk/Chunk.html). This subsystem is key to achieving mechanical sympathy in our implementation.

<iframe src="../assets/conceptual/architecture/chunk-architecture.html" title="Diagram showing chunk-oriented architecture for bulk data processing" loading="lazy" style={{width: '100%', aspectRatio: '840 / 1178', border: 'none'}} />

By working with chunks of data rather than single cells, we allow the engine to amortize data movement costs at every applicable level of the stack. For example, [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)s are also [`ChunkSource`](/core/javadoc/io/deephaven/engine/table/ChunkSource.html)s, which means they can transfer data in bulk through `getChunk` and `fillChunk` rather than one cell at a time. A [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html) may in turn implement these transfers by wrapping or copying arrays, by reading the appropriate region of a file, or by evaluating a formula once for each result element.

Deephaven implements chunks as regions of Java arrays. Chunks are strongly typed by the storage they represent:

- There is one chunk implementation for each Java primitive type and another for Java `Object`s.
- Chunks that code can modify are [`WritableChunk`](/core/javadoc/io/deephaven/chunk/WritableChunk.html)s. Each writable class, such as [`WritableIntChunk`](/core/javadoc/io/deephaven/chunk/WritableIntChunk.html), extends its read-only counterpart and adds mutators.
- Chunks take a generic [attribute](/core/javadoc/io/deephaven/chunk/attributes/package-summary.html) argument that names the kind of data they hold, such as values, hash codes, or chunk positions. This gives compile-time type safety with no runtime cost.

In the [`Chunk`](/core/javadoc/io/deephaven/chunk/Chunk.html) type hierarchy, per-cell accessors such as `get`, `set`, and `add` are `final`, so the JIT can inline them and per-cell access never makes a virtual method call. Bulk copy methods such as `copyToChunk` and `copyFromChunk` are `final` too. Only a few methods, such as `slice`, are virtual, with multiple implementations.

The engine pools chunks and reuses them as temporary data buffers, which reduces garbage generation and the frequency and duration of garbage collection.

**Performance impact**: Processing data in chunks instead of one element at a time reduces method call overhead by orders of magnitude and gives the JIT compiler tight loops that it can vectorize. Each operation sizes its chunks, typically in the thousands of elements.

### Columnar hash tables

Deephaven [join](../how-to-guides/joins-exact-relational.md) and [aggregation](../how-to-guides/combined-aggregations.md) operations often use a columnar hash table built on chunks. Where data patterns permit, it hashes, inserts, and probes whole chunks of single or multi-column keys at once. Each part of the hash table uses chunk-oriented kernels to hash data, sort data, and find runs of identical values. These kernels enable vectorization and avoid virtual method lookups.

### `RowSet` implementations

Every table operation relies on the [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) that enumerates its row keys, so how [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s are stored matters as much to mechanical sympathy as how column data moves. In practice, [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s switch between three implementations depending on size and sparsity:

1. **Single range**: One contiguous range of row keys. Ideal when all row keys in a [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) are contiguous.
2. **Sorted ranges**: Several contiguous ranges in sorted order. Ideal for tables with a small number of ranges, no matter how widely separated.
3. **Regular Space Partitioning (RSP)**: Our take on the popular [Roaring Bitmaps](https://roaringbitmap.org/) (RB) data structure, for large or widely distributed sets of row keys. It splits the row key space into fixed blocks of 2^16 keys, each held in a _container_. [Regular Space Partitioning (RSP) vs. median-split trees](#regular-space-partitioning-rsp-vs-median-split-trees) describes it in detail.

In all cases, the goal is to allow for efficient traversal or set operations with minimum storage cost and high throughput.

**Performance characteristics**:

| Implementation | Best for           | Memory        | Operations                                                |
| -------------- | ------------------ | ------------- | --------------------------------------------------------- |
| Single range   | Contiguous keys    | O(1)          | O(1)                                                      |
| Sorted ranges  | Few ranges         | O(ranges)     | O(log ranges) lookup; O(ranges) insert and set operations |
| RSP            | Sparse, large sets | O(containers) | O(log containers) lookup; O(containers) set operations    |

#### Regular Space Partitioning (RSP) vs. median-split trees

A simple way to accumulate a set of values, such as a [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html), is a sorted array. For example, the following sorted array of only a few values is very simple and efficient:

```
[ 1, 5, 9, 15, 16, 19, 25 ]
```

To keep an array sorted when performing operations such as an insert, the system must first find where to put the new value. This requires a binary search, which is an `O(log n)` operation. Next, the system makes space for the new value. As the set grows, the system splits the array at its median and arranges the two pieces in a tree structure. This page calls that structure a _median-split tree_.

To insert 10 into the array above, the system first checks whether the resulting array is too big. If it is, the system splits it at a median value, creating two sub-arrays and a parent node:

![A median-split tree after inserting 10: parent node 10, with the sub-arrays 1, 5, 9, 10 and 15, 16, 19, 25 below it](../assets/how-to/rsp-tree.png)

In this case, 10 is the parent node that tells a search which sub-branch to descend.

For big [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s — often accumulating millions of values — the overhead on median-split tree operations can be high. As a single [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) is created or grows, the binary subdivision operation can be expensive. Operations that combine two [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s, such as union, intersection, or set difference, can be particularly expensive because they need to look at the individual values in each [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) and decide which values go in the result.

RSP is Deephaven's general-purpose [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) representation for large, sparse sets of row keys, and it avoids the median-split tree costs above.

Deephaven's RSP is modeled on the [Roaring Bitmaps](https://roaringbitmap.org/) [RoaringArray](https://github.com/RoaringBitmap/RoaringBitmap/blob/master/roaringbitmap/src/main/java/org/roaringbitmap/RoaringArray.java):

- It divides the row key space into fixed blocks of 2^16 (65,536) keys, and each block's keys are held in a container.
- Its containers come from a library initially derived from the [Java RB implementation](https://github.com/lemire/RoaringBitmap/). Deephaven adds a few special-case containers that use less memory in important edge cases.
- Containers sit in an array, with a parallel array that holds each entry's key, as in RB.
- RSP stores a run of blocks that would be entirely full as a single entry in that array instead of one container per block.
- Deephaven adds substantial optimizations for widely distributed row key ranges.

Fixed split points give RSP three advantages over a median-split tree:

- **Known locations**: A median-split tree splits at the median of the values, such as 10 in the example above. RSP splits at every integer multiple of 2^16, whatever values the set holds. Row key 65,537 can only be in the second container, which holds keys 65,536 through 131,071.
- **Faster set operations**: Two [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s always split at the same boundaries. For large datasets, union, intersection, and set difference are usually much faster, because they process one pair of matching containers at a time and never cross array boundaries that fall at different points in each [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html).
- **Compact ranges**: Writing out every value in the set `[ 0, 1000 * 2^16 - 1 ]` would take 1,000 full containers. RSP instead stores a continuous range that spans full blocks, `[ n * 2^16, m * 2^16 - 1 ]` for `m > n`, as one entry.

RSP costs more for small [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)s, because each one still needs an array of containers and then the containers themselves. This matters for queries that create one [`RowSet`](/core/javadoc/io/deephaven/engine/rowset/RowSet.html) per row, such as [joins](../how-to-guides/joins-exact-relational.md). The single range implementation in [`RowSet` implementations](#rowset-implementations) covers that case.

## Formula and condition evaluation

Deephaven table operations often take complex, user-defined expressions. A [formula](../how-to-guides/formulas.md) creates a column, and a [condition](../how-to-guides/use-filters.md) filters rows. We've made several key design decisions that either enable interesting use cases or allow for significant optimization.

### Expression parsing

Deephaven uses [JavaParser](https://javaparser.org/) to turn user-specified [expressions](../how-to-guides/query-string-overview.md) into executable code:

1. **Direct column references**: An expression that is just an existing column name, or an alias for one (e.g., `"Y = X"`), bypasses compilation entirely and can reuse the existing [`ColumnSource`](/core/javadoc/io/deephaven/engine/table/ColumnSource.html), as [What happens when you call `update`](#what-happens-when-you-call-update) describes.
2. **Other formulas**: Deephaven compiles every other formula into a Java class, no matter how simple it looks. [Filter conditions](../how-to-guides/use-filters.md) have a shortcut, described below.

**Example of formula evaluation**:

```python order=source,result_alias,result_simple,result_complex
from deephaven import empty_table

# Create a source table
source = empty_table(10).update(["Value = ii * 10", "A = ii * 2", "B = ii * 3"])

# Direct reference - no compilation, reuses A's existing ColumnSource
result_alias = source.update("A2 = A")

# Simple-looking formula - still compiled into a class
result_simple = source.update("DoubleValue = Value * 2")

# Complex formula - also compiled, just does more work at runtime
threshold = 50
result_complex = source.update(
    "Computed = Math.sqrt(A * A + B * B) > " + str(threshold)
)
```

Deephaven caches compiled classes, so an identical formula reuses the class it already built.

Compilation also does two things for the formula:

- It replaces column name references with parameters, which the engine fills from data it loads in [chunks](#chunk-oriented-architecture).
- It lets the formula read a whole column as an [array](../how-to-guides/work-with-arrays.md) indexed by row position, such as `X_[ii - 1]`.

A [filter condition](../how-to-guides/use-filters.md) that compares a column with a literal value or a query scope variable, such as `"Price > 100"`, ``"Sym = `AAPL`"``, or ``"Sym in `AAPL`, `MSFT`"``, usually becomes a built-in range or match filter, and no class is compiled. A string-containment condition such as ``"Sym includes `AA`"`` also skips compilation. Most other conditions are compiled the same way as formulas. [Integrating user methods](#integrating-user-methods) describes an exception for Python functions.

### Integrating user methods

Formulas and conditions can call any user function that is callable from Java, either directly or through the Java Native Interface (JNI). A formula or condition can also call a [Python function](../how-to-guides/python-functions.md).

When a formula or filter condition is a single call to an eligible [Python function](../how-to-guides/python-functions.md), whether ordinary or [Numba](../how-to-guides/use-numba.md)-vectorized, the engine evaluates it with a chunked Python kernel. The kernel crosses from Java into Python once per chunk of rows instead of once per row, and the function runs once for each row in the chunk.

- **In a formula**: The engine compiles the expression, but the chunked kernel evaluates the column instead of the compiled class.
- **In a [filter condition](../how-to-guides/use-filters.md)**: The engine skips code generation and uses a chunked Python filter.

Users can extend the query language with their own functionality by integrating external models and algorithms into their queries. This, in turn, enables all manner of exciting integrations with data science toolkits, whether open source or proprietary.

### Further work on formula evaluation

Deephaven's formula and condition evaluation can still improve. We're especially interested in exploring alternative bytecode-generation strategies and integrating [GraalVM](https://www.graalvm.org/) as a compilation option.

## Language and client integration

The Deephaven engine is written in Java. The components in this section let Python code, client applications in other languages, and user interfaces work with it.

### Bridging Java and Python: jpy

Although the core of Deephaven is implemented in Java, we consider Python to be the most important language in use for writing Deephaven queries. We have invested significantly in a fork of the open source [jpy project](https://github.com/jpy-consortium/jpy) and work with the project's contributors to evolve its capabilities. Written in C with JNI and Python C APIs, [jpy](../how-to-guides/use-jpy.md) is a performant, low-level library.

To maximize the familiarity of the Deephaven data science and app-dev experience, we have developed a transparent, pure Python API that relieves the user of the details of calling [jpy](../how-to-guides/use-jpy.md). During the design and implementation of this Pythonic API, we took special care to minimize the number of crossings between JNI and the Python runtime.

### gRPC APIs for polyglot interoperability

Deephaven's core API is built on [gRPC](./deephaven-core-api.md) and [Apache Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md), which have implementations in almost every language, so developers can implement compatible clients, and even servers, in almost any language. The API has several complementary modules. The two most important are:

- **[Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md) service**: High-performance columnar data transport. With Deephaven's [Barrage](./what-is-barrage.md) extension, it also streams real-time, incremental updates.
- **[Table Service](./deephaven-core-api.md#the-table-service)**: A table manipulation API that mirrors the Deephaven engine's internal compute paradigm.

Read more in [Deephaven's core API](./deephaven-core-api.md).

### Building UIs

Deephaven provides two approaches for building custom user interfaces:

- **[`deephaven.ui`](https://deephaven.io/core/ui/docs/)**: A Python web framework for building real-time data-focused applications. Write scripts entirely in Python — no front-end engineering, JavaScript, or CSS required.

  **Key features**:
  - **Component-based**: Create user interfaces from components defined entirely with Python.
  - **Live dataframe aware**: Components can use Deephaven tables as a data source.
  - **Reactive**: UI components automatically update when the underlying Python data changes.
  - **Declarative**: Describe the UI as a function of the data and let the framework handle the rest.
  - **Composable**: Combine and reuse components to build complex interfaces.
  - **Broad**: Components range from simple text fields to complex tables and plots.

  For complete documentation, tutorials, and examples, see the [`deephaven.ui` documentation](https://deephaven.io/core/ui/docs/).

- **[`web-client-ui`](https://github.com/deephaven/web-client-ui)**: JavaScript/TypeScript components for building custom web applications with full control over the front end.

## The whole is greater than…

Deephaven is the result of evolution. Its architecture grew out of requests and feedback from sophisticated users and many years of R&D by a dedicated development team. Compelling problem sets deserve prolonged attention, and servicing real-time app-dev and data science use cases certainly qualifies.

Deephaven Community Core is modular by design. Modularity follows modern best practice and makes the software easier to extend. The engine, APIs, and user interfaces described on this page form one stack. Taken together, we hope you'll find Deephaven a compelling stack for your data-driven use case. Sometimes range matters, and range is a Deephaven superpower.

## Related documentation

### Conceptual guides

- [How Deephaven works: A mental model](./deephaven-mental-model.md)
- [Directed acyclic graph (DAG)](./dag.md)
- [Table update model](./table-update-model.md)
- [Periodic Update Graph](./periodic-update-graph-configuration.md)
- [Table types](./table-types.md)
- [What is Barrage?](./what-is-barrage.md)
- [Core API design](./deephaven-core-api.md)
- [Parallelizing queries](./query-engine/parallelization.md)
- [Deephaven Enterprise overview](/enterprise/docs/enterprise-overview)

### How-to guides

- [Use filters](../how-to-guides/use-filters.md)
- [Joins: exact and relational](../how-to-guides/joins-exact-relational.md)
- [Inexact, time-series, and range joins](../how-to-guides/joins-timeseries-range.md)
- [Combined aggregations](../how-to-guides/combined-aggregations.md)
- [Create and use partitioned tables](../how-to-guides/partitioned-tables.md)
- [Query strings](../how-to-guides/query-string-overview.md)
- [Formulas](../how-to-guides/formulas.md)
- [Arrays](../how-to-guides/work-with-arrays.md)
- [Python functions in query strings](../how-to-guides/python-functions.md)
- [Use Numba in Deephaven queries](../how-to-guides/use-numba.md)
- [Create a time table](../how-to-guides/time-table.md)
- [Synchronize multiple tables](../how-to-guides/synchronizing-tables.md)
- [Read Parquet files into Deephaven tables](../how-to-guides/data-import-export/parquet-import.md)
- [Connect to a Kafka stream](../how-to-guides/data-import-export/kafka-stream.md)
- [Iceberg and Deephaven](../how-to-guides/data-import-export/iceberg.md)
- [Read CSV or other delimited files](../how-to-guides/data-import-export/csv-import.md)
- [Execute SQL queries](../how-to-guides/data-import-export/execute-sql-queries.md)
- [Arrow Flight and Deephaven](../how-to-guides/data-import-export/arrow-flight.md)
- [Use jpy](../how-to-guides/use-jpy.md)

### Reference

- [`agg.avg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`agg.sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`consume`](../reference/data-import-export/Kafka/consume.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`exact_join`](../reference/table-operations/join/exact-join.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
- [`read`](../reference/data-import-export/Parquet/readTable.md)
- [`select`](../reference/table-operations/select/select.md)
- [`snapshot`](../reference/table-operations/snapshot/snapshot.md)
- [`sort`](../reference/table-operations/sort/sort.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`view`](../reference/table-operations/select/view.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`Chunk` Javadoc](/core/javadoc/io/deephaven/chunk/Chunk.html)
- [`ChunkSource` Javadoc](/core/javadoc/io/deephaven/engine/table/ChunkSource.html)
- [`ColumnSource` Javadoc](/core/javadoc/io/deephaven/engine/table/ColumnSource.html)
- [`RowRedirection` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/RowRedirection.html)
- [`RowSet` Javadoc](/core/javadoc/io/deephaven/engine/rowset/RowSet.html)
- [`TableUpdate` Javadoc](/core/javadoc/io/deephaven/engine/table/TableUpdate.html)
- [`WhereFilter` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/select/WhereFilter.html)
- [`WritableChunk` Javadoc](/core/javadoc/io/deephaven/chunk/WritableChunk.html)
- [`WritableIntChunk` Javadoc](/core/javadoc/io/deephaven/chunk/WritableIntChunk.html)
- [`pydeephaven.Table.to_arrow`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.to_arrow)

### UI frameworks

- [`deephaven.ui` documentation](https://deephaven.io/core/ui/docs/)
- [`deephaven.ui` architecture](https://deephaven.io/core/ui/docs/architecture)
- [`web-client-ui` on GitHub](https://github.com/deephaven/web-client-ui)

### External resources

- [Apache Parquet](https://parquet.apache.org/)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Arrow](https://arrow.apache.org/)
- [JavaParser](https://javaparser.org/)
- [GraalVM](https://www.graalvm.org/)
- [Lambda architecture](https://en.wikipedia.org/wiki/Lambda_architecture)
- [Roaring Bitmaps](https://roaringbitmap.org/)
- [jpy project](https://github.com/jpy-consortium/jpy)
- [Mechanical Sympathy blog](https://mechanical-sympathy.blogspot.com/)
