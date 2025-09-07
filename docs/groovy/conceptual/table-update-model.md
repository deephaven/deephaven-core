---
title: Incremental update model
---

<div className="comment-title">

Making incremental table updates a reality

</div>

Data in the real world is constantly in flux. Market data updates with new quotes and trades at sub-millisecond latency. Telemetry data flows in real time, allowing computer systems to monitor fleets of trucks, racks of servers, and swarms of sensors. Modern data engineers spend significant resources architecting to address these types of flows.

**Deephaven’s query engine provides a scalable solution to some of the hardest problems in this area, freeing compute and engineering resources to address domain-specific issues.**

Classic, general-purpose data systems often operate entirely on static data. When they do add streaming or real-time capabilities, they often rely on re-evaluation of entire analyses on a static snapshot of the underlying data. The problem size they consider is thus always scaled to the entire data set. This approach has an enormous negative impact on compute and data transfer resource utilization, development hours, analytical iteration latency, and time-to-market, resulting in very real monetary and opportunity costs.

Incrementally-updating systems, on the other hand, are able to consider only new data and operation-specific state describing the intermediate results. This can radically reduce the size of the problem that must be solved for each interval, allowing for lower resource consumption, shorter intervals, or both. Non-trivial analyses and data-driven applications often involve multiple steps, applying chains of logic expressed via SQL queries or streaming pipelines. Incrementally computing updates allows for problem size reduction at every stage, compounding performance and efficiency gains.

In many cases, incrementally-updating systems are the best (or only) way to satisfy requirements when solving real-world data problems. Unfortunately, such architectures are often overlooked because they come with material complexity trade-offs. Off-the-shelf systems are often expensive or limited in scope. Bespoke solutions require significant upfront investment in development time and carry project risks. **The Deephaven team has aimed to solve this problem by providing a well-optimized, easy-to-use system that internalizes much of the complexity, while presenting developers with only the choices they need in order to architect their solution.**

## Deephaven’s approach

<div className="comment-title">

Deephaven’s query engine was built from the ground up with real-time data processing in mind.

</div>

Deephaven uses an incremental table update model to unify two distinct concepts encountered in data systems: _streams_ and _tables_:

- A stream can be defined as a sequence of events, and lends itself well to certain types of incremental processing.
- A table is a structured data set consisting of columns and rows in a two-dimensional coordinate system.

There are typically notable trade-offs made in either model. Stream-processing systems are often unable to offer the full set of operations encountered in table-oriented systems (e.g., joins) without significant compromises. Table-oriented systems, like databases and dataframe packages, offer powerful tools for analyzing data, but these typically operate in a static or snapshot-driven manner.

Deephaven’s engine operates on tables, but distributes table updates incrementally via a [directed acyclic graph (DAG)](./dag.md) modeling relationships between source tables and their dependents. Each node in the DAG is a table (or map of related tables); its edges are the listeners that apply parent updates to child tables. Listeners may also couple the engine to external systems, e.g., publishers for remote clients, reactive event processors, or other application components. Changes flow through the entire [DAG](./dag.md) on each update cycle, effectively micro-batching updates based on the configured cycle interval.

Note that Deephaven tables are _always_ ordered, with strict guarantees for observable evaluation order within an operation; this is somewhat common in dataframe systems and time series databases, but usually not a property of relational databases. This makes it more natural for Deephaven to model ordered streams as tables, while also simplifying the programming model for time series analyses and related applications.

The result marries the best aspects of stream and table processing engines. Incremental updates flow from parent table to child table. Deephaven’s engine logic handles the complexity required to present a consistent result at each stage, while offering a full suite of table operations. Moreover, this architecture allows real-time data tables to be seamlessly integrated with often-voluminous static data sets. **As long as a source can be modeled as a structured table, it can be brought into Deephaven and commingled with other tables.** We have invested significant effort to support in-situ access to partitioned, columnar data sources (e.g., Apache Parquet), as well as in-memory ingestion capabilities for other data sources like CSV files, pandas dataframes, and more. As a result, Deephaven offers an empowering development solution for a wide range of data-driven applications, along with powerful interactive and offline data analysis for real-time data, static data, and combinations thereof.

## Describing table updates

In order to deliver on the ideas described above, Deephaven’s query engine must describe table updates programmatically in an efficient manner.

This section introduces you to important vocabulary and defines our data structures. While this isn’t intended to be a full description of our query engine’s design, it will help to clarify a few terms before we proceed.

In Deephaven, a **table** consists of a **row set** and zero or more named columns, each of which is backed by a **column source**:

- A table’s row set is a sequence of **row keys** (non-negative 64-bit integers) in monotonically-increasing order. The row set additionally provides a mapping from each row key to its corresponding **row position** (ordinal) with the same relative order.
  - Row keys provide a compact way to describe each unique row of a table that is distinct from ordinal position, allowing certain [freedoms](#sparse-row-sets). That said, if row key "A" is less than row key "B", then the row identified by "A" must come before the row identified by "B".
  - Row sets are typically expressed as a set of closed ranges. This notation will be used in the explanations that follow.
- Column sources provide a mapping from row keys to their corresponding data values, implementing the columns of a Deephaven table.

Note that multiple tables may share column sources or row sets. For example, filtering a table produces a child table that shares the parent table’s column sources, with its own row set enclosed by the parent table’s row set. Alternatively, adding derived columns to a table produces a child table that shares the parent table’s row set, while adding at least one column source that belongs solely to the child table. This sharing lets Deephaven avoid creating redundant data structures which may become expensive in memory consumption or maintenance.

Deephaven’s table update model uses an **update notification** data structure with five components which will be explained below:

- [added row set](#adds)
- [removed row set](#removes)
- [modified row set](#modifies)
- [modified columns](#modified-columns)
- [row shift data](#shifts)

We additionally describe some of the details of row set and column source usage, incremental update processing, and safe concurrent data access.

### Adds

<div className="comment-title">

Sometimes data only grows

</div>

The simplest updating tables represent an append-only data set. For a concrete example, consider a structured log of telemetry data that grows in linear fashion. To describe such a table, it’s sufficient to present a simple, contiguous row set.

For a table with size _N_, this could be written:

`{[0 .. N-1]}`

Since the only allowed change is an append, we need only communicate an _added row set_ in order to keep our dependents up to date. If on a given update cycle we observe that the table size grew from _N_ to _N′_, we notify dependent listeners of an added row set:

`{[N .. N′-1]}`

This produces a result row set:

`{[0 .. N′-1]}`

### Sparse row sets

<div className="comment-title">

Sometimes it’s not quite that simple

</div>

Before advancing further, we should note that row sets need not be a single contiguous range. Consider a data set with multiple partitions, each of which grows in append-only fashion. For example, think about our telemetry data log, but this time with three distinct publishers. In describing such a data set, we might reasonably choose to allocate a non-overlapping row key space to each partition.

For a table with three partitions of size _m_, _n_, and _o_ respectively, and an assumed upper bound of _s_ rows in each partition, we could use the following row set:

`{[0 .. m-1], [s .. s+n-1], [2s .. 2s+o-1]}`

If each partition grew by 10 rows, the added row set would then be:

`{[m .. m+9], [s+n .. s+n+9], [2s+o .. 2s+o+9]}`

The result row set would then be:

`{[0 .. m+9], [s .. s+n+9], [2s .. 2s+o+9]}`

### Removes

<div className="comment-title">

Sometimes data goes away

</div>

Now imagine an updating table where data can be removed. To keep the discussion simple, let’s consider a source that allows older data to become unavailable. In order to update our dependents, we’ll need to communicate a **removed row set** to describe the newly-unavailable rows.

Take the result row set from the previous section as our initial row set:

`{[0 .. m+9], [s .. s+n+1], [2s .. 2s+o+9]}`

If the first 100 rows from each partition became unavailable, the removed row set would be:

`{[0 .. 99], [s .. s+99], [2s .. 2s+99]}`

The result row set would then be:

`{[100 .. m+9], [s+100 .. s+n+1], [2s+100 .. 2s+o+9]}`

### Modifies

<div className="comment-title">

What do we do if data changes?

</div>

Sometimes the existing data at a row changes. For example, if a table is modeling the latest mappings from a key-value store, it often makes sense to store the latest for a given key in a row that remains stable for the life of the table. Key insertions translate to row key adds, and key deletions translate to row key removes. Value updates, on the other hand, need a new tool: a **modified row set**.

Let’s assign some mappings for our example. If the source data has three unique keys "A", "B", and "D", we might map those to row keys 0, 1, and 2 respectively, and our overall row set would thus be `{[0 .. 2]}`.

If a new value arrives for key "B", then we need to communicate the following modified row set to our dependents:

`{[1]}`

Note that the table’s row set does **not** change as a result of this update.

### Removed or Modified values

<div className="comment-title">

Previous vs. Current

</div>

A key component of incremental removal and modification processing is knowing what the previous value was for a removed or modified cell. For example, an aggregation that computes the arithmetic mean of each group of rows might do so by recording the sum of all cell values for a given column and dividing it by the size on each update cycle. Updating this sum then looks like:

```none
Sum′ = Sum
    - (previous values of removed or modified cells)
    + (current values of modified or added cells)
```

For all cells, the previous value is the value as of the beginning of the current update cycle, which implies that unchanged cells have the same value for previous and current. In order to provide this capability, all Deephaven column sources are required to be able to provide the previous values of removed or modified cells, and to recognize which cells are unchanged. This requirement only holds for the duration of the update phase of a cycle; the necessary data structures are released as part of intra-cycle cleanup, and accessing previous values outside of an updating phase produces undefined results, including the possibility of exceptions or inconsistent data.

### Modified columns

<div className="comment-title">

Sometimes we don’t have to do anything at all, phew!

</div>

Sometimes the data for a row changes, but not in every column. Let’s continue our key-value store example from the [modifications](#modifies) section, but with some names for the table and its columns: `KVTable`, `Key`, and `Value`.

Now, imagine we want to sort `KVTable` on `Key` for a reliable output order, producing a dependent table `SortedKVTable`.

Say we again modify the upstream `Value` for `Key` "B", and send modified row set `{[1]}`. Does the sort operation need to do any work? No, because no data in the sort column changed!

In order to make downstream listeners aware of this optimization potential, we communicate a **modified column set**. In our example update, this is:

`{Value}`

This approach represents a compromise: while reporting the full matrix of modified cells is possible, doing so would come at a material cost in compute and memory usage for many data sets. Reporting the modified row set is necessary and sufficient for correctness. Reporting the modified column set is inexpensive relative to the full matrix approach, but allows for significant optimization and maps nicely to real-world data change patterns.

### Shifts

<div className="comment-title">

Sometimes we make things more complicated, for good reasons!

</div>

Sometimes it’s necessary to insert a new row between two existing rows. Let’s continue developing our `SortedKVTable` example from the previous section. Say a new row arrives because a new key "C" is added to the underlying data source. `KVTable` informs the sort listener that produces `SortedKVTable` of an added row set:

`{[3]}`

Let's discuss `SortedKVTable`’s row set. When a Deephaven table is sorted, the result table has its own row set, along with a **row redirection** data structure that maps each result row key to a source row key. Naively (since we’re a lot smarter about how we allocate sort address space than this), `SortedKVTable`’s row set could be `{[0 .. 2]}`, representing the existing keys "A", "B", and "D" with an identity row redirection `(0→0, 1→1, 2→2)`. Since the new key "C" must fit between existing keys "B" and "D", we have a problem!

One solution for `SortedKVTable` would be to report a modification at row key 2, re-mapping it to the "C" row, accompanied by an addition of row key 3 with the "D" row. Modifications can be expensive to process, however, and one could easily imagine a scenario where this strategy results in millions of spuriously modified rows. Moreover, this approach is unsatisfying in that it does not accurately describe the data change.

Instead, Deephaven’s table update model communicates **row shift data**, moving ranges of row keys by a positive or negative **shift delta**. In our example, `SortedKVTable` would report an added row set of `{[2]}` accompanied by a shift of `{[2]}` to `{[3]}` (that is, a shift delta of +1). This allows downstream listeners to update their data structures without reacting to spurious modifications. Note that the row redirection mapping `SortedKVTable`’s row set onto `KVTable`’s row set must also change as a result of this update to `(0→0, 1→1, 3→2, 2→3)`, but that this is an implementation detail rather than a part of the downstream update notification communicated by `SortedKVTable`.

It’s worth noting that the allocation of row keys, the actual row shift data communicated, and any kind of row redirections are details left up to the implementation of each table operation. The update model imposes certain correctness and consistency constraints, but does not dictate any other limitations. To illustrate this, consider that the row set for `SortedKVTable` need not start from 0. Had we started from a higher row key, it would have allowed for a negative shift delta while remaining perfectly correct.

### Putting data structures in a wider context

Now that we’ve described the data structures backing a Deephaven table and the components of a table update notification, you should have a pretty good grasp of the kinds of data and changes Deephaven can operate on.

The following sections should help fit all of this new knowledge into a wider context.

## Scaling table update processing

Let’s discuss a few of the additional design features of Deephaven’s DAG-driven table update processing that allow for scaling to shorter update cycles and larger problem sizes.

### Dependency-respecting parallelism

Sometimes available compute and I/O resources are sufficient to allow for table update processing to proceed in parallel using multiple threads. This can allow for significant speedup when the problem is sufficiently parallelizable, e.g., for wide DAGs with multiple inputs or parent tables that have many children and minimal contention in data access.

Each table within the [DAG](./dag.md) is allowed to produce exactly one notification for its dependent listeners per cycle, and must not further mutate its current state after producing said notification. These notifications are individually processed in single-threaded fashion, but the internal work scheduler built into Deephaven’s query engine allows for a pool of threads to be utilized when multiple notifications are available concurrently.

Some tables have multiple parents (e.g., join or merge results). Ensuring a single consistent update per table in these cases requires the use of a batching listener that produces notifications that aren’t available for execution until all parents are deemed _satisfied_. A table is satisfied when it has delivered its downstream update notification for this cycle, or when it can be proven that no such notification is forthcoming. This safety constraint complicates the work scheduler, but allows downstream listeners to operate on complete, consistent data.

### Consistent, concurrent initialization and snapshotting

Reading everything above, one might reasonably wonder: if this update processing is going on all the time, when and how can we actually safely read data or create new derived tables? There are three answers to this question, depending on the use case.

In the simplest case, it’s sufficient to simply block update processing. Other subsystems running in-process with a Deephaven query engine can acquire a [shared lock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#sharedLock()) for the duration of any otherwise unsafe operations they need to perform, guaranteeing that data will remain consistent across all nodes in the DAG. When you type a command for execution in the Deephaven console, this is done for you.

Sometimes it’s preferable to perform potentially unsafe operations from _within_ the update processing system. There are two ways to accomplish this. Firstly, by performing work reactively in a table listener; this is ideal for publishing updates to external subscribers or reactive systems. Secondly, by scheduling special terminal notifications that are processed at the end of each update cycle; this is ideal for post-update maintenance.

All of the approaches described so far have liveness implications for real-time applications. The third strategy, on the other hand, trades concurrency in exchange for giving up a guarantee of success. Each update cycle has two _phases_ (_updating_ and _idle_), and also a _step_ tracked by a logical clock. This state (phase and step) can be read concurrently and atomically by external code, and is augmented by per-table tracking of the _last update step_.

This allows a concurrent consumer to determine two important things. Firstly, whether it should attempt to use a source table’s current row set and current column source data, or its previous row set and previous column source data. Secondly, whether an optimistic operation cannot be proven to have succeeded consistently, and thus must be retried or abandoned.

This third, optimistically concurrent approach generally requires a fallback strategy of acquiring the [shared lock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#sharedLock()). Deephaven provides utilities to encapsulate all of this complexity for internal use as well as user code. This enables many table operations to be initialized concurrently with the update cycle, including all the operations that are performed automatically when rendering UI components via Deephaven’s Javascript Client. It’s also used for client-driven snapshots and table subscriptions via Deephaven’s implementation of [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).

None of these consistent data access mechanisms obviates the need to use good engineering sense. Developers should keep interactions with updating data as efficient and infrequent as possible. That said, together these strategies represent a toolbox with a wealth of possibilities for real-time application development when harnessed to the rest of the Deephaven query engine.

### Multi-process data pipelines

Deephaven also provides mechanisms for consistently replicating table data to other processes. Our Apache Arrow Flight implementation uses custom metadata to implement a protocol we call [Barrage](https://github.com/deephaven/barrage), which communicates table updates in the same way as we described [previously](#describing-table-updates) via a language agnostic gRPC API. Our Apache Kafka integration allows for streaming data ingestion (and soon publication) via one of the most popular distributed event streaming platforms in the world.

When coupled with the Deephaven query engine or with external publishers and subscribers that understand Apache Arrow Flight and Barrage or Apache Kafka, this allows for the creation of a multi-process DAG with remote links from publisher to subscriber. This simple primitive allows for consistent, asynchronous processing of data without inherent limitations on data size or resources. The Deephaven team intends to grow the toolset for this kind of data backplane system substantially over the coming months, but the building blocks are already in place for a huge variety of real-time data driven applications.

## Concluding thoughts

The update model described above serves as a cornerstone enabler of Deephaven’s data engine. It is fundamental to Deephaven’s unique and empowering approach to building modern data-driven applications and analytics. Developers need no longer turn to stream-oriented systems or snapshot-driven analysis when serving real-time data needs; Deephaven handles the inherent complexity of table updates, allowing developers to concentrate on solving their domain-specific problems without making unnecessary compromises.

## Related documentation

- [Core API design](./deephaven-core-api.md)
- [Deephaven’s Directed-Acyclic-Graph (DAG)](./dag.md)
- [Kafka basic terminology](./kafka-basic-terms.md)
- [Deephaven Barrage](/barrage/docs)
