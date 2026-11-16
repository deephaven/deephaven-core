---
title: Incremental update model
---

<div className="comment-title">

Making incremental table updates a reality

</div>

Data in the real world is constantly in flux. Market data updates with new quotes and trades at sub-millisecond latency. Telemetry data flows in real time, allowing computer systems to monitor fleets of trucks, racks of servers, and swarms of sensors. Modern data engineers spend significant resources architecting to address these types of flows.

**Deephaven's query engine provides a scalable solution to some of the hardest problems in this area, freeing compute and engineering resources to address domain-specific issues.**

Classic, general-purpose data systems often operate entirely on static data. When they do add streaming or real-time capabilities, they often rely on re-evaluation of entire analyses on a static snapshot of the underlying data. Their problem size therefore always scales with the entire data set. This approach has an enormous negative impact on compute and data transfer resource utilization, development hours, analytical iteration latency, and time-to-market, resulting in very real monetary and opportunity costs.

Incrementally updating systems, on the other hand, are able to consider only new data and operation-specific state describing the intermediate results. This can radically reduce the size of the problem that must be solved for each interval, allowing for lower resource consumption, shorter intervals, or both. Non-trivial analyses and data-driven applications often involve multiple steps, applying chains of logic expressed via SQL queries or streaming pipelines. Incrementally computing updates allows for problem size reduction at every stage, compounding performance and efficiency gains.

In many cases, incrementally updating systems are the best (or only) way to satisfy requirements when solving real-world data problems. Unfortunately, such architectures are often overlooked because they come with material complexity tradeoffs. Off-the-shelf systems are often expensive or limited in scope. Bespoke solutions require significant upfront investment in development time and carry project risks. **The Deephaven team has aimed to solve this problem by providing a well-optimized, easy-to-use system that internalizes much of the complexity, while presenting developers with only the choices they need in order to architect their solution.**

This page explains how that system works:

- How Deephaven combines streams and tables.
- How Deephaven describes each table update as adds, removes, modifies, modified columns, and shifts.
- How Deephaven processes updates at scale, lets you read consistent data, and moves updates between processes.

## Deephaven's approach

<div className="comment-title">

Deephaven's query engine was built from the ground up with real-time data processing in mind.

</div>

Deephaven uses an incremental table update model to unify two distinct concepts encountered in data systems. These are _streams_ and _tables_:

- A stream is a sequence of events, which lends itself well to certain types of incremental processing.
- A table is a structured data set consisting of columns and rows in a two-dimensional coordinate system.

Each model typically involves notable tradeoffs. Stream-processing systems are often unable to offer the full set of operations found in table-oriented systems, such as [joins](../how-to-guides/joins-exact-relational.md), without significant compromises. Table-oriented systems, like databases and dataframe packages, offer powerful tools for analyzing data, but these typically operate in a static or snapshot-driven manner.

Deephaven tables are _always_ ordered: every row has a well-defined position, and each operation either preserves its parent's row order or defines a new one (for example, [`sort`](../reference/table-operations/sort/sort.md)). Ordering is somewhat common in dataframe systems and time series databases. It is usually not a property of relational databases. Because its tables are ordered, Deephaven can model ordered streams as tables naturally. Ordering also simplifies the programming model for time series analyses and related applications.

Deephaven's engine operates on tables, but distributes table updates incrementally via a [directed acyclic graph (DAG)](./dag.md). The DAG models relationships between source tables and their dependents:

- Each node is a table, or a [listener](../how-to-guides/table-listeners-python.md) that consumes a table's updates without producing a table of its own.
- Each edge carries a parent table's updates to a dependent. For a derived table, the engine attaches a listener to the parent that applies those updates to the child.

Listeners can also connect the engine to external systems, such as publishers for remote clients, reactive event processors, or other application components.

An update cycle is one pass in which the engine applies pending source changes and propagates them from the source tables that changed to every table that depends on them. Each cycle applies all the changes that accumulated since the previous cycle, so updates are micro-batched. The engine's _logical clock_ is always in one of two _phases_:

- **Updating**: the engine propagates a cycle's changes through the DAG.
- **Idle**: the time between cycles.

The result marries the best aspects of stream and table processing engines. Incremental updates flow from parent table to child table. Deephaven's engine logic handles the complexity required to present a consistent result at each stage, while offering a full suite of table operations.

This architecture also lets you combine real-time tables with large static data sets in the same queries. **As long as you can model a source as a structured table, you can bring it into Deephaven and combine it with other tables.** Deephaven reads partitioned, columnar data sources such as [Apache Parquet](../how-to-guides/data-import-export/parquet-import.md) in place, and ingests other sources, such as [CSV files](../how-to-guides/data-import-export/csv-import.md) and [pandas DataFrames](../how-to-guides/use-pandas.md), into memory. As a result, Deephaven offers an empowering development solution for a wide range of data-driven applications, along with powerful interactive and offline data analysis for real-time data, static data, and combinations thereof.

## Describing table updates

To propagate changes incrementally through the [DAG](./dag.md), Deephaven's query engine must describe each table update programmatically and efficiently.

This section introduces you to important vocabulary and defines our data structures. While this isn't intended to be a full description of our query engine's design, it clarifies a few terms before we proceed.

In Deephaven, a **table** consists of a **row set** and zero or more named columns, each of which is backed by a **column source**:

- A table's row set is a sequence of **row keys** (non-negative 64-bit integers) in monotonically increasing order. The row set additionally provides a mapping from each row key to its corresponding **row position** (ordinal) with the same relative order.
  - Row keys identify each unique row of a table independently of its ordinal position, so a row set can contain gaps, as [Sparse row sets](#sparse-row-sets) shows. That said, if row key _a_ is less than row key _b_, then the row identified by _a_ must come before the row identified by _b_.
  - Row sets are typically expressed as a set of closed ranges. The explanations that follow use this notation.
- Column sources provide a mapping from row keys to their corresponding data values, implementing the columns of a Deephaven table.

Note that multiple tables may share column sources or row sets. For example, [filtering](../reference/table-operations/filter/where.md) a table produces a child table that shares the parent table's column sources, with its own row set enclosed by the parent table's row set. Alternatively, [adding derived columns](../reference/table-operations/select/update.md) to a table produces a child table that shares the parent table's row set, while adding at least one column source that belongs solely to the child table. This sharing lets Deephaven avoid creating redundant data structures which may become expensive in memory consumption or maintenance.

Deephaven's table update model uses an **update notification** data structure with five components, explained below:

- [added row set](#adds): row keys that are new this cycle
- [removed row set](#removes): row keys that are gone this cycle
- [modified row set](#modifies): row keys whose data changed
- [modified column set](#modified-columns): which columns those changes touched
- [row shift data](#shifts): ranges of existing row keys that moved to new keys

Along the way, [Sparse row sets](#sparse-row-sets) shows that a row set can contain gaps, and [Removed or modified values](#removed-or-modified-values) explains how listeners read the value a cell held before the update.

### Adds

<div className="comment-title">

Sometimes data only grows

</div>

The simplest updating tables represent an append-only data set. For a concrete example, consider a structured log of telemetry data that grows in linear fashion. To describe such a table, it's sufficient to present a simple, contiguous row set.

For a table with size _N_, this could be written:

`{[0 .. N-1]}`

Since the only allowed change is an append, we need only communicate an **added row set** to keep our dependents up to date. If on a given update cycle we observe that the table size grew from _N_ to _N′_, we notify dependent listeners of an added row set:

`{[N .. N′-1]}`

This produces a result row set:

`{[0 .. N′-1]}`

### Sparse row sets

<div className="comment-title">

Sometimes it's not quite that simple

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

Now imagine an updating table where data can be removed. To keep the discussion simple, let's consider a source that allows older data to become unavailable. To update our dependents, we communicate a **removed row set** that describes the newly unavailable rows.

Take the result row set from [Sparse row sets](#sparse-row-sets) as the initial row set. It describes three partitions that each grew by 10 rows:

`{[0 .. m+9], [s .. s+n+9], [2s .. 2s+o+9]}`

If the first 100 rows from each partition became unavailable, the removed row set would be:

`{[0 .. 99], [s .. s+99], [2s .. 2s+99]}`

The result row set would then be:

`{[100 .. m+9], [s+100 .. s+n+9], [2s+100 .. 2s+o+9]}`

### Modifies

<div className="comment-title">

What do we do if data changes?

</div>

Sometimes the existing data at a row changes. For example, if a table is modeling the latest mappings from a key-value store, it often makes sense to store the latest value for a given key in a row that remains stable for the life of the table. Key insertions translate to row key adds, and key deletions translate to row key removes. Value updates, on the other hand, need a new tool: a **modified row set**.

Let's assign some mappings for our example. If the source data has three unique keys `"A"`, `"B"`, and `"D"`, we might map those to row keys 0, 1, and 2 respectively, and our overall row set would thus be `{[0 .. 2]}`.

If a new value arrives for key `"B"`, then we need to communicate the following modified row set to our dependents:

`{[1]}`

Note that the table's row set does **not** change as a result of this update.

### Modified columns

<div className="comment-title">

Sometimes we don't have to do anything at all, phew!

</div>

Sometimes the data for a row changes, but not in every column. Let's continue our key-value store example from [Modifies](#modifies), but with some names for the table and its columns: `KVTable`, `Key`, and `Value`.

Now, imagine we want to [sort](../reference/table-operations/sort/sort.md) `KVTable` on `Key` for a reliable output order, producing a dependent table `SortedKVTable`.

Say we again modify the upstream `Value` for key `"B"` and send the modified row set `{[1]}`. Does the [sort](../reference/table-operations/sort/sort.md) operation need to reorder any rows? No, because no data in the sort column changed! It only needs to pass the modification along to its own dependents.

In order to make downstream listeners aware of this optimization potential, we communicate a **modified column set**. In our example update, this is:

`{Value}`

The modified column set is a compromise. Reporting the full matrix of modified cells is possible, but for many data sets it would cost a material amount of compute and memory. Reporting the modified row set is necessary and sufficient for correctness. Reporting the modified column set is inexpensive relative to the full matrix approach, but allows for significant optimization and maps nicely to real-world data change patterns.

### Shifts

<div className="comment-title">

Sometimes we make things more complicated, for good reasons!

</div>

Sometimes it's necessary to insert a new row between two existing rows. This section builds on the example from [Modified columns](#modified-columns). There, `SortedKVTable` is `KVTable` sorted on `Key`, and it holds the keys `"A"`, `"B"`, and `"D"`. Say a new row arrives because a new key `"C"` is added to the underlying data source. `KVTable` sends this added row set to the [sort](../reference/table-operations/sort/sort.md) listener that produces `SortedKVTable`:

`{[3]}`

Let's discuss `SortedKVTable`'s row set. A [sorted](../reference/table-operations/sort/sort.md) table has its own row set and a **row redirection**, a data structure that maps each result row key to a source row key. Suppose the sort uses a naive allocation (we're a lot smarter about how we allocate sort address space than this):

- `SortedKVTable`'s row set is `{[0 .. 2]}`.
- The row redirection is the identity mapping `(0→0, 1→1, 2→2)`, so row keys 0, 1, and 2 hold `"A"`, `"B"`, and `"D"`.
- The new key `"C"` must sort between `"B"` and `"D"`, but no free row key exists between 1 and 2.

That's a problem!

One solution for `SortedKVTable` would be to report a modification at row key 2, re-mapping it to the `"C"` row, accompanied by an addition of row key 3 with the `"D"` row. Modifications can be expensive to process, however, and one could easily imagine a scenario where this strategy results in millions of spuriously modified rows. Moreover, this approach is unsatisfying in that it does not accurately describe the data change.

Instead, Deephaven's table update model communicates **row shift data**, moving ranges of row keys by a positive or negative **shift delta**. In our example, `SortedKVTable` would report an added row set of `{[2]}` accompanied by a shift of `{[2]}` to `{[3]}` (that is, a shift delta of +1). This allows downstream listeners to update their data structures without reacting to spurious modifications.

The row redirection that maps `SortedKVTable`'s row set onto `KVTable`'s row set also changes, to `(0→0, 1→1, 2→3, 3→2)`. That redirection change is not part of the update notification that `SortedKVTable` sends downstream.

It's worth noting that the allocation of row keys, the actual row shift data communicated, and any kind of row redirections are details left up to the implementation of each table operation. The real [sort](../reference/table-operations/sort/sort.md) operation, for example, allocates row keys more cleverly than the naive allocation above. The update model imposes certain correctness and consistency constraints, but does not dictate any other limitations.

To illustrate this, consider that the row set for `SortedKVTable` need not start from 0. Had we started from a higher row key, it would have allowed for a negative shift delta while remaining perfectly correct.

### Removed or modified values

<div className="comment-title">

Previous vs. current

</div>

A key component of incremental removal and modification processing is knowing what the previous value was for a removed or modified cell. For example, an [aggregation](../how-to-guides/dedicated-aggregations.md) such as [`avg_by`](../reference/table-operations/group-and-aggregate/avgBy.md) that computes the arithmetic mean of each group of rows might do so by recording the sum of all cell values for a given column and dividing it by the size on each update cycle. Updating this sum then looks like:

```none
Sum′ = Sum
    - (previous values of removed or modified cells)
    + (current values of modified or added cells)
```

For all cells, the previous value is the value as of the beginning of the current update cycle. Unchanged cells therefore have the same previous and current value.

To provide this capability, every Deephaven column source must be able to provide the previous values of removed or modified cells, and must return the current value as the previous value of any unchanged cell. Likewise, a table's row set can report its previous state: the row keys it held at the start of the cycle.

This requirement holds only during the cycle's _updating_ phase, when the engine propagates changes through the DAG. At the end of the cycle, the engine releases the data structures that hold previous values. Outside the updating phase, previous values are identical to current values, so they carry no change information.

In Python, a [listener](../how-to-guides/table-listeners-python.md) receives a [`TableUpdate`](https://deephaven.io/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate) object. [`modified_prev`](https://deephaven.io/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableUpdate.modified_prev) reads previous values at the modified rows' current row keys. Previous values are keyed by each row's key before this cycle's [shifts](#shifts), so if the same update also shifts the modified rows, as a sorted table's update can, `modified_prev` can return values from the wrong rows. The example below listens to a [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) result, which modifies its rows without shifting them:

```python ticking-table order=null reset
from deephaven import time_table
from deephaven.table_listener import listen


def on_update(update, is_replay):
    # Read current and previous values of the modified rows (dict of column name -> NumPy array)
    modified_data = update.modified(cols="Value")
    modified_prev_data = update.modified_prev(cols="Value")

    # Example: compute change in values
    if modified_data and len(modified_data["Value"]) > 0:
        deltas = modified_data["Value"] - modified_prev_data["Value"]
        print(f"Change in Value for modified rows: {deltas}")


# A last_by result modifies its one row each cycle and never shifts it
my_table = time_table("PT1s").update("Value = i").last_by()
handle = listen(my_table, on_update)
```

## Scaling table update processing

Now that we've described the data structures backing a Deephaven table and the components of a table update notification, you should have a pretty good grasp of the kinds of data and changes Deephaven can operate on. Let's fit that knowledge into a wider context. This section discusses a few of the additional design features of Deephaven's DAG-driven table update processing that allow for scaling to shorter update cycles and larger problem sizes.

### Dependency-respecting parallelism

When enough compute and I/O resources are available, the engine can process table updates in parallel on multiple threads. Parallel processing gives a significant speedup under two conditions:

- The DAG is wide, with many inputs or with parent tables that have many children.
- There is little contention for data access.

Each table within the [DAG](./dag.md) produces at most one update notification for its dependent listeners per cycle, and must not further mutate its current state after producing it. Delivering an update to a listener is itself a unit of work, which the engine also calls a notification. The engine schedules that work as follows:

- The work scheduler runs notifications on a thread pool, so it can process several at once when more than one is ready.
- Some operations, such as [`update`](../reference/table-operations/select/update.md), [`select`](../reference/table-operations/select/select.md), and [`where`](../reference/table-operations/filter/where.md), can also split the work of a single notification across that pool.

Some tables have multiple refreshing parents, such as the results of a [join](../how-to-guides/joins-exact-relational.md) or a [merge](../reference/table-operations/merge/merge.md) of ticking tables. To give each such table a single consistent update, the engine uses a [`MergedListener`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/MergedListener.html). Its work can't run until all parents are _satisfied_. A parent is satisfied when either of these is true:

- It has delivered its update for this cycle.
- The engine can prove it won't send one this cycle.

This safety constraint complicates the work scheduler, but allows downstream listeners to operate on complete, consistent data. To listen to several tables from your own code, see [Listen to multiple ticking tables](../how-to-guides/table-listeners-python.md#listen-to-multiple-ticking-tables).

### Consistent, concurrent initialization and snapshotting

Reading everything above, you might reasonably wonder: if Deephaven processes table updates continuously, one update cycle after another, when and how can code safely read data or create new derived tables? There are three answers to this question, depending on the use case:

1. **Block updates with a lock.** In the simplest case, it's enough to block update processing. Other subsystems running in-process with a Deephaven query engine can acquire a [shared lock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#sharedLock()) for the duration of any otherwise unsafe operations, which keeps data consistent across all nodes in the DAG. When you run a command in the Deephaven console, the engine holds the update graph's [exclusive lock](https://deephaven.io/core/javadoc/io/deephaven/engine/updategraph/UpdateGraph.html#exclusiveLock()) for you, which also blocks update processing.
2. **Work inside the update cycle.** Sometimes it's preferable to perform potentially unsafe operations from _within_ the update processing system. There are two ways to accomplish this:
   - Perform work reactively in a [table listener](../how-to-guides/table-listeners-python.md). This is ideal for publishing updates to external subscribers or reactive systems.
   - Schedule a special terminal notification, which runs after all other notifications in the cycle, once the logical clock has returned to idle but while the engine still holds the exclusive lock. This is ideal for post-update maintenance.
3. **Read concurrently with an optimistic snapshot.** Read data concurrently with update processing, then retry or fall back to a lock if the read can't be proven consistent. The rest of this section explains how this works.

The first two approaches block or delay update processing while they run, which slows how quickly real-time results appear. The third strategy, on the other hand, gains concurrency in exchange for giving up a guarantee that any one attempt succeeds.

Recall that the logical clock is always in one of two phases, _updating_ or _idle_. It also counts cycles with a _step_ number. External code can read the phase and step together, atomically, without a lock. Each table also records the last step on which it finished its update.

With that information, a concurrent consumer decides two things:

- Which data to read. If the cycle is idle, or the source table has already finished updating on the current step, the consumer reads the current row set and column source data. Otherwise, it reads the previous row set and previous column values, both described in [Removed or modified values](#removed-or-modified-values).
- Whether the read succeeded. If a new cycle started while the consumer was reading, or the cycle finished while it was reading previous values, the result can't be proven consistent. The consumer then retries the read, and it falls back to acquiring the shared lock if retries keep failing.

Deephaven provides utilities that encapsulate all of this complexity, for internal use as well as user code. These utilities let many table operations initialize concurrently with the update cycle. Client-driven snapshots and table subscriptions through Deephaven's implementation of [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html), such as the viewports the web UI uses to display ticking tables, use them too.

For practical guidance, see [Initialization and updates](../how-to-guides/initialization-and-updates.md) and [Multithreading: synchronization, locks, and snapshots](./query-engine/engine-locking.md).

None of these consistent data access mechanisms obviates the need to use good engineering sense. Developers should keep interactions with updating data as efficient and infrequent as possible. That said, together these strategies represent a toolbox with a wealth of possibilities for real-time application development when harnessed to the rest of the Deephaven query engine.

### Multi-process data pipelines

Deephaven also provides mechanisms for moving table data consistently between processes. Our [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) implementation uses custom metadata to implement a protocol we call [Barrage](./what-is-barrage.md). Barrage communicates table updates over a language-agnostic gRPC API, using the model described in [Describing table updates](#describing-table-updates). Our [Apache Kafka](./kafka-basic-terms.md) integration allows for streaming data ingestion and publication via one of the most popular distributed event streaming platforms in the world.

Together with the Deephaven query engine, or with external publishers and subscribers that understand Apache Arrow Flight and Barrage or Apache Kafka, these mechanisms let you build a multi-process DAG with remote links from publisher to subscriber. Each remote link is a simple primitive that processes data consistently and asynchronously. The building blocks are in place for a wide variety of real-time, data-driven applications.

## Concluding thoughts

The update model described above is a cornerstone of Deephaven's data engine. It is fundamental to Deephaven's unique and empowering approach to building modern data-driven applications and analytics. Developers no longer need to turn to stream-oriented systems or snapshot-driven analysis to serve real-time data needs. Deephaven handles the inherent complexity of table updates. That frees developers to concentrate on their domain-specific problems without unnecessary compromises.

## Related documentation

- [Core API design](./deephaven-core-api.md)
- [Directed acyclic graph (DAG)](./dag.md)
- [Kafka basic terminology](./kafka-basic-terms.md)
- [Deephaven Barrage](/barrage/docs)
- [What is Barrage?](./what-is-barrage.md)
- [Table listeners](../how-to-guides/table-listeners-python.md)
- [Initialization and updates](../how-to-guides/initialization-and-updates.md)
- [Multithreading: synchronization, locks, and snapshots](./query-engine/engine-locking.md)
