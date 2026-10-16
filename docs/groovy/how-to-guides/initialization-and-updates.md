---
title: Initialization and updates
---

Deephaven runs your script in two phases: initialization and real-time updates. Ticking tables, which update as new data arrives, behave differently in each phase. This guide explains the two phases and covers safe patterns for reading ticking tables from your script, from listeners, and from threads you start yourself.

## How Deephaven executes your script

Deephaven execution has two phases:

1. **Initialization** — Your script runs, creating tables and building the dependency graph ([DAG](../conceptual/dag.md)). Tables from external sources, such as [Kafka](./data-import-export/kafka-stream.md), have no data yet.
2. **Real-time updates** — Data flows in continuously. The engine propagates changes through the DAG. [Listeners](./table-listeners-groovy.md) fire.

The engine component that processes real-time updates is the _update graph_. It works in _update cycles_: in each cycle, it applies new data from sources and propagates the changes through the DAG. For more details on how updates propagate, see [Incremental update model](../conceptual/table-update-model.md).

Deephaven runs each console command and [Application Mode](./application-mode.md) script while holding the update graph's [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock), so no update cycle can run while your script runs. This has two consequences:

- Reads in your script see a consistent table. Your script doesn't need a lock or a snapshot for consistency.
- Data from external sources, such as a [Kafka consumer](./data-import-export/kafka-stream.md), can't arrive until the script finishes, or until it calls [`awaitUpdate`](../reference/table-operations/table-listeners/await-update.md), which releases the lock while it waits. A read right after you create such a table returns nothing.

Code on threads you start yourself doesn't hold this lock. See [Use locking for reads from your own threads](#use-locking-for-reads-from-your-own-threads-advanced).

## Safe patterns

### Let the engine do the work

The safest approach is to express your logic as Deephaven table operations rather than reading values into Groovy variables. The engine keeps every table operation consistent as data ticks.

```groovy ticking-table order=null
source = timeTable("PT1s").update("X = ii", "Y = X * 2")

// Filtering and aggregating - all safe
filtered = source.where("X > 10")
aggregated = source.view("X", "Y").sumBy()
```

### Use `awaitUpdate` to wait for data

In [Application Mode](./application-mode.md) scripts, which run when the server starts, use [`awaitUpdate`](../reference/table-operations/table-listeners/await-update.md) to wait for external data before proceeding:

```groovy syntax
import io.deephaven.engine.table.vectors.ColumnVectors

// trades is a ticking table from an external source, such as a Kafka consumer

// Wait for the first update before extracting data
while (trades.isEmpty()) {
    trades.awaitUpdate()
}

// Now safe to read - data has arrived
firstValue = ColumnVectors.ofDouble(trades, "Price").get(0)
```

[`awaitUpdate`](../reference/table-operations/table-listeners/await-update.md) can occasionally return early on a spurious wakeup, so the example loops until the table has rows. You can specify a timeout: `trades.awaitUpdate(5000)` returns `false` if no update occurs within 5 seconds.

### Use `snapshot` for point-in-time extraction

To keep a static copy of a ticking table's current data, use [`snapshot`](../reference/table-operations/snapshot/snapshot.md). This example starts the [time table](./time-table.md) 10 seconds in the past, so it already has rows when the snapshot runs:

```groovy ticking-table order=null
source = timeTable(minus(now(), parseDuration("PT10s")), "PT0.5s").update("X = ii")

// Static copy: later update cycles don't change it
staticCopy = source.snapshot()
println "Captured ${staticCopy.size()} rows"
```

### Use `snapshotWhen` for periodic extraction

To extract data at controlled intervals, use [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md):

```groovy ticking-table order=null
source = timeTable("PT0.1s").update("X = ii")
// The "trigger" table updates every 5 seconds
trigger = timeTable("PT5s").renameColumns("TriggerTime = Timestamp")

// On every change to "trigger", snapshot with a consistent view
periodicSnapshot = source.snapshotWhen(trigger)
```

### Use listeners for ongoing updates

For reacting to changes, use [table listeners](./table-listeners-groovy.md):

```groovy ticking-table order=null reset
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

source = timeTable("PT1s").update("X = ii")

listener = new InstrumentedTableUpdateListenerAdapter(source, false) {
    @Override
    void onUpdate(TableUpdate upstream) {
        println "Added ${upstream.added().size()} rows"
        // Safe value extraction example:
        if (upstream.added().isNonempty()) {
            latestX = source.getColumnSource("X").get(upstream.added().lastRowKey())
        }
    }
}

source.addUpdateListener(listener)
```

> [!CAUTION]
> Keep listener execution fast. Until a running listener returns, the current update cycle can't finish and the next one can't start.

A listener that keeps state, such as a running total, usually needs to start from the rows the table already has. The [`InstrumentedTableUpdateListenerAdapter`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/InstrumentedTableUpdateListenerAdapter.html) above can't replay those rows. There are two ways to start from them:

- **Read the existing rows first**: Read them in the script, for example from a [`snapshot`](../reference/table-operations/snapshot/snapshot.md), then call [`source.addUpdateListener(listener)`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#addUpdateListener(io.deephaven.engine.table.TableUpdateListener)). The script holds the [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock), so no update cycle runs in between and the listener doesn't miss any rows.
- **Replay the existing rows**: Extend [`ShiftObliviousInstrumentedListenerAdapter`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/ShiftObliviousInstrumentedListenerAdapter.html) instead. Register it with [`source.addUpdateListener(listener, true)`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#addUpdateListener(io.deephaven.engine.table.ShiftObliviousListener,boolean)). The `true` argument replays the existing rows as the listener's first update, as [Replay data](./table-listeners-groovy.md#replay-data) shows.

### Use locking for reads from your own threads (advanced)

Code on a thread you start yourself doesn't hold the exclusive lock that script code runs under. When several reads on such a thread must see the same update cycle, hold the update graph's [shared lock](../conceptual/query-engine/engine-locking.md#shared-lock) around all of them:

```groovy ticking-table order=null
source = timeTable("PT0.5s").update("X = ii")
totals = source.view("X").sumBy()

// Run the reads on a thread you start yourself
Thread.start {
    source.getUpdateGraph().sharedLock().doLocked {
        // Tables are consistent while lock is held
        println "Source rows: ${source.size()}, totals rows: ${totals.size()}"
    }
}
```

The thread can't take the shared lock until the script finishes and releases the exclusive lock. Don't call the thread's `join` method in the same script, or the script hangs.

> [!CAUTION]
> Keep lock duration minimal. While your thread holds the shared lock, no update cycle can run.

## What not to do

- **Don't read ticking tables from a thread you started unless you hold a lock.** Script code already runs under the [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock), but code on other threads doesn't. Hold the [shared lock](../conceptual/query-engine/engine-locking.md#shared-lock), as shown in [Use locking for reads from your own threads](#use-locking-for-reads-from-your-own-threads-advanced), or read from a [`snapshot`](../reference/table-operations/snapshot/snapshot.md).
- **Don't read undeclared ticking tables inside a listener.** The engine doesn't guarantee that other tables have finished updating for the current cycle. If the listener needs another table, register it as a [dependency](./table-listeners-groovy.md#dependent-tables).
- **Don't hold locks during long computations.** On a thread you start yourself, extract data quickly inside [`doLocked`](/core/javadoc/io/deephaven/util/locks/FunctionalLock.html), then process after the block. Script code holds the exclusive lock for its whole run, so run long computations on your own thread rather than in the script itself.
- **Don't mix data from different update cycles.** Row data and row keys saved from an earlier cycle may no longer match the table, because rows can be removed, modified, or shifted. Instead:
  - To carry information to the next cycle, store only computed results, not raw row data.
  - To compare before and after values within one update, read the previous values of the modified rows, as the [modified-rows example in Table listeners](./table-listeners-groovy.md#access-table-data) shows.

## Related documentation

- [Table listeners](./table-listeners-groovy.md) — Full guide to listeners in Groovy
- [Incremental update model](../conceptual/table-update-model.md) — How updates work internally
- [Synchronization and locking](../conceptual/query-engine/engine-locking.md) — Advanced locking
