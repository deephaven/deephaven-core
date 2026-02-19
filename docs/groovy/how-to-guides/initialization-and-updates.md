---
title: Initialization and updates
sidebar_label: Initialization and updates
---

This guide covers safe patterns for working with ticking tables in Groovy, particularly during script initialization when external data sources may not have data yet.

## How Deephaven executes your script

Deephaven execution has two phases:

1. **Initialization** — Your script runs, creating tables and building the dependency graph ([DAG](../conceptual/dag.md)). External data sources (Kafka, etc.) may not have data yet.
2. **Real-time updates** — Data flows in continuously. Changes propagate through the DAG automatically. Listeners fire.

The key issue: when your script creates a table from an external source, the table may be **empty** during initialization. Data arrives asynchronously. Trying to read data immediately can fail or return nothing.

For more details on how updates propagate, see [Incremental update model](../conceptual/table-update-model.md).

## Safe patterns

### Use `snapshot()` for point-in-time extraction

When you need to read data from a ticking table, create a static copy first:

```groovy ticking-table order=null
source = timeTable("PT0.5s").update("X = ii")

// Create a static copy, then read safely
staticCopy = source.snapshot()
println "Captured ${staticCopy.size()} rows"
```

### Use `snapshotWhen()` for periodic extraction

To extract data at controlled intervals:

```groovy ticking-table order=null
source = timeTable("PT0.1s").update("X = ii")
trigger = timeTable("PT5s").renameColumns("TriggerTime = Timestamp")

// Updates every 5 seconds with a consistent view
periodicSnapshot = source.snapshotWhen(trigger)
```

### Use `awaitUpdate()` to wait for data

In app mode scripts, wait for external data before proceeding:

```groovy syntax
// Wait for the first update before extracting data
trades.awaitUpdate()

// Now safe to read - data has arrived
firstValue = trades.getColumn("Price").get(0)
```

You can specify a timeout: `trades.awaitUpdate(5000)` returns `false` if no update occurs within 5 seconds.

### Use listeners for ongoing updates

For reacting to changes, use [table listeners](./table-listeners-groovy.md):

```groovy ticking-table order=null
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

source = timeTable("PT1s").update("X = ii")

listener = new InstrumentedTableUpdateListenerAdapter(source, false) {
    @Override
    void onUpdate(TableUpdate upstream) {
        println "Added ${upstream.added().size()} rows"
    }
}

source.addUpdateListener(listener)
```

### Use locking for direct access (advanced)

When you must read directly from a ticking table:

```groovy ticking-table order=null
import io.deephaven.engine.context.ExecutionContext

source = timeTable("PT0.5s").update("X = ii")

def ug = ExecutionContext.getContext().getUpdateGraph()
ug.exclusiveLock().doLocked {
    // Table is consistent while lock is held
    println "Row count: ${source.size()}"
}
```

> [!CAUTION]
> Keep lock duration minimal — it blocks all table updates.

## What NOT to do

- **Don't read ticking data without synchronization** — use `snapshot()` or locking.
- **Don't create tables inside listeners** — create derived tables before attaching the listener.
- **Don't hold locks during long computations** — extract data quickly, then process outside the lock.

## Related documentation

- [Table listeners](./table-listeners-groovy.md) — Full guide to listeners in Groovy
- [Incremental update model](../conceptual/table-update-model.md) — How updates work internally
- [Synchronization and locking](../conceptual/query-engine/engine-locking.md) — Advanced locking
