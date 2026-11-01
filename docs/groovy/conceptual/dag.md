---
title: Deephaven's live directed acyclic graph (DAG)
---

<div className="comment-title">

Representing queries as DAGs allows efficient, real-time updates

</div>

A directed acyclic graph (DAG) is a conceptual representation of a series of activities. The graph depicts the order of each activity as a set of nodes connected by lines (edges) that illustrate the flow from one activity to another.

DAGs are useful for representing many different types of flows, including data processing flows. They make the order of steps in a processing flow easy to see and organize, especially in large-scale flows. Deephaven uses DAGs to organize the flow of real-time data through queries.

## Why DAGs for real-time data?

Traditional batch processing typically reprocesses an entire dataset even when only a small portion changes. Deephaven's DAG-based execution model avoids that and provides significant advantages for real-time data processing:

- **Incremental updates**: Most operations process only the rows a change affects, not entire tables. See [Incremental updates](#incremental-updates).
- **Automatic dependency tracking**: The engine tracks which tables depend on each source table (a table where data enters the graph), so you never have to tell it which tables to recompute or in what order. See [Update graph cycles](#update-graph-cycles).
- **Efficient resource usage**: Portions of the DAG whose inputs never change never recompute. See [Incremental updates](#incremental-updates).
- **Clear data lineage**: The graph structure makes it easy to trace how data flows from sources to results. See [Query as a graph](#query-as-a-graph).
- **Natural parallelization**: The engine can process independent branches of the DAG concurrently. See [Parallelizing query updates](./query-engine/parallelization.md#parallelizing-query-updates).

## Query as a graph

Deephaven's query syntax is natural and readable: a query reads like a sequence of table operations. Under the hood, each table operation adds a node to a DAG, which the engine uses for efficient real-time processing. Let's look at an example to understand DAGs.

```groovy order=t1,t2,t3 test-set=dag-example
t1 = timeTable("PT1S").update("Label = (ii % 2)")
t2 = t1.lastBy("Label")
t3 = t1.naturalJoin(t2, "Label", "T2 = Timestamp")
```

Here, table `t1` is a real-time table with two columns, `Timestamp` and `Label`. The [time table](../reference/table-operations/create/timeTable.md) adds a new row every second, and `Label` alternates between zero and one. Table `t2` uses [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md) to hold the most recent row for each `Label` value. Table `t3` uses [`naturalJoin`](../reference/table-operations/join/natural-join.md) to take the most recent `Timestamp` for each `Label` from `t2` and add it to `t1`.

Represented as a DAG, this query looks like:

<Svg src='../assets/conceptual/dag-updated.svg' style={{height: 'auto', maxWidth: '900px'}} />

Here you can see each part of the query as a set of connected components, which together form a graph. Each node in this graph is a table produced by one operation, and each edge points from a table to a table computed from it. The graph starts with a source table, which is a table where data enters the graph. In this query, the source table is a [time table](../reference/table-operations/create/timeTable.md). In real time, data flows in one direction, from the source tables through the graph, dynamically updating the tables.

A table can only be built from tables that already exist, so no table can depend on itself, and the graph has no loops. This is where the "directed acyclic" part of the DAG name comes from.

The variables `t1`, `t2`, and `t3` are references to tables within the DAG. You use them to display the tables and to build further queries on them. Some tables have no variable of their own, such as the [`timeTable`](../reference/table-operations/create/timeTable.md) result that `t1` is built on. Such a table stays in the DAG as long as a table you reference depends on it, but you cannot access it directly. Garbage collection removes a table that nothing references, as the next section shows.

### Garbage collection and the DAG

When no variable references a table and no table still in use depends on it, the JVM can garbage collect it. This automatic cleanup frees the table's memory and removes it from the DAG, which keeps memory usage efficient. For example:

```groovy order=null test-set=dag-example
// The DAG has three named tables: t2 depends on t1, and t3 depends on both
t3 = null  // Remove reference to the join result

// Now the DAG has two named tables: t2 depends on t1
// The naturalJoin result is removed once the JVM garbage collects it
```

When you set `t3` to `null`, nothing references the result of the [`naturalJoin`](../reference/table-operations/join/natural-join.md), so that table becomes eligible for garbage collection. Once the JVM collects it, the table and its associated computations are removed from the DAG. Until then, the table keeps updating. However, `t1` and `t2` remain because they still have active references.

To remove the table and its computations immediately, create them in a [liveness scope](./liveness-scope-concept.md) and release it.

<Svg src='../assets/conceptual/dag-garbage-collection.svg' style={{height: 'auto', maxWidth: '900px'}} />

If you later need `t3` again, rerun the [`naturalJoin`](../reference/table-operations/join/natural-join.md):

```groovy order=t3 test-set=dag-example
// Recreate the join, which adds it back to the DAG
t3 = t1.naturalJoin(t2, "Label", "T2 = Timestamp")
```

## Incremental updates

The Deephaven query engine is smart. When data flows through the DAG, it does not recompute entire tables. Each operation processes only the rows affected by a change, so a one-row change usually costs far less than a full recompute. How many rows are affected depends on the operation. For example, a change to one row on the right side of a [join](../how-to-guides/joins-exact-relational.md) updates every matching row on the left side.

The engine tells each dependent table what changed by sending it an update notification. An update notification lists the rows that were added, removed, or modified, the rows whose row keys shifted, and which columns the modifications touched. A table whose inputs never change receives no update notifications, so the static portions of a query never recompute. This is one reason Deephaven is so fast and scalable.

### Update graph cycles

Deephaven groups real-time data changes and processes each group in one update graph cycle. The update graph is the engine component that runs the DAG and its cycles. By default, the engine aims to start a cycle every 1000 ms, but a cycle can take longer when there is more work than fits in that interval. To change the interval, see [Periodic update graph configuration](./periodic-update-graph-configuration.md).

Here's how an update graph cycle works:

1. **Collect changes**: The engine collects every change made to source tables since the previous cycle.
2. **Propagate updates**: Changes flow through the DAG in topological order (sources first, then dependents).
3. **Process each table once**: Each table processes all its input changes for the cycle in one operation.
4. **Run listeners**: A [listener](#custom-listeners) (code that runs when a table changes) runs once its table, and any tables it declares as dependencies, have finished updating for the cycle. Listeners do not run in one fixed global order, and listeners on independent tables can run at the same time.
5. **Finish the cycle**: The cycle ends once every table has processed its changes, so all tables reflect the same source changes.

Steps 2 to 4 repeat table by table in dependency order, so different parts of the DAG can be at different steps at the same time.

For example, take the query from [Query as a graph](#query-as-a-graph), where `t2` holds the most recent row of `t1` for each `Label`, and `t3` uses [`naturalJoin`](../reference/table-operations/join/natural-join.md) to join `t2` onto `t1`. Suppose the previous cycle ran long, so about 5 seconds pass and `t1` receives 5 new rows in one cycle:

- `t1` processes 5 additions and notifies `t2` and `t3`.
- `t2` (the [`lastBy`](../reference/table-operations/group-and-aggregate/lastBy.md)) updates the rows for the affected `Label` values.
- `t3` (the [`naturalJoin`](../reference/table-operations/join/natural-join.md)) incorporates both sets of changes. It adds the 5 new rows, and because `t2` changed, it also modifies every existing row whose `Label` matches a changed `t2` row.
- When the cycle completes, all three tables reflect the same 5 new rows.

<Svg src='../assets/conceptual/dag-ug-cycle.svg' style={{height: 'auto', maxWidth: '1000px'}} />

You can learn more about these update notifications in our concept guide on [the table update model](./table-update-model.md).

## Performance and update cycles

Thinking in terms of DAGs, [update graph cycles](#update-graph-cycles), and update notifications helps explain the real-time performance of a Deephaven query. Each source table change creates a cascade of changes. The engine must process that cascade before the next update graph cycle can begin. Some source table changes trigger many update notifications, many changed rows, or slow operations. The engine may then be unable to finish the updates before the next cycle is due. The next cycle starts late, and the query becomes less responsive.

Deephaven's performance analysis tools help you dig into an unresponsive query and locate the operations that slow down update graph cycles. To find and fix slow operations, see:

- [Performance tables](../how-to-guides/performance/performance-tables.md): The update performance log ([`updatePerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#updatePerformanceLog())) shows how much time each operation spends processing updates. It sums that time over each reporting interval, which spans many update graph cycles. The update performance ancestors log ([`updatePerformanceAncestorsLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#updatePerformanceAncestorsLog())) shows which upstream operations feed each operation.
- [Reduce update frequency](../how-to-guides/performance/reduce-update-frequency.md): Use [`snapshotWhen`](../reference/table-operations/snapshot/snapshot-when.md) with a slower trigger table to update a result periodically instead of on every change.
- [Track processing time and measure latency](../how-to-guides/performance/track-processing-time.md): Record when Deephaven processes row modifications to measure end-to-end latency.

## Extending the DAG

The engine builds the DAG from your table operations. You can also add your own code to the DAG as a [listener](../how-to-guides/table-listeners-groovy.md), and you can share tables between the DAGs on different Deephaven servers.

### Custom listeners

Most users never work with update notifications directly, but you can write a custom listener, which is code that runs when a table changes. A listener is a node in the DAG, but it is not a table. It receives the update notifications of the table it listens to and produces no table of its own. For a full explanation, see [Listen to ticking tables](../how-to-guides/table-listeners-groovy.md).

Here's a practical example of a custom listener that monitors disk usage:

```groovy ticking-table order=null
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

// Create a monitoring table
diskMonitor = timeTable("PT5S").update(
    "Server = `server-` + (ii % 3)",
    "DiskUsagePct = 60 + Math.random() * 35"  // Simulated disk usage
)

// Filter for critical alerts (>90% usage)
criticalAlerts = diskMonitor.where("DiskUsagePct > 90")

// Custom listener that "sends an alert" when disk usage is critical
handleAlert = new InstrumentedTableUpdateListenerAdapter("DiskAlertListener", criticalAlerts, false) {
    @Override
    void onUpdate(TableUpdate upstream) {
        def server = criticalAlerts.getColumnSource("Server", String.class)
        def usage = criticalAlerts.getColumnSource("DiskUsagePct", double.class)
        upstream.added().forAllRowKeys { long rowKey ->
            println "ALERT: ${server.get(rowKey)} disk usage at ${String.format('%.1f', usage.getDouble(rowKey))}%"
            // In production: sendEmail() or postToSlack()
        }
    }
}

// Attach the listener to the table
criticalAlerts.addUpdateListener(handleAlert)
```

In this example, a new row in `criticalAlerts` means a server's disk usage exceeds 90%. Each time one appears, the listener prints an alert. In production, it could send the alert to your monitoring system instead.

### Sharing tables across servers

DAGs are not limited to one server. A Deephaven server can use a [URI](../how-to-guides/use-uris.md) to subscribe to a table on another server. The subscription uses the [Barrage](./what-is-barrage.md) protocol, so the downstream server receives the table's updates as they happen and does not recompute them. For example, you may have one server perform a difficult or secret calculation, and another server use the shared results. Because it receives only the table's data, not the query code that produced it, it never sees the secret sauce that went into the calculation.

This lets several servers reuse an expensive computation instead of each repeating it.

## Related documentation

- [Create a time table](../how-to-guides/time-table.md)
- [Core API design](./deephaven-core-api.md)
- [Table update model](./table-update-model.md)
- [Liveness scopes](./liveness-scope-concept.md)
- [Periodic update graph configuration](./periodic-update-graph-configuration.md)
- [Listen to ticking tables](../how-to-guides/table-listeners-groovy.md)
- [Use URIs to share tables](../how-to-guides/use-uris.md)
- [What is Barrage?](./what-is-barrage.md)
- [Performance tables](../how-to-guides/performance/performance-tables.md)
- [Reduce update frequency](../how-to-guides/performance/reduce-update-frequency.md)
