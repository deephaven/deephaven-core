---
title: Deephaven’s live Directed-Acyclic-Graph (DAG)
sidebar_label: Live DAG
---

<div className="comment-title">

Representing queries as DAGs allows efficient, real-time updates

</div>

A directed acyclic graph (DAG) is a conceptual representation of a series of activities. The graph depicts the order of each activity as a set of nodes connected by lines (edges) that illustrate the flow from one activity to another.

DAGs are useful for representing many different types of flows, including data processing flows. They make it easy to clearly visualize and organize the order of steps associated for these jobs, particularly large-scale processing flows. In Deephaven, DAGs are used to organize the flow of real-time data through queries.

## Why DAGs for real-time data?

Deephaven's DAG-based execution model provides significant advantages for real-time data processing:

- **Incremental updates**: Only recompute what changes, not entire tables. If one row updates, only that row is recomputed.
- **Automatic dependency tracking**: The engine knows exactly which tables depend on which sources, eliminating manual orchestration.
- **Efficient resource usage**: Static portions of the DAG never recompute, and unchanged data flows through without processing.
- **Clear data lineage**: The graph structure makes it easy to trace how data flows from sources to results.
- **Natural parallelization**: Independent branches of the DAG can be processed concurrently.

This is fundamentally different from traditional batch processing, where entire datasets are typically reprocessed even when only small portions change.

## Query as a graph

Deephaven’s query syntax is very natural and readable. Under the hood, queries are converted into directed acyclic graphs (DAGs) for efficient real-time processing. Let’s look at an example to understand DAGs.

```python order=t1,t2,t3 test-set=dag-example
from deephaven import time_table

t1 = time_table("PT1S").update(formulas=["Label=(ii%2)"])
t2 = t1.last_by(by=["Label"])
t3 = t1.natural_join(table=t2, on=["Label"], joins=["T2=Timestamp"])
```

Here, table `t1` is a real-time table with two columns: `Timestamp` and `Label`. A new row is appended every second, and `Label` alternates between zero and one. Table `t2` contains the most recent row for each Label value, and `t3` joins the most recent `Timestamp` for a `Label`, from `t2`, onto `t1`.

Represented as a DAG, this query looks like:

<Svg src='../assets/conceptual/dag-updated.svg' style={{height: 'auto', maxWidth: '900px'}} />

Here you can see each part of the query as connected components - aka a “graph”. The graph starts with the data sources; in this case, a [time table](../reference/table-operations/create/timeTable.md). In real time, data flows from the data sources through the graph, dynamically updating the tables. Because this data flow is in one direction, the graph has no loops. This is where the “directed acyclic” part of the DAG name comes from.

The variables `t1`, `t2`, and `t3` are simply references to tables within the DAG. These variables allow the tables to be displayed, as well as used in further query operations. If a table is not associated with a variable, it is still part of the DAG, but it is not accessible to users.

### Garbage collection and the DAG

Tables that are not used in the calculation of any variables are garbage collected and removed from the DAG. This automatic cleanup keeps memory usage efficient. For example:

```python order=t3
# Original DAG has three tables: t1 -> t2 -> t3
t3 = None  # Remove reference to the join result

# Now the DAG only has two tables: t1 -> t2
# The natural_join operation is removed because nothing references it
```

When `t3` is set to `None`, there are no references to the result of the [`natural_join`](../reference/table-operations/join/natural-join.md), so that table and its associated computations are removed from the DAG. However, `t1` and `t2` remain because they still have active references.

<Svg src='../assets/conceptual/dag-garbage-collection.svg' style={{height: 'auto', maxWidth: '900px'}} />

If you later need to recreate `t3`, you can simply rerun the join operation:

```python order=t3 test-set=dag-example
# Recreate the join - this adds it back to the DAG
t3 = t1.natural_join(table=t2, on=["Label"], joins=["T2=Timestamp"])
```

## Smart engine

The Deephaven query engine is smart. When data flows through the DAG, instead of recomputing entire tables, only necessary changes are recomputed. For example, if only one row changes, only one row is recomputed. If 11 rows change, only 11 rows are recomputed. If you use static data tables, large sections of the DAG may never recompute. This is one reason Deephaven is so fast and scalable.

### Update Graph (UG) cycles

When processing real-time data changes, Deephaven batches the changes together at a configurable interval (defaulting to 1000 ms). All of the changes within a batch are processed together. These processing events are called Periodic Update Graph (UG) cycles, and the update notifications propagated through the DAG indicate which rows have been added, modified, deleted, or reindexed (reordered).

Here's how a UG cycle works:

1. **Collect changes**: All modifications to source tables during the cycle period are collected.
2. **Propagate updates**: Changes flow through the DAG in topological order (sources first, then dependents).
3. **Batch processing**: Each table processes all its input changes in one operation.
4. **Notification**: UI and listeners are notified once all updates complete.

For example, in our earlier query, if `t1` receives 5 new rows in one cycle:

- `t1` processes 5 additions and notifies `t2` and `t3`
- `t2` (the `last_by`) processes updates for affected labels
- `t3` (the `natural_join`) incorporates both sets of changes
- All three tables update simultaneously from the user's perspective

<Svg src='../assets/conceptual/dag-ug-cycle.svg' style={{height: 'auto', maxWidth: '1000px'}} />

You can learn more about these update notifications in our concept guide on [the table update model](./table-update-model.md).

### Custom listeners

Most users never interact directly with update notifications, but it is possible to write custom listeners, which execute code when a table changes. Custom listeners are non-table components of the DAG, which listen to table update notifications.

Here's a practical example of a custom listener that monitors disk usage:

```python order=disk_monitor,critical_alerts,handle_alert
from deephaven import time_table
from deephaven.table_listener import listen

# Create a monitoring table
disk_monitor = time_table("PT5S").update(
    [
        "Server = `server-` + (ii % 3)",
        "DiskUsagePct = 60 + Math.random() * 35",  # Simulated disk usage
    ]
)

# Filter for critical alerts (>90% usage)
critical_alerts = disk_monitor.where("DiskUsagePct > 90")


# Custom listener that "sends an alert" when disk usage is critical
def handle_alert(update, is_replay):
    if is_replay:
        return  # Skip historical data

    added_rows = update.added()
    for row in added_rows:
        server = row["Server"]
        usage = row["DiskUsagePct"]
        print(f"ALERT: {server} disk usage at {usage:.1f}%")
        # In production: send_email() or post_to_slack()


# Attach the listener to the table
handle = listen(critical_alerts, handle_alert)
```

In this example, whenever a new row appears in `critical_alerts` (indicating a server exceeds 90% disk usage), the custom listener executes and could send notifications to your monitoring system.

### Cross-query sharing

DAGs are not limited to one query or even one host. Preemptive tables allow tables and update notifications to be shared between queries. You may have Query1 perform a difficult or secret calculation. Query2 can use the shared results of Query1 without having to recompute and without being able to see the secret sauce that went into Query1's calculation.

This distributed DAG capability enables efficient reuse of expensive computations across multiple query sessions, reducing redundant processing and improving overall system performance.

## Performance analysis

Thinking in terms of DAGs, UG cycles, and update notifications can be insightful when trying to understand the real-time performance of a Deephaven query. Each source table change creates a cascade of changes, which must be processed before the next UG cycle can begin. If the source table changes trigger many update notifications, many changed rows, or slow-to-compute operations, it may be impossible to calculate all of the necessary updates before the next UG cycle is expected to begin — delaying the start of the next UG cycle, and decreasing query responsiveness.

### Identifying bottlenecks

Deephaven's performance analysis tools help you dig into an unresponsive query to locate which operations are causing slow UG cycles. Use the Update Graph Processor (UGP) metrics to identify problems:

```python syntax
# from deephaven.ugp import exclusive_lock_metrics

# View which operations take the most time
# metrics = exclusive_lock_metrics()
```

Common performance bottlenecks include:

- **Large joins**: Joining tables with millions of rows on each update.
- **Complex formulas**: Expensive calculations that run on every changed row.
- **Cascading updates**: One small change triggering updates across many dependent tables.
- **Unnecessary computations**: Operations on data that doesn't actually change.

### Optimization strategies

Once you understand what operations are slow, you can optimize your query:

- **Use `coalesce()`**: Reduce update frequency by batching changes.
- **Add `.snapshot()`**: Create periodic snapshots instead of continuous updates.
- **Restructure dependencies**: Break long dependency chains into parallel branches.
- **Pre-aggregate data**: Move expensive aggregations upstream in the DAG.
- **Filter early**: Apply `where()` clauses before expensive operations.

For example, instead of:

```python order=live_data,result
from deephaven import time_table
from deephaven import agg

live_data = time_table("PT1S").update(["Group = ii % 3", "ExpensiveCalc = ii * 2"])

# Expensive: recalculates complex aggregation on every update
result = live_data.agg_by([agg.sum_("ExpensiveCalc")], by=["Group"])
```

Consider:

```python order=result test-set=optimization-example
from deephaven import time_table
from deephaven import agg

live_data = time_table("PT1S").update(["Group = ii % 3", "ExpensiveCalc = ii * 2"])

# Optimized: snapshot reduces update frequency
result = live_data.snapshot().agg_by([agg.sum_("ExpensiveCalc")], by=["Group"])
```

## Related documentation

- [Create a time table](../how-to-guides/time-table.md)
- [Core API design](./deephaven-core-api.md)
- [Table update model](./table-update-model.md)
