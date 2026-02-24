---
title: Deephaven’s live Directed-Acyclic-Graph (DAG)
sidebar_label: Live DAG
---

<div className="comment-title">

Representing queries as DAGs allows efficient, real-time updates

</div>

A directed acyclic graph (DAG) is a conceptual representation of a series of activities. The graph depicts the order of each activity as a set of nodes connected by lines (edges) that illustrate the flow from one activity to another.

DAGs are useful for representing many different types of flows, including data processing flows. They make it easy to clearly visualize and organize the order of steps associated for these jobs, particularly large-scale processing flows. In Deephaven, DAGs are used to organize the flow of real-time data through queries.

## Query as a graph

Deephaven’s query syntax is very natural and readable. Under the hood, queries are converted into directed acyclic graphs (DAGs) for efficient real-time processing. Let’s look at an example to understand DAGs.

```python order=t1,t2,t3
from deephaven import time_table

t1 = time_table("PT1S").update(formulas=["Label=(ii%2)"])
t2 = t1.last_by(by=["Label"])
t3 = t1.natural_join(table=t2, on=["Label"], joins=["T2=Timestamp"])
```

Here, table `t1` is a real-time table with two columns: `Timestamp` and `Label`. A new row is appended every second, and `Label` alternates between zero and one. Table `t2` contains the most recent row for each Label value, and `t3` joins the most recent `Timestamp` for a `Label`, from `t2`, onto `t1`.

Represented as a DAG, this query looks like:

<Svg src='../assets/conceptual/dag.svg' style={{height: 'auto', maxWidth: '500px'}} />

Here you can see each part of the query as connected components - aka a "graph". The graph starts with the data sources; in this case, a [time table](../reference/table-operations/create/timeTable.md). In real time, data flows from the data sources through the graph, dynamically updating the tables. Because this data flow is in one direction, the graph has no loops. This is where the "directed acyclic" part of the DAG name comes from.

The variables `t1`, `t2`, and `t3` are simply references to tables within the DAG. These variables allow the tables to be displayed, as well as used in further query operations. If a table is not associated with a variable, it is still part of the DAG, but it is not accessible to users.

There is one exception. Tables that are not used in the calculation of any variables are garbage collected and removed from the DAG. For example, if the variable `t3` is set to `None`, there are now no references to the result of the [`natural_join`](../reference/table-operations/join/natural-join.md), so that table is removed from the DAG.

## Smart engine

The Deephaven query engine is smart. When data flows through the DAG, instead of recomputing entire tables, only necessary changes are recomputed. For example, if only one row changes, only one row is recomputed. If 11 rows change, only 11 rows are recomputed. If you use static data tables, large sections of the DAG may never recompute. This is one reason Deephaven is so fast and scalable.

When processing real-time data changes, Deephaven batches the changes together at a configurable interval (defaulting to 1000 ms). All of the changes within a batch are processed together. These processing events are called Periodic Update Graph (UG) cycles, and the update notifications propagated through the DAG indicate which rows have been added, modified, deleted, or reindexed (reordered). You can learn more about these update notifications in our concept guide on [the table update model](./table-update-model.md).

Most users never interact directly with update notifications, but it is possible to write custom listeners, which execute code when a table changes. Custom listeners are non-table components of the DAG, which listen to table update notifications. For example, if you have a query that monitors how full disks are on a cluster, you can write a custom listener to send an email or Slack message every time a table in your monitor query gets a new row, indicating that disks are starting to fill up.

Finally, DAGs are not limited to one query or even one host. Preemptive tables allow tables and update notifications to be shared between queries. You may have Query1 perform a difficult or secret calculation. Query2 can use the shared results of Query1 without having to recompute and without being able to see the secret sauce that went into Query1’s calculation.

## Performance analysis

Thinking in terms of DAGs, UG cycles, and update notifications can be insightful when trying to understand the real-time performance of a Deephaven query. Each source table change creates a cascade of changes, which must be processed before the next UG cycle can begin. If the source table changes trigger many update notifications, many changed rows, or slow-to-compute operations, it may be impossible to calculate all of the necessary updates before the next UG cycle is expected to begin — delaying the start of the next UG cycle, and decreasing query responsiveness.

Deephaven’s performance analysis tools help you dig into an unresponsive query to locate which operations are causing slow UG cycles. Once you understand what operations are slow, you can rearrange your query so there are fewer changes to process, add table snapshotting to reduce the frequency of changes, etc.

## Related documentation

- [Create a time table](../how-to-guides/time-table.md)
- [Core API design](./deephaven-core-api.md)
- [Table update model](./table-update-model.md)
