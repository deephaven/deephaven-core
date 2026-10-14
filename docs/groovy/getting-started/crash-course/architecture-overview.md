---
title: An Overview of Deephaven's Architecture
---

Deephaven's power is largely due to the concept that _everything_ is a table. Think of Deephaven tables like spreadsheets or database tables, except that they support real-time operations! The Deephaven table is the key abstraction that unites static and real-time data. This chapter covers the three main pieces of Deephaven's architecture: tables and the operations that act on them, the engine that runs your queries, and the web user interface.

## Tables and table operations

Beyond representing data as tables, Deephaven lets you manipulate, transform, extract, and analyze that data to produce valuable insight. Deephaven represents transformations to the data stored in a table as _operations_ on that table. You can chain these [table operations](./table-ops.md) together to transform the data step by step:

```groovy order=resultTable,sourceTable
sourceTable = newTable(stringCol("Group", "A", "B", "A", "B"), intCol("Data", 1, 2, 3, 4))

resultTable = sourceTable.updateView("NewData = Data * 2")
    .sumBy("Group")
    .renameColumns("DataSum=Data", "NewDataSum=NewData")
```

Here's what each step does:

- [`newTable`](../../reference/table-operations/create/newTable.md) builds a small static table, which the [next chapter](./create-tables.md) covers in detail.
- [`updateView`](../../reference/table-operations/select/update-view.md) adds a `NewData` column.
- [`sumBy`](../../reference/table-operations/group-and-aggregate/sumBy.md) sums the `Data` and `NewData` columns for each value in `Group`.
- [`renameColumns`](../../reference/table-operations/select/rename-columns.md) gives the sums clearer names.

This paradigm of representing data transformations with methods that act on tables is not new. However, Deephaven takes this a step further. In addition to its table operations, Deephaven provides a large library of functions that you call through _query strings_. You pass query strings _into_ table operations. The `"NewData = Data * 2"` argument in the example above is a query string. Query strings are one of Deephaven's most powerful features. The [query strings chapter](./query-strings.md) covers them in detail.

Together, tables, the table operations that act on them, and the query strings inside those operations make up the _Deephaven Query Language_, or DQL. Code written in DQL, like the example above, is a _Deephaven query_. The fundamental principle of the Deephaven experience is this:

<Pullquote>
Most Deephaven queries are indifferent to whether the underlying data source is static or streaming.
</Pullquote>

The same query written to analyze data from a simple CSV file can usually analyze a similarly structured live dataset without changing the query itself. A few operations, such as [`rangeJoin`](../../reference/table-operations/join/range-join.md), support only static tables.

### Immutable schema, mutable data

Every Deephaven table has a _schema_: the number of columns, the column names, and their data types. A table's schema _cannot_ change after the table is created. Even so, the data stored in a table can change. Here's a simple example that uses [`timeTable`](../../reference/table-operations/create/timeTable.md):

```groovy test-set=1 ticking-table order=null
// Create a table that ticks once per second — defaults to a single "Timestamp" column
tickingTable = timeTable("PT1s")
```

![A ticking table with a Timestamp column that gains a row each second](../../assets/tutorials/crash-course/crash-course-1.gif)

The table gains a new row each second, but its schema stays the same. Deephaven calls a table whose data changes over time a _ticking_ table. Earlier sections of this page call this kind of data real-time, live, or streaming. All four terms mean the same thing.

Some queries appear to modify a table's schema. They may add or remove a column, modify a column's type, or rename a column. For example, the following query appears to rename the `Timestamp` column in the table to `RenamedTimestamp`:

```groovy test-set=1 ticking-table order=null
// Apply 'renameColumns' table operation to tickingTable
tickingTable = tickingTable.renameColumns("RenamedTimestamp=Timestamp")
```

![The same ticking table with the column renamed to RenamedTimestamp](../../assets/tutorials/crash-course/crash-course-2.gif)

The original table's schema is immutable, so no operation can rename its columns. Instead, [`renameColumns`](../../reference/table-operations/select/rename-columns.md) creates a new table with a new schema, and the assignment points the `tickingTable` variable at that new table. Operations like `renameColumns` reuse the existing column data, so the new table uses very little additional memory.

Because table operations return new tables, Deephaven tables do not support in-place operations, which would change the original table:

```groovy skip-test
// Attempted in-place operation — DON'T DO THIS! The result is discarded and tickingTable is not changed.
tickingTable.updateView("NewColumn = 1")
```

That operation creates a new table and immediately discards it, so `tickingTable` itself is unchanged. Instead, always assign the result of a table operation to a variable, either a new one or the same one:

```groovy test-set=1 ticking-table order=newTickingTable,tickingTable
tickingTable = tickingTable.updateView("NewColumn = 1")
newTickingTable = tickingTable.updateView("DoubledColumn = NewColumn * 2")
```

This is the same [`updateView`](../../reference/table-operations/select/update-view.md) you used on the static `sourceTable` earlier. The query code doesn't change when the data is ticking.

## The engine

The Deephaven engine implements everything you've seen so far. Every Deephaven query runs through the engine, which processes both static and streaming data.

Internally, the engine represents queries as [directed acyclic graphs (DAGs)](../../conceptual/dag.md). In the graph:

- Each table is a node.
- Each table operation connects one or more parent (upstream) tables to the child (downstream) table it produces.

The engine processes data changes in repeating rounds called [_update cycles_](../../conceptual/table-update-model.md). In each update cycle:

- When a parent table changes, the engine sends an update notification describing those changes to its child tables.
- Child tables generally process only those changes, not the whole table.

This incremental update model is key to Deephaven's efficiency.

For the sake of efficiency and scalability, the engine is implemented mostly in [Java](https://en.wikipedia.org/wiki/Java_(programming_language)). Groovy runs on the Java Virtual Machine, so your Groovy queries work directly with the Java data structures that hold a table's data and the Java code that implements its operations.

## User interface

The Deephaven user interface (UI) is a web-based application that runs in your browser and communicates with the Deephaven engine using the [JavaScript API](../../how-to-guides/use-jsapi.md). In the UI, you can explore tables, write queries, and visualize data.

You can also extend the UI with [JavaScript plugins](../../how-to-guides/configuration/js-plugins.md) that add custom widgets and visualizations.

Next, [Create Your First Tables](./create-tables.md) shows how to build static and ticking tables of your own.
