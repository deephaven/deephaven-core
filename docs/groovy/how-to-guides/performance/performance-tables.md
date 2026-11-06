---
title: How to use Deephaven's performance tables for system monitoring
---

This guide shows how to use Deephaven's internal performance tables.

Deephaven provides several ways to monitor query performance and resolve issues as they arise. Many of them are available directly in the user interface. For example, to see how long a query has been running, open the History Panel and look at the command details. You can also hover over the console command, and the tooltip shows the formatted duration of the query.

The most common performance issues are slow initialization, a query failing to update quickly, and a sluggish user interface. If a query seems to be taking too long or throws an error, Deephaven's performance tables are a great place to start. Performance tables are a powerful tool for analyzing performance and debugging issues. They show where the time goes, so you can troubleshoot queries on your own.

Which table to open depends on the problem:

- For slow initialization or a slow command, start with the [Query Operation Performance Tree Table](#query-operation-performance-tree-table). It shows every query with its operations nested underneath, so you can find the slow query and then the slow operation in one place. Once you know the query's [evaluation number](#evaluation-numbers), [Query Operation Performance](#query-operation-performance) narrows the view to that one query.
- For a [ticking](../../conceptual/table-types.md#standard-streaming-tables) query that falls behind, start with the [Update Performance Log](#update-performance-log), which shows how much time each operation spends processing updates in each logging interval (one minute by default).

The [example](#example-find-slow-operations) below shows how to find slow operations in the [Query Operation Performance Tree Table](#query-operation-performance-tree-table), then opens the [Query Operation Performance Log](#query-operation-performance-log) and the [Update Performance Log](#update-performance-log) directly for a small query.

## Evaluation numbers

Several [`PerformanceQueries`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html) methods take an evaluation number as their `evaluationNumber` argument. An evaluation number uniquely identifies a query. A query is one request to the server, such as:

- A command you run in the console by pressing _Enter_.
- A sort, filter, or custom column you apply in the UI.
- A request from a client API application.

A subquery that runs as part of another query gets its own evaluation number. In the subquery's row of the performance tables, the `ParentEvaluationNumber` column holds the evaluation number of the query that started it. The individual operations within a query, such as single method calls, share the query's evaluation number, and each has its own `OperationNumber` in the [Query Operation Performance Log](#query-operation-performance-log).

You can find evaluation numbers in the `EvaluationNumber` column of the tables that [`queryPerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#queryPerformanceLog()) and [`queryOperationPerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#queryOperationPerformanceLog()) return. The [Query Performance Log](#query-performance-log) and [Query Operation Performance Log](#query-operation-performance-log) sections describe these tables.

## Available performance tables

Deephaven's [`TableLoggers`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html) class contains methods that create and open the raw performance logs in the Deephaven IDE, and [`PerformanceQueries`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html) provides derived views and [tree tables](../tree-table.md). Groovy sessions import the [`TableLoggers`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html) methods automatically, so you can call them without a class name.

```groovy order=qpl,qopl,upl,upal,pml,pil,ssl,ss,qptt,qoptt
import io.deephaven.engine.table.impl.util.PerformanceQueries

qpl = queryPerformanceLog()
qopl = queryOperationPerformanceLog()
upl = updatePerformanceLog()
upal = updatePerformanceAncestorsLog()
pml = processMetricsLog()
pil = processInfoLog()
ssl = serverStateLog()

ss = PerformanceQueries.serverState()

qptt = PerformanceQueries.queryPerformanceAsTreeTable()
qoptt = PerformanceQueries.queryOperationPerformanceAsTreeTable()
```

## Example: find slow operations

The tables available from [`TableLoggers`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html) and [`PerformanceQueries`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html) contain _a lot_ of information, and it can seem overwhelming without some context. The following examples show two ways to find the operations that slow a query down. The first is quicker.

### In the performance tree tables

The performance tree tables are useful tools for pinpointing the cause of unexpected slow performance in your queries. They are also the quickest way to find slow operations, because you don't need to add any columns. Open the [Query Operation Performance Tree Table](#query-operation-performance-tree-table) with [`queryOperationPerformanceAsTreeTable`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryOperationPerformanceAsTreeTable()) and sort it in descending order by the `UsageNanos` column so that the slowest queries appear at the top. The screenshots below come from a session that ran the [performance table query](#in-the-performance-logs) below, so your rows will differ:

![The Query Operation Performance Tree Table, sorted by the `UsageNanos` column](../../assets/how-to/qoptt-sorted.png)

Expand a parent row to see the sub-operations that make up its total time and find the ones that slow the query down:

![The sorted Query Operation Performance Tree Table, expanded to show sub-operations](../../assets/how-to/qoptt-expanded.png)

### In the performance logs

To add your own columns, or to examine a ticking query, work from the raw logs. Let's explore an example that focuses on two of the tables: the [Query Operation Performance Log](#query-operation-performance-log) and the [Update Performance Log](#update-performance-log).

Below is the entire query.

<details>
<summary>Performance table query</summary>

```groovy ticking-table order=null
// Key perf tables for this example
upl = updatePerformanceLog()
qopl = queryOperationPerformanceLog()

fast = { x -> return (x * (x + 1)).intdiv(2) }

slow = { x ->
    sleep(250)
    return (x * (x + 1)).intdiv(2)
}

// Static table and query startup performance go to qopl table
t = emptyTable(100).updateView("A = ii")
t2 = t.update("X = fast(A)")
t3 = t2.update("Y = slow(A)")

// Add a duration column and move the key columns to the front for the static or initialization cases
qoplUpdate = qopl.updateView("DurationNanos = EndTime - StartTime").moveColumnsUp(
    "Description",
    "StartTime",
    "EndTime",
    "DurationNanos",
    "CpuNanos",
    "UserCpuNanos",
    "FreeMemoryChange",
    "TotalMemoryChange"
)

// Dynamic table updates go to the upl table
td = timeTable("PT00:00:01").updateView("A = ii")
td2 = td.update("X = fast(A)")
td3 = td2.update("Y = slow(A)")

// Add duration and Ratio columns, move the key columns to the front, and sort by Ratio for the dynamic case
uplUpdate = (
    upl.updateView(
        "IntervalDurationNanos = IntervalEndTime - IntervalStartTime",
        "Ratio = UsageNanos / IntervalDurationNanos"
    )
    .moveColumnsUp(
        "EntryDescription",
        "IntervalStartTime",
        "IntervalEndTime",
        "IntervalDurationNanos",
        "UsageNanos",
        "CpuNanos",
        "UserCpuNanos",
        "Ratio"
    )
    .sortDescending("Ratio")
)
```

</details>

Some key pieces of the above query are worth noting:

- The closures `fast` and `slow` compute the same result. `slow` is slow because it waits a quarter of a second before it returns the result.
- `qoplUpdate` adds a `DurationNanos` column, the time each operation took when its query started.
- `uplUpdate` adds two columns, `IntervalDurationNanos` and `Ratio`. `Ratio` is the fraction of the interval spent performing a particular operation.
- [`sortDescending`](../../reference/table-operations/sort/sort-descending.md) orders `uplUpdate` by `Ratio`, so the operations that take the most time appear at the top.

Two of the [derived views](#derived-views) already have these columns. [Query Operation Performance](#query-operation-performance) has `DurationNanos`, and [Query Update Performance](#query-update-performance) has `IntervalDurationNanos` and `Ratio`. Each view shows only one query, though. Adding the columns to the raw logs covers every query at once.

For the static tables `t`, `t2`, and `t3`, open `qoplUpdate`. Each row is one operation that ran when its query started, and `DurationNanos` shows how long it took. The `Update([Y])` row for `t3` takes about 25 seconds, because `slow` sleeps for a quarter of a second on each of its 100 rows. `Update([X])` for `t2` is fast. The first time the query runs, most of that row's time is formula compilation.

For the [ticking](../../conceptual/table-types.md#standard-streaming-tables) tables `td`, `td2`, and `td3`, open `uplUpdate`:

![The above `uplUpdate` table](../../assets/how-to/perf-ratio.png)

In the image above, `Update([Y])`, which calls the `slow` closure, takes a whopping 25% of each full interval while `td` ticks:

- The [ticking](../../conceptual/table-types.md#standard-streaming-tables) table `td` adds one row every second.
- For each new row, the engine sits and waits a quarter of a second for `slow` to return.

Clearly, `slow` is the bottleneck in this code. Real slowdowns are rarely this obvious, but [sorting](../../reference/table-operations/sort/sort-descending.md) by `Ratio` puts the operations worth optimizing at the top.

## Performance table reference

This section describes the raw performance logs first, then the derived views and [tree tables](../tree-table.md) built from them. Each table's section lists its columns in a collapsible block.

### Raw performance logs

#### Query Performance Log

The Query Performance Log (QPL) contains query-level performance data. Each row describes one [query](#evaluation-numbers), so a Deephaven instance that runs many queries has many rows. For the individual operations within each query, see the [Query Operation Performance Log](#query-operation-performance-log). Create the QPL with [`queryPerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#queryPerformanceLog()).

The most significant columns in this table are:

- `UsageNanos` — Time spent running this query in nanoseconds, summed over all threads.
- `FreeMemory` — The free memory at the end of this query.
- `TotalMemory` — The total size of the JVM heap memory at the end of this query.
- `FreeMemoryChange` — The change in free memory during this query. Other threads can change free memory at the same time, so pair it with `AllocatedBytes`.

<details>
<summary>Query Performance Log columns</summary>

| Column Name              | Data Type | Description                                                                                                                                                                                                                                                                                                                                                                 |
| ------------------------ | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EvaluationNumber`       | `long`    | An increasing integral value for requests made to the worker.                                                                                                                                                                                                                                                                                                               |
| `ParentEvaluationNumber` | `long`    | When this query is a [subquery](#evaluation-numbers), the evaluation number of the query that started it.                                                                                                                                                                                                                                                                   |
| `SessionId`              | `String`  | A unique identifier for the user-session performing the query.                                                                                                                                                                                                                                                                                                              |
| `Description`            | `String`  | A textual description of the query, generally including the name and some of the arguments.                                                                                                                                                                                                                                                                                 |
| `StartTime`              | `Instant` | The time this query began.                                                                                                                                                                                                                                                                                                                                                  |
| `EndTime`                | `Instant` | The time this query finished.                                                                                                                                                                                                                                                                                                                                               |
| `UsageNanos`             | `long`    | Time spent running this query, in nanoseconds, summed over every thread that worked on it. Parallel work can make it larger than the wall-clock time from `StartTime` to `EndTime`.                                                                                                                                                                                         |
| `CpuNanos`               | `long`    | How long, in nanoseconds, this query used the CPU.                                                                                                                                                                                                                                                                                                                          |
| `UserCpuNanos`           | `long`    | How long, in nanoseconds, this query used the CPU in user-space. Subtract `UserCpuNanos` from `CpuNanos` to get system time.                                                                                                                                                                                                                                                |
| `FreeMemory`             | `long`    | The free memory at the end of this query.                                                                                                                                                                                                                                                                                                                                   |
| `TotalMemory`            | `long`    | The total size of the JVM heap memory at the end of this query.                                                                                                                                                                                                                                                                                                             |
| `FreeMemoryChange`       | `long`    | The change in free memory during this query. Other threads can change free memory at the same time, so this value alone doesn't show what the query used. To find operations that use significant heap, combine this column with profiling and the `AllocatedBytes` column, which also counts memory that became eligible for garbage collection during or after the query. |
| `TotalMemoryChange`      | `long`    | The change in total JVM heap memory during this query.                                                                                                                                                                                                                                                                                                                      |
| `Collections`            | `long`    | The number of garbage collections while executing this query.                                                                                                                                                                                                                                                                                                               |
| `CollectionTimeNanos`    | `long`    | The number of nanoseconds spent in the garbage collector while executing this query.                                                                                                                                                                                                                                                                                        |
| `AllocatedBytes`         | `long`    | The number of bytes allocated by this query.                                                                                                                                                                                                                                                                                                                                |
| `PoolAllocatedBytes`     | `long`    | The number of bytes allocated from pools by this query.                                                                                                                                                                                                                                                                                                                     |
| `DataReadNanos`          | `long`    | How long, in nanoseconds, this query spent blocking while reading data.                                                                                                                                                                                                                                                                                                     |
| `DataReadCount`          | `long`    | The number of blocking data reads that occurred during this query.                                                                                                                                                                                                                                                                                                          |
| `DataReadBytes`          | `long`    | How many bytes this query read while performing blocking operations accounted for in `DataReadNanos` and `DataReadCount`.                                                                                                                                                                                                                                                   |
| `MetadataOperationNanos` | `long`    | How long, in nanoseconds, this query spent on blocking metadata operations.                                                                                                                                                                                                                                                                                                 |
| `MetadataOperationCount` | `long`    | The number of blocking metadata operations that occurred during this query.                                                                                                                                                                                                                                                                                                 |
| `WasInterrupted`         | `boolean` | Was this query interrupted?                                                                                                                                                                                                                                                                                                                                                 |
| `Exception`              | `String`  | If this query produced an exception, the exception's message. Otherwise null.                                                                                                                                                                                                                                                                                               |
| `AuthContext`            | `String`  | The authentication context used for executing this query.                                                                                                                                                                                                                                                                                                                   |
| `WorkerHeapSize`         | `long`    | The maximum heap size of the worker, in bytes.                                                                                                                                                                                                                                                                                                                              |

</details>

#### Query Operation Performance Log

The Query Operation Performance Log (QOPL) contains performance data for the individual operations within each query. It has one row for each operation. By default, it omits operations that finish very quickly without reading data or performing metadata operations. Create the QOPL with [`queryOperationPerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#queryOperationPerformanceLog()).

The QOPL and the [Update Performance Log](#update-performance-log) (UPL) record different work:

- The QOPL records the work an operation does when its query runs, such as a table operation's initialization.
- The UPL summarizes, over an interval, the work operations do afterward to process updates to [ticking](../../conceptual/table-types.md#standard-streaming-tables) tables.

The QOPL describes each operation in its `Description` column, while the UPL uses `EntryDescription`. To match a UPL row to the QOPL row for the same operation, use the `EvaluationNumber` and `OperationNumber` columns, which both tables share. The QOPL always keeps the row for an operation that has UPL rows, even when that operation finished quickly, so this match is exact.

The most significant columns in this table are:

- `StartTime` — The time at which this operation started.
- `EndTime` — The time at which this operation ended.
- `OperationNumber` — A monotonically increasing sequence number for each operation in a query.
- `FreeMemory` — The free memory at the end of this operation.
- `TotalMemory` — The total size of the JVM heap memory at the end of this operation.
- `FreeMemoryChange` — The change in free memory during this operation.

<details>
<summary>Query Operation Performance Log columns</summary>

| Column Name              | Data Type | Description                                                                                                                                                                             |
| ------------------------ | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EvaluationNumber`       | `long`    | An increasing integral value for requests made to the worker.                                                                                                                           |
| `ParentEvaluationNumber` | `long`    | When this query is a [subquery](#evaluation-numbers), the evaluation number of the query that started it.                                                                               |
| `OperationNumber`        | `int`     | An identifier for an operation within an evaluation.                                                                                                                                    |
| `ParentOperationNumber`  | `int`     | The `OperationNumber` of the operation that encloses this one in the same query. Null when the operation is not nested inside another operation.                                        |
| `Depth`                  | `int`     | The operation's nesting level: 0 for an operation directly under its query, plus 1 for each enclosing operation.                                                                        |
| `SessionId`              | `String`  | A unique identifier for the user-session performing the operation.                                                                                                                      |
| `Description`            | `String`  | A textual description of the operation, generally including the name and some of the arguments.                                                                                         |
| `CallerLine`             | `String`  | The file and line number where the operation was initiated, if available.                                                                                                               |
| `IsCompilation`          | `boolean` | Does this represent a formula compilation operation?                                                                                                                                    |
| `StartTime`              | `Instant` | The time this operation began.                                                                                                                                                          |
| `EndTime`                | `Instant` | The time this operation finished.                                                                                                                                                       |
| `UsageNanos`             | `long`    | Time spent running this operation, in nanoseconds, summed over every thread that worked on it. Parallel work can make it larger than the wall-clock time from `StartTime` to `EndTime`. |
| `CpuNanos`               | `long`    | How long, in nanoseconds, this operation used the CPU.                                                                                                                                  |
| `UserCpuNanos`           | `long`    | How long, in nanoseconds, this operation used the CPU in user-space. Subtract `UserCpuNanos` from `CpuNanos` to get system time.                                                        |
| `FreeMemory`             | `long`    | The free memory at the end of this operation.                                                                                                                                           |
| `TotalMemory`            | `long`    | The total size of the JVM heap memory at the end of this operation.                                                                                                                     |
| `FreeMemoryChange`       | `long`    | The change in free memory during this operation.                                                                                                                                        |
| `TotalMemoryChange`      | `long`    | The change in total JVM heap memory during this operation.                                                                                                                              |
| `Collections`            | `long`    | The number of garbage collections while executing this operation.                                                                                                                       |
| `CollectionTimeNanos`    | `long`    | The number of nanoseconds spent in the garbage collector while executing this operation.                                                                                                |
| `AllocatedBytes`         | `long`    | The number of bytes allocated by this operation.                                                                                                                                        |
| `PoolAllocatedBytes`     | `long`    | The number of bytes allocated from pools by this operation.                                                                                                                             |
| `InputSizeLong`          | `long`    | The number of rows in the input for this operation.                                                                                                                                     |
| `DataReadNanos`          | `long`    | How long, in nanoseconds, this operation spent blocking while reading data.                                                                                                             |
| `DataReadCount`          | `long`    | The number of blocking data reads that occurred during this operation.                                                                                                                  |
| `DataReadBytes`          | `long`    | How many bytes this operation read while performing blocking operations accounted for in `DataReadNanos` and `DataReadCount`.                                                           |
| `MetadataOperationNanos` | `long`    | How long, in nanoseconds, this operation spent on blocking metadata operations.                                                                                                         |
| `MetadataOperationCount` | `long`    | The number of blocking metadata operations that occurred during this operation.                                                                                                         |
| `WasInterrupted`         | `boolean` | Was this operation interrupted?                                                                                                                                                         |
| `AuthContext`            | `String`  | The authentication context used for executing this operation.                                                                                                                           |
| `WorkerHeapSize`         | `long`    | The maximum heap size of the worker, in bytes.                                                                                                                                          |

</details>

#### Update Performance Log

The [update graph](../../conceptual/periodic-update-graph-configuration.md) is the engine component that updates [ticking](../../conceptual/table-types.md#standard-streaming-tables) tables. The Update Performance Log (UPL) describes the time and resources the update graph uses to keep each table operation's results up to date. Create the UPL with [`updatePerformanceLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#updatePerformanceLog()). The UPL reports in intervals:

- Each interval lasts one minute by default. The `UpdatePerformanceTracker.reportIntervalMillis` configuration property sets its length in milliseconds.
- At the end of each interval, each operation that processed updates gets its own row, except for small operations.
- Small operations, which used very little time in the interval and did not read data or perform metadata operations, share a single `Aggregated Small Updates` row.
- A row named `UpdatePerformanceTracker Flush` records the time the tracker spends writing the interval's rows.

The most significant columns in this table are:

- `EntryDescription` — The operation this row describes.
- `IntervalStartTime` and `IntervalEndTime` — The beginning and end of the logging interval.
- `UsageNanos` — How long, in nanoseconds, the operation spent updating during the interval, summed over all threads.
- `CpuNanos` — How long, in nanoseconds, the operation used the CPU during the interval.
- `RowsAdded`, `RowsRemoved`, `RowsModified`, and `RowsShifted` — The rows the operation processed in the interval.

<details>
<summary>Update Performance Log columns</summary>

| Column Name              | Data Type | Description                                                                                                                                                                                                               |
| ------------------------ | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EntryId`                | `long`    | A numeric identifier for this performance entry. Use it to identify the same operation across cycles.                                                                                                                     |
| `EvaluationNumber`       | `long`    | The evaluation number of the corresponding entry in the [Query Operation Performance Log](#query-operation-performance-log).                                                                                              |
| `OperationNumber`        | `int`     | The operation number of the corresponding entry in the [Query Operation Performance Log](#query-operation-performance-log).                                                                                               |
| `EntryDescription`       | `String`  | A textual description of the operation, generally including the name and some of the arguments.                                                                                                                           |
| `EntryCallerLine`        | `String`  | The file and line number where the operation was initiated, if available.                                                                                                                                                 |
| `IntervalStartTime`      | `Instant` | The beginning of the interval this row represents.                                                                                                                                                                        |
| `IntervalEndTime`        | `Instant` | The end of the interval this row represents.                                                                                                                                                                              |
| `UsageNanos`             | `long`    | Time this operation spent processing updates in this interval, in nanoseconds, summed over all threads.                                                                                                                   |
| `CpuNanos`               | `long`    | How long, in nanoseconds, this operation used the CPU in this interval.                                                                                                                                                   |
| `UserCpuNanos`           | `long`    | How long, in nanoseconds, this operation used the CPU in user-space in this interval. Subtract `UserCpuNanos` from `CpuNanos` to get system time.                                                                         |
| `RowsAdded`              | `long`    | How many rows were added in this interval.                                                                                                                                                                                |
| `RowsRemoved`            | `long`    | How many rows were removed in this interval.                                                                                                                                                                              |
| `RowsModified`           | `long`    | How many rows were modified in this interval.                                                                                                                                                                             |
| `RowsShifted`            | `long`    | How many rows were shifted in this interval. The count can be too high. The engine counts every row key in each shifted range, including keys with no row, so a shift over a sparse region overstates the number of rows. |
| `InvocationCount`        | `long`    | How many times this entry was executed in this interval.                                                                                                                                                                  |
| `MinFreeMemory`          | `long`    | The minimum free heap memory sampled at the start or end of any of this operation's runs in this interval.                                                                                                                |
| `MaxTotalMemory`         | `long`    | The maximum total heap memory sampled at the start or end of any of this operation's runs in this interval.                                                                                                               |
| `Collections`            | `long`    | The number of garbage collections while executing this operation in this interval.                                                                                                                                        |
| `CollectionTimeNanos`    | `long`    | The number of nanoseconds spent in the garbage collector while executing this operation in this interval.                                                                                                                 |
| `AllocatedBytes`         | `long`    | The number of bytes allocated by this operation in this interval.                                                                                                                                                         |
| `PoolAllocatedBytes`     | `long`    | The number of bytes allocated from pools by this operation in this interval.                                                                                                                                              |
| `DataReadNanos`          | `long`    | How long, in nanoseconds, this operation spent blocking while reading data in this interval.                                                                                                                              |
| `DataReadCount`          | `long`    | The number of blocking data reads that occurred during this operation in this interval.                                                                                                                                   |
| `DataReadBytes`          | `long`    | How many bytes this operation read while performing blocking operations accounted for in `DataReadNanos` and `DataReadCount`.                                                                                             |
| `MetadataOperationNanos` | `long`    | How long, in nanoseconds, this operation spent on blocking metadata operations in this interval.                                                                                                                          |
| `MetadataOperationCount` | `long`    | The number of blocking metadata operations that occurred during this operation in this interval.                                                                                                                          |
| `AuthContext`            | `String`  | The authentication context used for executing this operation.                                                                                                                                                             |
| `UpdateGraph`            | `String`  | The name of the update graph for this entry.                                                                                                                                                                              |
| `WorkerHeapSize`         | `long`    | The maximum heap size of the worker, in bytes.                                                                                                                                                                            |

</details>

#### Update Performance Ancestors Log

The Update Performance Ancestors Log contains the `UpdateGraph`, `EntryId`, and `EntryDescription` columns from the [Update Performance Log](#update-performance-log) and an additional `Ancestors` column. Each row describes one operation, which is a node in the [update graph](../../conceptual/periodic-update-graph-configuration.md). The `Ancestors` column lists the operations whose updates feed into it, so you can trace a slow operation back to its sources. An operation that gains new ancestors later, such as a join, gets another row, and the methods below combine its rows. Create the Update Performance Ancestors Log with [`updatePerformanceAncestorsLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#updatePerformanceAncestorsLog()).

<details>
<summary>Update Performance Ancestors Log columns</summary>

| Column Name        | Data Type    | Description                                                                                           |
| ------------------ | ------------ | ----------------------------------------------------------------------------------------------------- |
| `UpdateGraph`      | `String`     | The name of the update graph for this entry.                                                          |
| `EntryId`          | `long`       | A numeric identifier for this performance entry. Use it to identify the same operation across cycles. |
| `EntryDescription` | `String`     | A textual description of the operation, generally including the name and some of the arguments.       |
| `Ancestors`        | `LongVector` | A vector of ancestor `EntryId` values.                                                                |

</details>

To draw the ancestor graph of one or more entries, pass their `EntryId` values, the Update Performance Log, and the Update Performance Ancestors Log to one of these [`UpdateAncestorViz`](/core/javadoc/io/deephaven/engine/table/impl/util/UpdateAncestorViz.html) methods:

- [`svg`](/core/javadoc/io/deephaven/engine/table/impl/util/UpdateAncestorViz.html#svg(long%5B%5D,io.deephaven.engine.table.Table,io.deephaven.engine.table.Table,java.io.File)) returns the graph as SVG image bytes, and can also write it to a file. Pass `null` as the last argument to skip writing a file.
- [`dot`](/core/javadoc/io/deephaven/engine/table/impl/util/UpdateAncestorViz.html#dot(long%5B%5D,io.deephaven.engine.table.Table,io.deephaven.engine.table.Table)) returns the graph as a Graphviz DOT string.

In the following example, `entryId` is the `EntryId` of the operation you want to examine, and `upl` and `upal` are the tables created in [Available performance tables](#available-performance-tables):

```groovy skip-test
import io.deephaven.engine.table.impl.util.UpdateAncestorViz

ancestorDot = UpdateAncestorViz.dot([entryId] as long[], upl, upal)
```

> [!NOTE]
> You must have [Graphviz](https://graphviz.org/) installed to produce an ancestors image with [`UpdateAncestorViz.svg`](/core/javadoc/io/deephaven/engine/table/impl/util/UpdateAncestorViz.html#svg(long%5B%5D,io.deephaven.engine.table.Table,io.deephaven.engine.table.Table,java.io.File)). The published Deephaven Docker images don't include Graphviz, so install it in a custom image to use this method. [`UpdateAncestorViz.dot`](/core/javadoc/io/deephaven/engine/table/impl/util/UpdateAncestorViz.html#dot(long%5B%5D,io.deephaven.engine.table.Table,io.deephaven.engine.table.Table)) doesn't need Graphviz.

#### Process Metrics Log

This table contains metrics collected for the current Deephaven engine process. Create it with [`processMetricsLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#processMetricsLog()).

<details>
<summary>Process Metrics Log columns</summary>

| Column Name       | Data Type | Description                                                       |
| ----------------- | --------- | ----------------------------------------------------------------- |
| `ProcessUniqueId` | `String`  | The process's unique identifier String.                           |
| `Timestamp`       | `Instant` | The time this metric sample was recorded.                         |
| `Name`            | `String`  | The name of the metric being reported.                            |
| `Interval`        | `String`  | The length of the interval being reported, such as `1s` or `15m`. |
| `Type`            | `String`  | The metric type, such as `state` or `counter`.                    |
| `N`               | `long`    | The number of samples.                                            |
| `Sum`             | `long`    | The sum of all samples in this interval.                          |
| `Last`            | `long`    | The last sample in this interval.                                 |
| `Min`             | `long`    | The minimum sample in this interval.                              |
| `Max`             | `long`    | The maximum sample in this interval.                              |
| `Avg`             | `long`    | The mean of all samples in this interval.                         |
| `Sum2`            | `long`    | The sum of the squares of all samples in this interval.           |
| `Stdev`           | `long`    | The standard deviation of all samples in this interval.           |

</details>

#### Process Info Log

This static table contains process information for the current Deephaven engine process. Create it with [`processInfoLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#processInfoLog()).

<details>
<summary>Process Info Log columns</summary>

| Column Name | Data Type | Description                                                  |
| ----------- | --------- | ------------------------------------------------------------ |
| `Id`        | `String`  | An identifier string for this process.                       |
| `Type`      | `String`  | The category of the property described in this row.          |
| `Key`       | `String`  | A key describing the process or property.                    |
| `Value`     | `String`  | The value of the property or process described by the `Key`. |

</details>

#### Server State Log

This table contains JVM information about memory use, garbage collection, and the [update graph](../../conceptual/periodic-update-graph-configuration.md), sampled periodically. Columns with `UGP` in their names describe the [update graph](../../conceptual/periodic-update-graph-configuration.md)'s refresh cycles. A cycle is _on budget_ when it finishes within the update graph's target cycle duration. Create the Server State Log with [`serverStateLog`](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html#serverStateLog()).

<details>
<summary>Server State Log columns</summary>

| Column Name                            | Data Type | Description                                                                                          |
| -------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------- |
| `IntervalStartTime`                    | `Instant` | The start time of the interval.                                                                      |
| `IntervalDurationMicros`               | `int`     | The number of microseconds in the interval.                                                          |
| `TotalMemoryMiB`                       | `int`     | The JVM heap memory currently reserved, in MiB.                                                      |
| `FreeMemoryMiB`                        | `int`     | The free JVM heap memory, in MiB.                                                                    |
| `IntervalCollections`                  | `short`   | The number of garbage collection events in the interval.                                             |
| `IntervalCollectionTimeMicros`         | `int`     | The number of microseconds (approximately) spent collecting garbage in the interval.                 |
| `IntervalUGPCyclesOnBudget`            | `short`   | The number of update graph cycles in the interval that finished within the target cycle duration.    |
| `IntervalUGPCyclesTimeMicros`          | `int[]`   | An array of update graph cycle durations in the interval, in microseconds.                           |
| `IntervalUGPCyclesSafePoints`          | `short`   | The number of JVM safepoint pauses during update graph cycles in the interval.                       |
| `IntervalUGPCyclesSafePointTimeMicros` | `int`     | The number of microseconds spent in JVM safepoint pauses during update graph cycles in the interval. |

</details>

### Derived views

The [`PerformanceQueries`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html) methods in this section build views on top of the raw logs. They add computed columns, such as durations in seconds and memory use as a fraction of the heap.

Three of the views show one query:

- [Query Performance](#query-performance)
- [Query Operation Performance](#query-operation-performance)
- [Query Update Performance](#query-update-performance)

To use them, find the query in the [Query Performance Log](#query-performance-log), read its `EvaluationNumber`, and pass that number to the method. In the examples below, `n` is that [evaluation number](#evaluation-numbers).

[Server State](#server-state) takes no argument.

#### Query Performance

[`queryPerformance`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryPerformance(long)) takes an [evaluation number](#evaluation-numbers) and returns that query's performance data from the [Query Performance Log](#query-performance-log), including how long the query took to run and how its memory use changed.

```groovy skip-test
import io.deephaven.engine.table.impl.util.PerformanceQueries

qp = PerformanceQueries.queryPerformance(n)
```

<details>
<summary>Query Performance columns</summary>

| Column Name              | Data Type | Description                                                                                                                                                                         |
| ------------------------ | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EvaluationNumber`       | `long`    | An increasing integral value for requests made to the worker.                                                                                                                       |
| `ParentEvaluationNumber` | `long`    | When this query is a [subquery](#evaluation-numbers), the evaluation number of the query that started it.                                                                           |
| `QueryMemUsed`           | `long`    | The JVM heap memory in use at the end of the query, in bytes (`TotalMemory - FreeMemory`). This is the whole heap, not only this query's memory.                                    |
| `QueryMemFree`           | `long`    | The heap memory remaining before the max heap size is reached, in bytes.                                                                                                            |
| `QueryMemUsedPct`        | `double`  | `QueryMemUsed` as a fraction of the max heap size (`WorkerHeapSize`).                                                                                                               |
| `EndTime`                | `Instant` | The time this query finished.                                                                                                                                                       |
| `TimeSecs`               | `double`  | The time this query took in seconds.                                                                                                                                                |
| `NetMemoryChange`        | `long`    | `FreeMemoryChange - TotalMemoryChange`: the change in used heap memory during this query, with the sign reversed. A negative value means heap use grew.                             |
| `SessionId`              | `String`  | A unique identifier for the user-session performing the query.                                                                                                                      |
| `Description`            | `String`  | A textual description of the query, generally including the name and some of the arguments.                                                                                         |
| `StartTime`              | `Instant` | The time this query began.                                                                                                                                                          |
| `UsageNanos`             | `long`    | Time spent running this query, in nanoseconds, summed over every thread that worked on it. Parallel work can make it larger than the wall-clock time from `StartTime` to `EndTime`. |
| `CpuNanos`               | `long`    | How long, in nanoseconds, this query used the CPU.                                                                                                                                  |
| `UserCpuNanos`           | `long`    | How long, in nanoseconds, this query used the CPU in user-space. Subtract `UserCpuNanos` from `CpuNanos` to get system time.                                                        |
| `FreeMemory`             | `long`    | The free memory at the end of this query.                                                                                                                                           |
| `TotalMemory`            | `long`    | The total size of the JVM heap memory at the end of this query.                                                                                                                     |
| `FreeMemoryChange`       | `long`    | The change in free memory during this query.                                                                                                                                        |
| `TotalMemoryChange`      | `long`    | The change in total JVM heap memory during this query.                                                                                                                              |
| `Collections`            | `long`    | The number of garbage collections while executing this query.                                                                                                                       |
| `CollectionTimeNanos`    | `long`    | The number of nanoseconds spent in the garbage collector while executing this query.                                                                                                |
| `AllocatedBytes`         | `long`    | The number of bytes allocated by this query.                                                                                                                                        |
| `PoolAllocatedBytes`     | `long`    | The number of bytes allocated from pools by this query.                                                                                                                             |
| `DataReadNanos`          | `long`    | How long, in nanoseconds, this query spent blocking while reading data.                                                                                                             |
| `DataReadCount`          | `long`    | The number of blocking data reads that occurred during this query.                                                                                                                  |
| `DataReadBytes`          | `long`    | How many bytes this query read while performing blocking operations accounted for in `DataReadNanos` and `DataReadCount`.                                                           |
| `MetadataOperationNanos` | `long`    | How long, in nanoseconds, this query spent on blocking metadata operations.                                                                                                         |
| `MetadataOperationCount` | `long`    | The number of blocking metadata operations that occurred during this query.                                                                                                         |
| `WasInterrupted`         | `boolean` | Was this query interrupted?                                                                                                                                                         |
| `Exception`              | `String`  | If this query produced an exception, the exception's message. Otherwise null.                                                                                                       |
| `AuthContext`            | `String`  | The authentication context used for executing this query.                                                                                                                           |
| `WorkerHeapSize`         | `long`    | The maximum heap size of the worker, in bytes.                                                                                                                                      |
| `DurationNanos`          | `long`    | The wall-clock time from `StartTime` to `EndTime`, in nanoseconds.                                                                                                                  |

</details>

#### Query Operation Performance

[`queryOperationPerformance`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryOperationPerformance(long)) takes an [evaluation number](#evaluation-numbers) and returns that query's operations from the [Query Operation Performance Log](#query-operation-performance-log), including how long each operation took to execute and how resource consumption changed while it ran. Operations include table operations such as [`where`](../../reference/table-operations/filter/where.md), [`update`](../../reference/table-operations/select/update.md), and [`naturalJoin`](../../reference/table-operations/join/natural-join.md), as well as internal functions.

```groovy skip-test
import io.deephaven.engine.table.impl.util.PerformanceQueries

qop = PerformanceQueries.queryOperationPerformance(n)
```

<details>
<summary>Query Operation Performance columns</summary>

| Column Name              | Data Type | Description                                                                                                                                                                             |
| ------------------------ | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EvaluationNumber`       | `long`    | An increasing integral value for requests made to the worker.                                                                                                                           |
| `ParentEvaluationNumber` | `long`    | When this query is a [subquery](#evaluation-numbers), the evaluation number of the query that started it.                                                                               |
| `OperationNumber`        | `int`     | An identifier for an operation within an evaluation.                                                                                                                                    |
| `ParentOperationNumber`  | `int`     | The `OperationNumber` of the operation that encloses this one in the same query. Null when the operation is not nested inside another operation.                                        |
| `EndTime`                | `Instant` | The time this operation finished.                                                                                                                                                       |
| `TimeSecs`               | `double`  | The time this operation took in seconds.                                                                                                                                                |
| `NetMemoryChange`        | `long`    | `FreeMemoryChange - TotalMemoryChange`: the change in used heap memory during this operation, with the sign reversed. A negative value means heap use grew.                             |
| `Depth`                  | `int`     | The operation's nesting level: 0 for an operation directly under its query, plus 1 for each enclosing operation.                                                                        |
| `SessionId`              | `String`  | A unique identifier for the user-session performing the operation.                                                                                                                      |
| `Description`            | `String`  | A textual description of the operation, generally including the name and some of the arguments.                                                                                         |
| `CallerLine`             | `String`  | The file and line number where the operation was initiated, if available.                                                                                                               |
| `IsCompilation`          | `boolean` | Does this represent a formula compilation operation?                                                                                                                                    |
| `StartTime`              | `Instant` | The time this operation began.                                                                                                                                                          |
| `UsageNanos`             | `long`    | Time spent running this operation, in nanoseconds, summed over every thread that worked on it. Parallel work can make it larger than the wall-clock time from `StartTime` to `EndTime`. |
| `CpuNanos`               | `long`    | How long, in nanoseconds, this operation used the CPU.                                                                                                                                  |
| `UserCpuNanos`           | `long`    | How long, in nanoseconds, this operation used the CPU in user-space. Subtract `UserCpuNanos` from `CpuNanos` to get system time.                                                        |
| `FreeMemory`             | `long`    | The free memory at the end of this operation.                                                                                                                                           |
| `TotalMemory`            | `long`    | The total size of the JVM heap memory at the end of this operation.                                                                                                                     |
| `FreeMemoryChange`       | `long`    | The change in free memory during this operation.                                                                                                                                        |
| `TotalMemoryChange`      | `long`    | The change in total JVM heap memory during this operation.                                                                                                                              |
| `Collections`            | `long`    | The number of garbage collections while executing this operation.                                                                                                                       |
| `CollectionTimeNanos`    | `long`    | The number of nanoseconds spent in the garbage collector while executing this operation.                                                                                                |
| `AllocatedBytes`         | `long`    | The number of bytes allocated by this operation.                                                                                                                                        |
| `PoolAllocatedBytes`     | `long`    | The number of bytes allocated from pools by this operation.                                                                                                                             |
| `InputSizeLong`          | `long`    | The number of rows in the input for this operation.                                                                                                                                     |
| `DataReadNanos`          | `long`    | How long, in nanoseconds, this operation spent blocking while reading data.                                                                                                             |
| `DataReadCount`          | `long`    | The number of blocking data reads that occurred during this operation.                                                                                                                  |
| `DataReadBytes`          | `long`    | How many bytes this operation read while performing blocking operations accounted for in `DataReadNanos` and `DataReadCount`.                                                           |
| `MetadataOperationNanos` | `long`    | How long, in nanoseconds, this operation spent on blocking metadata operations.                                                                                                         |
| `MetadataOperationCount` | `long`    | The number of blocking metadata operations that occurred during this operation.                                                                                                         |
| `WasInterrupted`         | `boolean` | Was this operation interrupted?                                                                                                                                                         |
| `AuthContext`            | `String`  | The authentication context used for executing this operation.                                                                                                                           |
| `WorkerHeapSize`         | `long`    | The maximum heap size of the worker, in bytes.                                                                                                                                          |
| `DurationNanos`          | `long`    | The wall-clock time from `StartTime` to `EndTime`, in nanoseconds.                                                                                                                      |

</details>

#### Query Update Performance

[`queryUpdatePerformance`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryUpdatePerformance(long)) takes an [evaluation number](#evaluation-numbers) and returns that query's update performance data from the [Update Performance Log](#update-performance-log).

```groovy skip-test
import io.deephaven.engine.table.impl.util.PerformanceQueries

qup = PerformanceQueries.queryUpdatePerformance(n)
```

<details>
<summary>Query Update Performance columns</summary>

| Column Name              | Data Type | Description                                                                                                                                                                                                               |
| ------------------------ | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EvaluationNumber`       | `long`    | An increasing integral value for requests made to the worker.                                                                                                                                                             |
| `OperationNumber`        | `int`     | An identifier for an operation within an evaluation.                                                                                                                                                                      |
| `Ratio`                  | `double`  | The fraction of the logging interval this operation spent processing updates (`UsageNanos / IntervalDurationNanos`). `1` means the entire interval. Parallel work can push it above `1`.                                  |
| `QueryMemUsed`           | `long`    | An upper estimate of JVM heap memory in use during the interval, in bytes (`MaxTotalMemory - MinFreeMemory`). This is the whole heap, not only this operation's memory.                                                   |
| `QueryMemUsedPct`        | `double`  | The fraction of the max heap size (`WorkerHeapSize`) that `QueryMemUsed` represents.                                                                                                                                      |
| `IntervalEndTime`        | `Instant` | The end of the interval this row represents.                                                                                                                                                                              |
| `RowsPerSec`             | `long`    | Rows added, removed, and modified (`NRows`) per second of the interval.                                                                                                                                                   |
| `RowsPerCPUSec`          | `long`    | `NRows` per second of CPU time (`CpuNanos`).                                                                                                                                                                              |
| `RowsPerOpSec`           | `long`    | `NRows` per second the operation spent processing updates (`UsageNanos`).                                                                                                                                                 |
| `EntryDescription`       | `String`  | A textual description of the operation, generally including the name and some of the arguments.                                                                                                                           |
| `EntryId`                | `long`    | A numeric identifier for this performance entry. Use it to identify the same operation across cycles.                                                                                                                     |
| `EntryCallerLine`        | `String`  | The file and line number where the operation was initiated, if available.                                                                                                                                                 |
| `IntervalStartTime`      | `Instant` | The beginning of the interval this row represents.                                                                                                                                                                        |
| `UsageNanos`             | `long`    | Time this operation spent processing updates in this interval, in nanoseconds, summed over all threads.                                                                                                                   |
| `CpuNanos`               | `long`    | How long, in nanoseconds, this operation used the CPU in this interval.                                                                                                                                                   |
| `UserCpuNanos`           | `long`    | How long, in nanoseconds, this operation used the CPU in user-space in this interval. Subtract `UserCpuNanos` from `CpuNanos` to get system time.                                                                         |
| `RowsAdded`              | `long`    | How many rows were added in this interval.                                                                                                                                                                                |
| `RowsRemoved`            | `long`    | How many rows were removed in this interval.                                                                                                                                                                              |
| `RowsModified`           | `long`    | How many rows were modified in this interval.                                                                                                                                                                             |
| `RowsShifted`            | `long`    | How many rows were shifted in this interval. The count can be too high. The engine counts every row key in each shifted range, including keys with no row, so a shift over a sparse region overstates the number of rows. |
| `InvocationCount`        | `long`    | How many times this entry was executed in this interval.                                                                                                                                                                  |
| `MinFreeMemory`          | `long`    | The minimum free heap memory sampled at the start or end of any of this operation's runs in this interval.                                                                                                                |
| `MaxTotalMemory`         | `long`    | The maximum total heap memory sampled at the start or end of any of this operation's runs in this interval.                                                                                                               |
| `Collections`            | `long`    | The number of garbage collections while executing this operation in this interval.                                                                                                                                        |
| `CollectionTimeNanos`    | `long`    | The number of nanoseconds spent in the garbage collector while executing this operation in this interval.                                                                                                                 |
| `AllocatedBytes`         | `long`    | The number of bytes allocated by this operation in this interval.                                                                                                                                                         |
| `PoolAllocatedBytes`     | `long`    | The number of bytes allocated from pools by this operation in this interval.                                                                                                                                              |
| `DataReadNanos`          | `long`    | How long, in nanoseconds, this operation spent blocking while reading data in this interval.                                                                                                                              |
| `DataReadCount`          | `long`    | The number of blocking data reads that occurred during this operation in this interval.                                                                                                                                   |
| `DataReadBytes`          | `long`    | How many bytes this operation read while performing blocking operations accounted for in `DataReadNanos` and `DataReadCount`.                                                                                             |
| `MetadataOperationNanos` | `long`    | How long, in nanoseconds, this operation spent on blocking metadata operations in this interval.                                                                                                                          |
| `MetadataOperationCount` | `long`    | The number of blocking metadata operations that occurred during this operation in this interval.                                                                                                                          |
| `AuthContext`            | `String`  | The authentication context used for executing this operation.                                                                                                                                                             |
| `UpdateGraph`            | `String`  | The name of the update graph for this entry.                                                                                                                                                                              |
| `WorkerHeapSize`         | `long`    | The maximum heap size of the worker, in bytes.                                                                                                                                                                            |
| `IntervalDurationNanos`  | `long`    | The duration of the interval in nanoseconds.                                                                                                                                                                              |
| `QueryMemFree`           | `long`    | The heap memory remaining before the max heap size is reached, in bytes.                                                                                                                                                  |
| `NRows`                  | `long`    | The total number of rows added, removed, and modified in the interval.                                                                                                                                                    |

</details>

For more views of the same data, [`queryUpdatePerformanceMap`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryUpdatePerformanceMap(long)) takes the same evaluation number and returns a map of tables built from this data, with the keys `QueryUpdatePerformance`, `UpdateWorst`, `WorstInterval`, `UpdateMostRecent`, `UpdateAggregate`, and `UpdateSummaryStats`. In a Groovy session, the `queryUpdatePerformanceSet(n)` closure builds the same tables and assigns each one to a variable named after its key. The closure takes an `int`, so pass the evaluation number as an integer literal, such as `queryUpdatePerformanceSet(20)`.

#### Server State

The Server State table contains basic memory, garbage collection (GC), and [update graph](../../conceptual/periodic-update-graph-configuration.md) statistics for the current engine process, sampled periodically. [`serverState`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#serverState()) builds it from the [Server State Log](#server-state-log). Columns that start with `UGP` describe the refresh cycles of the [update graph](../../conceptual/periodic-update-graph-configuration.md).

```groovy skip-test
import io.deephaven.engine.table.impl.util.PerformanceQueries

ss = PerformanceQueries.serverState()
```

<details>
<summary>Server State columns</summary>

| Column Name             | Data Type | Description                                                                         |
| ----------------------- | --------- | ----------------------------------------------------------------------------------- |
| `IntervalStart`         | `Instant` | The beginning of the interval this row represents.                                  |
| `IntervalSecs`          | `double`  | The length of the interval being reported (in seconds).                             |
| `IntervalEnd`           | `Instant` | The end of the interval this row represents.                                        |
| `UsedMemMiB`            | `int`     | The amount of used memory in MiB.                                                   |
| `AvailMemMiB`           | `int`     | The amount of available memory in MiB.                                              |
| `MaxMemMiB`             | `int`     | The maximum amount of memory in MiB.                                                |
| `AvailMemRatio`         | `double`  | The ratio of available memory to maximum memory.                                    |
| `UsedMemRatio`          | `double`  | The ratio of used memory to maximum memory.                                         |
| `GcTimeRatio`           | `float`   | The ratio of GC time to total time.                                                 |
| `UGPCycles`             | `long`    | The number of update graph cycles.                                                  |
| `UGPOnBudgetRatio`      | `float`   | The fraction of update graph cycles that finished within the target cycle duration. |
| `UGPCycleMaxSecs`       | `double`  | The maximum cycle time in seconds.                                                  |
| `UGPCycleMedianSecs`    | `double`  | The median cycle time in seconds.                                                   |
| `UGPCycleMeanSecs`      | `double`  | The mean cycle time in seconds.                                                     |
| `UGPCycleP90Secs`       | `double`  | The 90th percentile cycle time in seconds.                                          |
| `UGPTimeRatio`          | `float`   | The ratio of update graph cycle time to total time.                                 |
| `UGPSafePointTimeRatio` | `float`   | The ratio of JVM safepoint pause time during update graph cycles to total time.     |

</details>

### Tree tables

#### Query Performance Tree Table

[`queryPerformanceAsTreeTable`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryPerformanceAsTreeTable()) returns a [tree table](../tree-table.md) of query performance data. It shows the same information as the [Query Performance Log](#query-performance-log), in tree form. Like a traditional call graph, it places every query that has a parent query into an expandable group. In the image below, a sort applied in the UI ran as the query with `EvaluationNumber = 68`. Expanding its row shows the subquery that did the sort, `EvaluationNumber = 69`:

![The Query Performance Tree Table, with the row where `EvaluationNumber = 68` expanded to show its subquery](../../assets/how-to/perf-qptt.png)

To see _individual_ sub-operations as a [tree table](../tree-table.md), call [`queryOperationPerformanceAsTreeTable`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryOperationPerformanceAsTreeTable()).

<details>
<summary>Query Performance Tree Table columns</summary>

| Column Name              | Data Type | Description                                                                                                                                                                         |
| ------------------------ | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EvaluationNumber`       | `long`    | The evaluation number of the corresponding entry in the [Query Performance Log](#query-performance-log).                                                                            |
| `ParentEvaluationNumber` | `long`    | The evaluation number of this row's parent in the [Query Performance Log](#query-performance-log).                                                                                  |
| `SessionId`              | `String`  | A unique identifier for the user-session performing the query.                                                                                                                      |
| `Description`            | `String`  | A textual description of the query, generally including the name and some of the arguments.                                                                                         |
| `StartTime`              | `Instant` | The time this query began.                                                                                                                                                          |
| `EndTime`                | `Instant` | The time this query finished.                                                                                                                                                       |
| `UsageNanos`             | `long`    | Time spent running this query, in nanoseconds, summed over every thread that worked on it. Parallel work can make it larger than the wall-clock time from `StartTime` to `EndTime`. |
| `CpuNanos`               | `long`    | How long, in nanoseconds, this query used the CPU.                                                                                                                                  |
| `UserCpuNanos`           | `long`    | How long, in nanoseconds, this query used the CPU in user-space. Subtract `UserCpuNanos` from `CpuNanos` to get system time.                                                        |
| `FreeMemory`             | `long`    | The free memory at the end of this query.                                                                                                                                           |
| `TotalMemory`            | `long`    | The total size of the JVM heap memory at the end of this query.                                                                                                                     |
| `FreeMemoryChange`       | `long`    | The change in free memory during this query.                                                                                                                                        |
| `TotalMemoryChange`      | `long`    | The change in total JVM heap memory during this query.                                                                                                                              |
| `Collections`            | `long`    | The number of garbage collections while executing this query.                                                                                                                       |
| `CollectionTimeNanos`    | `long`    | The number of nanoseconds spent in the garbage collector while executing this query.                                                                                                |
| `AllocatedBytes`         | `long`    | The number of bytes allocated by this query.                                                                                                                                        |
| `PoolAllocatedBytes`     | `long`    | The number of bytes allocated from pools by this query.                                                                                                                             |
| `DataReadNanos`          | `long`    | How long, in nanoseconds, this query spent blocking while reading data.                                                                                                             |
| `DataReadCount`          | `long`    | The number of blocking data reads that occurred during this query.                                                                                                                  |
| `DataReadBytes`          | `long`    | How many bytes this query read while performing blocking operations accounted for in `DataReadNanos` and `DataReadCount`.                                                           |
| `MetadataOperationNanos` | `long`    | How long, in nanoseconds, this query spent on blocking metadata operations.                                                                                                         |
| `MetadataOperationCount` | `long`    | The number of blocking metadata operations that occurred during this query.                                                                                                         |
| `WasInterrupted`         | `boolean` | Was this query interrupted?                                                                                                                                                         |
| `Exception`              | `String`  | If this query produced an exception, the exception's message. Otherwise null.                                                                                                       |
| `AuthContext`            | `String`  | The authentication context used for executing this query.                                                                                                                           |
| `WorkerHeapSize`         | `long`    | The maximum heap size of the worker, in bytes.                                                                                                                                      |

</details>

#### Query Operation Performance Tree Table

[`queryOperationPerformanceAsTreeTable`](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html#queryOperationPerformanceAsTreeTable()) returns a [tree table](../tree-table.md) of every query and its operations. It combines the rows of the [Query Performance Log](#query-performance-log) and the [Query Operation Performance Log](#query-operation-performance-log). Like a traditional call graph, it places each operation in an expandable group under its query or parent operation. Unlike [Query Operation Performance](#query-operation-performance), it takes no evaluation number, so use it when you don't yet know which query is slow.

![The Query Operation Performance Tree Table](../../assets/how-to/perf-qoptt.png)

<details>
<summary>Query Operation Performance Tree Table columns</summary>

| Column Name              | Data Type | Description                                                                                                                                                                             |
| ------------------------ | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EvalKey`                | `String`  | The row's key in the tree: the `EvaluationNumber` for a query row, or `EvaluationNumber:OperationNumber` for an operation row.                                                          |
| `ParentEvalKey`          | `String`  | The `EvalKey` of this row's parent, used to construct the tree.                                                                                                                         |
| `SessionId`              | `String`  | A unique identifier for the user-session performing the operation.                                                                                                                      |
| `Description`            | `String`  | A textual description of the operation, generally including the name and some of the arguments.                                                                                         |
| `StartTime`              | `Instant` | The time this operation began.                                                                                                                                                          |
| `EndTime`                | `Instant` | The time this operation finished.                                                                                                                                                       |
| `UsageNanos`             | `long`    | Time spent running this operation, in nanoseconds, summed over every thread that worked on it. Parallel work can make it larger than the wall-clock time from `StartTime` to `EndTime`. |
| `CpuNanos`               | `long`    | How long, in nanoseconds, this operation used the CPU.                                                                                                                                  |
| `UserCpuNanos`           | `long`    | How long, in nanoseconds, this operation used the CPU in user-space. Subtract `UserCpuNanos` from `CpuNanos` to get system time.                                                        |
| `FreeMemory`             | `long`    | The free memory at the end of this operation.                                                                                                                                           |
| `TotalMemory`            | `long`    | The total size of the JVM heap memory at the end of this operation.                                                                                                                     |
| `FreeMemoryChange`       | `long`    | The change in free memory during this operation.                                                                                                                                        |
| `TotalMemoryChange`      | `long`    | The change in total JVM heap memory during this operation.                                                                                                                              |
| `Collections`            | `long`    | The number of garbage collections while executing this operation.                                                                                                                       |
| `CollectionTimeNanos`    | `long`    | The number of nanoseconds spent in the garbage collector while executing this operation.                                                                                                |
| `AllocatedBytes`         | `long`    | The number of bytes allocated by this operation.                                                                                                                                        |
| `PoolAllocatedBytes`     | `long`    | The number of bytes allocated from pools by this operation.                                                                                                                             |
| `DataReadNanos`          | `long`    | How long, in nanoseconds, this operation spent blocking while reading data.                                                                                                             |
| `DataReadCount`          | `long`    | The number of blocking data reads that occurred during this operation.                                                                                                                  |
| `DataReadBytes`          | `long`    | How many bytes this operation read while performing blocking operations accounted for in `DataReadNanos` and `DataReadCount`.                                                           |
| `MetadataOperationNanos` | `long`    | How long, in nanoseconds, this operation spent on blocking metadata operations.                                                                                                         |
| `MetadataOperationCount` | `long`    | The number of blocking metadata operations that occurred during this operation.                                                                                                         |
| `WasInterrupted`         | `boolean` | Was this operation interrupted?                                                                                                                                                         |
| `Exception`              | `String`  | If this query produced an exception, the exception's message. Otherwise null. Always null for operation rows.                                                                           |
| `AuthContext`            | `String`  | The authentication context used for executing this operation.                                                                                                                           |
| `WorkerHeapSize`         | `long`    | The maximum heap size of the worker, in bytes.                                                                                                                                          |
| `Depth`                  | `int`     | The operation's nesting level: 0 for an operation directly under its query, plus 1 for each enclosing operation. Null for query rows.                                                   |
| `CallerLine`             | `String`  | The file and line number where the operation was initiated, if available.                                                                                                               |
| `IsCompilation`          | `boolean` | Does this represent a formula compilation operation? Null for query rows.                                                                                                               |
| `InputSizeLong`          | `long`    | The number of rows in the input for this operation. Null for query rows.                                                                                                                |
| `EvaluationNumber`       | `long`    | The evaluation number of the query this row belongs to.                                                                                                                                 |
| `ParentEvaluationNumber` | `long`    | When this row's query is a [subquery](#evaluation-numbers), the evaluation number of the query that started it.                                                                         |
| `OperationNumber`        | `int`     | An identifier for an operation within an evaluation.                                                                                                                                    |
| `ParentOperationNumber`  | `int`     | The `OperationNumber` of the operation that encloses this one in the same query. Null when the operation is not nested inside another operation.                                        |

</details>

### Other performance tables

The tables above are not the only performance tables. The Barrage subscription and snapshot performance logs record statistics for [Barrage](../../conceptual/what-is-barrage.md), the protocol Deephaven uses to send tables between processes. Get them from the [`BarragePerformanceLog`](/core/javadoc/io/deephaven/extensions/barrage/BarragePerformanceLog.html) singleton:

```groovy skip-test
import io.deephaven.extensions.barrage.BarragePerformanceLog

bsl = BarragePerformanceLog.getInstance().getSubscriptionTable()
bspl = BarragePerformanceLog.getInstance().getSnapshotTable()
```

The [Barrage Performance](../../reference/cheat-sheets/performance-tables-cheat-sheet.md#barrage-performance) section of the performance tables cheat sheet shows how to use them, and the [Barrage metrics guide](../../conceptual/barrage-metrics.md) explains what they record.

For every method that creates a performance table, see the [`TableLoggers` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html), the [`PerformanceQueries` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html), and the [`BarragePerformanceLog` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/BarragePerformanceLog.html).

## Get help

If you can't find an obvious cause for an error or slow performance in your query, you might have found a bug. [File a bug report](https://github.com/deephaven/deephaven-core/issues) and include the following:

- The version of the Deephaven system.
- The complete stack trace, not just the `Caused by:` lines.
- A code snippet or a description of the query.
- Any logs from your user console. If you run Deephaven in Docker, include the logs from your terminal.
- Support logs exported from the browser. In the **Settings** menu, click the **Export Logs** button to download a zip with the browser logs and the current state of the application.

You can also get help by asking questions in our [Slack Community](/slack) or on the Community [Discussions](https://github.com/deephaven/deephaven-core/discussions) page.

## Related documentation

- [Interpret Barrage metrics](../../conceptual/barrage-metrics.md)
- [Periodic Update Graph](../../conceptual/periodic-update-graph-configuration.md)
- [Table types](../../conceptual/table-types.md)
- [Troubleshoot query performance](../../conceptual/troubleshooting-steps.md)
- [What is Barrage?](../../conceptual/what-is-barrage.md)
- [Triage errors in queries](../triage-errors.md)
- [Create a hierarchical tree table](../tree-table.md)
- [Track processing time and measure latency](./track-processing-time.md)
- [Learning session on Performance Analysis](https://youtu.be/bz0mQasSNcg)
- [Performance tables cheat sheet](../../reference/cheat-sheets/performance-tables-cheat-sheet.md)
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md)
- [`moveColumnsUp`](../../reference/table-operations/select/move-columns-up.md)
- [`naturalJoin`](../../reference/table-operations/join/natural-join.md)
- [`sortDescending`](../../reference/table-operations/sort/sort-descending.md)
- [`timeTable`](../../reference/table-operations/create/timeTable.md)
- [`update`](../../reference/table-operations/select/update.md)
- [`updateView`](../../reference/table-operations/select/update-view.md)
- [`where`](../../reference/table-operations/filter/where.md)
- [`BarragePerformanceLog` Javadoc](/core/javadoc/io/deephaven/extensions/barrage/BarragePerformanceLog.html)
- [`PerformanceQueries` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/PerformanceQueries.html)
- [`TableLoggers` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/TableLoggers.html)
- [`UpdateAncestorViz` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/UpdateAncestorViz.html)
