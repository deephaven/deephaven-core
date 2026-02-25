---
title: Parallelization
sidebar_label: Parallelization
---

Parallelization is running multiple calculations at the same time on different CPU cores instead of one after another. Deephaven automatically parallelizes table operations like [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) to make queries faster. This guide explains how parallelization works and when you need to control it.

> [!IMPORTANT]
> **Breaking change in Deephaven 0.41+**: Queries now run in parallel by default. Code that modifies shared variables or depends on row order will produce incorrect results.
>
> **Quick check**: Does your code use global variables, depend on row order, or modify external state? If yes, the [crash course guide](../../getting-started/crash-course/parallelization.md) shows how to fix it.

## How Deephaven parallelizes queries

Deephaven uses all available CPU cores to process queries faster. You don't need to configure anything - parallelization happens automatically.

Parallelization occurs at two levels:

### Across multiple tables

When you create multiple tables from the same source, Deephaven computes them simultaneously. In this example, three independent tables derive from `marketData`:

```groovy order=marketData,withMetrics,highVolume,recentTrades
// Create a live table that adds a row every second
marketData = timeTable("PT1s").update(
    "Symbol = `SYM` + (int)(i % 5)",
    "Price = 100 + randomGaussian(0, 10)",
    "Volume = randomInt(100, 2000000)"
)

// Three independent transformations from the same source
withMetrics = marketData.update("Value = Price * Volume")
highVolume = marketData.where("Volume > 1000000")
recentTrades = marketData.tail(10)
```

When new data arrives in `marketData`, Deephaven computes `withMetrics`, `highVolume`, and `recentTrades` simultaneously on different cores.

Deephaven tracks which tables depend on which through an internal structure called the [update graph](../dag.md). Independent tables (those that don't depend on each other) run in parallel automatically.

### Within a single table

Deephaven also parallelizes calculations within a single table. When you run `source.update("Total = Price * Quantity")`, Deephaven:

1. Divides the rows into groups.
2. Assigns each group to a different CPU core.
3. Each core calculates `Total` for its rows independently.
4. Combines results into the final `Total` column.

**What gets parallelized**:

- Column calculations in [`update`](../../reference/table-operations/select/update.md), [`select`](../../reference/table-operations/select/select.md), [`view`](../../reference/table-operations/select/view.md), and [`updateView`](../../reference/table-operations/select/update-view.md).
- Filters in [`where`](../../reference/table-operations/filter/where.md) clauses.
- [Aggregations](../../reference/table-operations/group-and-aggregate/aggBy.md) and [group-by](../../reference/table-operations/group-and-aggregate/groupBy.md) operations.
- [Join](../../reference/table-operations/join/join.md) operations.

**What doesn't get parallelized**:

- Operations marked with `.withSerial` (you control this).
- Operations waiting for dependencies (automatic in the update graph).

## Controlling parallelization

Most queries work correctly with automatic parallelization. However, some code requires sequential processing - for example, code that uses a counter or modifies shared state.

Deephaven provides two mechanisms:

- **Serialization**: Process rows one at a time, in order, using `.withSerial`. Use this when a single operation needs sequential execution.
- **Barriers**: Ensure one operation completes before another starts. Use this when operation A must finish before operation B begins.

For detailed information and examples, see [Controlling concurrency](#controlling-concurrency) below.

## Query phases

Queries execute in two phases, and parallelization works differently in each.

### Initialization

When you first create a table operation (like `.where()` or `.update()`), Deephaven computes the initial result using all existing data. During initialization, Deephaven divides the rows among CPU cores so each core processes a portion simultaneously.

For live (refreshing) tables, Deephaven also registers the table in the [update graph](../dag.md) so it can receive future updates.

### Updates

After initialization, live tables update whenever their source data changes. During updates, Deephaven parallelizes in two ways:

1. **Within each operation**: Deephaven divides rows among cores, just like during initialization.
2. **Across operations**: Deephaven computes independent tables in the update graph simultaneously on different cores.

## Thread pools

Deephaven uses two separate groups of worker threads (called "thread pools") to manage parallelization. Each pool handles a different phase of query execution.

### Operation Initialization Thread Pool

This pool processes queries when they are first created. When you call `.update()`, `.where()`, or similar operations, this pool divides the existing data among its threads to compute the initial result.

**Configuration**: `OperationInitializationThreadPool.threads`

- Default: `-1` (use all available cores).
- Set to a specific number to limit parallelism during initialization.

**When it's used**:

- Initial calculation of [`update`](../../reference/table-operations/select/update.md), [`select`](../../reference/table-operations/select/select.md), [`where`](../../reference/table-operations/filter/where.md), etc.
- Processing existing data when creating derived tables.
- Join operations on static tables.

### Update Graph Processor Thread Pool

This pool processes live table updates. When source data changes, this pool computes updates for all affected tables. It also runs independent tables in parallel.

**Configuration**: `PeriodicUpdateGraph.updateThreads`

- Default: `-1` (use all available cores).
- Set to a specific number to limit parallelism during updates.

**When it's used**:

- Processing new or modified rows in live tables.
- Propagating changes through dependent tables.
- Running independent tables simultaneously.

Both thread pools default to using all CPU cores, determined by [`Runtime.availableProcessors()`](<https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html#availableProcessors()>) at startup.

## Controlling concurrency

This section explains when and how to override automatic parallelization for code that requires sequential processing.

**Key concepts**:

- **[`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html)**: An object representing a column expression, used in `select` or `update` operations.
- **Serial execution**: Forces Deephaven to process rows one at a time, in order, using `.withSerial`.
- **[`Barrier`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.Barrier.html)**: Ensures one operation completes before another starts.

### Parallelization (default)

By default, Deephaven parallelizes operations that are **stateless** - meaning each row's result depends only on that row's input values.

**An operation is stateless if it**:

- Doesn't read or modify global variables.
- Doesn't depend on which row is processed first.
- Produces the same output for the same input, regardless of when or how it runs.

**Examples of stateless operations**:

```groovy order=source1,result1,source2,result2,source3,result3,source4,result4
// Pure column arithmetic
source1 = emptyTable(10).update("Price = i * 10.0", "Quantity = i")
result1 = source1.update("Total = Price * Quantity")

// String manipulation
source2 = emptyTable(10).update("FirstName = `First` + i", "LastName = `Last` + i")
result2 = source2.update("FullName = FirstName + ' ' + LastName")

// Conditional logic
source3 = emptyTable(10).update("Age = i + 18")
result3 = source3.where("Age > 21")

// Built-in functions
source4 = emptyTable(10).update("X = i * 2.0")
result4 = source4.update("Squared = sqrt(X)")
```

> [!WARNING]
> **Breaking change in Deephaven 0.41+**
>
> **Deephaven 0.40 and earlier**: Assumed all formulas required sequential processing by default.
>
> **Deephaven 0.41 and later**: Assumes all formulas can run in parallel by default.
>
> If your formula uses global state or depends on row order, you **must** mark it with `.withSerial` or it will produce incorrect results.

You can change the default behavior using configuration properties:

- For [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md): set `QueryTable.statelessSelectByDefault`.
- For filters: set `QueryTable.statelessFiltersByDefault`.

### Serialization

Serialization forces Deephaven to process rows one at a time, in order, on a single thread. Use it when your code cannot safely run in parallel.

**When serialization is required**:

- The formula reads or modifies global variables.
- The formula calls external functions that aren't safe to call from multiple threads simultaneously.
- The formula depends on rows being processed in a specific order.
- Parallel execution produces incorrect results (duplicates, gaps, wrong values).

> [!NOTE]
> Most queries don't need serial execution. Use `.withSerial` only when parallelization causes incorrect results.

#### How serialization works

Marking an operation as serial tells Deephaven:

- Process rows one at a time, in order.
- Don't parallelize this operation across CPU cores.
- Ensure thread-safe execution for stateful code.

The [`ConcurrencyControl`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) interface provides the [`.withSerial`](../../reference/table-operations/select/update.md#serial-execution) method for [`Filter`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) ([`where`](../../reference/table-operations/filter/where.md#serial-execution)) and [`Selectable`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) ([`update`](../../reference/table-operations/select/update.md#serial-execution) and [`select`](../../reference/table-operations/select/select.md)).

> [!IMPORTANT]
> You cannot use `.withSerial` with [`view`](../../reference/table-operations/select/view.md) or [`updateView`](../../reference/table-operations/select/update-view.md). These operations compute values on-demand (when cells are accessed), so they cannot guarantee processing order. Use [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) instead when you need serial execution.

#### Example: Global state requires serialization

This example demonstrates why some code needs serialization. A function maintains global state:

```groovy order=t
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)
t = emptyTable(1_000_000).update("A = counter.getAndIncrement()", "B = counter.getAndIncrement()")
```

Without serialization, parallel execution causes race conditions where multiple threads read and update `counter` simultaneously. This doesn't throw an error — it silently produces wrong values:

```groovy should-fail
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)
bad_result = emptyTable(10).update("A = counter.getAndIncrement()", "B = counter.getAndIncrement()")
```

Parallel execution causes inconsistent values because multiple threads increment `counter` concurrently. You may see results like:

| A   | B   |
| --- | --- |
| 0   | 2   |
| 1   | 1   |
| 3   | 5   |
| 4   | 4   |
| 6   | 7   |

Notice the duplicates (1 appears twice), gaps (no 8 or 9), and `B` not following `A + 1`.

#### Using `.withSerial` for Selectables

To force serial execution for a column calculation, create a `Selectable` object and apply `.withSerial`:

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Force serial execution - rows processed one at a time, in order
col = Selectable.parse("ID = counter.getAndIncrement()").withSerial()
result = emptyTable(10).update([col])
```

#### Stateful Partition Filters

When you mark a _partition filter_ (a filter that only accesses partitioning columns) as serial, Deephaven cannot reorder it and must evaluate it on all rows of the table. However, if you don't explicitly mark a partition filter as serial, the engine treats it as stateless for performance reasons — even when Deephaven is configured to treat filters as stateful by default.

Specifically, Deephaven may relax ordering constraints for filters on partitioning columns and evaluate them per location rather than on every row. This allows Deephaven to reorder common partition filters ahead of other filters and avoid repeated evaluation against the same value. For example, the formula filter `Date=today()` is stateful if filters are stateful by default, but in nearly every case users prefer Deephaven to evaluate it early, location-by-location.

When a Selectable is serial:

- Every row is evaluated in order (row 0, then row 1, then row 2, etc.).
- Only one thread processes the column at a time.
- Global state updates happen sequentially without race conditions.

#### Using `.withSerial` for Filters

Serial filters are needed when filter evaluation has stateful side effects. Deephaven parallelizes string-based filters in [`where`](../../reference/table-operations/filter/where.md) by default, so construct Filter objects explicitly:

```groovy order=result
import io.deephaven.api.filter.Filter
import io.deephaven.api.ColumnName

// Create filters with serial evaluation
filter1 = Filter.isNull(ColumnName.of("X")).withSerial()
filter2 = Filter.isNotNull(ColumnName.of("Y")).withSerial()

result = emptyTable(1000)
    .update("X = i % 5 == 0 ? null : i", "Y = i % 7 == 0 ? null : i")
    .where(Filter.and(filter1, filter2))
```

When a [`Filter`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) is serial:

- Every input row is evaluated in order.
- Filter cannot be reordered with respect to other Filters.
- Stateful side effects happen sequentially.

### Barriers

[`Barriers`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.Barrier.html) ensure one operation completes before another starts. Use barriers when operation A must finish before operation B begins.

**When you need barriers**:

- One operation populates data that another operation reads.
- Multiple operations share a resource that can only be used by one at a time.
- You need explicit control over which operation runs first.

#### How barriers work

A [`Barrier`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.Barrier.html) creates an ordering dependency between two operations:

1. One operation **declares** the barrier (marks itself as the one that must finish first).
2. Another operation **respects** the barrier (waits for the declaring operation to finish).
3. Deephaven guarantees the declaring operation completes before the respecting operation starts.

#### Barriers for Selectables

To ensure that all rows of column `A` are evaluated before any rows of column `B` begin evaluation:

```groovy order=t
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Create a barrier - any Java object works
barrier = new Object()

// Column A declares the barrier (must finish first)
colA = Selectable.parse("A = counter.getAndIncrement()").withDeclaredBarriers(barrier)

// Column B respects the barrier (waits for A to finish)
colB = Selectable.parse("B = counter.getAndIncrement()").withRespectedBarriers(barrier)

t = emptyTable(1_000_000).update([colA, colB])
```

With this barrier:

- Column `A` processes all 1,000,000 rows completely.
- Only after `A` finishes does column `B` start processing.
- Deephaven can still parallelize both columns internally (unless you also mark them serial).

#### Barriers for Filters

Barriers control evaluation order between filters when one depends on another's side effects:

```groovy order=result
import io.deephaven.api.filter.Filter
import io.deephaven.api.ColumnName

// Create a barrier - any Java object works
barrier = new Object()

// Filter1 declares the barrier
filter1 = Filter.isNull(ColumnName.of("X")).withDeclaredBarriers(barrier)

// Filter2 respects the barrier and won't start until filter1 completes
filter2 = Filter.isNull(ColumnName.of("Y")).withRespectedBarriers(barrier)

result = emptyTable(1000)
    .update("X = i % 10 == 0 ? null : i", "Y = i % 5 == 0 ? null : i")
    .where(Filter.and(filter1, filter2))
```

#### Implicit barriers

By default, serial operations automatically create barriers between each other. This means if you have two serial columns in the same `update`, the first one finishes completely before the second one starts.

This behavior is controlled by `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS`:

**Stateful mode (default)**: Serial operations automatically wait for each other. This is usually what you want when operations share global state.

**Stateless mode**: Serial operations only enforce row order within themselves, not between each other. Use explicit barriers if you need cross-operation ordering.

Most users don't need to change this setting.

### Choosing the right approach

Use this guide to pick the right concurrency control method.

#### When to use parallelization (default)

**Use default parallel execution when**:

- The formula only uses values from the current row.
- The formula has no side effects (doesn't modify global state).
- The formula doesn't depend on row processing order.
- The formula is thread-safe.

**Examples**:

```groovy order=source1,result1,source2,result2,source3,result3,source4,result4
// These all parallelize safely by default
source1 = emptyTable(10).update("Price = i * 10.0", "Quantity = i")
result1 = source1.update("Total = Price * Quantity")

source2 = emptyTable(10).update("FirstName = `First` + i", "LastName = `Last` + i")
result2 = source2.update("FullName = FirstName + ' ' + LastName")

source3 = emptyTable(10).update("Age = i + 18")
result3 = source3.where("Age > 21")

source4 = emptyTable(10).update("Value = i * 50")
result4 = source4.update("Category = Value > 100 ? `High` : `Low`")
```

#### When to use `.withSerial`

**Use `.withSerial` when**:

- Rows must be processed in order within a single operation.
- The formula updates global state sequentially.
- The formula depends on row evaluation order.
- A single Filter or Selectable has order-dependent logic.

**Examples**:

- Sequential numbering with a counter.
- Processing events in chronological sequence.
- Cumulative calculations within one column.
- File I/O or logging operations.

**Code example**:

```groovy order=source,result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

// Global state requires serial execution
counter = new AtomicInteger(0)

col = Selectable.parse("ID = counter.getAndIncrement()").withSerial()
source = emptyTable(10)
result = source.update([col])
```

#### When to use barriers

**Use explicit barriers when**:

- You need to control ordering between different operations.
- Operation A must finish before operation B starts.
- Multiple Filters or Selectables have dependencies.
- One operation populates state that another consumes.

**Examples**:

- Filter A populates a cache that Filter B reads from.
- Column A initializes a resource that Column B uses.
- Sequential operations with cross-dependencies.

**Code example**:

```groovy order=source,result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Create a barrier - any Java object works
barrier = new Object()

// A must complete before B starts - A sets values, B uses them
colA = Selectable.parse("A = counter.getAndIncrement()").withDeclaredBarriers(barrier)
colB = Selectable.parse("B = counter.get()").withRespectedBarriers(barrier)

source = emptyTable(10)
result = source.update([colA, colB])
```

#### Quick reference table

| Scenario                             | Solution                      | Why                                 |
| ------------------------------------ | ----------------------------- | ----------------------------------- |
| Pure column math                     | Default (parallel)            | Thread-safe, no shared state        |
| Global counter                       | `.withSerial`                 | Needs sequential row processing     |
| Column A must finish before Column B | Barriers                      | Controls cross-operation ordering   |
| File I/O or logging                  | `.withSerial`                 | Serialize access to shared resource |
| Multiple operations sharing state    | Barriers or implicit barriers | Coordinates access to shared state  |
| Non-thread-safe library              | `.withSerial`                 | Forces single-threaded access       |

## Key takeaways

Deephaven automatically parallelizes queries across all available CPU cores. Most code works correctly without changes.

- Deephaven assumes all formulas can run in parallel by default.
- Use [`.withSerial`](../../reference/query-language/types/Selectable.md#withserial) when your code uses global variables, depends on row order, or calls functions that aren't safe to run from multiple threads.
- Use **barriers** when one operation must complete before another starts.
- Both thread pools use all CPU cores by default.

For a quick introduction, see the [crash course](../../getting-started/crash-course/parallelization.md).

### Related documentation

- [Update graph (table dependencies)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
- [ConcurrencyControl API (Javadoc)](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
