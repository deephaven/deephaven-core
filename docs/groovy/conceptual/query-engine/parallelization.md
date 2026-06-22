---
title: Parallelization
sidebar_label: Parallelization
---

Parallelization is running multiple calculations at the same time on different CPU cores instead of one after another. Deephaven automatically parallelizes table operations like [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) to make queries faster. This guide explains how parallelization works and when you need to control it.

> [!IMPORTANT]
> **Breaking change in Deephaven 41+**: Queries now run in parallel by default. Code that modifies shared variables or depends on row order will produce incorrect results.
>
> **Quick check**: Does your code use global variables, depend on row order, or modify external state? If yes, the [crash course guide](../../getting-started/crash-course/parallelization.md) shows how to fix it.

## How Deephaven parallelizes queries

Deephaven uses all available CPU cores to process queries faster. You don't need to configure anything — parallelization happens automatically.

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

- Column calculations in [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md).
- Filters in [`where`](../../reference/table-operations/filter/where.md) clauses.

**What does NOT get parallelized**:

- [`view`](../../reference/table-operations/select/view.md) and [`updateView`](../../reference/table-operations/select/update-view.md) — these are lazily evaluated when cells are accessed, not computed upfront.
- Operations marked with `withSerial` (you control this).
- Operations waiting for dependencies (automatic in the update graph).

## Controlling parallelization

Most queries work correctly with automatic parallelization. However, some code requires sequential processing — for example, code that uses a counter or modifies shared state.

Deephaven provides two mechanisms:

- **Serialization**: Process rows one at a time, in order, using `withSerial`. Use this when a single operation needs sequential execution.
- **Barriers**: Ensure one operation completes before another starts. Use this when operation A must finish before operation B begins.

For detailed information and examples, see [Controlling concurrency](#controlling-concurrency) below.

## Query phases

Queries execute in two phases, and parallelization works differently in each.

### Initialization

When you first create a table operation (like `.where` or `.update`), Deephaven computes the initial result using all existing data. During initialization, Deephaven divides the rows among CPU cores so each core processes a portion simultaneously.

For live (refreshing) tables, Deephaven also registers the table in the [update graph](../dag.md) so it can receive future updates.

### Updates

After initialization, live tables update whenever their source data changes. During updates, Deephaven parallelizes in two ways:

1. **Within each operation**: Deephaven divides rows among cores, just like during initialization.
2. **Across operations**: Deephaven computes independent tables in the update graph simultaneously on different cores.

## Thread pools

Deephaven uses two separate groups of worker threads (called "thread pools") to manage parallelization. Each pool handles a different phase of query execution.

### Operation Initialization Thread Pool

This pool processes queries when they are first created. When you call `.update`, `.where`, or similar operations, this pool divides the existing data among its threads to compute the initial result.

**Configuration**: `OperationInitializationThreadPool.threads`

- Default: `-1` (use all available cores).
- Set to a specific number to limit parallelism during initialization.

### Update Graph Processor Thread Pool

This pool processes live table updates. When source data changes, this pool computes updates for all affected tables. It also runs independent tables in parallel.

**Configuration**: `PeriodicUpdateGraph.updateThreads`

- Default: `-1` (use all available cores).
- Set to a specific number to limit parallelism during updates.

**When it's used**:

- Processing new or modified rows in live tables.
- Propagating changes through dependent tables.
- Running independent tables simultaneously.

Both thread pools default to using all CPU cores, determined by [`Runtime.availableProcessors()`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html#availableProcessors()) at startup.

## Controlling concurrency

This section explains when and how to override automatic parallelization for code that requires sequential processing.

**Key concepts**:

- **[`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html)**: An object representing a column expression, used in `select` or `update` operations.
- **Serial execution**: Forces Deephaven to process rows one at a time, in order, using `withSerial`.
- **[`Barrier`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.Barrier.html)**: Ensures one operation completes before another starts.

### Parallelization (default)

By default, Deephaven parallelizes operations that are **stateless** — meaning each row's result depends only on that row's input values.

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
> **Breaking change in Deephaven 41+**
>
> **Deephaven 40 and earlier**: Assumed all formulas required sequential processing by default.
>
> **Deephaven 41 and later**: Assumes all formulas can run in parallel by default.
>
> If your formula uses global state or depends on row order, you **must** mark it with `withSerial` or it will produce incorrect results.

You can change the default behavior using configuration properties:

- For [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md): set `QueryTable.statelessSelectByDefault`.
- For filters: set `QueryTable.statelessFiltersByDefault`.

### Serialization

Serialization forces Deephaven to process rows one at a time, in order, on a single thread. Use it when your code cannot safely run in parallel.

**When serialization is required**:

- The formula reads or modifies global variables.
- The formula calls external functions that aren't safe to call from multiple threads simultaneously.
- The formula depends on rows being processed in a specific order.
- Parallel execution produces incorrect results (out-of-order values, gaps, wrong values).

> [!NOTE]
> Most queries don't need serial execution. Use `withSerial` only when parallelization causes incorrect results.

The [`ConcurrencyControl`](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) interface provides the `withSerial` method for `Filter` (`where`) and `Selectable` (`update` and `select`).

> [!IMPORTANT]
> You cannot use `withSerial` with `view` or `updateView`. These operations compute values on-demand (when cells are accessed), so they cannot guarantee processing order. Use `select` or `update` instead when you need serial execution.

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

| A | B |
| - | - |
| 0 | 2 |
| 1 | 3 |
| 5 | 6 |
| 4 | 7 |
| 9 | 8 |

Notice the out-of-order values (row 4 has `A=4` after row 3 has `A=5`), gaps (no 10-19 visible), and `B` not following `A + 1`.

#### Using `withSerial` for Selectables

To force serial execution for a column calculation, create a `Selectable` object and apply `withSerial`:

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Force serial execution - rows processed one at a time, in order
col = Selectable.parse("ID = counter.getAndIncrement()").withSerial()
result = emptyTable(10).update([col])
```

When a Selectable is serial:

- Every row is evaluated in order (row 0, then row 1, then row 2, etc.).
- Only one thread processes the column at a time.
- Global state updates happen sequentially without race conditions.

#### Stateful partition filters

When you mark a _partition filter_ (a filter that only accesses partitioning columns) as serial, Deephaven cannot reorder it and must evaluate it on all rows of the table. However, if you don't explicitly mark a partition filter as serial, the engine treats it as stateless for performance reasons — even when Deephaven is configured to treat filters as stateful by default.

Specifically, Deephaven may relax ordering constraints for filters on partitioning columns and evaluate them per location rather than on every row. This allows Deephaven to reorder common partition filters ahead of other filters and avoid repeated evaluation against the same value. For example, the formula filter `Date=today()` is stateful if filters are stateful by default, but in nearly every case users prefer Deephaven to evaluate it early, location-by-location.

#### Using `withSerial` for Filters

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

`withSerial` controls row order _within_ a single column. **Barriers** control the order _between_ columns or filters. Use barriers when one operation must finish all its rows before another operation begins.

**When you need barriers**:

- Column A populates a map or list that column B reads from. Without a barrier, B might read before A has written all entries.
- Column A computes a running total into a shared variable, and column B uses that total to normalize values. Without a barrier, B would see an incomplete total.
- Column A assigns sequential IDs (0, 1, 2, ...) and column B should continue where A left off. Without a barrier, both columns start counting from 0.
- Column A writes to a file or external resource that column B reads. Without a barrier, B might read before A finishes writing.
- Multiple operations share a resource that can only be used by one at a time.

#### Barriers vs serial

These solve different problems:

- **`withSerial`**: Rows within one column are processed sequentially (row 0, then row 1, etc.). Other columns can still run at the same time.
- **Barriers**: One column finishes all its rows before another column starts. Rows within each column can still be parallelized.

When shared state is involved, you often need both:

- `withSerial` to protect row-level access to the shared state
- Barriers to ensure one column is completely done before the other starts

#### How barriers work

In Groovy, any Java object can serve as a barrier. A barrier object creates an ordering dependency between two operations:

1. One operation **declares** the barrier — it goes first
2. Another operation **respects** the barrier — it waits
3. Deephaven guarantees the declaring operation completes all rows before the respecting operation begins

Each barrier can only be declared by one operation. Multiple operations can respect the same barrier.

#### Example: shared counter

Consider two columns that share a counter. Column A assigns IDs 0–9, and column B should continue from 10–19.

**Without barriers**, both columns start simultaneously. Both read the counter starting at 0 and produce overlapping, incorrect results.

**With barriers**, column A runs first (0–9), then column B starts where A left off (10–19):

```groovy order=t
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

barrier = new Object()

// Column A: serial (protect counter) + declares barrier (must finish first)
colA = Selectable.parse("A = counter.getAndIncrement()")
    .withSerial()
    .withDeclaredBarriers(barrier)

// Column B: serial (protect counter) + respects barrier (waits for A)
colB = Selectable.parse("B = counter.getAndIncrement()")
    .withSerial()
    .withRespectedBarriers(barrier)

t = emptyTable(10).update([colA, colB])
```

Column A gets values 0–9. Column B gets values 10–19. Without the barrier, both columns would race and produce unpredictable results. Without `withSerial`, rows within each column would also race.

> [!IMPORTANT]
> Barriers don't make a column execute serially. If your formula has shared mutable state, you typically need **both** `withSerial` (for sequential row processing within a column) **and** a barrier (for ordering between columns).

#### Multiple barriers

You can create multiple barriers when columns have different dependencies. Each barrier is an independent constraint:

```groovy order=t
import io.deephaven.api.Selectable

barrierA = new Object()
barrierB = new Object()

// Column A declares barrierA
colA = Selectable.parse("A = i * 2").withDeclaredBarriers(barrierA)

// Column B declares barrierB
colB = Selectable.parse("B = i * 3").withDeclaredBarriers(barrierB)

// Column C respects BOTH barriers — waits for A and B to finish
colC = Selectable.parse("C = i * 4").withRespectedBarriers(barrierA, barrierB)

// Column D respects only barrierA — waits for A, but not B
colD = Selectable.parse("D = i * 5").withRespectedBarriers(barrierA)

t = emptyTable(10).update([colA, colB, colC, colD])
```

Execution order:

- A and B run in parallel (they don't depend on each other)
- D starts after A finishes (doesn't wait for B)
- C starts after both A and B finish

#### Barriers for Filters

Barriers work the same way for [`Filter`](../../reference/query-language/types/Filter.md) objects in `where` operations. Use them when one filter has side effects that another depends on. This is uncommon — most filters are stateless and don't need barriers.

#### Implicit barriers

When `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is enabled, serial operations automatically create barriers between each other — two serial columns in the same `update` will execute one after the other without explicit barriers.

This behavior is controlled by the `QueryTable.serialSelectImplicitBarriers` configuration property:

- **Stateless mode (default)**: Serial operations only enforce row order within themselves, not between each other. Use explicit barriers if you need cross-operation ordering.
- **Stateful mode**: Serial operations automatically wait for each other. This is useful when operations share global state. Enable by setting `QueryTable.serialSelectImplicitBarriers=true`.

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

#### When to use `withSerial`

**Use `withSerial` when**:

- Rows must be processed in order within a single operation.
- The formula updates global state sequentially.
- The formula depends on row evaluation order.
- A single [`Filter`](../../reference/query-language/types/Filter.md) or [`Selectable`](../../reference/query-language/types/Selectable.md) has order-dependent logic.

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

// A must complete before B starts - A sets values, B reads the final count
colA = Selectable.parse("A = counter.getAndIncrement()").withSerial().withDeclaredBarriers(barrier)
colB = Selectable.parse("B = counter.get()").withRespectedBarriers(barrier)

source = emptyTable(10)
result = source.update([colA, colB])
```

#### Quick reference table

| Scenario                             | Solution                      | Why                                 |
| ------------------------------------ | ----------------------------- | ----------------------------------- |
| Pure column math                     | Default (parallel)            | Thread-safe, no shared state        |
| Global counter                       | `withSerial`                  | Needs sequential row processing     |
| Column A must finish before Column B | Barriers                      | Controls cross-operation ordering   |
| File I/O or logging                  | `withSerial`                  | Serialize access to shared resource |
| Multiple operations sharing state    | Barriers or implicit barriers | Coordinates access to shared state  |
| Non-thread-safe library              | `withSerial`                  | Forces single-threaded access       |

## Key takeaways

Deephaven automatically parallelizes queries across all available CPU cores. Most code works correctly without changes.

- Deephaven assumes all formulas can run in parallel by default.
- Use [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) when your code has side effects, depends on row order, or calls functions that aren't safe to run from multiple threads.
- Use **barriers** when one operation must complete before another starts.
- Both thread pools use all CPU cores by default.

For a quick introduction, see the [crash course](../../getting-started/crash-course/parallelization.md).

## Related documentation

- [Update graph (table dependencies)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
- [ConcurrencyControl API (Javadoc)](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
