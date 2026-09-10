---
title: Parallelization
sidebar_label: Parallelization
---

Parallelization is running multiple calculations at the same time on different CPU cores instead of one after another. Deephaven automatically parallelizes table operations like [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) to make queries faster, with no configuration required. This guide explains how that parallelization works and when you need to control it.

> [!IMPORTANT]
> **Breaking change in Deephaven 41+**: Deephaven 40 and earlier assumed all formulas required sequential processing by default. Deephaven 41 and later assumes all formulas can run in parallel by default. Code that modifies shared variables or depends on rows being processed in a specific order will now produce incorrect results unless you mark it with [`withSerial`](../../reference/query-language/types/Selectable.md#withserial).
>
> **Quick check**: Does your code use global variables, depend on rows being processed in a specific order, or modify external state? If yes, see [Controlling execution order](#controlling-execution-order) below, or the [crash course guide](../../getting-started/crash-course/parallelization.md) for a faster introduction.

## Quick reference

| Scenario                             | Solution                      | Why                                 |
| ------------------------------------ | ----------------------------- | ----------------------------------- |
| Pure column math                     | Default (parallel)            | Thread-safe, no shared state        |
| Global counter                       | `withSerial`                  | Needs sequential row processing     |
| Column A must finish before Column B | Barriers                      | Controls cross-operation ordering   |
| File I/O or logging                  | `withSerial`                  | Serialize access to shared resource |
| Multiple operations sharing state    | Barriers or implicit barriers | Coordinates access to shared state  |
| Non-thread-safe library              | `withSerial`                  | Forces single-threaded access       |

## How parallelization works

Deephaven uses all available CPU cores to process queries faster, in three ways: across tables, across rows, and across columns.

### Across tables

When you create multiple tables from the same source, Deephaven computes them simultaneously. In this example, three independent tables derive from `marketData`:

```groovy ticking-table order=null
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

When new data arrives in `marketData`, the update graph schedules `withMetrics`, `highVolume`, and `recentTrades` as independent notifications, which can run concurrently on different cores. (This depends on `PeriodicUpdateGraph.updateThreads` being greater than 1, which is the default — see [Thread pools](#query-phases-and-thread-pools) below.)

Deephaven tracks which tables depend on which through an internal structure called the [update graph](../dag.md). Independent tables (those that don't depend on each other) run in parallel automatically.

### Within a single table

Deephaven also parallelizes calculations within a single table, in two ways:

**Across rows**: When you run `source.update("Total = Price * Quantity")`, Deephaven divides the rows into groups, assigns each group to a different CPU core, has each core calculate `Total` for its rows independently, and combines the results into the final `Total` column.

**Across columns**: When you compute multiple columns in the same operation, Deephaven can calculate independent columns simultaneously. For example, in `source.update("A = X * 2", "B = Y + 1")`, columns `A` and `B` can be computed on different cores at the same time because neither depends on the other.

**What gets parallelized**:

- Column calculations in [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md).
- Filters in [`where`](../../reference/table-operations/filter/where.md) clauses.

**What does NOT get parallelized**:

- [`view`](../../reference/table-operations/select/view.md), [`updateView`](../../reference/table-operations/select/update-view.md), and [`lazyUpdate`](../../reference/table-operations/select/lazy-update.md) — these are lazily evaluated when cells are accessed, not computed upfront.
- Operations marked with [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) (you control this — see [Controlling execution order](#controlling-execution-order) below).
- Operations waiting for dependencies (automatic in the update graph).

### Query phases and thread pools

Queries execute in two phases, and Deephaven uses a separate thread pool for each.

**Initialization**: Each time you create a table operation (like [`where`](../../reference/table-operations/filter/where.md) or [`update`](../../reference/table-operations/select/update.md)) — whether it's the first line of a script or something you type into a running console later — Deephaven computes that operation's initial result using all existing data, dividing the rows among CPU cores (the "across rows" parallelism described above). This is handled by the **Operation Initialization Thread Pool**, configured with `OperationInitializationThreadPool.threads` (default `-1`, meaning use all available cores).

For live (refreshing) tables, Deephaven also registers the table in the [update graph](../dag.md) during initialization so it can receive future updates.

**Updates**: After initialization, live tables update whenever their source data changes, parallelizing across rows and across columns just like during initialization, plus across tables: independent downstream tables' update-graph notifications are processed concurrently, so two tables that both depend on the same changed source can each finish updating on their own core without waiting for each other. This is handled by the **Update Graph Processor Thread Pool**, configured with `PeriodicUpdateGraph.updateThreads` (default `-1`, meaning use all available cores).

Both thread pools default to using all CPU cores, determined by [`Runtime.availableProcessors()`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html#availableProcessors()) at startup. Set either property to a specific number to limit parallelism during that phase.

## When parallelization is safe by default

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

> [!NOTE]
> These examples use small tables for clarity. Deephaven only splits a `select`/`update` computation across cores once a table crosses `QueryTable.minimumParallelSelectRows` (about 4.2 million rows by default), and `where` has its own, much smaller per-segment threshold (`QueryTable.parallelWhereRowsPerSegment`, about 65,536 rows by default). Below those thresholds, Deephaven evaluates the formula on a single core regardless of whether it's marked stateless — these examples illustrate the correctness contract, not actual observed parallel speedup.

You can change the default behavior using configuration properties: `QueryTable.statelessSelectByDefault` for [`select`](../../reference/table-operations/select/select.md)/[`update`](../../reference/table-operations/select/update.md), and `QueryTable.statelessFiltersByDefault` for filters.

## Controlling execution order

Most queries work correctly with automatic parallelization. Some code doesn't — for example, code that uses a counter or modifies shared state. Deephaven provides two mechanisms to control execution order:

**Key concepts**:

- **[`Selectable`](../../reference/query-language/types/Selectable.md)**: Represents a column expression, used in `select` or `update` operations.
- **[`Filter`](../../reference/query-language/types/Filter.md)**: Represents a filter condition, used in `where` operations. Concurrency control works the same way for `Filter` as it does for `Selectable`.
- **[`withSerial`](../../reference/query-language/types/Selectable.md#withserial)**: Forces rows to be processed one at a time, in order.
- **[Barrier](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html#withDeclaredBarriers(java.lang.Object...))**: Ensures one operation completes before another starts. In Groovy, any Java object can serve as a barrier.

**`withSerial` vs. barriers** — these solve different problems:

- **`withSerial`**: Rows _within one column_ are processed sequentially (row 0, then row 1, etc.). Other columns can still run at the same time.
- **Barriers**: _Between columns_, one column finishes all its rows before another column starts. Rows within each column can still be parallelized.

When shared state is involved, you often need both: `withSerial` to protect row-level access to the shared state, and a barrier to ensure one column is completely done before the other starts.

### Serialization

Serialization forces Deephaven to process rows one at a time, in order, on a single thread. Use it when your code cannot safely run in parallel — for example, when a formula reads or modifies global variables, calls external functions that aren't safe to call from multiple threads simultaneously, or depends on rows being processed in a specific order. Without it, parallel execution produces incorrect results: out-of-order values, gaps, or values that don't match what the formula intended.

> [!NOTE]
> Most queries don't need serial execution. Use `withSerial` only when parallelization causes incorrect results.

The [`ConcurrencyControl`](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) interface provides the `withSerial` method for `Filter` (`where`) and `Selectable` (`update` and `select`).

> [!IMPORTANT]
> You cannot use `withSerial` with `view` or `updateView`. These operations compute values on-demand (when cells are accessed), so they cannot guarantee processing order. Use `select` or `update` instead when you need serial execution.

#### Example: a counter needs serialization

Consider a function that maintains global state — a counter:

```groovy skip-test
// Use a one-element int[] so parallel access can corrupt it
counter = [0] as int[]

getAndIncrement = { counter[0]++ }

bad_result = emptyTable(5_000_000).update("A = getAndIncrement()", "B = getAndIncrement()")
```

Without serialization, parallel execution causes race conditions where multiple threads read and update `counter` simultaneously. This doesn't throw an error — it silently produces wrong values. You may see results like:

| A | B |
| - | - |
| 0 | 2 |
| 1 | 3 |
| 5 | 6 |
| 4 | 7 |
| 9 | 8 |

Notice the out-of-order values (row 4 has `A=4` after row 3 has `A=5`), gaps (no 10-19 visible), and `B` not following `A + 1`.

To fix this, create a `Selectable` object and apply `withSerial`:

```groovy order=result
import io.deephaven.api.Selectable

// Use a one-element int[] so the result is only correct when serialized
counter = [0] as int[]

getAndIncrement = { counter[0]++ }

// Force serial execution - rows processed one at a time, in order
col = Selectable.parse("ID = getAndIncrement()").withSerial()
result = emptyTable(5_000_000).update([col])
```

When a Selectable is serial, every row is evaluated in order (row 0, then row 1, then row 2, etc.), only one thread processes the column at a time, and global state updates happen sequentially without race conditions.

The same applies to filters. Deephaven parallelizes string-based filters in [`where`](../../reference/table-operations/filter/where.md) by default, so construct `Filter` objects explicitly when a filter has stateful side effects:

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

When a [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) is serial, every input row is evaluated in order, the filter cannot be reordered with respect to other filters, and stateful side effects happen sequentially.

### Barriers

Use barriers when one operation must finish all its rows before another operation begins — for example, when column A populates a map that column B reads from, when column A computes a running total that column B normalizes against, or when column A assigns sequential IDs that column B should continue from.

A barrier creates an ordering dependency between two operations: one operation **declares** the barrier (it goes first), another **respects** it (it waits), and Deephaven guarantees the declaring operation completes all rows before the respecting operation begins. Each barrier can only be declared by one operation; multiple operations can respect the same barrier.

#### Example: extending the counter with a barrier

Building on the counter example above: consider two columns that share a counter, where column A should assign IDs 0–9 and column B should continue from 10–19. Without a barrier, both columns would start simultaneously, both read the counter starting at 0, and produce overlapping, incorrect results. With a barrier, column A runs first (0–9), then column B starts where A left off (10–19):

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

Execution order: A and B run in parallel (they don't depend on each other); D starts after A finishes (doesn't wait for B); C starts after both A and B finish.

Barriers work the same way for [`Filter`](../../reference/query-language/types/Filter.md) objects in `where` operations — use them when one filter has side effects that another depends on. This is uncommon; most filters are stateless and don't need barriers.

#### Implicit barriers

When `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is enabled, serial operations automatically create barriers between each other — two serial columns in the same `update` will execute one after the other without explicit barriers. This behavior is controlled by the `QueryTable.serialSelectImplicitBarriers` configuration property:

- **Stateless mode (default)**: Serial operations only enforce row order within themselves, not between each other. Use explicit barriers if you need cross-operation ordering.
- **Stateful mode**: Serial operations automatically wait for each other. This is useful when operations share global state. Enable by setting `QueryTable.serialSelectImplicitBarriers=true`.

Most users don't need to change this setting.

### Stateful partition filters

The serial/barrier rules above apply to ordinary filters. _Partition filters_ — filters that only access partitioning columns — are a special case: Deephaven evaluates them per location rather than per row, so marking one serial changes its evaluation strategy rather than just its ordering.

When you mark a partition filter as serial, Deephaven cannot reorder it and must evaluate it on all rows of the table. However, if you don't explicitly mark a partition filter as serial, the engine treats it as stateless for performance reasons — even when Deephaven is configured to treat filters as stateful by default. This lets Deephaven relax ordering constraints for filters on partitioning columns, evaluate them per location rather than on every row, reorder common partition filters ahead of others, and avoid repeated evaluation against the same value. For example, the formula filter `Date=today()` is stateful if Deephaven treats filters as stateful by default, but in nearly every case users prefer Deephaven to evaluate it early, location-by-location.

## Choosing an approach

Use the [Quick reference](#quick-reference) table above for a fast lookup. In more detail:

**Use default parallel execution when** the formula only uses values from the current row, has no side effects, doesn't depend on row processing order, and is thread-safe — this covers most formulas, including the [stateless examples above](#when-parallelization-is-safe-by-default).

**Use `withSerial` when** rows must be processed in order within a single operation, or the formula updates global state sequentially, as in the [counter example above](#example-a-counter-needs-serialization). Common cases: sequential numbering, processing events in chronological sequence, cumulative calculations, file I/O or logging.

**Use barriers when** you need to control ordering _between_ different operations — one must finish before another starts, as in the [barrier example above](#example-extending-the-counter-with-a-barrier). Common cases: one column or filter populates a cache or resource that another reads from.

## Key takeaways

Deephaven automatically parallelizes queries across all available CPU cores. Most code works correctly without changes.

- Deephaven assumes all formulas can run in parallel by default.
- Use [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) when your code has side effects, depends on rows being processed in a specific order, or calls functions that aren't safe to run from multiple threads.
- Use **barriers** when one operation must complete before another starts.
- Both thread pools use all CPU cores by default.

For a quick introduction, see the [crash course](../../getting-started/crash-course/parallelization.md).

## Related documentation

- [Update graph (table dependencies)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
- [ConcurrencyControl API (Javadoc)](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
