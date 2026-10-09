---
title: Parallelization
---

Parallelization is running multiple calculations at the same time on different CPU cores instead of one after another. Deephaven automatically parallelizes table operations like [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) to make queries faster, with no configuration required. This guide explains how that parallelization works and when you need to control it.

> [!IMPORTANT]
> **Breaking change in Deephaven 41.0**: In version 0.40.0 and earlier, Deephaven ran a formula in parallel only when it could tell the formula was safe, and ran the rest, such as formulas that call closures or read arrays, one row at a time. Deephaven 41.0 and later assumes all formulas can run in parallel by default. Code that modifies shared variables or depends on rows being processed in a specific order now produces incorrect results unless you mark it with [`withSerial`](../../reference/query-language/types/Selectable.md#withserial).
>
> **Quick check**: Does your code use global variables, depend on rows being processed in a specific order, or modify external state? If yes, see [Controlling execution order](#controlling-execution-order) below, or the [Crash Course guide](../../getting-started/crash-course/parallelization.md) for a faster introduction.

## Quick reference

| Situation                                                        | Example                                                       | Solution                                                                                             |
| ---------------------------------------------------------------- | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| A formula uses only its own row's values                         | `Total = Price * Quantity`                                    | Default (parallel)                                                                                   |
| One formula updates shared state or needs rows in order          | A running counter or sequential IDs                           | [`withSerial`](../../reference/query-language/types/Selectable.md#withserial)                        |
| One formula calls something that isn't thread-safe               | Writing to a file or log, or calling an unsynchronized client | [`withSerial`](../../reference/query-language/types/Selectable.md#withserial)                        |
| One column needs another column to finish first                  | Column `B` reads a cache that column `A` fills                | Barriers                                                                                             |
| Several columns share the same state or non-thread-safe resource | Two columns that call the same counter closure                | [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) on each, plus barriers |
| Several tables share the same state or non-thread-safe resource  | Two tables whose formulas call the same counter closure       | Thread-safe code, because barriers only work within one operation                                    |

## How parallelization works

Deephaven uses all available CPU cores to process queries faster in three ways: concurrent table updates, concurrent row calculations, and concurrent column calculations.

### Concurrent table updates

When you create multiple tables from the same source, Deephaven's update graph can update them concurrently. In this example, three independent tables derive from `marketData`:

```groovy ticking-table order=null
// Create a live table with rows arriving at one-second intervals
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

When new data arrives in `marketData`, the update graph schedules `withMetrics`, `highVolume`, and `recentTrades` as independent notifications, which can run concurrently on different cores.

Deephaven tracks which tables depend on which through an internal structure called the [update graph](../dag.md). Independent tables (those that don't depend on each other) run in parallel automatically.

### Concurrent row calculations

Within a single table, Deephaven can split one column's calculation across cores. When you run `source.update("Total = Price * Quantity")`, Deephaven divides the rows into groups, assigns each group to a different CPU core, has each core calculate `Total` for its rows independently, and combines the results into the final `Total` column.

### Concurrent column calculations

Also within a single table, when you compute multiple columns in the same operation, Deephaven can calculate independent columns simultaneously. For example, in `source.update("A = X * 2", "B = Y + 1")`, columns `A` and `B` can be computed on different cores at the same time because neither depends on the other.

### What is and isn't parallelized

Deephaven parallelizes work at two levels: across tables and within one operation.

**Across tables.** The update graph updates independent tables at the same time, as described in [Concurrent table updates](#concurrent-table-updates). Every operation benefits from this.

**Within one operation.** These operations split their own work across cores:

- Column calculations in [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md). Independent columns run at the same time, and the rows of a large column are split into groups.
- Filters in [`where`](../../reference/table-operations/filter/where.md), and the filters that [`whereIn`](../../reference/table-operations/filter/where-in.md) and [`whereNotIn`](../../reference/table-operations/filter/where-not-in.md) build.
- [`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md): the calculations for each group of rows that share key values.
- [`sort`](../../reference/table-operations/sort/sort.md): the first sort of a large table. When a live sorted table updates, Deephaven sorts the changed rows on one thread.
- [`rangeJoin`](../../reference/table-operations/join/range-join.md): each group of matching rows.
- [`transform`](../../reference/table-operations/partitioned-tables/transform.md) and [`proxy`](../../reference/table-operations/partitioned-tables/proxy.md) operations on a [partitioned table](../../how-to-guides/partitioned-tables.md): each constituent table is a separate task.

Joins other than `rangeJoin`, aggregations, [`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md), [`head`](../../reference/table-operations/filter/head.md), [`tail`](../../reference/table-operations/filter/tail.md), [`merge`](../../reference/table-operations/merge/merge.md), and [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) don't split their own work.

**What you can control.** [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) and [barriers](#barriers) apply only to formulas and filters, which is where your code usually runs. `whereIn`, `sort`, `updateBy`, and `rangeJoin` run only Deephaven's own code, which is always safe to run in parallel, so they have no per-call control. A `transform` function is your own code, but Deephaven runs it on several constituents at once and offers no per-call control, so make it thread-safe.

Two other cases work differently:

- **Deferred evaluation**: [`view`](../../reference/table-operations/select/view.md), [`updateView`](../../reference/table-operations/select/update-view.md), and [`lazyUpdate`](../../reference/table-operations/select/lazy-update.md) don't compute anything when you call them. They store the formula and evaluate it whenever a cell is read, on whichever thread reads it. That evaluation can itself happen in parallel, for example when a downstream `update` that reads the column is split across cores, and a row can be evaluated more than once. This is also why `view` and `updateView` reject [`withSerial`](../../reference/query-language/types/Selectable.md#withserial), and why `lazyUpdate` accepts it but doesn't honor it. There is no single evaluation pass to serialize.
- **Serialization you request**: an expression marked with [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) always runs one row at a time. See [Controlling execution order](#controlling-execution-order).

Separately, the update graph never updates a table before the tables it depends on have finished. That ordering is automatic.

### Query phases and thread pools

Queries execute in two phases, and Deephaven uses a separate thread pool for each.

**Initialization**: Every table operation is computed once when you call it, whether at the top of a script or later in a running console. Deephaven computes the initial result from all existing data, splitting the rows across cores as described above. This work runs on the operation initialization thread pool.

For live (refreshing) tables, Deephaven also registers the result in the [update graph](../dag.md) during initialization so it receives future updates.

**Updates**: After initialization, a live table updates whenever its source data changes. Each update uses the same concurrent row and column calculations as initialization, and independent tables update concurrently. This work runs on the update graph thread pool.

Both pools use all CPU cores by default. See [Configuration](#configuration) to size them.

## When parallelization is safe by default

By default, Deephaven safely parallelizes operations that are **stateless**, meaning each row's result depends only on that row's input values.

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
> These examples use small tables for clarity. Deephaven only splits one column's rows across cores once there are enough rows to be worth it, but separate columns can run at the same time on a table of any size. The examples illustrate the correctness contract, not a speedup.

## Controlling execution order

Most queries work correctly with automatic parallelization. Some code does not, such as code that uses a counter or modifies shared state. Deephaven provides two controls for these cases, [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) and [barriers](#barriers). You apply them to one of two objects:

- **[`Selectable`](../../reference/query-language/types/Selectable.md)**: a column expression, used in [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md).
- **[`Filter`](../../reference/query-language/types/Filter.md)**: a filter condition, used in [`where`](../../reference/table-operations/filter/where.md).

Concurrency control works the same way for a [`Filter`](../../reference/query-language/types/Filter.md) as for a [`Selectable`](../../reference/query-language/types/Selectable.md).

The two controls solve different problems:

- **[`withSerial`](../../reference/query-language/types/Selectable.md#withserial)** processes the rows _within one column_ one at a time, in order. Other columns can still run at the same time.
- **[Barriers](#barriers)** order columns _relative to each other_. One column finishes all its rows before another column starts. Rows within each column can still run in parallel.

When columns share state, you often need both. [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) protects the shared state within each column, and a barrier makes one column finish before the other starts.

### Serialization

Serialization processes rows one at a time, in order, and never runs a column concurrently with itself. Use it when your code cannot safely run in parallel. For example, a formula that:

- Reads or modifies global variables.
- Calls external functions that aren't safe to call from multiple threads at the same time.
- Depends on rows being processed in a specific order.

Without it, parallel execution can produce incorrect results, such as out-of-order values, gaps, or values that don't match what the formula intended.

> [!NOTE]
> Most queries don't need serial execution. Use [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) only when parallelization causes incorrect results.

The [`ConcurrencyControl`](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) interface provides the [`withSerial`](../../reference/table-operations/select/update.md#serial-execution) method for [`Filter`](../../reference/query-language/types/Filter.md) ([`where`](../../reference/table-operations/filter/where.md#serial-execution)) and [`Selectable`](../../reference/query-language/types/Selectable.md) ([`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md)).

> [!IMPORTANT]
> You cannot use [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) with [`view`](../../reference/table-operations/select/view.md) or [`updateView`](../../reference/table-operations/select/update-view.md), and [`lazyUpdate`](../../reference/table-operations/select/lazy-update.md) ignores it. These operations compute values on demand, when a cell is read, so they cannot guarantee processing order. Use [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) instead when you need serial execution.

#### Example: a counter needs serialization

Consider a function that keeps a counter in global state:

```groovy skip-test
// Use a one-element int[] so parallel access can corrupt it
counter = [0] as int[]

getAndIncrement = { counter[0]++ }

badResult = emptyTable(5_000_000).update("ID = getAndIncrement()")
```

Without serialization, Deephaven splits this column across cores, and several threads read and update `counter` at the same time. This doesn't throw an error. Instead, it silently produces wrong values. You may see results like:

| ID |
| -- |
| 0  |
| 1  |
| 1  |
| 4  |
| 3  |

Notice the duplicate value (two rows got `1`) and the out-of-order values (`4` before `3`).

To fix this, create a [`Selectable`](../../reference/query-language/types/Selectable.md) and apply [`withSerial`](../../reference/query-language/types/Selectable.md#withserial):

```groovy order=result
import io.deephaven.api.Selectable

// Use a one-element int[] so the result is only correct when serialized
counter = [0] as int[]

getAndIncrement = { counter[0]++ }

// Force serial execution - rows processed one at a time, in order
col = Selectable.parse("ID = getAndIncrement()").withSerial()
result = emptyTable(5_000_000).update([col])
```

When a `Selectable` is serial, Deephaven evaluates every row in order, starting with row 0, and never runs the column concurrently with itself. That protects state that only this column uses. If another column uses the same state, add a [barrier](#barriers) as well. Barriers only order columns and filters within one operation, so if another table's formulas use the same state, make the shared code itself thread-safe, for example by protecting it with a lock.

#### Serial filters

Deephaven parallelizes string-based filters in [`where`](../../reference/table-operations/filter/where.md) by default. When a filter has stateful side effects, construct a [`Filter`](../../reference/query-language/types/Filter.md) object and mark it serial:

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

When a [`Filter`](../../reference/query-language/types/Filter.md) is serial, Deephaven evaluates every input row in order, never reorders the filter relative to other filters, and runs stateful side effects one at a time. On a table from a partitioned source, such as a directory of Parquet files or an Iceberg table, Deephaven normally applies a filter on partitioning columns, the columns whose values identify each partition, to whole partitions before it reads any data. Marking that filter serial turns this off. See [Filters on partitioning columns](../../reference/table-operations/filter/where.md#filters-on-partitioning-columns).

> [!WARNING]
> When Deephaven cannot apply a filter on partitioning columns to whole partitions first, it includes every partition in the table and evaluates the filter against every row instead of once per partition. On a large partitioned source, that can be much slower. Mark a filter on partitioning columns serial only when its evaluation order matters.

### Barriers

Use barriers when one column must finish all its rows before another column begins. For example:

- Column `A` fills a map that column `B` reads from.
- Column `A` computes a running total that column `B` normalizes against.
- Column `A` assigns sequential IDs that column `B` continues from.

A [barrier](../../reference/query-language/types/Barrier.md) is an ordering dependency between two columns:

- One column **declares** the barrier. It runs first.
- Another column **respects** the barrier. It waits.

Deephaven guarantees that the declaring column finishes all of its rows before the respecting column starts. Only one column can declare a given barrier. Any number of columns can respect it. The declaring column must come before the respecting columns in the argument list. Respecting a barrier that no earlier column declared is an error. A barrier only orders columns and filters within one operation. It cannot order work across tables. In Groovy, any Java object can serve as a barrier. See [`withDeclaredBarriers`](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html#withDeclaredBarriers(java.lang.Object...)).

#### Example: extending the counter with a barrier

This example builds on the counter above. Two columns share the counter. Column `A` should assign IDs 0–9, and column `B` should continue from 10–19. `AtomicInteger.getAndIncrement` is itself thread-safe, so the counter cannot produce duplicate or corrupted values even without a barrier. There is still no guarantee which column's rows claim the lower values, so the ranges of `A` and `B` could interleave instead of landing as two clean blocks. With a barrier, column `A` runs first and takes 0–9, then column `B` starts where `A` left off and takes 10–19:

```groovy order=t
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

barrier = new Object()

// Column A: serial (keep rows in order) + declares barrier (must finish first)
colA = Selectable.parse("A = counter.getAndIncrement()")
    .withSerial()
    .withDeclaredBarriers(barrier)

// Column B: serial (keep rows in order) + respects barrier (waits for A)
colB = Selectable.parse("B = counter.getAndIncrement()")
    .withSerial()
    .withRespectedBarriers(barrier)

t = emptyTable(10).update([colA, colB])
```

Column `A` gets values 0–9. Column `B` gets values 10–19. Without the barrier, there is no guarantee that `A`'s rows claim the lower values, so the two ranges could interleave unpredictably. Without [`withSerial`](../../reference/query-language/types/Selectable.md#withserial), Deephaven could also evaluate a column's own rows out of row order, which breaks the correspondence between row and counter value even within a single column.

> [!IMPORTANT]
> Barriers don't make a column execute serially. If your formula has shared mutable state, you typically need both controls. [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) orders the rows within a column, and a barrier orders the columns relative to each other.

A respecting column that only reads what the declaring column wrote does not need [`withSerial`](../../reference/query-language/types/Selectable.md#withserial). The barrier alone guarantees that the write finished first.

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

- `A` and `B` can run in parallel. Neither depends on the other.
- `D` starts after `A` finishes. It does not wait for `B`.
- `C` starts after both `A` and `B` finish.

#### Barriers on filters

Barriers work the same way for [`Filter`](../../reference/query-language/types/Filter.md) objects in [`where`](../../reference/table-operations/filter/where.md) operations. Use them when one filter has side effects that another filter depends on. This is uncommon. Most filters are stateless and do not need barriers.

#### Implicit barriers

An implicit barrier is a barrier that Deephaven adds for you.

When the [`QueryTable.serialSelectImplicitBarriers`](../query-table-configuration.md#stateless-by-default) property is on:

- Every column that is not stateless in a [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) waits for all earlier such columns in the same operation to finish.
- With the default stateless-by-default setting, those are the columns you marked [`withSerial`](../../reference/query-language/types/Selectable.md#withserial).
- Each of them behaves as if it declared a barrier that every later one respects. You get the ordering without creating barrier objects.

The property is off by default:

- Serial columns only order their own rows.
- Two serial columns in the same `update` can still run at the same time.
- You add an explicit barrier when one must finish before the other.

Turn the property on when many serial columns share state and you would otherwise add a barrier between every pair. See [Configuration](#configuration).

## Configuration

Parallelization is enabled by default with reasonable settings. The properties below change those settings. The linked page for each property describes it in full and explains how to set it.

| Property                                                                                                      | Default                                          | What it controls                                                                                                                        |
| ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| [`OperationInitializationThreadPool.threads`](../../reference/community-questions/manage-thread-pool-size.md) | All CPU cores                                    | Number of threads that compute a new table's initial result.                                                                            |
| [`PeriodicUpdateGraph.updateThreads`](../../reference/community-questions/manage-thread-pool-size.md)         | All CPU cores                                    | Number of threads that process updates to live tables.                                                                                  |
| [`QueryTable.minimumParallelSelectRows`](../query-table-configuration.md#parallel-processing-with-select)     | 4,194,304                                        | Minimum rows to process before `select` and `update` split them across cores.                                                           |
| [`QueryTable.parallelWhereRowsPerSegment`](../query-table-configuration.md#parallel-processing-with-where)    | 65,536                                           | Rows per segment when `where` splits its work. Splitting starts above twice this many rows.                                             |
| [`QueryTable.parallelSort`](../query-table-configuration.md#parallel-sorting)                                 | `true`                                           | Whether `sort` may run in parallel.                                                                                                     |
| [`QueryTable.minimumParallelSortRows`](../query-table-configuration.md#parallel-sorting)                      | 1,048,576                                        | Minimum rows before `sort` runs in parallel.                                                                                            |
| [`QueryTable.statelessSelectByDefault`](../query-table-configuration.md#stateless-by-default)                 | `true`                                           | Whether formulas are assumed safe to run in parallel unless marked serial.                                                              |
| [`QueryTable.statelessFiltersByDefault`](../query-table-configuration.md#stateless-by-default)                | `true`                                           | Whether filters are assumed safe to run in parallel unless marked serial.                                                               |
| [`QueryTable.serialSelectImplicitBarriers`](../query-table-configuration.md#stateless-by-default)             | `false` (opposite of `statelessSelectByDefault`) | Whether each serial column in a `select` or `update` waits for the earlier serial columns. See [Implicit barriers](#implicit-barriers). |

## Key takeaways

Deephaven automatically parallelizes queries across all available CPU cores. Most code works correctly without changes.

- Deephaven assumes all formulas can run in parallel by default.
- Use [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) when a formula has side effects, depends on row order, or calls functions that are not thread-safe.
- [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) works with [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md). [`view`](../../reference/table-operations/select/view.md) and [`updateView`](../../reference/table-operations/select/update-view.md) compute values when they are read, so they do not support it, and `lazyUpdate` ignores it.
- [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) only orders rows within one column. It does not order columns relative to each other.
- Use [barriers](#barriers) when one column must finish before another column starts.
- When several columns share state, use [`withSerial`](../../reference/query-language/types/Selectable.md#withserial) and barriers together.
- When several tables share state, make the shared code itself thread-safe, for example with a lock.

## Related documentation

- [Query Parallelization (Crash Course)](../../getting-started/crash-course/parallelization.md)
- [Update graph (table dependencies)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
- [Query table configuration](../query-table-configuration.md)
- [Selectable](../../reference/query-language/types/Selectable.md)
- [Filter](../../reference/query-language/types/Filter.md)
- [Barrier](../../reference/query-language/types/Barrier.md)
- [ConcurrencyControl](../../reference/query-language/types/ConcurrencyControl.md)
- [ConcurrencyControl API (Javadoc)](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
