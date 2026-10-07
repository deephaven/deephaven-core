---
title: Parallelization
---

Parallelization is running multiple calculations at the same time on different CPU cores instead of one after another. Deephaven automatically parallelizes table operations like [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) to make queries faster, with no configuration required. This guide explains how that parallelization works and when you need to control it.

> [!IMPORTANT]
> **Breaking change in Deephaven 41.0**: In version 0.40.0 and earlier, Deephaven ran a formula in parallel only when it could tell the formula was safe, and ran the rest, such as formulas that call your own functions, one row at a time. Deephaven 41.0 and later assumes all formulas can run in parallel by default. Code that modifies shared variables or depends on rows being processed in a specific order will now produce incorrect results unless you mark it with [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial).
>
> **Quick check**: Does your code use global variables, depend on rows being processed in a specific order, or modify external state? If yes, see [Controlling execution order](#controlling-execution-order) below, or the [Crash Course guide](../../getting-started/crash-course/parallelization.md) for a faster introduction.

## Quick reference

| Situation                                                        | Example                                                  | Solution                                                   |
| ---------------------------------------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------- |
| A formula uses only its own row's values                         | `Total = Price * Quantity`                               | Default (parallel)                                         |
| One formula updates shared state or needs rows in order          | A running counter or sequential IDs                      | `with_serial`                                              |
| One formula calls something that isn't thread-safe               | Writing to a file or log; an unsynchronized client       | `with_serial`                                              |
| One column needs another column to finish first                  | Column `B` reads a cache that column `A` fills           | Barriers                                                   |
| Several columns share the same state or non-thread-safe resource | Two columns that call the same counter function          | `with_serial` on each, plus barriers                       |
| Several tables share the same state or non-thread-safe resource  | Two tables whose formulas call the same counter function | Thread-safe code (barriers only work within one operation) |

## How parallelization works

Deephaven uses all available CPU cores to process queries faster in three ways: concurrent table updates, concurrent row calculations, and concurrent column calculations.

### Concurrent table updates

When you create multiple tables from the same source, Deephaven's update graph can update them concurrently. In this example, three independent tables derive from `market_data`:

```python ticking-table order=null
from deephaven import time_table

# Create a live table with rows arriving at one-second intervals
market_data = time_table("PT1s").update(
    [
        "Symbol = `SYM` + (int)(i % 5)",
        "Price = 100 + randomGaussian(0, 10)",
        "Volume = randomInt(100, 2000000)",
    ]
)

# Three independent transformations from the same source
with_metrics = market_data.update("Value = Price * Volume")
high_volume = market_data.where("Volume > 1000000")
recent_trades = market_data.tail(10)
```

When new data arrives in `market_data`, the update graph schedules `with_metrics`, `high_volume`, and `recent_trades` as independent notifications, which can run concurrently on different cores.

Deephaven tracks which tables depend on which through an internal structure called the [update graph](../dag.md). Independent tables (those that don't depend on each other) run in parallel automatically.

### Concurrent row calculations

Within a single table, Deephaven can split one column's calculation across cores. When you run `source.update("Total = Price * Quantity")`, Deephaven divides the rows into groups, assigns each group to a different CPU core, has each core calculate `Total` for its rows independently, and combines the results into the final `Total` column.

### Concurrent column calculations

Also within a single table, when you compute multiple columns in the same operation, Deephaven can calculate independent columns simultaneously. For example, in `source.update(["A = X * 2", "B = Y + 1"])`, columns `A` and `B` can be computed on different cores at the same time because neither depends on the other.

### What is and isn't parallelized

The three mechanisms above apply to operations that compute and store their results when they run:

- Column calculations in [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md).
- Filters in [`where`](../../reference/table-operations/filter/where.md) clauses.
- [`sort`](../../reference/table-operations/sort/sort.md), once the table is large enough to be worth splitting.

Two other cases look like exceptions but work differently:

- **Deferred evaluation**: [`view`](../../reference/table-operations/select/view.md), [`update_view`](../../reference/table-operations/select/update-view.md), and [`lazy_update`](../../reference/table-operations/select/lazy-update.md) don't compute anything when you call them. They store the formula and evaluate it whenever a cell is read, on whichever thread reads it. That evaluation can itself happen in parallel, for example when a downstream `update` that reads the column is split across cores, and a row can be evaluated more than once. This is also why `with_serial` is rejected for `view` and `update_view`: there is no single evaluation pass to serialize.
- **Serialization you request**: an expression marked with [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) always runs one row at a time. See [Controlling execution order](#controlling-execution-order).

Separately, the update graph never updates a table before the tables it depends on have finished; that ordering is automatic.

### Query phases and thread pools

Queries execute in two phases, and Deephaven uses a separate thread pool for each.

**Initialization**: Every table operation is computed once when you call it, whether at the top of a script or later in a running console. Deephaven computes the initial result from all existing data, splitting the rows across cores as described above. This work runs on the operation initialization thread pool.

For live (refreshing) tables, Deephaven also registers the result in the [update graph](../dag.md) during initialization so it receives future updates.

**Updates**: After initialization, a live table updates whenever its source data changes. Each update uses the same concurrent row and column calculations as initialization, and independent tables update concurrently. This work runs on the update graph thread pool.

Both pools use all CPU cores by default. See [Configuration](#configuration) to size them.

## Python formulas and free-threading

Most Python builds use the GIL (global interpreter lock), which lets only one thread at a time run Python code. The GIL changes how Deephaven parallelizes Python-backed formulas and filters:

- On a standard (GIL-enabled) build, Deephaven never splits one Python-backed column or filter across cores.
- On a [free-threaded Python build](https://docs.python.org/3/howto/free-threading-python.html), Deephaven splits Python-backed columns and filters across cores like any other formula. No Deephaven configuration is needed.

Shared state is not safe on either build. On a GIL-enabled build, two different Python-backed columns in the same [`update`](../../reference/table-operations/select/update.md) can still run at the same time on different threads. A column that is not split across cores can also be evaluated out of row-set order. If your formula or filter has side effects that depend on row order, use [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) no matter which Python build you run.

## When parallelization is safe by default

By default, Deephaven parallelizes operations that are **stateless**, meaning each row's result depends only on that row's input values.

**An operation is stateless if it**:

- Doesn't read or modify global variables.
- Doesn't depend on which row is processed first.
- Produces the same output for the same input, regardless of when or how it runs.

**Examples of stateless operations**:

```python order=source1,result1,source2,result2,source3,result3,source4,result4
from deephaven import empty_table

# Pure column arithmetic
source1 = empty_table(10).update(["Price = i * 10.0", "Quantity = i"])
result1 = source1.update("Total = Price * Quantity")

# String manipulation
source2 = empty_table(10).update(["FirstName = `First` + i", "LastName = `Last` + i"])
result2 = source2.update("FullName = FirstName + ' ' + LastName")

# Conditional logic
source3 = empty_table(10).update("Age = i + 18")
result3 = source3.where("Age > 21")

# Built-in functions
source4 = empty_table(10).update("X = i * 2.0")
result4 = source4.update("Squared = sqrt(X)")
```

> [!NOTE]
> These examples use small tables for clarity. Deephaven only splits one column's rows across cores once there are enough rows to be worth it, but separate columns can run at the same time on a table of any size. The examples illustrate the correctness contract, not a speedup.

## Controlling execution order

Most queries work correctly with automatic parallelization. Some code does not, such as code that uses a counter or modifies shared state. Deephaven provides two controls for these cases, [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) and [barriers](#barriers). You apply them to one of two objects:

- **[`Selectable`](../../reference/query-language/types/Selectable.md)**: a column expression, used in [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md).
- **[`Filter`](../../reference/query-language/types/Filter.md)**: a filter condition, used in [`where`](../../reference/table-operations/filter/where.md).

Concurrency control works the same way for a `Filter` as for a `Selectable`.

The two controls solve different problems:

- **`with_serial`** processes the rows _within one column_ one at a time, in order. Other columns can still run at the same time.
- **Barriers** order columns _relative to each other_. One column finishes all its rows before another column starts. Rows within each column can still run in parallel.

When columns share state, you often need both. `with_serial` protects the shared state within each column, and a barrier makes one column finish before the other starts.

### Serialization

Serialization processes rows one at a time, in order, and never runs a column concurrently with itself. Use it when your code cannot safely run in parallel, for example when a formula reads or modifies global variables, calls external functions that aren't safe to call from multiple threads simultaneously, or depends on rows being processed in a specific order. Without it, parallel execution produces incorrect results: out-of-order values, gaps, or values that don't match what the formula intended.

> [!NOTE]
> Most queries don't need serial execution. Use `with_serial` only when parallelization causes incorrect results.

The [`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) interface provides the [`with_serial`](../../reference/table-operations/select/update.md#serial-execution) method for [`Filter`](../../reference/query-language/types/Filter.md) ([`where`](../../reference/table-operations/filter/where.md#serial-execution)) and [`Selectable`](../../reference/query-language/types/Selectable.md) ([`update`](../../reference/table-operations/select/update.md#serial-execution) and [`select`](../../reference/table-operations/select/select.md)).

> [!IMPORTANT]
> `with_serial` cannot be used with [`view`](../../reference/table-operations/select/view.md) or [`update_view`](../../reference/table-operations/select/update-view.md). These operations compute values on-demand (when cells are accessed), so they cannot guarantee processing order. Use [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) instead when you need serial execution.

#### Example: a counter needs serialization

Consider a function that keeps a counter in global state:

```python skip-test
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


t = empty_table(5_000_000).update("ID = get_and_increment_counter()")
```

When Deephaven splits this column across cores (on a free-threaded Python build), several threads read and update `counter` at the same time. You may see results like:

| ID |
| -- |
| 0  |
| 1  |
| 1  |
| 4  |
| 3  |

Notice the duplicate value (two rows got `1`) and the out-of-order values (`4` before `3`).

To fix this, create a [`Selectable`](../../reference/query-language/types/Selectable.md) and apply [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial):

```python order=result
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


# Force serial execution - rows processed one at a time, in order
col = Selectable.parse("ID = get_and_increment_counter()").with_serial()
result = empty_table(5_000_000).update(col)
```

When a `Selectable` is serial, every row is evaluated in order (row 0, then row 1, then row 2, etc.), and the column never runs concurrently with itself. That protects state that only this column uses. If another column uses the same state, add a [barrier](#barriers) as well. Barriers only order columns and filters within one operation, so if another table's formulas use the same state, make the shared code itself thread-safe (for example, protect it with a lock).

#### Serial filters

Deephaven parallelizes string-based filters in [`where`](../../reference/table-operations/filter/where.md) by default. When a filter has stateful side effects, construct a [`Filter`](../../reference/query-language/types/Filter.md) object and mark it serial:

```python order=result
from deephaven import empty_table
from deephaven.filters import is_null, not_

# Create filters with serial evaluation
filter1 = is_null("X").with_serial()
filter2 = not_(is_null("Y")).with_serial()

result = (
    empty_table(1000)
    .update(["X = i % 5 == 0 ? null : i", "Y = i % 7 == 0 ? null : i"])
    .where([filter1, filter2])
)
```

When a [`Filter`](../../reference/query-language/types/Filter.md) is serial, every input row is evaluated in order, the filter cannot be reordered with respect to other filters, and stateful side effects happen sequentially. On tables from partitioned sources, marking a filter on partitioning columns serial also stops Deephaven from applying the filter to whole partitions before reading data. See [Filters on partitioning columns](../../reference/table-operations/filter/where.md#filters-on-partitioning-columns).

> [!WARNING]
> When a filter on partitioning columns is not applied to whole partitions first, Deephaven reads every partition and evaluates the filter row by row. On a large partitioned source, that can make the query extremely slow. Mark a filter on partitioning columns serial only when its evaluation order matters.

### Barriers

Use barriers when one column must finish all its rows before another column begins. For example:

- Column `A` fills a dictionary that column `B` reads from.
- Column `A` computes a running total that column `B` normalizes against.
- Column `A` assigns sequential IDs that column `B` continues from.

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) is an ordering dependency between two columns:

- One column **declares** the barrier. It runs first.
- Another column **respects** the barrier. It waits.

Deephaven guarantees that the declaring column finishes all of its rows before the respecting column starts. Only one column can declare a given barrier. Any number of columns can respect it.

#### Example: extending the counter with a barrier

This example builds on the counter above. Two columns share the counter. Column `A` should assign IDs 0–9, and column `B` should continue from 10–19. Without a barrier, both columns start at the same time, both read `counter = 0`, and their ranges overlap. With a barrier, column `A` runs first and takes 0–9, then column `B` starts where `A` left off and takes 10–19:

```python order=t
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


barrier = Barrier()

# Column A: serial (protect counter) + declares barrier (must finish first)
col_a = (
    Selectable.parse("A = get_and_increment_counter()")
    .with_serial()
    .with_declared_barriers(barrier)
)

# Column B: serial (protect counter) + respects barrier (waits for A)
col_b = (
    Selectable.parse("B = get_and_increment_counter()")
    .with_serial()
    .with_respected_barriers(barrier)
)

t = empty_table(10).update([col_a, col_b])
```

Column `A` gets values 0–9. Column `B` gets values 10–19. Without the barrier, both columns would race and produce unpredictable results. Without `with_serial`, rows within each column would also race.

> [!IMPORTANT]
> Barriers don't make a column execute serially. If your formula has shared mutable state, you typically need **both** `with_serial` (for sequential row processing within a column) **and** a barrier (for ordering between columns).

Both columns need `with_serial` here because both change the shared `counter`. A respecting column that only reads what the declaring column wrote does not need `with_serial`. The barrier alone guarantees that the write finished first.

#### Multiple barriers

You can create multiple barriers when columns have different dependencies. Each barrier is an independent constraint:

```python order=t
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable
from deephaven import empty_table

barrier_a = Barrier()
barrier_b = Barrier()

# Column A declares barrier_a
col_a = Selectable.parse("A = i * 2").with_declared_barriers(barrier_a)

# Column B declares barrier_b
col_b = Selectable.parse("B = i * 3").with_declared_barriers(barrier_b)

# Column C respects BOTH barriers — waits for A and B to finish
col_c = Selectable.parse("C = i * 4").with_respected_barriers([barrier_a, barrier_b])

# Column D respects only barrier_a — waits for A, but not B
col_d = Selectable.parse("D = i * 5").with_respected_barriers(barrier_a)

t = empty_table(10).update([col_a, col_b, col_c, col_d])
```

Execution order:

- `A` and `B` run in parallel. Neither depends on the other.
- `D` starts after `A` finishes. It does not wait for `B`.
- `C` starts after both `A` and `B` finish.

Barriers work the same way for [`Filter`](../../reference/query-language/types/Filter.md) objects in [`where`](../../reference/table-operations/filter/where.md) operations. Use them when one filter has side effects that another filter depends on. This is uncommon. Most filters are stateless and do not need barriers.

#### Implicit barriers

An implicit barrier is a barrier that Deephaven adds for you. When the `QueryTable.serialSelectImplicitBarriers` property is on, every serial column in a [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) waits for all earlier serial columns in that same operation to finish. Each serial column behaves as if it declared a barrier that every later serial column respects, so you get the ordering without creating barrier objects.

The property is off by default, so serial columns only order their own rows. Two serial columns in the same `update` can still run at the same time, and you add an explicit barrier when one must finish before the other. Turn the property on when many serial columns share state and you would otherwise add a barrier between every pair. See [Configuration](#configuration).

## Configuration

Parallelization is enabled by default with reasonable settings. The properties below change those settings. [Query table configuration](../query-table-configuration.md) describes each one in full and explains how to set it.

| Property                                                                                                      | Default       | What it controls                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`OperationInitializationThreadPool.threads`](../../reference/community-questions/manage-thread-pool-size.md) | All CPU cores | Number of threads that compute a new table's initial result.                                                                                                                                    |
| [`PeriodicUpdateGraph.updateThreads`](../../reference/community-questions/manage-thread-pool-size.md)         | All CPU cores | Number of threads that process updates to live tables.                                                                                                                                          |
| [`QueryTable.minimumParallelSelectRows`](../query-table-configuration.md#parallel-processing-with-select)     | 4,194,304     | `select` and `update` split rows across cores only when they have at least this many rows to process.                                                                                           |
| [`QueryTable.parallelWhereRowsPerSegment`](../query-table-configuration.md#parallel-processing-with-where)    | 65,536        | Rows per segment when `where` splits its work. `where` splits only when it has more than twice this many rows to process.                                                                       |
| [`QueryTable.parallelSort`](../query-table-configuration.md#parallel-sorting)                                 | `true`        | Whether `sort` may run in parallel.                                                                                                                                                             |
| [`QueryTable.minimumParallelSortRows`](../query-table-configuration.md#parallel-sorting)                      | 1,048,576     | `sort` runs in parallel only on tables with at least this many rows.                                                                                                                            |
| [`QueryTable.statelessSelectByDefault`](../query-table-configuration.md#stateless-by-default)                 | `true`        | Whether formulas are assumed safe to run in parallel unless marked serial.                                                                                                                      |
| [`QueryTable.statelessFiltersByDefault`](../query-table-configuration.md#stateless-by-default)                | `true`        | Whether filters are assumed safe to run in parallel unless marked serial.                                                                                                                       |
| [`QueryTable.serialSelectImplicitBarriers`](../query-table-configuration.md#stateless-by-default)             | `false`       | Whether each serial column in a `select` or `update` waits for the earlier serial columns. Defaults to the opposite of `statelessSelectByDefault`. See [Implicit barriers](#implicit-barriers). |

## Key takeaways

Deephaven automatically parallelizes queries across all available CPU cores. Most code works correctly without changes.

- Deephaven assumes all formulas can run in parallel by default.
- Use [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) when a formula has side effects, depends on row order, or calls functions that are not thread-safe.
- Use [barriers](#barriers) when one column must finish before another column starts.
- `with_serial` only keeps one column from running concurrently with itself.
- When several columns share state, use `with_serial` and barriers together.
- When several tables share state, make the shared code itself thread-safe, for example with a lock.

For a quick introduction, see the [Crash Course](../../getting-started/crash-course/parallelization.md).

## Related documentation

- [Query Parallelization (Crash Course)](../../getting-started/crash-course/parallelization.md)
- [Update graph (table dependencies)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
- [Query table configuration](../query-table-configuration.md)
- [Selectable](../../reference/query-language/types/Selectable.md)
- [Filter](../../reference/query-language/types/Filter.md)
- [Barrier](../../reference/query-language/types/Barrier.md)
- [ConcurrencyControl](../../reference/query-language/types/ConcurrencyControl.md)
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl)
