---
title: Parallelization
sidebar_label: Parallelization
---

Parallelization is running multiple calculations at the same time on different CPU cores instead of one after another. Deephaven automatically parallelizes table operations like [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) to make queries faster, with no configuration required. This guide explains how that parallelization works and when you need to control it.

> [!IMPORTANT]
> **Breaking change in Deephaven 41.0**: In version 0.40 and earlier, Deephaven ran a formula in parallel only when it could tell the formula was safe, and ran the rest — such as formulas that call your own functions — one row at a time. Deephaven 41.0 and later assumes all formulas can run in parallel by default. Code that modifies shared variables or depends on rows being processed in a specific order will now produce incorrect results unless you mark it with [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial).
>
> **Quick check**: Does your code use global variables, depend on rows being processed in a specific order, or modify external state? If yes, see [Controlling execution order](#controlling-execution-order) below, or the [Crash Course guide](../../getting-started/crash-course/parallelization.md) for a faster introduction.

## Quick reference

| Situation                                                        | Example                                                  | Solution                                                   |
| ---------------------------------------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------- |
| A formula uses only its own row's values                         | `Total = Price * Quantity`                               | Default (parallel)                                         |
| One formula updates shared state or needs rows in order          | A running counter or sequential IDs                      | `with_serial`                                              |
| One formula calls something that isn't thread-safe               | Writing to a file or log; an unsynchronized client       | `with_serial`                                              |
| One column needs another column to finish first                  | Column B reads a cache that column A fills               | Barriers                                                   |
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

- **Deferred evaluation**: [`view`](../../reference/table-operations/select/view.md), [`update_view`](../../reference/table-operations/select/update-view.md), and [`lazy_update`](../../reference/table-operations/select/lazy-update.md) don't compute anything when you call them. They store the formula and evaluate it whenever a cell is read, on whichever thread reads it. That evaluation can itself happen in parallel — for example, when a downstream `update` that reads the column is split across cores — and a row can be evaluated more than once. This is also why `with_serial` is rejected for `view` and `update_view`: there is no single evaluation pass to serialize.
- **Serialization you request**: an expression marked with [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) always runs one row at a time — see [Controlling execution order](#controlling-execution-order).

Separately, the update graph never updates a table before the tables it depends on have finished; that ordering is automatic.

### Query phases and thread pools

Queries execute in two phases, and Deephaven uses a separate thread pool for each.

**Initialization**: Every table operation is computed once when you call it — whether at the top of a script or later in a running console. Deephaven computes the initial result from all existing data, splitting the rows across cores as described above. This work runs on the operation initialization thread pool.

For live (refreshing) tables, Deephaven also registers the result in the [update graph](../dag.md) during initialization so it receives future updates.

**Updates**: After initialization, a live table updates whenever its source data changes. Each update uses the same concurrent row and column calculations as initialization, and independent tables update concurrently. This work runs on the update graph thread pool.

Both pools use all CPU cores by default. See [Configuration](#configuration) to size them.

## Python formulas and free-threading

Most Python builds use the GIL (global interpreter lock), which prevents concurrent execution of Python code across threads. Deephaven only splits a Python-backed filter or formula across cores on a [free-threaded Python build](https://docs.python.org/3/howto/free-threading-python.html). On a standard (GIL-enabled) build, two different Python-backed columns in the same `update` can still run at the same time on different threads, so shared state is not safe there either. To get parallel execution of Python-backed formulas and filters, switch to a free-threaded Python build; no other Deephaven configuration is required.

Not being split across cores is not the same guarantee `with_serial` provides. The engine may still evaluate a non-parallelizable column out of row-set order. If your formula or filter has side effects that depend on row order, use `with_serial` regardless of which Python build you're running.

## When parallelization is safe by default

By default, Deephaven parallelizes operations that are **stateless** — meaning each row's result depends only on that row's input values.

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

Most queries work correctly with automatic parallelization. Some code doesn't — for example, code that uses a counter or modifies shared state. Deephaven provides two controls for these cases, `with_serial` and barriers, which you apply to these objects:

- **[`Selectable`](../../reference/query-language/types/Selectable.md)**: Represents a column expression, used in `select` or `update` operations.
- **[`Filter`](../../reference/query-language/types/Filter.md)**: Represents a filter condition, used in `where` operations. Concurrency control works the same way for `Filter` as it does for `Selectable`.

**`with_serial` vs. barriers** — these solve different problems:

- **`with_serial`**: Rows _within one column_ are processed sequentially (row 0, then row 1, etc.). Other columns can still run at the same time.
- **Barriers**: _Between columns_, one column finishes all its rows before another column starts. Rows within each column can still be parallelized.

When shared state is involved, you often need both: `with_serial` to protect row-level access to the shared state, and a barrier to ensure one column is completely done before the other starts.

### Serialization

Serialization processes rows one at a time, in order, and never runs a column concurrently with itself. Use it when your code cannot safely run in parallel — for example, when a formula reads or modifies global variables, calls external functions that aren't safe to call from multiple threads simultaneously, or depends on rows being processed in a specific order. Without it, parallel execution produces incorrect results: out-of-order values, gaps, or values that don't match what the formula intended.

> [!NOTE]
> Most queries don't need serial execution. Use `with_serial` only when parallelization causes incorrect results.

The [`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) interface provides the [`with_serial`](../../reference/table-operations/select/update.md#serial-execution) method for [`Filter`](../../reference/query-language/types/Filter.md) ([`where`](../../reference/table-operations/filter/where.md#serial-execution)) and [`Selectable`](../../reference/query-language/types/Selectable.md) ([`update`](../../reference/table-operations/select/update.md#serial-execution) and [`select`](../../reference/table-operations/select/select.md)).

> [!IMPORTANT]
> `with_serial` cannot be used with [`view`](../../reference/table-operations/select/view.md) or [`update_view`](../../reference/table-operations/select/update-view.md). These operations compute values on-demand (when cells are accessed), so they cannot guarantee processing order. Use [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) instead when you need serial execution.

#### Example: a counter needs serialization

Consider a function that maintains global state — a counter:

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

To fix this, create a `Selectable` object and apply `with_serial`:

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

The same applies to filters. Deephaven parallelizes string-based filters in [`where`](../../reference/table-operations/filter/where.md) by default, so construct `Filter` objects explicitly when a filter has stateful side effects:

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

When a [`Filter`](../../reference/query-language/types/Filter.md) is serial, every input row is evaluated in order, the filter cannot be reordered with respect to other filters, and stateful side effects happen sequentially. On tables from partitioned sources, marking a filter on partitioning columns serial also stops Deephaven from applying it to whole partitions before reading data — see [Filters on partitioning columns](../../reference/table-operations/filter/where.md#filters-on-partitioning-columns).

### Barriers

Use barriers when one operation must finish all its rows before another operation begins — for example, when column A populates a dictionary that column B reads from, when column A computes a running total that column B normalizes against, or when column A assigns sequential IDs that column B should continue from.

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) creates an ordering dependency between two operations: one operation **declares** the barrier (it goes first), another **respects** it (it waits), and Deephaven guarantees the declaring operation completes all rows before the respecting operation begins. Each barrier can only be declared by one operation; multiple operations can respect the same barrier.

#### Example: extending the counter with a barrier

Building on the counter example above: consider two columns that share a counter, where column A should assign IDs 0–9 and column B should continue from 10–19. Without a barrier, both columns would start simultaneously, both read `counter = 0`, and produce overlapping, incorrect results. With a barrier, column A runs first (0–9), then column B starts where A left off (10–19):

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

Column A gets values 0–9. Column B gets values 10–19. Without the barrier, both columns would race and produce unpredictable results. Without `with_serial`, rows within each column would also race.

> [!IMPORTANT]
> Barriers don't make a column execute serially. If your formula has shared mutable state, you typically need **both** `with_serial` (for sequential row processing within a column) **and** a barrier (for ordering between columns).

Both columns need `with_serial` here because both mutate the shared `counter`. That's not always true: if a respecting column only reads a value that the declaring column already finished writing (rather than mutating shared state itself), it doesn't need `with_serial` — the barrier alone guarantees the write happened first.

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

Execution order: A and B run in parallel (they don't depend on each other); D starts after A finishes (doesn't wait for B); C starts after both A and B finish.

Barriers work the same way for [`Filter`](../../reference/query-language/types/Filter.md) objects in `where` operations — use them when one filter has side effects that another depends on. This is uncommon; most filters are stateless and don't need barriers.

#### Implicit barriers

When implicit barriers are enabled, serial operations automatically create barriers between each other — two serial columns in the same `update` execute one after the other without explicit barriers:

- **Stateless mode (default)**: Serial operations only enforce row order within themselves, not between each other. Use explicit barriers if you need cross-operation ordering.
- **Stateful mode**: Serial operations automatically wait for each other. This is useful when operations share global state.

Most users don't need to change this setting — see [Configuration](#configuration).

## Configuration

Parallelization needs no configuration. These properties tune it; defaults and full descriptions are in [Query table configuration](../query-table-configuration.md).

| Property                                                                      | Controls                                                                 |
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `OperationInitializationThreadPool.threads`                                   | Thread count for the initialization phase (default: all cores)           |
| `PeriodicUpdateGraph.updateThreads`                                           | Thread count for the update phase (default: all cores)                   |
| `QueryTable.minimumParallelSelectRows`                                        | Minimum table size before `select`/`update` split rows across cores      |
| `QueryTable.parallelWhereRowsPerSegment`                                      | Segment size for `where`; splitting starts above twice this many rows    |
| `QueryTable.parallelSort`, `QueryTable.minimumParallelSortRows`               | Whether, and from what size, `sort` runs in parallel                     |
| `QueryTable.statelessSelectByDefault`, `QueryTable.statelessFiltersByDefault` | Whether formulas and filters are assumed stateless (parallel) by default |
| `QueryTable.serialSelectImplicitBarriers`                                     | Whether serial selectables get implicit barriers between each other      |

## Key takeaways

Deephaven automatically parallelizes queries across all available CPU cores. Most code works correctly without changes.

- Deephaven assumes all formulas can run in parallel by default.
- Use [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) when your code has side effects, depends on rows being processed in a specific order, or calls functions that aren't safe to run from multiple threads.
- Use **barriers** when one operation must complete before another starts.
- `with_serial` keeps one column from running concurrently with itself. When several columns share state, use `with_serial` and barriers together. When several tables share state, make the shared code itself thread-safe (for example, protect it with a lock).

For a quick introduction, see the [Crash Course](../../getting-started/crash-course/parallelization.md).

## Related documentation

- [Update graph (table dependencies)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl)
