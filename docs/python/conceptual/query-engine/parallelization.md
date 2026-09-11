---
title: Parallelization
sidebar_label: Parallelization
---

Parallelization is running multiple calculations at the same time on different CPU cores instead of one after another. Deephaven automatically parallelizes table operations like [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) to make queries faster, with no configuration required. This guide explains how that parallelization works and when you need to control it.

> [!IMPORTANT]
> **Breaking change in Deephaven 41+**: Deephaven 40 and earlier assumed all formulas required sequential processing by default. Deephaven 41 and later assumes all formulas can run in parallel by default. Code that modifies shared variables or depends on rows being processed in a specific order will now produce incorrect results unless you mark it with [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial).
>
> **Quick check**: Does your code use global variables, depend on rows being processed in a specific order, or modify external state? If yes, see [Controlling execution order](#controlling-execution-order) below, or the [crash course guide](../../getting-started/crash-course/parallelization.md) for a faster introduction.

## Quick reference

| Scenario                             | Solution                      | Why                                 |
| ------------------------------------ | ----------------------------- | ----------------------------------- |
| Pure column math                     | Default (parallel)            | Thread-safe, no shared state        |
| Global counter                       | `with_serial`                 | Needs sequential row processing     |
| Column A must finish before Column B | Barriers                      | Controls cross-operation ordering   |
| File I/O or logging                  | `with_serial`                 | Serialize access to shared resource |
| Multiple operations sharing state    | Barriers or implicit barriers | Coordinates access to shared state  |
| Non-thread-safe library              | `with_serial`                 | Forces single-threaded access       |

## How parallelization works

Deephaven uses all available CPU cores to process queries faster, in three ways: across tables, across rows, and across columns.

### Across tables

When you create multiple tables from the same source, Deephaven's update graph can update them concurrently. In this example, three independent tables derive from `market_data`:

```python ticking-table order=null
from deephaven import time_table

# Create a live table that adds a row every second
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

When new data arrives in `market_data`, the update graph schedules `with_metrics`, `high_volume`, and `recent_trades` as independent notifications, which can run concurrently on different cores. (This depends on `PeriodicUpdateGraph.updateThreads` being greater than 1, which is the default — see [Thread pools](#query-phases-and-thread-pools) below.)

Deephaven tracks which tables depend on which through an internal structure called the [update graph](../dag.md). Independent tables (those that don't depend on each other) run in parallel automatically.

### Within a single table

Deephaven also parallelizes calculations within a single table, in two ways:

**Across rows**: When you run `source.update("Total = Price * Quantity")`, Deephaven divides the rows into groups, assigns each group to a different CPU core, has each core calculate `Total` for its rows independently, and combines the results into the final `Total` column.

**Across columns**: When you compute multiple columns in the same operation, Deephaven can calculate independent columns simultaneously. For example, in `source.update(["A = X * 2", "B = Y + 1"])`, columns `A` and `B` can be computed on different cores at the same time because neither depends on the other.

**What gets parallelized**:

- Column calculations in [`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md).
- Filters in [`where`](../../reference/table-operations/filter/where.md) clauses.

**What does NOT get parallelized**:

- [`view`](../../reference/table-operations/select/view.md), [`update_view`](../../reference/table-operations/select/update-view.md), and [`lazy_update`](../../reference/table-operations/select/lazy-update.md) — these are lazily evaluated when cells are accessed, not computed upfront.
- Operations marked with [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) (you control this — see [Controlling execution order](#controlling-execution-order) below).
- Operations waiting for dependencies (automatic in the update graph).

> [!CAUTION]
> **Python GIL limitation**: Most Python builds use the GIL (global interpreter lock), which prevents concurrent execution of Python code across threads. Deephaven only considers Python-backed filters and selectables for parallel execution on a [free-threaded Python build](https://docs.python.org/3/howto/free-threading-python.html) — on a standard (GIL-enabled) build, they're never run concurrently. To get parallel execution of Python-backed formulas and filters, switch to a free-threaded Python build; no other Deephaven configuration is required.
>
> **This is not the same guarantee `with_serial` provides.** Not running concurrently isn't the same as running in row-set order, exactly once per row — the engine may still evaluate a non-parallelizable column out of order, or without evaluating every row through its own individual call. If your formula or filter has side effects that depend on row order or exactly-once evaluation, use `with_serial` regardless of which Python build you're running.

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
> These examples use small tables for clarity. Deephaven only splits a `select`/`update` computation across cores once a table crosses `QueryTable.minimumParallelSelectRows` (about 4.2 million rows by default), and `where` has its own, much smaller per-segment threshold (`QueryTable.parallelWhereRowsPerSegment`, about 65,536 rows by default). Below those thresholds, Deephaven evaluates the formula on a single core regardless of whether it's marked stateless — these examples illustrate the correctness contract, not actual observed parallel speedup.

You can change the default behavior using configuration properties: `QueryTable.statelessSelectByDefault` for [`select`](../../reference/table-operations/select/select.md)/[`update`](../../reference/table-operations/select/update.md), and `QueryTable.statelessFiltersByDefault` for filters. See [Query table configuration](../query-table-configuration.md) for details on these and other engine configuration properties.

## Controlling execution order

Most queries work correctly with automatic parallelization. Some code doesn't — for example, code that uses a counter or modifies shared state. Deephaven provides two mechanisms to control execution order:

**Key concepts**:

- **[`Selectable`](../../reference/query-language/types/Selectable.md)**: Represents a column expression, used in `select` or `update` operations.
- **[`Filter`](../../reference/query-language/types/Filter.md)**: Represents a filter condition, used in `where` operations. Concurrency control works the same way for `Filter` as it does for `Selectable`.
- **[`with_serial`](../../reference/query-language/types/Selectable.md#with_serial)**: Forces rows to be processed one at a time, in order.
- **[`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)**: Ensures one operation completes before another starts.

**`with_serial` vs. barriers** — these solve different problems:

- **`with_serial`**: Rows _within one column_ are processed sequentially (row 0, then row 1, etc.). Other columns can still run at the same time.
- **Barriers**: _Between columns_, one column finishes all its rows before another column starts. Rows within each column can still be parallelized.

When shared state is involved, you often need both: `with_serial` to protect row-level access to the shared state, and a barrier to ensure one column is completely done before the other starts.

### Serialization

Serialization processes rows one at a time, in order, on a single thread. Use it when your code cannot safely run in parallel — for example, when a formula reads or modifies global variables, calls external functions that aren't safe to call from multiple threads simultaneously, or depends on rows being processed in a specific order. Without it, parallel execution produces incorrect results: out-of-order values, gaps, or values that don't match what the formula intended.

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


t = empty_table(5_000_000).update(
    ["A = get_and_increment_counter()", "B = get_and_increment_counter()"]
)
```

Without serialization, parallel execution causes race conditions where multiple threads read and update `counter` simultaneously, producing inconsistent values. You may see results like:

| A | B |
| - | - |
| 0 | 2 |
| 1 | 3 |
| 5 | 6 |
| 4 | 7 |
| 9 | 8 |

Notice the out-of-order values (row 4 has `A=4` after row 3 has `A=5`), gaps (no 10-19 visible), and `B` not following `A + 1`.

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

When a `Selectable` is serial, every row is evaluated in order (row 0, then row 1, then row 2, etc.), only one thread processes the column at a time, and global state updates happen sequentially without race conditions.

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

When a [`Filter`](../../reference/query-language/types/Filter.md) is serial, every input row is evaluated in order, the filter cannot be reordered with respect to other filters, and stateful side effects happen sequentially.

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

When `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` is enabled, serial operations automatically create barriers between each other — two serial columns in the same `update` will execute one after the other without explicit barriers. This behavior is controlled by the `QueryTable.serialSelectImplicitBarriers` configuration property:

- **Stateless mode (default)**: Serial operations only enforce row order within themselves, not between each other. Use explicit barriers if you need cross-operation ordering.
- **Stateful mode**: Serial operations automatically wait for each other. This is useful when operations share global state. Enable by setting `QueryTable.serialSelectImplicitBarriers=true`.

Most users don't need to change this setting.

### Stateful partition filters

The serial/barrier rules above apply to ordinary filters. _Partition filters_ — filters that only access partitioning columns — are a special case: Deephaven evaluates them per location rather than per row, so marking one serial changes its evaluation strategy rather than just its ordering.

When you mark a partition filter as serial, Deephaven must evaluate it on all rows of the table and cannot reorder it. However, if you don't explicitly mark a partition filter as serial, the engine treats it as stateless for performance reasons — even when Deephaven is configured to treat filters as stateful by default. This lets Deephaven relax ordering constraints for filters on partitioning columns, evaluate them per location rather than on every row, reorder common partition filters ahead of others, and avoid repeated evaluation. For example, the formula filter `Date=today()` is stateful if filters are stateful by default, but in nearly every case users prefer Deephaven to evaluate it early, location-by-location.

## Choosing an approach

Use the [Quick reference](#quick-reference) table above for a fast lookup. In more detail:

**Use default parallel execution when** the formula only uses values from the current row, has no side effects, doesn't depend on row processing order, and is thread-safe — this covers most formulas, including the [stateless examples above](#when-parallelization-is-safe-by-default).

**Use `with_serial` when** rows must be processed in order within a single operation, or the formula updates global state sequentially, as in the [counter example above](#example-a-counter-needs-serialization). Common cases: sequential numbering, processing events in chronological sequence, cumulative calculations, file I/O or logging.

**Use barriers when** you need to control ordering _between_ different operations — one must finish before another starts, as in the [barrier example above](#example-extending-the-counter-with-a-barrier). Common cases: one column or filter populates a cache or resource that another reads from.

## Key takeaways

Deephaven automatically parallelizes queries across all available CPU cores. Most code works correctly without changes.

- Deephaven assumes all formulas can run in parallel by default.
- Use [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) when your code has side effects, depends on rows being processed in a specific order, or calls functions that aren't safe to run from multiple threads.
- Use **barriers** when one operation must complete before another starts.
- Both thread pools use all CPU cores by default.

For a quick introduction, see the [crash course](../../getting-started/crash-course/parallelization.md).

## Related documentation

- [Update graph (table dependencies)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl)
