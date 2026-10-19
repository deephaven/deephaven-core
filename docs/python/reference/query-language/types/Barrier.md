---
title: Barrier
---

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) is a marker object that orders one column calculation or [filter](./Filter.md) before another. One operation **declares** the [Barrier](#creating-a-barrier) and runs first. Other operations **respect** the [Barrier](#creating-a-barrier) and wait until the declaring operation has finished all of its rows.

A [Barrier](#creating-a-barrier) carries no data of its own. It is created once and shared between the operations that need to coordinate.

## Why use a Barrier?

Deephaven parallelizes column calculations and [filters](./Filter.md) that are eligible for it. Eligibility depends on these factors:

- Whether the operation is stateless. Deephaven assumes [formulas](../../../how-to-guides/formulas.md) and [filters](./Filter.md) are safe to run in parallel unless they are marked serial with [`with_serial`](./ConcurrencyControl.md#with_serial).
- How many rows the operation processes.
- How many threads are available.
- For a [formula](../../../how-to-guides/formulas.md) or [filter](./Filter.md) that calls a Python function or uses Python objects, whether the Python build is free-threaded, meaning it has no global interpreter lock (GIL). On a build that is not free-threaded, Deephaven never splits a Python-backed column or filter across cores. See [Python formulas and free-threading](../../../conceptual/query-engine/parallelization.md#python-formulas-and-free-threading).

As a result, two eligible operations can run at the same time, with no guaranteed order between them. A [Barrier](#creating-a-barrier) makes one operation finish all of its rows before another starts. Typical cases are one column that populates a cache that another column reads, or one column that computes a running total that another column depends on. [Example: what a Barrier changes](#example-what-a-barrier-changes) shows the difference.

## Creating a Barrier

```python syntax
from deephaven.concurrency_control import Barrier

barrier = Barrier()
```

A `Barrier` wraps a plain Java `Object` and holds no state.

Create one `Barrier` for each ordering constraint. Every expression that respects a `Barrier` waits for its single declarer, so one instance cannot express two independent constraints.

## Using a Barrier

Two methods set the roles. Both belong to the [`ConcurrencyControl`](./ConcurrencyControl.md) interface, which [`Selectable`](./Selectable.md) and [`Filter`](./Filter.md) both implement. [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) accept a `Selectable`, and [`where`](../../table-operations/filter/where.md) accepts a `Filter`. On this page, an expression is a `Selectable` or a `Filter`.

- [`with_declared_barriers(barriers)`](./ConcurrencyControl.md#with_declared_barriers): this operation declares the given [Barrier](#creating-a-barrier) objects. It runs to completion before any operation that respects the same [Barrier](#creating-a-barrier).
- [`with_respected_barriers(barriers)`](./ConcurrencyControl.md#with_respected_barriers): this operation respects the given [Barrier](#creating-a-barrier) objects. It does not start until the operation that declares each [Barrier](#creating-a-barrier) has finished.

These rules apply to every [Barrier](#creating-a-barrier):

- One expression declares a given [Barrier](#creating-a-barrier). Declaring the same instance twice is an error.
- The declaring and respecting expressions are arguments of the same [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), or [`where`](../../table-operations/filter/where.md) call. A [Barrier](#creating-a-barrier) cannot order operations across two separate calls.
- The declaring expression comes before every respecting expression, in left-to-right order. The engine raises an error if a [Barrier](#creating-a-barrier) is respected before it is declared, or never declared at all.
- A constant-valued [`Selectable`](./Selectable.md) cannot declare or respect a [Barrier](#creating-a-barrier). This restriction does not apply to a [`Filter`](./Filter.md).

A constant is a literal, or literals joined by binary operators (arithmetic, comparison, logical, bitwise, or shift) and optionally parenthesized, such as [`Selectable.parse("A = 1")`](./Selectable.md#creating-a-selectable) or [`Selectable.parse("A = 1 + 2")`](./Selectable.md#creating-a-selectable). The engine never evaluates constants during [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) processing, so it raises an error when a constant declares or respects a [Barrier](#creating-a-barrier).

An expression that calls a method, such as `counter.incrementAndGet()` in the examples below, is not constant. The engine evaluates it for each row, so it can declare and respect [Barriers](#creating-a-barrier).

### Barriers and serial execution

A [Barrier](#creating-a-barrier) orders operations relative to each other. It does not change how one operation processes its own rows. [`with_serial`](./ConcurrencyControl.md#with_serial) makes one operation process its rows one at a time, in row set order (ascending row key). [`with_serial` versus Barriers](./ConcurrencyControl.md#with_serial-versus-barriers) describes when an expression needs one control, the other, or both.

### Example: what a Barrier changes

Column `A` counts the rows it processes in a shared counter, and column `B` reads the counter. `B` is correct only after `A` has processed every row, so `A` declares a [Barrier](#creating-a-barrier) and `B` respects it.

The table has 6,000,000 rows, which is large enough that Deephaven can split each column across threads when more than one thread is available. The default [`QueryTable.minimumParallelSelectRows`](../../../conceptual/query-table-configuration.md#parallel-processing-with-select) is 4,194,304. The counter is a Java `AtomicInteger` instead of a Python object, so the example behaves the same on every Python build, including builds that are not free-threaded.

```python order=result
import jpy
from deephaven import empty_table, agg
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable

AtomicInteger = jpy.get_type("java.util.concurrent.atomic.AtomicInteger")

counter = AtomicInteger(0)
barrier = Barrier()

# Column A counts the rows it processes and declares the Barrier
col_a = Selectable.parse("A = counter.incrementAndGet()").with_declared_barriers(
    barrier
)

# Column B reads the counter, so it waits until A has processed every row
col_b = Selectable.parse("B = counter.get()").with_respected_barriers(barrier)

result = (
    empty_table(6_000_000)
    .update([col_a, col_b])
    .agg_by([agg.min_("MinB = B"), agg.max_("MaxB = B")])
)
```

With the [Barrier](#creating-a-barrier), every row of `B` holds `6000000`, so `MinB` and `MaxB` are equal.

Without the two [Barrier](#creating-a-barrier) calls, `A` and `B` are independent, so `B` can read the counter while `A` is still counting. `MinB` and `MaxB` can then differ, and the values change with hardware and scheduling. In one run without the [Barrier](#creating-a-barrier), `MinB` was 15,436 and `MaxB` was 908,154.

### Example: coordinating two filters

[`Filter`](./Filter.md) objects use [Barriers](#creating-a-barrier) in [`where`](../../table-operations/filter/where.md) the same way as columns do. In this example, `filter_a` counts the rows it examines, and `filter_b` keeps rows only when the count has reached 10.

[Filters](./Filter.md) normally run in argument order, but the engine can reorder them, for example to run a filter that a data source answers cheaply before an earlier one ([predicate pushdown](../../../how-to-guides/predicate-pushdown.md)). The [Barrier](#creating-a-barrier) keeps `filter_a` ahead of `filter_b`. For an in-memory table like this one, the result is the same with or without the [Barrier](#creating-a-barrier), so the example shows the syntax and the guarantee, not a difference in output.

```python order=result
import jpy
from deephaven import empty_table
from deephaven.concurrency_control import Barrier
from deephaven.filters import Filter

AtomicInteger = jpy.get_type("java.util.concurrent.atomic.AtomicInteger")

counter = AtomicInteger(0)
barrier = Barrier()

# Filter A: counts the rows it examines and declares the Barrier
filter_a = Filter.from_("counter.incrementAndGet() > 0").with_declared_barriers(barrier)

# Filter B: respects the Barrier, so it runs after A has examined every row
filter_b = Filter.from_("counter.get() == 10").with_respected_barriers(barrier)

source = empty_table(10).update("Key = i")
result = source.where([filter_a, filter_b])
```

Because `filter_a` examines all ten rows first, `filter_b` sees a count of 10 and `result` keeps all ten rows.

### Multiple Barriers

Create one [Barrier](#creating-a-barrier) for each independent dependency. One operation can respect more than one [Barrier](#creating-a-barrier). In this example, `A` and `B` each count the rows they process in their own counter and declare their own [Barrier](#creating-a-barrier):

- `C` respects both [Barriers](#creating-a-barrier), so it reads both counters after `A` and `B` finish. Every row of `C` holds `12000000`.
- `D` respects only `barrier_a`, so it reads `counter_a` after `A` finishes, without waiting for `B`. Every row of `D` holds `6000000`.

```python order=result
import jpy
from deephaven import empty_table, agg
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable

AtomicInteger = jpy.get_type("java.util.concurrent.atomic.AtomicInteger")

counter_a = AtomicInteger(0)
counter_b = AtomicInteger(0)
barrier_a = Barrier()
barrier_b = Barrier()

# Column A counts its rows and declares barrier_a
col_a = Selectable.parse("A = counter_a.incrementAndGet()").with_declared_barriers(
    barrier_a
)

# Column B counts its rows and declares barrier_b
col_b = Selectable.parse("B = counter_b.incrementAndGet()").with_declared_barriers(
    barrier_b
)

# Column C respects both Barriers
col_c = Selectable.parse(
    "C = counter_a.get() + counter_b.get()"
).with_respected_barriers([barrier_a, barrier_b])

# Column D respects only barrier_a
col_d = Selectable.parse("D = counter_a.get()").with_respected_barriers(barrier_a)

result = (
    empty_table(6_000_000)
    .update([col_a, col_b, col_c, col_d])
    .agg_by(
        [
            agg.min_("MinC = C"),
            agg.max_("MaxC = C"),
            agg.min_("MinD = D"),
            agg.max_("MaxD = D"),
        ]
    )
)
```

## Implicit Barriers

An implicit [Barrier](#creating-a-barrier) is one that Deephaven adds. When [implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are turned on, each serial column in a [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) waits for every earlier serial column in the same operation, without any explicit [Barrier](#creating-a-barrier) object. A serial column here is one that is not stateless.

[Implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are off by default. The [`QueryTable.serialSelectImplicitBarriers`](../../../conceptual/query-table-configuration.md#stateless-by-default) property turns them on.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Query table configuration](../../../conceptual/query-table-configuration.md)
- [Predicate pushdown](../../../how-to-guides/predicate-pushdown.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [Selectable](./Selectable.md)
- [Filter](./Filter.md)
- [`select`](../../table-operations/select/select.md)
- [`update`](../../table-operations/select/update.md)
- [`where`](../../table-operations/filter/where.md)
- [Barrier Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)
