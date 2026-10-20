---
title: Barrier
---

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) is a marker object that orders one expression before another. An expression is a column calculation ([`Selectable`](./Selectable.md)) or a [filter](./Filter.md). One expression **declares** the [Barrier](#create-a-barrier) and runs first. Other expressions **respect** the [Barrier](#create-a-barrier) and wait until the declaring expression has finished all of its rows.

A [Barrier](#create-a-barrier) carries no data of its own. It is created once and shared between the expressions that need to coordinate.

## Why use a Barrier?

Most queries need no Barrier. A column that references another column's result already waits for it as an ordinary data dependency. A Barrier is for expressions that depend on each other's side effects or shared state.

Deephaven parallelizes column calculations and [filters](./Filter.md) that are eligible for it. Eligibility depends on these factors:

- Whether the expression is stateless. Deephaven assumes [formulas](../../../how-to-guides/formulas.md) and [filters](./Filter.md) are stateless, meaning safe to run in parallel, unless they are marked serial with [`with_serial`](./ConcurrencyControl.md#with_serial).
- How many rows the expression processes.
- How many threads are available.
- For a [formula](../../../how-to-guides/formulas.md) or [filter](./Filter.md) that calls a Python function or uses Python objects, whether the Python build is free-threaded, meaning it has no global interpreter lock (GIL). On a build that is not free-threaded, Deephaven never splits a Python-backed column or filter across cores. See [Python formulas and free-threading](../../../conceptual/query-engine/parallelization.md#python-formulas-and-free-threading).

As a result, two eligible expressions can run at the same time, with no guaranteed order between them. A [Barrier](#create-a-barrier) makes one expression finish all of its rows before another starts. Typical cases are one column that populates a cache that another column reads, or one column that computes a running total that another column depends on. [Example: what a Barrier changes](#example-what-a-barrier-changes) shows the difference.

## Create a Barrier

```python syntax
from deephaven.concurrency_control import Barrier

barrier = Barrier()
```

Create one `Barrier` for each ordering constraint. A `Barrier` has one declaring expression, and every expression that respects it waits for that declarer, so a single `Barrier` cannot order two independent constraints.

## Use a Barrier

Two methods set the roles. Both belong to the [`ConcurrencyControl`](./ConcurrencyControl.md) interface, which [`Selectable`](./Selectable.md) and [`Filter`](./Filter.md) both implement. [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) accept a `Selectable`, and [`where`](../../table-operations/filter/where.md) accepts a `Filter`.

- [`with_declared_barriers(barriers)`](./ConcurrencyControl.md#with_declared_barriers): this expression declares the given [Barrier](#create-a-barrier) objects. It runs to completion before any expression that respects the same [Barrier](#create-a-barrier).
- [`with_respected_barriers(barriers)`](./ConcurrencyControl.md#with_respected_barriers): this expression respects the given [Barrier](#create-a-barrier) objects. It does not start until the expression that declares each [Barrier](#create-a-barrier) has finished.

These rules apply to every [Barrier](#create-a-barrier):

- One expression declares a given [Barrier](#create-a-barrier). Declaring the same instance twice is an error.
- The declaring and respecting expressions are arguments of the same [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), or [`where`](../../table-operations/filter/where.md) call. A [Barrier](#create-a-barrier) cannot order expressions across two separate calls.
- The declaring expression comes before every respecting expression, in left-to-right order. The engine raises an error if a [Barrier](#create-a-barrier) is respected before it is declared, or never declared at all.
- A constant-valued [`Selectable`](./Selectable.md) cannot declare or respect a [Barrier](#create-a-barrier). This restriction does not apply to a [`Filter`](./Filter.md).

A constant-valued `Selectable` is one whose expression is a literal, or literals joined by binary operators and optionally parenthesized, such as `A = 1` or `A = 1 + 2`. The engine never evaluates constants per row during [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md), so it raises an error when a constant declares or respects a [Barrier](#create-a-barrier). An expression that calls a method, such as `counter.incrementAndGet()` in the examples below, is not constant and can declare and respect [Barriers](#create-a-barrier).

### Barriers and serial execution

A [Barrier](#create-a-barrier) orders expressions relative to each other. It does not change how one expression processes its own rows. [`with_serial`](./ConcurrencyControl.md#with_serial) makes one expression process its rows one at a time, in row set order (ascending row key). [`with_serial` versus Barriers](./ConcurrencyControl.md#with_serial-versus-barriers) describes when an expression needs one control, the other, or both.

### Example: what a Barrier changes

Column `A` counts the rows it processes in a shared counter, and column `B` reads the counter. `B` is correct only after `A` has processed every row, so `A` declares a [Barrier](#create-a-barrier) and `B` respects it.

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

With the [Barrier](#create-a-barrier), every row of `B` holds `6000000`, so `MinB` and `MaxB` are equal.

Without the two [Barrier](#create-a-barrier) calls, `A` and `B` are independent, so `B` can read the counter while `A` is still counting. `MinB` and `MaxB` can then differ, and the values change with hardware and scheduling. In one run without the [Barrier](#create-a-barrier), `MinB` was 15,436 and `MaxB` was 908,154.

### Example: coordinate two filters

[`Filter`](./Filter.md) objects use [Barriers](#create-a-barrier) in [`where`](../../table-operations/filter/where.md) the same way as columns do. In this example, `filter_a` counts the rows it examines, and `filter_b` keeps rows only when the count has reached 10.

This example shows the syntax. The engine can reorder filters, for example to run a cheap one first when a data source can answer it ([predicate pushdown](../../../how-to-guides/predicate-pushdown.md)). A [Barrier](#create-a-barrier) keeps `filter_a` ahead of `filter_b` either way. For an in-memory table like this one, the output is the same with or without the [Barrier](#create-a-barrier).

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

Create one [Barrier](#create-a-barrier) for each independent dependency. One expression can respect more than one [Barrier](#create-a-barrier). In this example, `A` and `B` each count the rows they process in their own counter and declare their own [Barrier](#create-a-barrier):

- `C` respects both [Barriers](#create-a-barrier), so it reads both counters after `A` and `B` finish. Every row of `C` holds `12000000`.
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

`MinC` and `MaxC` are both 12000000, and `MinD` and `MaxD` are both 6000000.

## Implicit Barriers

An implicit [Barrier](#create-a-barrier) is one that Deephaven adds automatically. When [implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are on, each serial column in a [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) waits for every earlier serial column in the same call, without any explicit [Barrier](#create-a-barrier) object. A serial column is one that is not stateless. By default, that means a column marked with [`with_serial`](./ConcurrencyControl.md#with_serial). When [`QueryTable.statelessSelectByDefault`](../../../conceptual/query-table-configuration.md#stateless-by-default) is `false`, Deephaven can also treat an unmarked column as serial. Implicit Barriers let each serial column see the effect of the earlier serial columns without declaring Barriers by hand.

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
