---
title: Barrier
---

A **Barrier** is a marker object that orders one expression before another. An expression is a column calculation ([`Selectable`](./Selectable.md)) or a [filter](./Filter.md). One expression **declares** the [Barrier](#create-a-barrier) and runs first. Other expressions **respect** the [Barrier](#create-a-barrier) and wait until the declaring expression has finished all of its rows.

In Groovy, there is no dedicated [Barrier](#create-a-barrier) type. Any Java object can serve as a [Barrier](#create-a-barrier). Deephaven uses the object only as a marker that matches a declaring expression to the expressions that respect it. It never reads the data the object holds, although [matching](#how-deephaven-matches-a-barrier) can depend on the object's `equals` and `hashCode` methods.

## Why use a Barrier?

Most queries need no Barrier. A column that references another column's result already waits for it as an ordinary data dependency. A Barrier is for expressions that depend on each other's side effects or shared state.

Deephaven parallelizes column calculations and [filters](./Filter.md) that are eligible for it. Eligibility depends on these factors:

- Whether the expression is stateless. Deephaven assumes [formulas](../../../how-to-guides/formulas.md) and [filters](./Filter.md) are stateless, meaning safe to run in parallel, unless they are marked serial with [`withSerial`](./ConcurrencyControl.md#withserial).
- How many rows the expression processes.
- How many threads are available.

As a result, two eligible expressions can run at the same time, with no guaranteed order between them. A [Barrier](#create-a-barrier) makes one expression finish all of its rows before another starts. Typical cases are one column that populates a map that another column reads, or one column that computes a running total that another column depends on. [Example: what a Barrier changes](#example-what-a-barrier-changes) shows the difference.

## Create a Barrier

```groovy syntax
barrier = new Object()
```

A plain `new Object()` works well because it holds nothing that code could accidentally rely on. Its default `equals` is identity-based, which matters for the [matching rules](#how-deephaven-matches-a-barrier).

Create one object for each ordering constraint. A [Barrier](#create-a-barrier) has one declaring expression, and every expression that respects it waits for that declarer, so a single object cannot order two independent constraints.

## Use a Barrier

Two methods set the roles. Both belong to the [`ConcurrencyControl`](./ConcurrencyControl.md) interface, which [`Selectable`](./Selectable.md) and [`Filter`](./Filter.md) both implement. A `Selectable` is accepted by [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), [`view`](../../table-operations/select/view.md), [`updateView`](../../table-operations/select/update-view.md), and [`lazyUpdate`](../../table-operations/select/lazy-update.md), but only `select` and `update` order their columns by Barriers. [Table operations that accept a Selectable](./Selectable.md#table-operations-that-accept-a-selectable) describes the others.

A `Filter` is accepted by [`where`](../../table-operations/filter/where.md). The two methods are:

- [`withDeclaredBarriers(barriers)`](./ConcurrencyControl.md#withdeclaredbarriers): this expression declares the given [Barrier](#create-a-barrier) objects. It runs to completion before any expression that respects the same [Barrier](#create-a-barrier).
- [`withRespectedBarriers(barriers)`](./ConcurrencyControl.md#withrespectedbarriers): this expression respects the given [Barrier](#create-a-barrier) objects. It does not start until the expression that declares each [Barrier](#create-a-barrier) has finished.

These rules apply to every [Barrier](#create-a-barrier):

- One expression declares a given [Barrier](#create-a-barrier). Declaring the same instance twice is an error.
- The declaring and respecting expressions are arguments of the same [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), or [`where`](../../table-operations/filter/where.md) call. A [Barrier](#create-a-barrier) cannot order expressions across two separate calls.
- The declaring expression comes before every respecting expression, in left-to-right order. The engine raises an error if a [Barrier](#create-a-barrier) is respected before it is declared, or never declared at all.
- A constant-valued [`Selectable`](./Selectable.md) cannot declare or respect a [Barrier](#create-a-barrier). This restriction does not apply to a [`Filter`](./Filter.md).

A constant-valued `Selectable` is one whose expression is a literal, or literals joined by binary operators and optionally parenthesized, such as `A = 1` or `A = 1 + 2`. The engine never evaluates constants per row during [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md), so it raises an error when a constant declares or respects a [Barrier](#create-a-barrier). An expression that calls a method, such as `counter.incrementAndGet()` in the examples below, is not constant and can declare and respect [Barriers](#create-a-barrier).

### How Deephaven matches a Barrier

Deephaven matches a declaring expression to its respecting expressions differently for [filters](./Filter.md) and [selectables](./Selectable.md):

- A [filter](./Filter.md) compares Barriers with `equals` and `hashCode`. Two distinct instances that compare equal are the same [Barrier](#create-a-barrier).
- A [selectable](./Selectable.md) compares Barriers by identity. Only the exact same instance matches, regardless of `equals`. A distinct but equal instance does not match, so a respected [Barrier](#create-a-barrier) fails with a "not defined" error.

A value type that overrides `equals`, such as a `String`, a boxed number, or a `List`, can therefore cause a duplicate-declaration error for [filters](./Filter.md) and a "not defined" error for [selectables](./Selectable.md). A plain `new Object()` avoids both.

### Barriers and serial execution

A [Barrier](#create-a-barrier) orders expressions relative to each other. It does not change how one expression processes its own rows. [`withSerial`](./ConcurrencyControl.md#withserial) makes one expression process its rows one at a time, in row set order (ascending row key). [`withSerial` versus Barriers](./ConcurrencyControl.md#withserial-versus-barriers) describes when an expression needs one control, the other, or both.

### Example: what a Barrier changes

Column `A` counts the rows it processes in a shared counter, and column `B` reads the counter. `B` is correct only after `A` has processed every row, so `A` declares a [Barrier](#create-a-barrier) and `B` respects it.

The table has 6,000,000 rows, which is large enough that Deephaven can split each column across threads when more than one thread is available. The default [`QueryTable.minimumParallelSelectRows`](../../../conceptual/query-table-configuration.md#parallel-processing-with-select) is 4,194,304.

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger
import static io.deephaven.api.agg.Aggregation.AggMax
import static io.deephaven.api.agg.Aggregation.AggMin

counter = new AtomicInteger(0)
barrier = new Object()

// Column A counts the rows it processes and declares the Barrier
colA = Selectable.parse("A = counter.incrementAndGet()")
    .withDeclaredBarriers(barrier)

// Column B reads the counter, so it waits until A has processed every row
colB = Selectable.parse("B = counter.get()")
    .withRespectedBarriers(barrier)

result = emptyTable(6_000_000)
    .update([colA, colB])
    .aggBy([AggMin("MinB = B"), AggMax("MaxB = B")])
```

With the [Barrier](#create-a-barrier), every row of `B` holds `6000000`, so `MinB` and `MaxB` are equal.

Without the two [Barrier](#create-a-barrier) calls, `A` and `B` are independent, so `B` can read the counter while `A` is still counting. `MinB` and `MaxB` can then differ, and the values change with hardware and scheduling. In one run without the [Barrier](#create-a-barrier), `MinB` was 47,922 and `MaxB` was 1,241,553.

### Example: coordinate two filters

[`Filter`](./Filter.md) objects use [Barriers](#create-a-barrier) in [`where`](../../table-operations/filter/where.md) the same way as columns do. In this example, `filterA` counts the rows it examines, and `filterB` keeps rows only when the count has reached 10.

This example shows the syntax. The engine can reorder filters, for example to run a cheap one first when a data source can answer it ([predicate pushdown](../../../how-to-guides/predicate-pushdown.md)). A [Barrier](#create-a-barrier) keeps `filterA` ahead of `filterB` either way. For an in-memory table like this one, the output is the same with or without the [Barrier](#create-a-barrier).

```groovy order=result
import io.deephaven.api.filter.Filter
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)
barrier = new Object()

// Filter A: counts the rows it examines and declares the Barrier
filterA = Filter.from("counter.incrementAndGet() > 0")[0]
    .withDeclaredBarriers(barrier)

// Filter B: respects the Barrier, so it runs after A has examined every row
filterB = Filter.from("counter.get() == 10")[0]
    .withRespectedBarriers(barrier)

source = emptyTable(10).update("Key = i")
result = source.where(Filter.and(filterA, filterB))
```

Because `filterA` examines all ten rows first, `filterB` sees a count of 10 and `result` keeps all ten rows.

### Multiple Barriers

Create one [Barrier](#create-a-barrier) for each independent dependency. One expression can respect more than one [Barrier](#create-a-barrier). In this example, `A` and `B` each count the rows they process in their own counter and declare their own [Barrier](#create-a-barrier):

- `C` respects both [Barriers](#create-a-barrier), so it reads both counters after `A` and `B` finish. Every row of `C` holds `12000000`.
- `D` respects only `barrierA`, so it reads `counterA` after `A` finishes, without waiting for `B`. Every row of `D` holds `6000000`.

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger
import static io.deephaven.api.agg.Aggregation.AggMax
import static io.deephaven.api.agg.Aggregation.AggMin

counterA = new AtomicInteger(0)
counterB = new AtomicInteger(0)
barrierA = new Object()
barrierB = new Object()

// Column A counts its rows and declares barrierA
colA = Selectable.parse("A = counterA.incrementAndGet()")
    .withDeclaredBarriers(barrierA)

// Column B counts its rows and declares barrierB
colB = Selectable.parse("B = counterB.incrementAndGet()")
    .withDeclaredBarriers(barrierB)

// Column C respects both Barriers
colC = Selectable.parse("C = counterA.get() + counterB.get()")
    .withRespectedBarriers(barrierA, barrierB)

// Column D respects only barrierA
colD = Selectable.parse("D = counterA.get()")
    .withRespectedBarriers(barrierA)

result = emptyTable(6_000_000)
    .update([colA, colB, colC, colD])
    .aggBy([
        AggMin("MinC = C"),
        AggMax("MaxC = C"),
        AggMin("MinD = D"),
        AggMax("MaxD = D"),
    ])
```

`MinC` and `MaxC` are both 12000000, and `MinD` and `MaxD` are both 6000000.

## Implicit Barriers

An implicit [Barrier](#create-a-barrier) is one that Deephaven adds automatically. When [implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are on, each serial column in a [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) waits for every earlier serial column in the same call, without any explicit [Barrier](#create-a-barrier) object. A serial column is one that is not stateless. By default, that means a column marked with [`withSerial`](./ConcurrencyControl.md#withserial). When [`QueryTable.statelessSelectByDefault`](../../../conceptual/query-table-configuration.md#stateless-by-default) is `false`, Deephaven can also treat an unmarked column as serial. Implicit Barriers let each serial column see the effect of the earlier serial columns without declaring Barriers by hand.

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
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
