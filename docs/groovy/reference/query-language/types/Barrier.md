---
title: Barrier
---

A **Barrier** is a marker object that orders one column calculation or [filter](./Filter.md) before another. One operation **declares** the [Barrier](#create-a-barrier) and runs first. Other operations **respect** the [Barrier](#create-a-barrier) and wait until the declaring operation has finished all of its rows.

In Groovy, there is no dedicated [Barrier](#create-a-barrier) type. Any Java object can serve as a [Barrier](#create-a-barrier). Deephaven uses the object only as a marker that matches a declaring expression to the expressions that respect it. It never reads the data the object holds, although [matching](#match-barriers-by-equality-or-identity) can depend on the object's `equals` and `hashCode` methods.

## Decide whether a Barrier is needed

Deephaven parallelizes column calculations and [filters](./Filter.md) that are eligible for it. Eligibility depends on these factors:

- Whether the operation is stateless. Deephaven assumes [formulas](../../../how-to-guides/formulas.md) and [filters](./Filter.md) are safe to run in parallel unless they are marked serial with [`withSerial`](./ConcurrencyControl.md#withserial).
- How many rows the operation processes.
- How many threads are available.

As a result, two eligible operations can run at the same time, with no guaranteed order between them. A [Barrier](#create-a-barrier) makes one operation finish all of its rows before another starts. Typical cases are one column that populates a map that another column reads, or one column that computes a running total that another column depends on. [Example: what a Barrier changes](#see-what-a-barrier-changes) shows the difference.

## Create a Barrier

```groovy syntax
barrier = new Object()
```

A plain `new Object()` works well because it holds nothing that code could accidentally rely on. Its default `equals` is identity-based, which matters for the [matching rules](#match-barriers-by-equality-or-identity).

Create one object for each ordering constraint. Every expression that respects a [Barrier](#create-a-barrier) waits for its single declarer, so one instance cannot express two independent constraints.

## Use a Barrier

Two methods set the roles. Both belong to the [`ConcurrencyControl`](./ConcurrencyControl.md) interface, which [`Selectable`](./Selectable.md) and [`Filter`](./Filter.md) both implement. A `Selectable` is accepted by [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), [`view`](../../table-operations/select/view.md), [`updateView`](../../table-operations/select/update-view.md), and [`lazyUpdate`](../../table-operations/select/lazy-update.md), but only `select` and `update` order their columns by Barriers. [Table operations that accept a Selectable](./Selectable.md#pass-a-selectable-to-a-table-operation) describes the others.

A `Filter` is accepted by [`where`](../../table-operations/filter/where.md). On this page, an expression is a `Selectable` or a `Filter`. The two methods are:

- [`withDeclaredBarriers(barriers)`](./ConcurrencyControl.md#withdeclaredbarriers): this operation declares the given [Barrier](#create-a-barrier) objects. It runs to completion before any operation that respects the same [Barrier](#create-a-barrier).
- [`withRespectedBarriers(barriers)`](./ConcurrencyControl.md#withrespectedbarriers): this operation respects the given [Barrier](#create-a-barrier) objects. It does not start until the operation that declares each [Barrier](#create-a-barrier) has finished.

These rules apply to every [Barrier](#create-a-barrier):

- One expression declares a given [Barrier](#create-a-barrier). Declaring the same instance twice is an error.
- The declaring and respecting expressions are arguments of the same [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), or [`where`](../../table-operations/filter/where.md) call. A [Barrier](#create-a-barrier) cannot order operations across two separate calls.
- The declaring expression comes before every respecting expression, in left-to-right order. The engine raises an error if a [Barrier](#create-a-barrier) is respected before it is declared, or never declared at all.
- A constant-valued [`Selectable`](./Selectable.md) cannot declare or respect a [Barrier](#create-a-barrier). This restriction does not apply to a [`Filter`](./Filter.md).

A constant is a literal, or literals joined by binary operators (arithmetic, comparison, logical, bitwise, or shift) and optionally parenthesized, such as [`Selectable.parse("A = 1")`](./Selectable.md#create-a-selectable) or [`Selectable.parse("A = 1 + 2")`](./Selectable.md#create-a-selectable). The engine never evaluates constants during [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) processing, so it raises an error when a constant declares or respects a [Barrier](#create-a-barrier).

An expression that calls a method, such as `counter.incrementAndGet()` in the examples below, is not constant. The engine evaluates it for each row, so it can declare and respect [Barriers](#create-a-barrier).

### Match Barriers by equality or identity

Deephaven matches a declaring expression to its respecting expressions differently for [filters](./Filter.md) and [selectables](./Selectable.md):

- A [filter](./Filter.md) compares Barriers with `equals` and `hashCode`. Two distinct instances that compare equal are the same [Barrier](#create-a-barrier).
- A [selectable](./Selectable.md) compares Barriers by identity. Only the exact same instance matches, regardless of `equals`. A distinct but equal instance does not match, so a respected [Barrier](#create-a-barrier) fails with a "not defined" error.

A value type that overrides `equals`, such as a `String`, a boxed number, or a `List`, can therefore cause a duplicate-declaration error for [filters](./Filter.md) and a "not defined" error for [selectables](./Selectable.md). A plain `new Object()` avoids both.

### Combine Barriers with serial execution

A [Barrier](#create-a-barrier) orders operations relative to each other. It does not change how one operation processes its own rows. [`withSerial`](./ConcurrencyControl.md#withserial) makes one operation process its rows one at a time, in row set order (ascending row key). [`withSerial` versus Barriers](./ConcurrencyControl.md#choose-between-withserial-and-barriers) describes when an expression needs one control, the other, or both.

### See what a Barrier changes

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

### Coordinate two filters

[`Filter`](./Filter.md) objects use [Barriers](#create-a-barrier) in [`where`](../../table-operations/filter/where.md) the same way as columns do. In this example, `filterA` counts the rows it examines, and `filterB` keeps rows only when the count has reached 10.

Filters normally run in argument order, but the engine can reorder them, for example to run a filter that a data source answers cheaply before an earlier one ([predicate pushdown](../../../how-to-guides/predicate-pushdown.md)). The [Barrier](#create-a-barrier) keeps `filterA` ahead of `filterB`. For an in-memory table like this one, the result is the same with or without the [Barrier](#create-a-barrier), so the example shows the syntax and the guarantee, not a difference in output.

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

### Use multiple Barriers

Create one [Barrier](#create-a-barrier) for each independent dependency. One operation can respect more than one [Barrier](#create-a-barrier). In this example, `A` and `B` each count the rows they process in their own counter and declare their own [Barrier](#create-a-barrier):

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

## Enable implicit Barriers

An implicit [Barrier](#create-a-barrier) is one that Deephaven adds. When [implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are turned on, each serial column in a [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) waits for every earlier serial column in the same operation, without any explicit [Barrier](#create-a-barrier) object. A serial column here is one that is not stateless.

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
