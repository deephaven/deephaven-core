---
title: ConcurrencyControl
---

[`ConcurrencyControl`](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) is the interface that controls how Deephaven runs a column calculation or a filter. [`Selectable`](./Selectable.md) (used by [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), [`view`](../../table-operations/select/view.md), [`updateView`](../../table-operations/select/update-view.md), and [`lazyUpdate`](../../table-operations/select/lazy-update.md)) and [`Filter`](./Filter.md) (used by [`where`](../../table-operations/filter/where.md)) both implement it. Both therefore have the same three methods: [`withSerial`](#withserial), [`withDeclaredBarriers`](#withdeclaredbarriers), and [`withRespectedBarriers`](#withrespectedbarriers).

This page defines what each method guarantees and where the guarantees differ between a [`Selectable`](./Selectable.md) and a [`Filter`](./Filter.md). The [`Selectable`](./Selectable.md) and [`Filter`](./Filter.md) pages describe how to use each type.

This page uses these terms:

- **Expression**: a [`Selectable`](./Selectable.md) or a [`Filter`](./Filter.md).
- **Row set order**: ascending row key.
- **[Barrier](./Barrier.md)**: an object that orders one expression after another. In Groovy, any Java object can serve as a [Barrier](./Barrier.md).
- **Default parallelization**: Deephaven assumes an expression is safe to run in parallel unless it is marked [serial](#withserial). When the operation processes enough rows and enough threads are available, Deephaven splits the rows of the expression across threads. See [Parallelization](../../../conceptual/query-engine/parallelization.md).

## Methods

### `withSerial`

[`withSerial`](#withserial) marks an expression as serial. A serial expression has two guarantees:

- It never runs concurrently with itself.
- It evaluates its rows one at a time, in row set order.

The following example assigns IDs from a shared counter. Serial evaluation gives each row the next counter value, in row order.

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Force serial execution - never concurrent, rows processed in row set order
col = Selectable.parse("ID = counter.getAndIncrement()").withSerial()
result = emptyTable(10).update([col])
```

[`withSerial`](#withserial) protects:

- State used only by this expression.
- State shared with other [filters](./Filter.md) in the same [`where`](../../table-operations/filter/where.md) call, because a serial filter orders itself against every other filter in that call.

[`withSerial`](#withserial) does not protect:

- State shared with other [selectables](./Selectable.md) in the same [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) call. [Barriers](./Barrier.md) order those expressions.
- State shared with another table's [formulas](../../../how-to-guides/formulas.md). [Barriers](./Barrier.md) cannot order across tables, so that shared code must be thread-safe, meaning concurrent calls cannot corrupt it.

A serial [filter](./Filter.md) evaluates exactly the rows that earlier filters keep, so it does not skip rows that later filters would eliminate, and it forgoes some engine optimizations.

> [!NOTE]
> Not running concurrently is not the same as running in row set order. The engine can evaluate a non-serial expression out of row set order even when it does not run that expression concurrently.

### `withDeclaredBarriers`

[`withDeclaredBarriers`](#withdeclaredbarriers) marks an expression as declaring the given [Barrier](./Barrier.md) objects. A declaring expression evaluates all of its rows before any expression that respects the same [Barrier](./Barrier.md) begins.

The following snippet declares a [Barrier](./Barrier.md) on a [`Selectable`](./Selectable.md).

```groovy syntax
import io.deephaven.api.Selectable

barrier = new Object()
col = Selectable.parse("A = someFunction()").withDeclaredBarriers(barrier)
```

A declared [Barrier](./Barrier.md) has these constraints:

- Only one expression declares a given [Barrier](./Barrier.md).
- The declaring and respecting expressions belong to the same [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), or [`where`](../../table-operations/filter/where.md) call.
- A constant-valued [`Selectable`](./Selectable.md) cannot declare or respect a [Barrier](./Barrier.md). A [`Filter`](./Filter.md) has no such restriction.

See [Barrier](./Barrier.md) for the full constraints and a complete worked example.

### `withRespectedBarriers`

[`withRespectedBarriers`](#withrespectedbarriers) marks an expression as respecting the given [Barrier](./Barrier.md) objects. A respecting expression does not start until the expression that declares that [Barrier](./Barrier.md) has finished.

The following snippet respects a [Barrier](./Barrier.md) on a [`Selectable`](./Selectable.md).

```groovy syntax
import io.deephaven.api.Selectable

barrier = new Object()
col = Selectable.parse("B = someFunction()").withRespectedBarriers(barrier)
```

A respected [Barrier](./Barrier.md) has these rules:

- Several expressions can respect the same [Barrier](./Barrier.md), and one expression can respect several [Barriers](./Barrier.md).
- A respecting expression comes after the declaring expression, in left-to-right order within the same call. The engine raises an error otherwise.

See [Barrier](./Barrier.md) for the full constraints and a complete worked example, including how to coordinate more than two expressions.

## Choose between `withSerial` and Barriers

The two controls order different things:

- **[`withSerial`](#withserial)**: orders the rows _within one_ expression. Those rows are evaluated one at a time, from the lowest row key to the highest.
- **[Barriers](./Barrier.md)**: order expressions _relative to each other_. One expression finishes all of its rows before another starts, and the rows within each expression can still be parallelized.

A column that references another column's result needs no [Barrier](./Barrier.md) for that ordering. The engine evaluates the referenced column first as an ordinary data dependency.

Expressions that share state usually need both controls. [`withSerial`](#withserial) evaluates the rows of each expression one at a time. For [selectables](./Selectable.md), a [Barrier](./Barrier.md) also orders the expressions. [Filters](./Filter.md) need only `withSerial`, because a serial filter orders itself against the other filters.

## Compare Selectable and Filter

The two types share these methods but differ in the following ways.

|                                                                              | [`Selectable`](./Selectable.md)                                                                                                               | [`Filter`](./Filter.md)                                                                                                                       |
| ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Table operations                                                             | The five operations listed above, with restrictions described in [Selectable](./Selectable.md#pass-a-selectable-to-a-table-operation)         | [`where`](../../table-operations/filter/where.md)                                                                                             |
| Effect of [`withSerial`](#withserial) on other expressions                   | By default, none. Two independent [selectables](./Selectable.md) can still run at the same time unless a [Barrier](./Barrier.md) orders them. | A serial filter is never reordered: no filter in the same [`where`](../../table-operations/filter/where.md) call runs out of order around it. |
| Constant-valued expression declaring or respecting a [Barrier](./Barrier.md) | Not allowed.                                                                                                                                  | Allowed.                                                                                                                                      |

The default for [`Selectable`](./Selectable.md) comes from the [`QueryTable.serialSelectImplicitBarriers`](../../../conceptual/query-table-configuration.md#stateless-by-default) property. [Implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) describes the alternative.

## Related documentation

- [Selectable](./Selectable.md)
- [Filter](./Filter.md)
- [Barrier](./Barrier.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Query table configuration](../../../conceptual/query-table-configuration.md)
- [`select`](../../table-operations/select/select.md)
- [`update`](../../table-operations/select/update.md)
- [`where`](../../table-operations/filter/where.md)
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
