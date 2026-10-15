---
title: ConcurrencyControl
---

[`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) is the interface that controls how Deephaven runs a column calculation or a filter. [`Selectable`](./Selectable.md) (used by [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md)) and [`Filter`](./Filter.md) (used by [`where`](../../table-operations/filter/where.md)) both implement it. Both therefore have the same three methods: [`with_serial`](#with_serial), [`with_declared_barriers`](#with_declared_barriers), and [`with_respected_barriers`](#with_respected_barriers).

This page defines what each method guarantees and where the guarantees differ between a [`Selectable`](./Selectable.md) and a [`Filter`](./Filter.md). Those two pages describe how to use them.

This page uses these terms:

- **Expression**: a [`Selectable`](./Selectable.md) or a [`Filter`](./Filter.md).
- **Row set order**: ascending row key.
- **[Barrier](./Barrier.md)**: an object that orders one expression after another.
- **Default parallelization**: Deephaven assumes an expression is safe to run in parallel unless it is marked serial. It then splits the expression's rows across threads when the operation processes enough rows and threads are available. See [Parallelization](../../../conceptual/query-engine/parallelization.md).
- **Free-threaded Python**: a Python build without the global interpreter lock (GIL). A column or filter that calls a Python function is split across cores only on a free-threaded build.

## Methods

### `with_serial`

[`with_serial`](#with_serial) marks an expression as serial. A serial expression has two guarantees:

- It never runs concurrently with itself.
- It evaluates its rows one at a time, in row set order.

The following example assigns IDs from a shared counter. Serial evaluation gives each row the next counter value, in row order.

```python order=result
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_and_increment_counter() -> int:
    global counter
    ret = counter
    counter += 1
    return ret


# Force serial execution - never concurrent, rows processed in row-set order
col = Selectable.parse("ID = get_and_increment_counter()").with_serial()
result = empty_table(10).update(col)
```

A serial expression protects state in these ways:

- State used only by this expression is protected.
- State shared with other selectables in the same [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) call also needs [Barriers](./Barrier.md).
- A serial filter orders itself against every other filter in the same [`where`](../../table-operations/filter/where.md) call, so filters need no [Barrier](./Barrier.md) for that.
- State shared with another table's formulas is outside the reach of [Barriers](./Barrier.md). That shared code must be thread-safe, meaning concurrent calls cannot corrupt it.

> [!NOTE]
> Not running concurrently is not the same as running in row set order. The engine can evaluate a non-serial expression out of row set order even when it does not run that expression concurrently.

### `with_declared_barriers`

[`with_declared_barriers`](#with_declared_barriers) marks an expression as declaring the given [Barrier](./Barrier.md) objects. A declaring expression evaluates all of its rows before any expression that respects the same [Barrier](./Barrier.md) begins.

The following snippet declares a [Barrier](./Barrier.md) on a [`Selectable`](./Selectable.md).

```python syntax
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable

barrier = Barrier()
col = Selectable.parse("A = some_function()").with_declared_barriers(barrier)
```

A declared [Barrier](./Barrier.md) has these constraints:

- Only one expression declares a given [Barrier](./Barrier.md).
- The declaring and respecting expressions belong to the same [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), or [`where`](../../table-operations/filter/where.md) call.
- A constant-valued [`Selectable`](./Selectable.md) cannot declare or respect a [Barrier](./Barrier.md). A [`Filter`](./Filter.md) has no such restriction.

See [Barrier](./Barrier.md) for the full constraints and a complete worked example.

### `with_respected_barriers`

[`with_respected_barriers`](#with_respected_barriers) marks an expression as respecting the given [Barrier](./Barrier.md) objects. A respecting expression does not start until the expression that declares that [Barrier](./Barrier.md) has finished.

The following snippet respects a [Barrier](./Barrier.md) on a [`Selectable`](./Selectable.md).

```python syntax
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable

barrier = Barrier()
col = Selectable.parse("B = some_function()").with_respected_barriers(barrier)
```

A respected [Barrier](./Barrier.md) has these rules:

- Several expressions can respect the same [Barrier](./Barrier.md), and one expression can respect several [Barriers](./Barrier.md).
- A respecting expression comes after the declaring expression, in left-to-right order within the same call. The engine raises an error otherwise.

See [Barrier](./Barrier.md) for the full constraints and a complete worked example, including how to coordinate more than two expressions.

## `with_serial` vs. Barriers

The two controls order different things:

- **[`with_serial`](#with_serial)**: orders the rows _within one_ expression. Those rows are evaluated one at a time, from the lowest row key to the highest.
- **[Barriers](./Barrier.md)**: order expressions _relative to each other_. One expression finishes all of its rows before another starts, and the rows within each expression can still be parallelized.

A column that references another column's result needs no [Barrier](./Barrier.md) for that ordering. The engine evaluates the referenced column first as an ordinary data dependency.

Expressions that share state typically need both controls: [`with_serial`](#with_serial) protects the rows within each expression, and, for selectables, a [Barrier](./Barrier.md) orders the expressions.

## Selectable versus Filter

The two types share these methods but differ in the following ways.

|                                                                              | [`Selectable`](./Selectable.md)                                                                                            | [`Filter`](./Filter.md)                                                                                                                                    |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table operations                                                             | [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md)                | [`where`](../../table-operations/filter/where.md)                                                                                                          |
| Effect of [`with_serial`](#with_serial) on other expressions                 | By default, none. Two independent selectables can still run at the same time unless a [Barrier](./Barrier.md) orders them. | A serial filter is an absolute ordering barrier: no filter in the same [`where`](../../table-operations/filter/where.md) call runs out of order around it. |
| Constant-valued expression declaring or respecting a [Barrier](./Barrier.md) | Not allowed.                                                                                                               | Allowed.                                                                                                                                                   |

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
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl)
