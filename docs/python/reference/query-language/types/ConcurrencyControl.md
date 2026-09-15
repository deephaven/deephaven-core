---
title: ConcurrencyControl
---

[`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) is the shared interface that provides concurrency control for column calculations and filters. [`Selectable`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) (used by [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md)) and [`Filter`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) (used by [`where`](../../table-operations/filter/where.md)) both implement it, so the same three methods work the same way for either one.

By default, Deephaven is free to parallelize column calculations and filter evaluation across multiple CPU cores when they're eligible for it — eligibility depends on statelessness, table size, available threads, and, for a formula that calls a Python function, a free-threaded (no-GIL) Python build. Use the methods below when your formula or filter has side effects, or depends on row order, that make parallel execution unsafe.

## Methods

### `with_serial`

Forces the expression to never run concurrently with itself; its rows are evaluated sequentially, in row-set order. Use this when the formula or filter has side effects or depends on row order.

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

When an expression is serial, every row is evaluated in order (row 0, then row 1, then row 2, etc.), only one thread processes the expression at a time, and shared state updates happen sequentially without race conditions.

> [!NOTE]
> Not running concurrently isn't the same guarantee `with_serial` provides — the engine may still evaluate a non-serial expression out of row-set order. Use `with_serial` any time your formula or filter depends on shared state or row order, not just when you expect concurrent execution.

### `with_declared_barriers`

Marks the expression as declaring the given [`Barrier`](./Barrier.md) object(s). The declaring expression runs to completion — all of its rows — before any expression that respects the same barrier begins.

```python syntax
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable

barrier = Barrier()
col = Selectable.parse("A = some_function()").with_declared_barriers(barrier)
```

Each barrier can only be declared by one expression. See [Barrier](./Barrier.md) for a complete worked example.

### `with_respected_barriers`

Marks the expression as respecting the given [`Barrier`](./Barrier.md) object(s). The respecting expression doesn't start until every expression that declares that barrier has finished.

```python syntax
from deephaven.concurrency_control import Barrier
from deephaven.table import Selectable

barrier = Barrier()
col = Selectable.parse("B = some_function()").with_respected_barriers(barrier)
```

Multiple expressions can respect the same barrier, and one expression can respect more than one barrier. See [Barrier](./Barrier.md) for a complete worked example, including how to coordinate more than two expressions.

## `with_serial` vs. barriers

These solve different problems:

- **`with_serial`**: Rows _within one_ expression are processed sequentially (row 0, then row 1, etc.). For a **filter**, a serial filter also acts as an absolute ordering barrier against every other filter in the same `where` call — no filter can execute out of order around it. For a **selectable**, `with_serial` gives no such guarantee relative to other expressions by default; other expressions, serial or not, can still run at the same time unless you add an explicit barrier.
- **Barriers**: _Between_ expressions, one finishes all its rows before another starts. Rows within each expression can still be parallelized.

When shared state is involved, you often need both: `with_serial` to protect row-level access to the shared state, and — especially for selectables — a barrier to ensure one expression is completely done before another starts.

## Related documentation

- [Barrier](./Barrier.md) — The marker object used with `with_declared_barriers` and `with_respected_barriers`
- [Query table configuration](../../../conceptual/query-table-configuration.md) — Configuration properties that control default parallelization behavior
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl)
- [Selectable Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable)
- [Filter Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html)
