---
title: Selectable
---

A [`Selectable`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) is a column definition: a formula such as `NewColumn = ExistingColumn * 2`, optionally with concurrency controls attached. [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) accept it in place of a [formula string](../../../how-to-guides/formulas.md). The controls force [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution) or order execution between columns with [Barriers](./Barrier.md).

## Creating a Selectable

Use [`Selectable.parse`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable.parse) to create a `Selectable` from a [formula string](../../../how-to-guides/formulas.md). The string follows the same `"ColumnName = expression"` syntax as a [formula string](../../../how-to-guides/formulas.md) passed to [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md).

```python syntax
from deephaven.table import Selectable

col = Selectable.parse("NewColumn = ExistingColumn * 2")
```

The new `Selectable` supports the concurrency methods described under [Methods](#methods). Chain them before passing the `Selectable` to a table operation.

## Methods

A `Selectable` has the three methods defined in [`ConcurrencyControl`](./ConcurrencyControl.md): [`with_serial`](./ConcurrencyControl.md#with_serial), [`with_declared_barriers`](./ConcurrencyControl.md#with_declared_barriers), and [`with_respected_barriers`](./ConcurrencyControl.md#with_respected_barriers). This section describes how they apply to column calculations.

### `with_serial`

[`with_serial`](./ConcurrencyControl.md#with_serial) returns a `Selectable` that is serial, as [`ConcurrencyControl`](./ConcurrencyControl.md) defines the term: the column calculation never runs concurrently with itself, and it evaluates its rows one at a time in row set order.

With default settings, Deephaven considers a column calculation for parallel evaluation once it processes about 4.2 million rows or more. That count is the whole table on initialization, or the added and modified rows of each cycle on a refreshing table.

A formula backed by a Python callback is eligible for parallel evaluation only on a free-threaded Python build. On a non-free-threaded build, Deephaven never splits that column's rows across cores. That alone does not guarantee row set order, which only [`with_serial`](./ConcurrencyControl.md#with_serial) guarantees.

The following example evaluates 10 rows, far below the parallelization threshold. It shows the ordering guarantee rather than a speedup.

```python order=result
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_next_id() -> int:
    global counter
    counter += 1
    return counter


col = Selectable.parse("ID = get_next_id()").with_serial()
result = empty_table(10).update(col)
```

### `with_declared_barriers` and `with_respected_barriers`

These two methods work together to order columns. One column declares a [Barrier](./Barrier.md), and another column respects it.

A [Barrier](./Barrier.md) is a synchronization object shared between columns. The two roles are:

- [`with_declared_barriers`](./ConcurrencyControl.md#with_declared_barriers): this column goes first. It computes all of its rows before any respecting column computes its rows.
- [`with_respected_barriers`](./ConcurrencyControl.md#with_respected_barriers): this column waits. It computes no rows until the declaring column has finished.

See [Barrier](./Barrier.md) and [`ConcurrencyControl`](./ConcurrencyControl.md) for the full constraints and worked examples, and [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

### Table operations that accept a Selectable

> [!IMPORTANT]
> The table operations differ in what they accept:
>
> - [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) accept `Selectable` objects, so all three concurrency methods apply.
> - [`view`](../../table-operations/select/view.md), [`update_view`](../../table-operations/select/update-view.md), and [`lazy_update`](../../table-operations/select/lazy-update.md) accept [formula strings](../../../how-to-guides/formulas.md) only. They cannot take a `Selectable`, so none of the concurrency methods attach to their formulas.

## When to use Selectable

A `Selectable` adds concurrency controls to a column formula.

### Do you need a Selectable at all?

A [formula string](../../../how-to-guides/formulas.md) passed to [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) becomes a `Selectable` internally, and Deephaven parallelizes it by default. A string suffices for a formula that meets both of these conditions:

- It uses only values from the current row, such as `"Total = Price * Quantity"`.
- It has no side effects. It does not modify global variables, write to files, or depend on processing order.

Such a formula gains nothing from an explicit `Selectable` object.

### When you need explicit control

A formula is stateful when it reads or writes shared state that changes between rows. It is safe to run in parallel only if it produces the same result when multiple threads process the rows in any order. Parallel execution can produce incorrect results for a stateful formula, which then needs a `Selectable` with one of these controls:

- [`with_serial`](./ConcurrencyControl.md#with_serial): the formula evaluates its rows in row set order, never in parallel. Typical cases are:
  - A counter or accumulator that increments for each row.
  - Logging or file writes that must happen sequentially.
  - A formula whose result for row N depends on what happened in row N-1.
- [Barriers](./Barrier.md): one column finishes all of its rows before another column starts. A [Barrier](./Barrier.md) does not make a column serial, so columns that share state use a [Barrier](./Barrier.md) together with [`with_serial`](./ConcurrencyControl.md#with_serial) on each column. See [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

## Related documentation

- [Filter](./Filter.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Formulas](../../../how-to-guides/formulas.md)
- [`select`](../../table-operations/select/select.md)
- [`update`](../../table-operations/select/update.md)
- [Selectable Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable)
