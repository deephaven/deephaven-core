---
title: Selectable
---

A [`Selectable`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) represents a column expression used in [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) operations. Use `Selectable` objects when you need to control how Deephaven processes column calculations — specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

## Creating a Selectable

Use `Selectable.parse` to create a `Selectable` from a formula string. The formula follows the same `"ColumnName = expression"` syntax used in [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) calls:

```python syntax
from deephaven.table import Selectable

col = Selectable.parse("NewColumn = ExistingColumn * 2")
```

Once created, you can chain concurrency control methods like `with_serial` before passing the `Selectable` to a table operation.

## Methods

These methods control how Deephaven executes the column calculation. By default, Deephaven parallelizes calculations across multiple CPU cores. Use these methods when your formula requires sequential processing or coordination between columns.

### `with_serial`

Forces the column calculation to execute sequentially on a single core, processing rows one at a time in order. Use this when the formula has side effects or depends on row order.

```python order=result
from deephaven.table import Selectable
from deephaven import empty_table

counter = 0


def get_next_id():
    global counter
    counter += 1
    return counter


col = Selectable.parse("ID = get_next_id()").with_serial()
result = empty_table(10).update(col)
```

> [!IMPORTANT]
> `Selectable` objects can only be used with [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md). The [`view`](../../table-operations/select/view.md), [`update_view`](../../table-operations/select/update-view.md), and [`lazy_update`](../../table-operations/select/lazy-update.md) methods do not accept `Selectable` objects because they compute values on-demand and cannot guarantee processing order.

### `with_declared_barriers` and `with_respected_barriers`

These two methods work together to enforce execution order between columns. One column **declares** the barrier (goes first), and another column **respects** the barrier (waits).

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) is a synchronization object you create and share between columns:

- `with_declared_barriers(barrier)` — This column **goes first**. All of its rows are computed before any respecting column's rows are computed.
- `with_respected_barriers(barrier)` — This column **waits**. Its rows are not computed until all declaring columns have finished.

For examples and detailed usage, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Selectable

### Do you need a Selectable at all?

Most of the time, no. When you pass a string formula to [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md), Deephaven creates a `Selectable` internally and parallelizes the computation across multiple cores. This is the default behavior and it works well for any formula that:

- Only uses values from the current row (e.g., `"Total = Price * Quantity"`).
- Has no side effects — it doesn't modify global variables, write to files, or depend on processing order.

If both of those are true, use string formulas directly. There is no benefit to constructing a `Selectable` object.

### When you need explicit control

You need a `Selectable` object when the default parallel behavior would produce incorrect results. This happens when your formula is **stateful** — it reads or writes shared state that changes between rows.

**Use `with_serial`** when your formula must process rows one at a time, in order. Common cases include:

- A counter or accumulator that increments for each row.
- Logging or file writes that must happen sequentially.
- Any formula where the result for row N depends on what happened in row N-1.

**Use barriers** when you have multiple columns with shared state and one column must finish all its rows before another column starts. See the [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) section in the parallelization guide for details.

If you're unsure whether your formula is safe for parallel execution, ask: "Would this produce the same result if the rows were processed in a random order by multiple threads?" If the answer is no, you need a `Selectable`.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [Filter](./Filter.md) - Similar concurrency controls for filter operations
- [`select`](../../table-operations/select/select.md) - Uses Selectable objects
- [`update`](../../table-operations/select/update.md) - Uses Selectable objects
- [Barrier Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl)
- [Selectable Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable)
