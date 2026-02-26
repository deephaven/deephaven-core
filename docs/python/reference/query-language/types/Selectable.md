---
title: Selectable
---

A [`Selectable`](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) represents a column expression used in [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) operations. Use `Selectable` objects when you need to control how Deephaven processes column calculations — specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

> [!NOTE]
> `Selectable` extends [`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl), which defines the concurrency methods. You may see these methods documented in multiple places in the Pydoc — use the `Selectable` class for column expressions.

## Creating a Selectable

### From a formula string

```python syntax
from deephaven.table import Selectable

col = Selectable.parse("NewColumn = ExistingColumn * 2")
```

### From column name and expression

```python syntax
from deephaven.table import Selectable

col = Selectable.of_str("NewColumn", "ExistingColumn * 2")
```

## Methods

### `.with_serial`

Forces the column calculation to execute sequentially on a single core, processing rows one at a time in order. Use this when the formula modifies global state or depends on row order.

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
> `.with_serial` cannot be used with [`view`](../../table-operations/select/view.md) or [`update_view`](../../table-operations/select/update-view.md). These operations compute values on-demand and cannot guarantee processing order.

### `.with_declared_barriers`

Declares that this column creates a barrier - other columns that respect this barrier will wait until this column finishes processing all rows.

```python syntax
from deephaven.table import Selectable
from deephaven.concurrency_control import Barrier

barrier = Barrier()
col_a = Selectable.parse("A = compute_first()").with_declared_barriers(barrier)
```

### `.with_respected_barriers`

Declares that this column respects a barrier - it will not begin processing until all columns that declare the same barrier have finished.

```python syntax
from deephaven.table import Selectable
from deephaven.concurrency_control import Barrier

barrier = Barrier()
col_b = Selectable.parse("B = compute_second()").with_respected_barriers(barrier)
```

### Barriers example

Use barriers when one column must complete before another starts:

```python order=result
from deephaven.table import Selectable
from deephaven.concurrency_control import Barrier
from deephaven import empty_table

counter = 0


def get_and_increment():
    global counter
    counter += 1
    return counter


barrier = Barrier()

# Column A declares the barrier (must finish first)
col_a = Selectable.parse("A = get_and_increment()").with_declared_barriers(barrier)

# Column B respects the barrier (waits for A to finish)
col_b = Selectable.parse("B = get_and_increment()").with_respected_barriers(barrier)

result = empty_table(10).update([col_a, col_b])
```

All rows of column `A` are processed before any rows of column `B` begin.

## When to use Selectable

| Scenario                             | Approach                                              |
| ------------------------------------ | ----------------------------------------------------- |
| Simple column math                   | Use string formulas directly - no `Selectable` needed |
| Global counter or state              | `Selectable.parse(...).with_serial()`                 |
| Column A must finish before Column B | Use barriers                                          |
| File I/O or logging                  | `Selectable.parse(...).with_serial()`                 |

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [`update`](../../table-operations/select/update.md) - Uses Selectable objects
- [`select`](../../table-operations/select/select.md) - Uses Selectable objects
- [Filter](./Filter.md) - Similar concurrency controls for filter operations
- [Selectable Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable)
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) - base class defining `with_serial` and barrier methods
- [Barrier Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)
