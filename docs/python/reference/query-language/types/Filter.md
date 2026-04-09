---
title: Filter
---

A [`Filter`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) represents a filter condition used in [`where`](../../table-operations/filter/where.md) operations. Use `Filter` objects when you need to control how Deephaven evaluates filter conditions — specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

## Creating a Filter

There are two ways to create a `Filter` object: from a condition string or using filter functions for null checks and boolean logic.

### From a condition string

Use `Filter.from_()` when you have a filter condition as a string. This is the most direct approach.

```python syntax
from deephaven.filters import Filter

my_filter = Filter.from_("X > 5")
```

### Using filter functions

Use the filter functions for null checks, boolean combinations, and special value tests. These functions return `Filter` objects that you can combine or modify.

```python syntax
from deephaven.filters import is_null, not_, and_, or_

null_filter = is_null("X")
not_null_filter = not_(is_null("Y"))
and_filter = and_([Filter.from_("X > 5"), Filter.from_("Y < 10")])
or_filter = or_([Filter.from_("X > 5"), Filter.from_("Y < 10")])
```

## Methods

These methods control how Deephaven evaluates the filter. By default, Deephaven parallelizes filter evaluation across multiple CPU cores. Use these methods when your filter has side effects or requires coordination with other filters.

### `with_serial`

Forces the filter to evaluate sequentially on a single core, processing rows one at a time in order. Use this when the filter has side effects or depends on row order.

```python order=source,result
from deephaven.filters import Filter
from deephaven import empty_table

rows_checked = 0


def check_value(x) -> bool:
    global rows_checked
    rows_checked += 1  # Side effect: modifies external state
    return x > 5


source = empty_table(100).update("X = i")

# Use .with_serial because the filter has side effects
my_filter = Filter.from_("(boolean)check_value(X)").with_serial()
result = source.where(my_filter)
```

> [!NOTE]
> Most filters don't need serial execution. Use `with_serial` only when the filter modifies external state or has side effects.

### `with_declared_barriers` and `with_respected_barriers`

These two methods work together to enforce execution order between filters. One filter **declares** the barrier (goes first), and another filter **respects** the barrier (waits).

A [`Barrier`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier) is a synchronization object you create and share between filters:

- `with_declared_barriers(barrier)` — This filter **goes first**. All rows are evaluated before any respecting filter begins.
- `with_respected_barriers(barrier)` — This filter **waits**. It does not start until all declaring filters finish.

For examples and detailed usage, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## Filter functions

The `deephaven.filters` module provides functions for creating filters. These return `Filter` objects that you can use with `where()` or modify with concurrency methods.

| Function                       | Description                                     |
| ------------------------------ | ----------------------------------------------- |
| `Filter.from_(condition)`      | Create from condition string                    |
| `is_null(column)`              | True if column value is null                    |
| `not_(filter)`                 | Logical NOT                                     |
| `and_(filters)`                | Logical AND of multiple filters                 |
| `or_(filters)`                 | Logical OR of multiple filters                  |
| `pattern(column, regex, mode)` | Regex pattern match                             |
| `is_inf(column)`               | True if value is infinite                       |
| `is_nan(column)`               | True if value is NaN                            |
| `is_normal(column)`            | True if value is a normal floating-point number |

## When to use Filter objects

Most queries don't need `Filter` objects — string conditions work fine for simple filters. Use `Filter` when you need explicit control over evaluation or complex boolean logic.

| Scenario                             | Approach                                                |
| ------------------------------------ | ------------------------------------------------------- |
| Simple conditions                    | Use string filters directly - no `Filter` object needed |
| Filter with side effects             | `Filter.from_(...).with_serial()`                       |
| Filter A must finish before Filter B | Use barriers                                            |
| Complex boolean logic                | Use `and_()`, `or_()`, `not_()`                         |

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [`where`](../../table-operations/filter/where.md) - Uses Filter objects
- [Selectable](./Selectable.md) - Similar concurrency controls for column calculations
- [Filter Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html)
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) - base class defining `with_serial` and barrier methods
- [Barrier Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)
