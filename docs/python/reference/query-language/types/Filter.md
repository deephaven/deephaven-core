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

- `with_declared_barriers(barrier)` — This filter **goes first**. All rows are evaluated by this filter before any respecting filter's rows are evaluated.
- `with_respected_barriers(barrier)` — This filter **waits**. Its rows are not evaluated until all declaring filters have finished.

For examples and detailed usage, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## Filter functions

The `deephaven.filters` module provides functions for creating filters. These return `Filter` objects that you can use with `where` or modify with concurrency methods.

| Function                                       | Description                                       |
| ---------------------------------------------- | ------------------------------------------------- |
| `Filter.from_(condition)`                      | Create from condition string                      |
| `is_null(col)`                                 | True if column value is null                      |
| `is_not_null(col)`                             | True if column value is not null                  |
| `not_(filter)`                                 | Logical NOT                                       |
| `and_(filters)`                                | Logical AND of multiple filters                   |
| `or_(filters)`                                 | Logical OR of multiple filters                    |
| `in_(col, values)`                             | True if column value is in the given values       |
| `pattern(mode, col, regex, invert_pattern)`    | Regex pattern match                               |
| `eq`, `ne`, `lt`, `le`, `gt`, `ge`             | Comparison filters (e.g., `eq(left, right)`)      |
| `incremental_release(initial_rows, increment)` | Progressively release rows from an add-only table |

## When to use Filter objects

### Do you need a Filter object at all?

Most of the time, no. When you pass a string condition to [`where`](../../table-operations/filter/where.md), Deephaven creates a `Filter` internally and parallelizes its evaluation across multiple cores. This works well for any condition that:

- Only examines values in the current row (e.g., `"Price > 100"`).
- Has no side effects — it doesn't modify global variables, write to files, or depend on evaluation order.

If both of those are true, use string conditions directly. There is no benefit to constructing a `Filter` object.

### When you need explicit control

You need a `Filter` object in two situations:

**Stateful filters**: If your filter modifies shared state (e.g., counting how many rows pass), use `with_serial` to force sequential evaluation. Without it, multiple threads evaluating rows simultaneously could corrupt the shared state.

**Complex boolean logic**: Use `and_`, `or_`, and `not_` to compose filters programmatically. This is useful when building filter conditions dynamically or combining multiple conditions that are easier to express as separate objects.

**Barriers between filters** are rarely needed — most filters are stateless. If you do have filters with shared state where one must complete before another, see the [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) section in the parallelization guide.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [`where`](../../table-operations/filter/where.md) - Uses Filter objects
- [Selectable](./Selectable.md) - Similar concurrency controls for column calculations
- [Filter Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html)
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) - base class defining `with_serial` and barrier methods
- [Barrier Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)
