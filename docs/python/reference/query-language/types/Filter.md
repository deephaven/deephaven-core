---
title: Filter
---

A [`Filter`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) represents a filter condition used in [`where`](../../table-operations/filter/where.md) operations. Use `Filter` objects when you need to compose conditions programmatically (combining filters with boolean logic) or to control how Deephaven evaluates them — specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

## Creating a Filter

Here are two common ways to create a `Filter` object: from a condition string, or by combining multiple filters with boolean logic. The [Filter functions](#filter-functions) below cover additional direct factories.

### From a condition string

Use [`Filter.from_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.from_) when you have a filter condition as a string. This is the most direct approach.

```python syntax
from deephaven.filters import Filter

my_filter = Filter.from_("X > 5")
```

### Combining filters with boolean logic

Use filter functions for null checks, boolean combinations, and special value tests. These functions return `Filter` objects that you can combine or modify.

```python syntax
from deephaven.filters import Filter, is_null, not_, and_, or_

null_filter = is_null("X")
not_null_filter = not_(is_null("Y"))
and_filter = and_([Filter.from_("X > 5"), Filter.from_("Y < 10")])
or_filter = or_([Filter.from_("X > 5"), Filter.from_("Y < 10")])
```

## Filter functions

The `deephaven.filters` module provides functions for creating filters. These return `Filter` objects that you can use with [`where`](../../table-operations/filter/where.md) or modify with concurrency methods.

| Function                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Description                                       |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| [`Filter.from_(condition)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.from_)                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Create from condition string                      |
| [`is_null(col)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.is_null)                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | True if column value is null                      |
| [`is_not_null(col)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.is_not_null)                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | True if column value is not null                  |
| [`not_(filter)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.not_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Logical NOT                                       |
| [`and_(filters)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.and_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Logical AND of multiple filters                   |
| [`or_(filters)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.or_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Logical OR of multiple filters                    |
| [`in_(col, values)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.in_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | True if column value is in the given values       |
| [`pattern(mode, col, regex, invert_pattern=False)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.pattern)                                                                                                                                                                                                                                                                                                                                                                                                                                          | Regex pattern match                               |
| [`eq`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.eq), [`ne`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.ne), [`lt`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.lt), [`le`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.le), [`gt`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.gt), [`ge`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.ge) | Comparison filters (e.g., `eq(left, right)`)      |
| [`incremental_release(initial_rows, increment)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.incremental_release)                                                                                                                                                                                                                                                                                                                                                                                                                                 | Progressively release rows from an add-only table |

## Methods

These methods control how Deephaven evaluates the filter. By default, Deephaven can parallelize filter evaluation across multiple CPU cores when the input is large enough. Use these methods when your filter has side effects or requires coordination with other filters.

### `with_serial`

Forces the filter to never run concurrently with itself; its rows are evaluated sequentially, in row-set order. Use this when the filter has side effects or depends on row order. With default settings, a filter becomes eligible for parallel evaluation once more than about 131,072 rows reach it. That is the rows passed to this filter, not the source's size: later filters see only the rows that survive earlier ones, and on a refreshing table a typical update filters only that cycle's changed rows, though a filter can request a full refilter when its own inputs change. Use [`with_serial`](./ConcurrencyControl.md#with_serial) to protect filters that cannot tolerate parallel evaluation. A filter backed by a Python callback is only eligible for that parallel (concurrent) evaluation on a free-threaded Python build — on the standard GIL-enabled build, it is never invoked concurrently. However, that alone does not guarantee row-set order the way [`with_serial`](./ConcurrencyControl.md#with_serial) does: use [`with_serial`](./ConcurrencyControl.md#with_serial) for any filter with order-dependent side effects, regardless of Python build.

```python order=source,result
from deephaven.filters import Filter
from deephaven import empty_table

rows_checked = 0


def check_value(x) -> bool:
    global rows_checked
    rows_checked += 1  # Side effect: modifies external state
    return x > 5


source = empty_table(100).update("X = i")

# Use with_serial because the filter has side effects
my_filter = Filter.from_("check_value(X)").with_serial()
result = source.where(my_filter)
```

> [!NOTE]
> Most filters do not need serial execution. Use [`with_serial`](./ConcurrencyControl.md#with_serial) only when the filter modifies external state, has side effects, or depends on row order.

### `with_declared_barriers` and `with_respected_barriers`

These two methods work together to enforce execution order between filters. One filter **declares** the barrier (goes first), and another filter **respects** the barrier (waits).

A [`Barrier`](./Barrier.md) is a synchronization object you create and share between filters:

- `with_declared_barriers(barriers)` — This filter **goes first**. This filter evaluates all of its rows before any respecting filter evaluates its own.
- `with_respected_barriers(barriers)` — This filter **waits**. This filter does not evaluate its rows until the filter that declares the barrier finishes.

For the full reference, constraints, and worked examples, see [Barrier](./Barrier.md) and [ConcurrencyControl](./ConcurrencyControl.md); for broader context on when barriers matter, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Filter objects

### Do you need a Filter object at all?

Most of the time, no. When you pass a string condition to [`where`](../../table-operations/filter/where.md), Deephaven creates a `Filter` internally and can parallelize its evaluation across multiple cores. This works well for any condition that:

- Only examines values in the current row (e.g., `"Price > 100"`).
- Has no side effects — it does not modify global variables, write to files, or depend on evaluation order.

If both of those are true, string conditions are usually all you need. The cases below are the exceptions.

### When you need explicit control

You need a `Filter` object in three situations:

**Stateful filters**: If your filter modifies shared state (e.g., counting how many rows pass), use [`with_serial`](./ConcurrencyControl.md#with_serial) to force sequential evaluation. Without it, multiple threads evaluating rows simultaneously could corrupt the shared state.

**Complex boolean logic**: Use `and_`, `or_`, and `not_` to compose filters programmatically. This is useful when building filter conditions dynamically or combining multiple conditions that are easier to express as separate objects.

**Barriers between filters** are rarely needed — most filters are stateless. If you do have filters with shared state where one must complete before another, see [Barrier](./Barrier.md) for the full reference or the [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) section in the parallelization guide for broader context.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](./Selectable.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [`where`](../../table-operations/filter/where.md)
- [Filter Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html)
