---
title: Filter
---

A [`Filter`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) is an object that represents a filter condition. [`where`](../../table-operations/filter/where.md) accepts `Filter` objects as well as strings.

A string condition covers most filters. Create a `Filter` object to compose conditions with boolean logic in code, to mark a filter serial with [`with_serial`](./ConcurrencyControl.md#with_serial), or to order a filter relative to other filters with [Barriers](./Barrier.md).

## Creating a Filter

A `Filter` object comes from a condition string or from combining other filters with boolean logic. The [Filter functions](#filter-functions) table lists the remaining factories.

### From a condition string

Use [`Filter.from_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.from_) when you have a condition string, the same kind of string that you pass to [`where`](../../table-operations/filter/where.md), such as `"X > 5"`.

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

The `deephaven.filters` module provides functions for creating filters. These return `Filter` objects that you can use with [`where`](../../table-operations/filter/where.md) or modify with the [concurrency methods](#methods).

| Function                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Description                                                                                                               |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [`Filter.from_(conditions)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.from_)                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Create from one or more condition strings; returns a list when given several                                              |
| [`is_null(col)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.is_null)                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | True if column value is null                                                                                              |
| [`is_not_null(col)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.is_not_null)                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | True if column value is not null                                                                                          |
| [`not_(filter_)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.not_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Logical NOT                                                                                                               |
| [`and_(filters)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.and_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Logical AND of multiple filters                                                                                           |
| [`or_(filters)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.or_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Logical OR of multiple filters                                                                                            |
| [`in_(col, values)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.in_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | True if column value is in the given values                                                                               |
| [`pattern(mode, col, regex, invert_pattern=False)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.pattern)                                                                                                                                                                                                                                                                                                                                                                                                                                          | Regex pattern match; `mode` is `PatternMode.MATCHES` (whole value) or `PatternMode.FIND` (any subsequence)                |
| [`eq`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.eq), [`ne`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.ne), [`lt`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.lt), [`le`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.le), [`gt`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.gt), [`ge`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.ge) | Comparison filters, e.g. `eq(ColumnName("X"), 5)`; wrap a column name in `ColumnName`, because a bare string is a literal |
| [`incremental_release(initial_rows, increment)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.incremental_release)                                                                                                                                                                                                                                                                                                                                                                                                                                 | Progressively release rows from an add-only table                                                                         |

## Methods

`Filter` implements [`ConcurrencyControl`](./ConcurrencyControl.md), which provides three methods that control how Deephaven evaluates the filter. By default, Deephaven parallelizes the evaluation of a single filter across CPU cores when enough rows reach it. Use these methods when a filter has side effects or needs coordination with other filters.

### `with_serial`

[`with_serial`](./ConcurrencyControl.md#with_serial) marks the filter serial. Deephaven never runs a serial filter concurrently with itself, and it evaluates the filter's rows one at a time in row-set order. Use it when the filter has side effects or depends on row order.

A serial filter also keeps its position among the other filters in a [`where`](../../table-operations/filter/where.md). [Filter evaluation order](../../table-operations/filter/where.md#filter-evaluation-order) describes the rules, and [Serial execution](../../table-operations/filter/where.md#serial-execution) describes when Deephaven parallelizes a filter that is not serial.

The following example marks a filter serial because it modifies external state.

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
> Most filters do not need [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution). Use [`with_serial`](./ConcurrencyControl.md#with_serial) only when the filter modifies external state, has side effects, or depends on row order.

### `with_declared_barriers` and `with_respected_barriers`

These two methods enforce execution order between filters. One filter declares a [Barrier](./Barrier.md) and goes first. Another filter respects the [Barrier](./Barrier.md) and waits.

A [Barrier](./Barrier.md) is a synchronization object that you create and share between filters. The two methods apply it as follows:

- [`with_declared_barriers(barriers)`](./ConcurrencyControl.md#with_declared_barriers): this filter goes first. It evaluates all of its rows before any respecting filter evaluates its own.
- [`with_respected_barriers(barriers)`](./ConcurrencyControl.md#with_respected_barriers): this filter waits. It does not evaluate its rows until the filter that declares the [Barrier](./Barrier.md) finishes.

For the full reference, constraints, and worked examples, see [Barrier](./Barrier.md) and [ConcurrencyControl](./ConcurrencyControl.md). For broader context, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Filter objects

When you pass a string condition to [`where`](../../table-operations/filter/where.md), Deephaven creates a `Filter` internally and parallelizes its evaluation across cores when enough rows reach it. A string condition is enough when the condition examines only values in the current row, such as `"Price > 100"`, and has no side effects. A condition with no side effects does not modify global variables, write to files, or depend on evaluation order.

Use a `Filter` object in these three situations:

- **Stateful filters**: a filter that modifies shared state, such as counting how many rows pass, is stateful. Mark it serial with [`with_serial`](./ConcurrencyControl.md#with_serial) so that threads do not corrupt the shared state by evaluating rows at the same time.
- **Complex boolean logic**: use [`and_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.and_), [`or_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.or_), and [`not_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.not_) to compose filters programmatically when you build conditions dynamically or when separate objects express the conditions more clearly.
- **[Barriers](./Barrier.md) between filters**: use them when one filter with shared state must finish before another starts. This is rare because most filters are stateless. See [Barrier](./Barrier.md) for the full reference or [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](./Selectable.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [`where`](../../table-operations/filter/where.md)
- [Filter Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html)
