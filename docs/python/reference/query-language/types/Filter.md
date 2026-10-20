---
title: Filter
---

A [`Filter`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) is an object that represents a filter condition. [`where`](../../table-operations/filter/where.md) accepts `Filter` objects as well as strings. When it receives a string, Deephaven creates a `Filter` from it internally.

A string condition covers most filters. A `Filter` object composes conditions with boolean logic in code, marks a filter serial with [`with_serial`](./ConcurrencyControl.md#with_serial), or orders a filter relative to other filters with [Barriers](./Barrier.md).

## Create a Filter

A `Filter` object comes from a condition string or from combining other filters with boolean logic. The [Filter functions](#use-filter-functions) table lists the remaining factories.

### Create a Filter from a condition string

[`Filter.from_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.from_) takes a condition string, the same kind of string that [`where`](../../table-operations/filter/where.md) accepts, such as `"X > 5"`. It returns one `Filter` for a single condition string and a list of `Filter` objects for several.

```python order=source,result
from deephaven.filters import Filter
from deephaven import empty_table

source = empty_table(10).update("X = i")

my_filter = Filter.from_("X > 5")
result = source.where(my_filter)
```

### Combine filters with boolean logic

Filter functions build conditions in code, such as null checks and boolean combinations. These functions return `Filter` objects that can be combined or modified.

```python order=source,result
from deephaven.filters import Filter, is_null, not_, and_, or_
from deephaven import empty_table

source = empty_table(20).update(["X = i", "Y = 20 - i"])

null_filter = is_null("X")
not_null_filter = not_(is_null("Y"))
and_filter = and_([Filter.from_("X > 5"), Filter.from_("Y < 10")])
or_filter = or_([Filter.from_("X > 5"), Filter.from_("Y < 10")])
result = source.where(or_filter)
```

## Use filter functions

The [`deephaven.filters`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) module provides functions that create filters, and the `Filter` class provides the [`Filter.from_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.from_) factory and the [`Filter.not_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.not_) method. Each returns a `Filter` object that [`where`](../../table-operations/filter/where.md) accepts and that the [concurrency methods](#methods) can modify.

| Function                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Description                                                                                                                                                                                                                                                        |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`Filter.from_(conditions)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.from_)                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Creates filters from one or more condition strings. Returns one `Filter` for a single string and a list for several strings.                                                                                                                                       |
| [`Filter.not_()`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.Filter.not_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Instance method that returns the logical NOT of this filter                                                                                                                                                                                                        |
| [`is_null(col)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.is_null)                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | True if the column value is null                                                                                                                                                                                                                                   |
| [`is_not_null(col)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.is_not_null)                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | True if the column value is not null                                                                                                                                                                                                                               |
| [`not_(filter_)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.not_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Logical NOT                                                                                                                                                                                                                                                        |
| [`and_(filters)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.and_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Logical AND of multiple filters                                                                                                                                                                                                                                    |
| [`or_(filters)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.or_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Logical OR of multiple filters                                                                                                                                                                                                                                     |
| [`in_(col, values)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.in_)                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | True if the column value is in the given values                                                                                                                                                                                                                    |
| [`pattern(mode, col, regex, invert_pattern=False)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.pattern)                                                                                                                                                                                                                                                                                                                                                                                                                                          | Regular-expression match that never matches null. `mode` is a [`PatternMode`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.PatternMode): `PatternMode.MATCHES` for the whole value or `PatternMode.FIND` for any subsequence |
| [`eq`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.eq), [`ne`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.ne), [`lt`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.lt), [`le`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.le), [`gt`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.gt), [`ge`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.ge) | Comparison filters, such as `eq(ColumnName("X"), 5)`                                                                                                                                                                                                               |
| [`incremental_release(initial_rows, increment)`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.incremental_release)                                                                                                                                                                                                                                                                                                                                                                                                                                 | Progressively releases rows from an add-only table                                                                                                                                                                                                                 |

In the comparison filters, a bare string argument is a literal. A [`ColumnName`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.ColumnName) argument refers to a column.

## Methods

`Filter` implements [`ConcurrencyControl`](./ConcurrencyControl.md), which provides three methods that control how Deephaven evaluates the filter. By default, Deephaven [parallelizes](../../../conceptual/query-engine/parallelization.md) the evaluation of a single filter across CPU cores when enough rows reach it. A filter that calls a Python function or uses Python objects runs in parallel only on a free-threaded Python build. See [Python formulas and free-threading](../../../conceptual/query-engine/parallelization.md#python-formulas-and-free-threading).

### `with_serial`

[`with_serial`](./ConcurrencyControl.md#with_serial) marks the filter serial. Deephaven never runs a serial filter concurrently with itself, and it evaluates the rows of the filter one at a time in row set order. A filter that has side effects, such as modifying shared state, or that depends on row order needs this guarantee.

A serial filter also keeps its position among the other filters in a [`where`](../../table-operations/filter/where.md). [Filter evaluation order](../../table-operations/filter/where.md#control-filter-evaluation-order) describes the rules, and [Serial execution](../../table-operations/filter/where.md#run-a-filter-serially) describes when Deephaven parallelizes a filter that is not serial.

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

The serial filter evaluates each of the 100 rows exactly once, one at a time, so `rows_checked` is 100 after the [`where`](../../table-operations/filter/where.md) call.

### `with_declared_barriers` and `with_respected_barriers`

These two methods enforce execution order between filters. One filter declares a [Barrier](./Barrier.md) and goes first. Another filter respects the [Barrier](./Barrier.md) and waits.

A [Barrier](./Barrier.md) is a synchronization object that is created once and shared between filters. The two methods apply it as follows:

- [`with_declared_barriers(barriers)`](./ConcurrencyControl.md#with_declared_barriers): this filter goes first. It evaluates all of its rows before any respecting filter evaluates its own.
- [`with_respected_barriers(barriers)`](./ConcurrencyControl.md#with_respected_barriers): this filter waits. It does not evaluate its rows until the filter that declares the [Barrier](./Barrier.md) finishes.

For the full reference, constraints, and worked examples, see [Barrier](./Barrier.md) and [ConcurrencyControl](./ConcurrencyControl.md). For broader context, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## Decide when to use Filter objects

A string condition is enough when the condition examines only values in the current row, such as `"Price > 100"`, and has no side effects. A condition with no side effects does not modify global variables, write to files, or depend on evaluation order. [`where`](../../table-operations/filter/where.md) creates a `Filter` from the string and [parallelizes](../../../conceptual/query-engine/parallelization.md) its evaluation across cores when enough rows reach it. A string condition that calls a Python function runs in parallel only on a free-threaded Python build.

Use a `Filter` object in these three situations:

- **Filters with shared state**: a filter that modifies shared state, such as counting how many rows pass, can corrupt that state when threads evaluate rows at the same time. Mark it serial with [`with_serial`](./ConcurrencyControl.md#with_serial) so that the engine evaluates its rows one at a time.
- **Complex boolean logic**: use [`and_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.and_), [`or_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.or_), and [`not_`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html#deephaven.filters.not_) to compose filters programmatically when conditions are built dynamically or when separate objects express the conditions more clearly.
- **[Barriers](./Barrier.md) between filters**: use them when one filter with shared state must finish before another starts. This is rare because most filters do not share state. See [Barrier](./Barrier.md) for the full reference or [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Filter table data](../../../how-to-guides/use-filters.md)
- [Selectable](./Selectable.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [`where`](../../table-operations/filter/where.md)
- [Filter Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html)
