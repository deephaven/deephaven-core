---
title: Filter
---

A [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) is an object that represents a filter condition. [`where`](../../table-operations/filter/where.md) accepts `Filter` objects as well as strings. When it receives a string, Deephaven creates a `Filter` from it internally.

A string condition covers most filters. A `Filter` object composes conditions with boolean logic in code, marks a filter serial with [`withSerial`](./ConcurrencyControl.md#withserial), or orders a filter relative to other filters with [Barriers](./Barrier.md).

## Creating a Filter

A `Filter` object comes from a condition string or from combining other filters with boolean logic. The [Filter functions](#filter-functions) table lists the remaining factories.

### From a condition string

[`Filter.from`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) takes condition strings, the same kind of string that [`where`](../../table-operations/filter/where.md) accepts, such as `"X > 5"`. `Filter.from` returns a collection, so `[0]` selects a single filter.

```groovy order=source,result
import io.deephaven.api.filter.Filter

source = emptyTable(10).update("X = i")

// Filter.from() returns a collection; use [0] to get the single filter
myFilter = Filter.from("X > 5")[0]
result = source.where(myFilter)
```

### Combining filters with boolean logic

[`Filter.or`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) and [`Filter.and`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) combine multiple filters. They accept any `Filter` instances, as varargs or as a collection, such as the filters that [`Filter.from`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) creates.

```groovy order=source,result
import io.deephaven.api.filter.Filter

source = emptyTable(20).update("X = i", "Y = 20 - i")

// Multiple conditions with OR
orFilter = Filter.or(Filter.from("X > 5", "Y < 10"))

// Multiple conditions with AND
andFilter = Filter.and(Filter.from("X > 5", "Y < 10"))

result = source.where(orFilter)
```

## Filter functions

The [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) interface provides static factory methods for common conditions. The comparison, `in`, and pattern filters are the separate classes [`FilterComparison`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/FilterComparison.html), [`FilterIn`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/FilterIn.html), and [`FilterPattern`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/FilterPattern.html). Each factory returns a `Filter` object that [`where`](../../table-operations/filter/where.md) accepts, that [`Filter.and`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) and [`Filter.or`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) can combine, and that the [concurrency methods](#methods) can modify.

| Function                                                                                                                                                               | Description                                                                   |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| [`Filter.from(condition)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)                                                                      | Creates filters from condition strings                                        |
| [`Filter.and(filters)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)                                                                         | Logical AND of multiple filters                                               |
| [`Filter.or(filters)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)                                                                          | Logical OR of multiple filters                                                |
| [`Filter.isNull(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isNull(io.deephaven.api.expression.Expression))                    | True if the expression is null                                                |
| [`Filter.isNotNull(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isNotNull(io.deephaven.api.expression.Expression))              | True if the expression is not null                                            |
| [`Filter.not(filter)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#not(F))                                                                   | Logical NOT                                                                   |
| [`Filter.isNaN(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isNaN(io.deephaven.api.expression.Expression))                      | True if the expression is NaN                                                 |
| [`Filter.isNotNaN(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isNotNaN(io.deephaven.api.expression.Expression))                | True if the expression is not NaN                                             |
| [`Filter.isTrue(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isTrue(io.deephaven.api.expression.Expression))                    | True if the boolean expression is true                                        |
| [`Filter.isFalse(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isFalse(io.deephaven.api.expression.Expression))                  | True if the boolean expression is false                                       |
| [`Filter.ofTrue()`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#ofTrue())                                                                    | Always true (matches every row)                                               |
| [`Filter.ofFalse()`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#ofFalse())                                                                  | Always false (matches no rows)                                                |
| [`FilterComparison.eq(lhs, rhs)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/FilterComparison.html) and the `neq`, `lt`, `leq`, `gt`, and `geq` methods | Comparison filters                                                            |
| [`FilterIn.of(expression, values)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/FilterIn.html)                                                           | True if the expression equals one of the values                               |
| [`FilterPattern.of(expression, pattern, mode, invertPattern)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/FilterPattern.html)                           | Regular-expression match that never matches null, in `MATCHES` or `FIND` mode |

## Methods

`Filter` implements [`ConcurrencyControl`](./ConcurrencyControl.md), which provides three methods that control how Deephaven evaluates the filter. By default, Deephaven [parallelizes](../../../conceptual/query-engine/parallelization.md) the evaluation of a single filter across CPU cores when enough rows reach it.

### `withSerial`

[`withSerial`](./ConcurrencyControl.md#withserial) marks the filter serial. Deephaven never runs a serial filter concurrently with itself, and it evaluates the rows of the filter one at a time in row set order. A filter that has side effects, such as modifying shared state, or that depends on row order needs this guarantee.

A serial filter also keeps its position among the other filters in a [`where`](../../table-operations/filter/where.md). [Filter evaluation order](../../table-operations/filter/where.md#filter-evaluation-order) describes the rules, and [Serial execution](../../table-operations/filter/where.md#serial-execution) describes when Deephaven parallelizes a filter that is not serial.

The following example marks a filter serial because it modifies external state.

```groovy order=source,result
import io.deephaven.api.filter.Filter

rowsChecked = [0] as int[]

checkValue = { int x ->
    rowsChecked[0]++  // Side effect: modifies external state
    return x > 5
}

source = emptyTable(100).update("X = i")

// Use withSerial because the filter has side effects
myFilter = Filter.from("(boolean)checkValue(X)")[0].withSerial()
result = source.where(myFilter)
```

The serial filter evaluates each of the 100 rows exactly once, one at a time, so `rowsChecked[0]` is 100 after the [`where`](../../table-operations/filter/where.md) call.

### `withDeclaredBarriers` and `withRespectedBarriers`

These two methods enforce execution order between filters. One filter declares a [Barrier](./Barrier.md) and goes first. Another filter respects the [Barrier](./Barrier.md) and waits.

A [Barrier](./Barrier.md) is a synchronization object that is created once and shared between filters. In Groovy, any Java object can serve as a [Barrier](./Barrier.md). The two methods apply it as follows:

- [`withDeclaredBarriers(barrier)`](./ConcurrencyControl.md#withdeclaredbarriers): this filter goes first. It evaluates all of its rows before any respecting filter evaluates its own.
- [`withRespectedBarriers(barrier)`](./ConcurrencyControl.md#withrespectedbarriers): this filter waits. It does not evaluate its rows until the filter that declares the [Barrier](./Barrier.md) finishes.

For the full reference, constraints, and worked examples, see [Barrier](./Barrier.md) and [ConcurrencyControl](./ConcurrencyControl.md). For broader context, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Filter objects

A string condition is enough when the condition examines only values in the current row, such as `"Price > 100"`, and has no side effects. A condition with no side effects does not modify global variables, write to files, or depend on evaluation order. [`where`](../../table-operations/filter/where.md) creates a `Filter` from the string and [parallelizes](../../../conceptual/query-engine/parallelization.md) its evaluation across cores when enough rows reach it.

Use a `Filter` object in these three situations:

- **Filters with shared state**: a filter that modifies shared state, such as counting how many rows pass, can corrupt that state when threads evaluate rows at the same time. Mark it serial with [`withSerial`](./ConcurrencyControl.md#withserial) so that the engine evaluates its rows one at a time.
- **Complex boolean logic**: use [`Filter.and`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html), [`Filter.or`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html), and [`Filter.not`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) to compose filters programmatically when conditions are built dynamically or when separate objects express the conditions more clearly.
- **[Barriers](./Barrier.md) between filters**: use them when one filter with shared state must finish before another starts. This is rare because most filters do not share state. See [Barrier](./Barrier.md) for the full reference or [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Filter table data](../../../how-to-guides/use-filters.md)
- [Selectable](./Selectable.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [`where`](../../table-operations/filter/where.md)
- [Filter Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)
