---
title: Filter
---

A [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) is an object that represents a filter condition. [`where`](../../table-operations/filter/where.md) accepts `Filter` objects as well as strings.

## Why use a Filter?

Most conditions need no `Filter` object. Use [`where`](../../table-operations/filter/where.md) with a string condition, or [`whereIn`](../../table-operations/filter/where-in.md), [`whereNotIn`](../../table-operations/filter/where-not-in.md), and the other filter methods. A string condition is enough when it reads only values in the current row and has no side effects, such as `"Price > 100"`.

[`where`](../../table-operations/filter/where.md) turns a string condition into a `Filter` and evaluates it in [parallel](../../../conceptual/query-engine/parallelization.md) across cores when enough rows reach it. A condition has no side effects if it does not modify global variables, write to files, or depend on evaluation order.

Use a `Filter` object in these three situations:

- **Filters with shared state**: a filter that modifies shared state, such as counting how many rows pass, can corrupt that state when threads evaluate rows at the same time. Mark it serial with [`withSerial`](./ConcurrencyControl.md#withserial) so that the engine evaluates its rows one at a time.
- **Boolean logic built in code**: a string condition can already combine tests with `&&` and `||`. Use [`Filter.and`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html), [`Filter.or`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html), and [`Filter.not`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) when code builds the conditions, such as from a list assembled at run time, or when the parts are `Filter` objects.
- **Ordering between filters**: a [Barrier](./Barrier.md) makes one filter finish before another starts. This is rare because most filters do not share state. See [Barrier](./Barrier.md) for the full reference.

## Create a Filter

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

This example only shows how to create a `Filter`. It is equivalent to `source.where("X > 5")`, because the `Filter` has no control method and is not combined with other filters.

### Combine filters with boolean logic

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

`Filter` implements [`ConcurrencyControl`](./ConcurrencyControl.md), which provides three methods that control how Deephaven evaluates the filter. [Parallelization](../../../conceptual/query-engine/parallelization.md) describes the defaults.

### `withSerial`

[`withSerial`](./ConcurrencyControl.md#withserial) marks the filter serial. Deephaven never runs a serial filter concurrently with itself, and it evaluates the rows of the filter one at a time in row set order.

A serial filter also keeps its position among the other filters in a [`where`](../../table-operations/filter/where.md), so no later filter runs ahead of it. [Filter evaluation order](../../table-operations/filter/where.md#control-filter-evaluation-order) describes the rules.

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

A [Barrier](./Barrier.md) is an object that is created once and shared between filters. These two methods give filters their roles, and together they enforce execution order between filters:

- [`withDeclaredBarriers(barrier)`](./ConcurrencyControl.md#withdeclaredbarriers): this filter goes first. It evaluates all of its rows before any respecting filter evaluates its own.
- [`withRespectedBarriers(barrier)`](./ConcurrencyControl.md#withrespectedbarriers): this filter waits. It does not evaluate its rows until the filter that declares the [Barrier](./Barrier.md) finishes.

For the full reference, constraints, and worked examples, see [Barrier](./Barrier.md) and [ConcurrencyControl](./ConcurrencyControl.md). For broader context, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Filter table data](../../../how-to-guides/use-filters.md)
- [Selectable](./Selectable.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [`where`](../../table-operations/filter/where.md)
- [Filter Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)
