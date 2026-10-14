---
title: Filter
---

A [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) is an object that represents a filter condition. [`where`](../../table-operations/filter/where.md) accepts [`Filter`](./Filter.md) objects as well as strings.

A string condition covers most filters. Create a [`Filter`](./Filter.md) object to compose conditions with boolean logic in code, to mark a filter serial with [`withSerial`](./ConcurrencyControl.md#withserial), or to order a filter relative to other filters with [Barriers](./Barrier.md).

## Creating a Filter

A [`Filter`](./Filter.md) object comes from a condition string or from combining other filters with boolean logic. The [Filter functions](#filter-functions) table lists the remaining factories.

### From a condition string

Use [`Filter.from`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) when you have condition strings, the same kind of string that you pass to [`where`](../../table-operations/filter/where.md). Note that `Filter.from` returns a collection, so use `[0]` to get a single filter.

```groovy syntax
import io.deephaven.api.filter.Filter

// Filter.from() returns a collection; use [0] to get the single filter
myFilter = Filter.from("X > 5")[0]
```

### Combining filters with boolean logic

Use [`Filter.or`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) and [`Filter.and`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) to combine multiple filters. These accept any [`Filter`](./Filter.md) instances, as varargs or as a collection, such as filters created with [`Filter.from`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html).

```groovy syntax
import io.deephaven.api.filter.Filter

// Multiple conditions with OR
orFilter = Filter.or(Filter.from("X > 5", "Y < 10"))

// Multiple conditions with AND
andFilter = Filter.and(Filter.from("X > 5", "Y < 10"))
```

## Filter functions

The [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) interface provides static factory methods for common conditions. These return [`Filter`](./Filter.md) objects that you can combine with [`Filter.and`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) and [`Filter.or`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) or modify with the [concurrency methods](#methods).

| Function                                                                                                                                                  | Description                             |
| --------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| [`Filter.from(condition)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)                                                         | Create from condition string(s)         |
| [`Filter.and(filters)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)                                                            | Logical AND of multiple filters         |
| [`Filter.or(filters)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)                                                             | Logical OR of multiple filters          |
| [`Filter.isNull(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isNull(io.deephaven.api.expression.Expression))       | True if the expression is null          |
| [`Filter.isNotNull(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isNotNull(io.deephaven.api.expression.Expression)) | True if the expression is not null      |
| [`Filter.not(filter)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#not(F))                                                      | Logical NOT                             |
| [`Filter.isNaN(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isNaN(io.deephaven.api.expression.Expression))         | True if the expression is NaN           |
| [`Filter.isNotNaN(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isNotNaN(io.deephaven.api.expression.Expression))   | True if the expression is not NaN       |
| [`Filter.isTrue(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isTrue(io.deephaven.api.expression.Expression))       | True if the boolean expression is true  |
| [`Filter.isFalse(expression)`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#isFalse(io.deephaven.api.expression.Expression))     | True if the boolean expression is false |
| [`Filter.ofTrue()`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#ofTrue())                                                       | Always true (matches every row)         |
| [`Filter.ofFalse()`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html#ofFalse())                                                     | Always false (matches no rows)          |

## Methods

[`Filter`](./Filter.md) implements [`ConcurrencyControl`](./ConcurrencyControl.md), which provides three methods that control how Deephaven evaluates the filter. By default, Deephaven parallelizes the evaluation of a single filter across CPU cores when enough rows reach it. Use these methods when a filter has side effects or needs coordination with other filters.

### `withSerial`

[`withSerial`](./ConcurrencyControl.md#withserial) marks the filter serial. Deephaven never runs a serial filter concurrently with itself, and it evaluates the filter's rows one at a time in row-set order. Use it when the filter has side effects or depends on row order.

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

> [!NOTE]
> Most filters do not need [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution). Use [`withSerial`](./ConcurrencyControl.md#withserial) only when the filter modifies external state, has side effects, or depends on row order.

### `withDeclaredBarriers` and `withRespectedBarriers`

These two methods enforce execution order between filters. One filter declares a [Barrier](./Barrier.md) and goes first. Another filter respects the [Barrier](./Barrier.md) and waits.

A [Barrier](./Barrier.md) is a synchronization object that you create and share between filters. In Groovy, any Java object can serve as a [Barrier](./Barrier.md). The two methods apply it as follows:

- [`withDeclaredBarriers(barrier)`](./ConcurrencyControl.md#withdeclaredbarriers): this filter goes first. It evaluates all of its rows before any respecting filter evaluates its own.
- [`withRespectedBarriers(barrier)`](./ConcurrencyControl.md#withrespectedbarriers): this filter waits. It does not evaluate its rows until the filter that declares the [Barrier](./Barrier.md) finishes.

For the full reference, constraints, and worked examples, see [Barrier](./Barrier.md) and [ConcurrencyControl](./ConcurrencyControl.md). For broader context, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Filter objects

When you pass a string condition to [`where`](../../table-operations/filter/where.md), Deephaven creates a [`Filter`](./Filter.md) internally and parallelizes its evaluation across cores when enough rows reach it. A string condition is enough when the condition examines only values in the current row, such as `"Price > 100"`, and has no side effects. A condition with no side effects does not modify global variables, write to files, or depend on evaluation order.

Use a [`Filter`](./Filter.md) object in these three situations:

- **Stateful filters**: a filter that modifies shared state, such as counting how many rows pass, is stateful. Mark it serial with [`withSerial`](./ConcurrencyControl.md#withserial) so that threads do not corrupt the shared state by evaluating rows at the same time.
- **Complex boolean logic**: use [`Filter.and`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html), [`Filter.or`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html), and [`Filter.not`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) to combine filters programmatically when you build conditions dynamically or when separate objects express the conditions more clearly.
- **[Barriers](./Barrier.md) between filters**: use them when one filter with shared state must finish before another starts. This is rare because most filters are stateless. See [Barrier](./Barrier.md) for the full reference or [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](./Selectable.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [`where`](../../table-operations/filter/where.md)
- [Filter Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)
