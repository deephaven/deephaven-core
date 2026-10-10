---
title: Filter
---

A [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) represents a filter condition used in [`where`](../../table-operations/filter/where.md) operations. Use `Filter` objects when you need to control how Deephaven evaluates filter conditions — specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

## Creating a Filter

Here are two common ways to create a `Filter` object: from a condition string, or by combining multiple filters with boolean logic. The [Filter functions](#filter-functions) below cover additional direct factories.

### From a condition string

Use `Filter.from` when you have filter conditions as strings. Note that `Filter.from` returns a collection, so use `[0]` to get a single filter.

```groovy syntax
import io.deephaven.api.filter.Filter

// Filter.from() returns a collection; use [0] to get the single filter
myFilter = Filter.from("X > 5")[0]
```

### Combining filters with boolean logic

Use `Filter.or` and `Filter.and` to combine multiple filters. These accept any `Filter` instances — as varargs or as a collection — such as filters created with `Filter.from`.

```groovy syntax
import io.deephaven.api.filter.Filter

// Multiple conditions with OR
orFilter = Filter.or(Filter.from("X > 5", "Y < 10"))

// Multiple conditions with AND
andFilter = Filter.and(Filter.from("X > 5", "Y < 10"))
```

## Filter functions

The [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) interface provides static factory methods for common conditions. These return `Filter` objects that you can combine with `Filter.and`/`Filter.or` or modify with concurrency methods.

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

These methods control how Deephaven evaluates the filter. By default, Deephaven can parallelize filter evaluation across multiple CPU cores when the input is large enough. Use these methods when your filter has side effects or requires coordination with other filters.

### `withSerial`

Forces the filter to never run concurrently with itself; its rows are evaluated sequentially, in row-set order. Use this when the filter has side effects or depends on row order. With default settings, a filter becomes eligible for parallel evaluation once more than about 131,072 rows reach it. That's the rows passed to this filter, not the source's size: later filters see only the rows that survive earlier ones, and on a refreshing table a typical update filters only that cycle's changed rows, though a filter can request a full refilter when its own inputs change. Use [`withSerial`](./ConcurrencyControl.md#withserial) to protect filters that cannot tolerate parallel evaluation.

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
> Most filters do not need serial execution. Use [`withSerial`](./ConcurrencyControl.md#withserial) only when the filter modifies external state, has side effects, or depends on row order.

### `withDeclaredBarriers` and `withRespectedBarriers`

These two methods work together to enforce execution order between filters. One filter **declares** the barrier (goes first), and another filter **respects** the barrier (waits).

A [barrier](./Barrier.md) is a synchronization object you create and share between filters. In Groovy, any Java object can serve as a barrier:

- `withDeclaredBarriers(barrier)` — This filter **goes first**. All rows are evaluated by this filter before any respecting filter's rows are evaluated.
- `withRespectedBarriers(barrier)` — This filter **waits**. Its rows are not evaluated until the filter that declares the barrier has finished.

For the full reference, constraints, and worked examples, see [Barrier](./Barrier.md) and [ConcurrencyControl](./ConcurrencyControl.md); for broader context on when barriers matter, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Filter objects

### Do you need a Filter object at all?

Most of the time, no. When you pass a string condition to [`where`](../../table-operations/filter/where.md), Deephaven creates a `Filter` internally and can parallelize its evaluation across multiple cores. This works well for any condition that:

- Only examines values in the current row (e.g., `"Price > 100"`).
- Has no side effects — it does not modify global variables, write to files, or depend on evaluation order.

If both of those are true, string conditions are usually all you need. The cases below are the exceptions.

### When you need explicit control

You need a `Filter` object in three situations:

**Stateful filters**: If your filter modifies shared state (e.g., counting how many rows pass), use [`withSerial`](./ConcurrencyControl.md#withserial) to force sequential evaluation. Without it, multiple threads evaluating rows simultaneously could corrupt the shared state.

**Complex boolean logic**: Use `Filter.and` and `Filter.or` to combine filters programmatically. This is useful when building filter conditions dynamically or combining multiple conditions that are easier to express as separate objects.

**Barriers between filters** are rarely needed — most filters are stateless. If you do have filters with shared state where one must complete before another, see [Barrier](./Barrier.md) for the full reference or the [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) section in the parallelization guide for broader context.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Selectable](./Selectable.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [`where`](../../table-operations/filter/where.md)
- [Filter Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)
