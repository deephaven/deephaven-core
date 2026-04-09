---
title: Filter
---

A [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) represents a filter condition used in [`where`](../../table-operations/filter/where.md) operations. Use `Filter` objects when you need to control how Deephaven evaluates filter conditions — specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

## Creating a Filter

There are two ways to create a `Filter` object: from a condition string or using filter combinators for boolean logic.

### From a condition string

Use `Filter.from` when you have filter conditions as strings. Note that `Filter.from` returns a collection, so use `[0]` to get a single filter.

```groovy syntax
import io.deephaven.api.filter.Filter

// Filter.from() returns an array; use [0] to get the single filter
myFilter = Filter.from("X > 5")[0]
```

### Using filter combinators

Use `FilterOr` and `FilterAnd` to combine multiple filters with boolean logic. These accept collections of filters created with `Filter.from`.

```groovy syntax
import io.deephaven.api.filter.Filter
import io.deephaven.api.filter.FilterOr
import io.deephaven.api.filter.FilterAnd

// Multiple conditions with OR
orFilter = FilterOr.of(Filter.from("X > 5", "Y < 10"))

// Multiple conditions with AND
andFilter = FilterAnd.of(Filter.from("X > 5", "Y < 10"))
```

## Methods

These methods control how Deephaven evaluates the filter. By default, Deephaven parallelizes filter evaluation across multiple CPU cores. Use these methods when your filter has side effects or requires coordination with other filters.

### `withSerial`

Forces the filter to evaluate sequentially on a single core, processing rows one at a time in order. Use this when the filter has side effects or depends on row order.

```groovy order=source,result
import io.deephaven.api.filter.Filter
import java.util.concurrent.atomic.AtomicInteger

rowsChecked = new AtomicInteger(0)

checkValue = { int x ->
    rowsChecked.incrementAndGet()  // Side effect: modifies external state
    return x > 5
}

source = emptyTable(100).update("X = i")

// Use .withSerial because the filter has side effects
// Filter.from() returns an array; [0] gets the single filter
myFilter = Filter.from("(boolean)checkValue(X)")[0].withSerial()
result = source.where(myFilter)
```

> [!NOTE]
> Most filters don't need serial execution. Use `withSerial` only when the filter modifies external state or has side effects.

### `withDeclaredBarriers` and `withRespectedBarriers`

These two methods work together to enforce execution order between filters. One filter **declares** the barrier (goes first), and another filter **respects** the barrier (waits).

A barrier is a synchronization object you create and share between filters. In Groovy, any Java object can serve as a barrier:

- `withDeclaredBarriers(barrier)` — This filter **goes first**. All rows are evaluated by this filter before any respecting filter's rows are evaluated.
- `withRespectedBarriers(barrier)` — This filter **waits**. Its rows are not evaluated until all declaring filters have finished.

For examples and detailed usage, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Filter objects

### Do you need a Filter object at all?

Most of the time, no. When you pass a string condition to [`where`](../../table-operations/filter/where.md), Deephaven creates a `Filter` internally and parallelizes its evaluation across multiple cores. This works well for any condition that:

- Only examines values in the current row (e.g., `"Price > 100"`).
- Has no side effects — it doesn't modify global variables, write to files, or depend on evaluation order.

If both of those are true, use string conditions directly. There is no benefit to constructing a `Filter` object.

### When you need explicit control

You need a `Filter` object in two situations:

**Stateful filters**: If your filter modifies shared state (e.g., counting how many rows pass), use `withSerial` to force sequential evaluation. Without it, multiple threads evaluating rows simultaneously could corrupt the shared state.

**Complex boolean logic**: Use `FilterAnd.of` and `FilterOr.of` to compose filters programmatically. This is useful when building filter conditions dynamically or combining multiple conditions that are easier to express as separate objects.

**Barriers between filters** are rarely needed — most filters are stateless. If you do have filters with shared state where one must complete before another, see the [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) section in the parallelization guide.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [`where`](../../table-operations/filter/where.md) - Uses Filter objects
- [Selectable](./Selectable.md) - Similar concurrency controls for column calculations
- [Filter Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) - base interface defining `withSerial` and barrier methods
