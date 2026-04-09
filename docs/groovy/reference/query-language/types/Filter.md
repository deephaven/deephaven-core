---
title: Filter
---

A [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) represents a filter condition used in [`where`](../../table-operations/filter/where.md) operations. Use `Filter` objects when you need to control how Deephaven evaluates filter conditions - specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

## Creating a Filter

There are two ways to create a `Filter` object: from a condition string or using filter combinators for boolean logic.

### From a condition string

Use `Filter.from()` when you have filter conditions as strings. Note that `Filter.from()` returns an array, so use `[0]` to get a single filter.

```groovy syntax
import io.deephaven.api.filter.Filter

// Filter.from() returns an array; use [0] to get the single filter
myFilter = Filter.from("X > 5")[0]
```

### Using filter combinators

Use `FilterOr` and `FilterAnd` to combine multiple filters with boolean logic. These accept arrays of filters created with `Filter.from()`.

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

- `withDeclaredBarriers(barrier)` — This filter **goes first**. All rows are evaluated before any respecting filter begins.
- `withRespectedBarriers(barrier)` — This filter **waits**. It does not start until all declaring filters finish.

For examples and detailed usage, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Filter objects

Most queries don't need `Filter` objects — string conditions work fine for simple filters. Use `Filter` when you need explicit control over evaluation or complex boolean logic.

| Scenario                             | Approach                                                |
| ------------------------------------ | ------------------------------------------------------- |
| Simple conditions                    | Use string filters directly - no `Filter` object needed |
| Filter with side effects             | `Filter.from(...)[0].withSerial()`                      |
| Filter A must finish before Filter B | Use barriers                                            |
| Complex boolean logic                | Use `FilterOr.of()`, `FilterAnd.of()`                   |

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [`where`](../../table-operations/filter/where.md) - Uses Filter objects
- [Selectable](./Selectable.md) - Similar concurrency controls for column calculations
- [Filter Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) - base interface defining `withSerial` and barrier methods
