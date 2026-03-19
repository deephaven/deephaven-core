---
title: Filter
---

A [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) represents a filter condition used in [`where`](../../table-operations/filter/where.md) operations. Use `Filter` objects when you need to control how Deephaven evaluates filter conditions - specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

> [!NOTE]
> `Filter` extends [`ConcurrencyControl`](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html), which defines the concurrency methods. You may see these methods documented in multiple places in the Javadoc (including `FilterSerial`, `WhereFilter`) - use the `Filter` interface for filter operations.

## Creating a Filter

### From a condition string

```groovy syntax
import io.deephaven.api.filter.Filter

// Filter.from() returns an array; use [0] to get the single filter
f = Filter.from("X > 5")[0]
```

### Using filter combinators

```groovy syntax
import io.deephaven.api.filter.Filter
import io.deephaven.api.filter.FilterOr
import io.deephaven.api.filter.FilterAnd

// Multiple conditions with OR
fOr = FilterOr.of(Filter.from("X > 5", "Y < 10"))

// Multiple conditions with AND
fAnd = FilterAnd.of(Filter.from("X > 5", "Y < 10"))
```

## Methods

### `.withSerial`

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
f = Filter.from("(boolean)checkValue(X)")[0].withSerial()
result = source.where(f)
```

> [!NOTE]
> Most filters don't need serial execution. Use `.withSerial` only when the filter modifies external state or has side effects.

### `.withDeclaredBarriers`

Declares that this filter creates a barrier - other filters that respect this barrier will wait until this filter finishes evaluating all rows.

```groovy syntax
import io.deephaven.api.filter.Filter

// Any Java object can serve as a barrier
barrier = new Object()
filter1 = Filter.from("X > 5")[0].withDeclaredBarriers(barrier)
```

### `.withRespectedBarriers`

Declares that this filter respects a barrier - it will not begin evaluating until all filters that declare the same barrier have finished.

```groovy syntax
import io.deephaven.api.filter.Filter

// Use the same barrier object
filter2 = Filter.from("Y < 10")[0].withRespectedBarriers(barrier)
```

### Barriers example

Use barriers when one filter must complete before another starts:

```groovy order=result
import io.deephaven.api.filter.Filter

// Any Java object can serve as a barrier
barrier = new Object()

// filter1 declares the barrier (must finish first)
filter1 = Filter.from("X > 50")[0].withDeclaredBarriers(barrier)

// filter2 respects the barrier (waits for filter1 to finish)
filter2 = Filter.from("Y < 50")[0].withRespectedBarriers(barrier)

result = emptyTable(100)
    .update("X = i", "Y = 100 - i")
    .where(Filter.and(filter1, filter2))
```

## When to use Filter objects

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
