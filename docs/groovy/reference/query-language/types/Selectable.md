---
title: Selectable
---

A [`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) represents a column expression used in [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) operations. Use `Selectable` objects when you need to control how Deephaven processes column calculations - specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

> [!NOTE]
> `Selectable` extends [`ConcurrencyControl`](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html), which defines the concurrency methods. You may see these methods documented in multiple places in the Javadoc - use the `Selectable` interface for column expressions.

## Creating a Selectable

### From a formula string

```groovy syntax
import io.deephaven.api.Selectable

col = Selectable.parse("NewColumn = ExistingColumn * 2")
```

### From column name and expression

```groovy syntax
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString

col = Selectable.of(ColumnName.of("NewColumn"), RawString.of("ExistingColumn * 2"))
```

## Methods

### `.withSerial`

Forces the column calculation to execute sequentially on a single core, processing rows one at a time in order. Use this when the formula modifies global state or depends on row order.

```groovy order=result
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

col = Selectable.of(ColumnName.of("ID"), RawString.of("counter.getAndIncrement()")).withSerial()
result = emptyTable(10).update([col])
```

> [!IMPORTANT]
> `.withSerial` cannot be used with [`view`](../../table-operations/select/view.md) or [`updateView`](../../table-operations/select/update-view.md). These operations compute values on-demand and cannot guarantee processing order.

### `.withDeclaredBarriers`

Declares that this column creates a barrier - other columns that respect this barrier will wait until this column finishes processing all rows.

```groovy syntax
import io.deephaven.api.Selectable

// Any Java object can serve as a barrier
barrier = new Object()
colA = Selectable.parse("A = computeFirst()").withDeclaredBarriers(barrier)
```

### `.withRespectedBarriers`

Declares that this column respects a barrier - it will not begin processing until all columns that declare the same barrier have finished.

```groovy syntax
import io.deephaven.api.Selectable

// Use the same barrier object
colB = Selectable.parse("B = computeSecond()").withRespectedBarriers(barrier)
```

### Barriers example

Use barriers when one column must complete before another starts:

```groovy order=result
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Any Java object can serve as a barrier
barrier = new Object()

// Column A declares the barrier (must finish first)
colA = Selectable.of(ColumnName.of("A"), RawString.of("counter.getAndIncrement()")).withDeclaredBarriers(barrier)

// Column B respects the barrier (waits for A to finish)
colB = Selectable.of(ColumnName.of("B"), RawString.of("counter.getAndIncrement()")).withRespectedBarriers(barrier)

result = emptyTable(10).update([colA, colB])
```

All rows of column `A` are processed before any rows of column `B` begin.

## When to use Selectable

| Scenario                             | Approach                                              |
| ------------------------------------ | ----------------------------------------------------- |
| Simple column math                   | Use string formulas directly - no `Selectable` needed |
| Global counter or state              | `Selectable.parse(...).withSerial()`                  |
| Column A must finish before Column B | Use barriers                                          |
| File I/O or logging                  | `Selectable.parse(...).withSerial()`                  |

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [`update`](../../table-operations/select/update.md) - Uses Selectable objects
- [`select`](../../table-operations/select/select.md) - Uses Selectable objects
- [Filter](./Filter.md) - Similar concurrency controls for filter operations
- [Selectable Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html)
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) - base interface defining `withSerial` and barrier methods
