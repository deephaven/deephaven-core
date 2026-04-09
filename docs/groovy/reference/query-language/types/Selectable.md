---
title: Selectable
---

A [`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) represents a column expression used in [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) operations. Use `Selectable` objects when you need to control how Deephaven processes column calculations - specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

## Creating a Selectable

There are two ways to create a `Selectable` object, depending on whether you want to parse a complete formula string or build one from separate components.

### From a formula string

Use `Selectable.parse()` when you have a complete column assignment as a string. This is the most common approach.

```groovy syntax
import io.deephaven.api.Selectable

col = Selectable.parse("NewColumn = ExistingColumn * 2")
```

### From column name and expression

Use `Selectable.of()` with `ColumnName` and `RawString` when the column name and expression are separate values, such as when they come from variables or user input.

```groovy syntax
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString

col = Selectable.of(ColumnName.of("NewColumn"), RawString.of("ExistingColumn * 2"))
```

## Methods

These methods control how Deephaven executes the column calculation. By default, Deephaven parallelizes calculations across multiple CPU cores. Use these methods when your formula requires sequential processing or coordination between columns.

### `withSerial`

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
> `Selectable` objects can only be used with [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md). The [`view`](../../table-operations/select/view.md), [`updateView`](../../table-operations/select/update-view.md), and [`lazyUpdate`](../../table-operations/select/lazy-update.md) methods do not accept `Selectable` objects because they compute values on-demand and cannot guarantee processing order.

### `withDeclaredBarriers` and `withRespectedBarriers`

These two methods work together to enforce execution order between columns. One column **declares** the barrier (goes first), and another column **respects** the barrier (waits).

A barrier is a synchronization object you create and share between columns. In Groovy, any Java object can serve as a barrier:

- `withDeclaredBarriers(barrier)` — This column **goes first**. All rows are computed before any respecting column begins.
- `withRespectedBarriers(barrier)` — This column **waits**. It does not start until all declaring columns finish.

For examples and detailed usage, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Selectable

Most queries don't need `Selectable` objects — string formulas work fine for stateless calculations. Use `Selectable` when you need explicit control over execution.

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
