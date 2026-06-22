---
title: Selectable
---

A [`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) represents a column expression used in [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) operations. Use `Selectable` objects when you need to control how Deephaven processes column calculations — specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

## Creating a Selectable

There are two ways to create a `Selectable` object, depending on whether you want to parse a complete formula string or build one from separate components.

### From a formula string

Use `Selectable.parse` when you have a complete column assignment as a string. This is the most common approach.

```groovy syntax
import io.deephaven.api.Selectable

col = Selectable.parse("NewColumn = ExistingColumn * 2")
```

### From column name and expression

Use `Selectable.of` with [`ColumnName`](https://deephaven.io/core/javadoc/io/deephaven/api/ColumnName.html) and [`RawString`](https://deephaven.io/core/javadoc/io/deephaven/api/RawString.html) when the column name and expression are separate values, such as when they come from variables or user input.

```groovy syntax
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString

col = Selectable.of(ColumnName.of("NewColumn"), RawString.of("ExistingColumn * 2"))
```

## Methods

These methods control how Deephaven executes the column calculation. By default, Deephaven parallelizes calculations across multiple CPU cores. Use these methods when your formula requires sequential processing or coordination between columns.

### `withSerial`

Forces the column calculation to execute sequentially on a single core, processing rows one at a time in order. Use this when the formula has side effects or depends on row order.

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
> Concurrency control methods (`withSerial`, `withDeclaredBarriers`, `withRespectedBarriers`) only work with [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md). While [`view`](../../table-operations/select/view.md) and [`updateView`](../../table-operations/select/update-view.md) accept `Selectable` objects, they compute values on-demand and cannot enforce processing order. [`lazyUpdate`](../../table-operations/select/lazy-update.md) does not accept `Selectable` objects.

### `withDeclaredBarriers` and `withRespectedBarriers`

These two methods work together to enforce execution order between columns. One column **declares** the barrier (goes first), and another column **respects** the barrier (waits).

A barrier is a synchronization object you create and share between columns. In Groovy, any Java object can serve as a barrier:

- `withDeclaredBarriers(barrier)` — This column **goes first**. All of its rows are computed before any respecting column's rows are computed.
- `withRespectedBarriers(barrier)` — This column **waits**. Its rows are not computed until all declaring columns have finished.

For examples and detailed usage, see [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide.

## When to use Selectable

### Do you need a Selectable at all?

Most of the time, no. When you pass a string formula to [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md), Deephaven creates a `Selectable` internally and parallelizes the computation across multiple cores. This is the default behavior and it works well for any formula that:

- Only uses values from the current row (e.g., `"Total = Price * Quantity"`).
- Has no side effects — it doesn't modify global variables, write to files, or depend on processing order.

If both of those are true, use string formulas directly. There is no benefit to constructing a `Selectable` object.

### When you need explicit control

You need a `Selectable` object when the default parallel behavior would produce incorrect results. This happens when your formula is **stateful** — it reads or writes shared state that changes between rows.

**Use `withSerial`** when your formula must process rows one at a time, in order. Common cases include:

- A counter or accumulator that increments for each row.
- Logging or file writes that must happen sequentially.
- Any formula where the result for row N depends on what happened in row N-1.

**Use barriers** when you have multiple columns with shared state and one column must finish all its rows before another column starts. See the [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) section in the parallelization guide for details.

If you're unsure whether your formula is safe for parallel execution, ask: "Would this produce the same result if the rows were processed in a random order by multiple threads?" If the answer is no, you need a `Selectable`.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [Filter](./Filter.md) - Similar concurrency controls for filter operations
- [`select`](../../table-operations/select/select.md) - Uses Selectable objects
- [`update`](../../table-operations/select/update.md) - Uses Selectable objects
- [Barrier Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.Barrier.html)
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
- [Selectable Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html)
