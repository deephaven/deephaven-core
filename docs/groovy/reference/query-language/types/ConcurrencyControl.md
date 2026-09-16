---
title: ConcurrencyControl
---

[`ConcurrencyControl`](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) is the shared interface that provides concurrency control for column calculations and filters. [`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) (used by [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md)) and [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) (used by [`where`](../../table-operations/filter/where.md)) both implement it, so the same three methods are available on either one — though, as the `withSerial` vs. barriers comparison below shows, not every method's behavior is identical between the two.

By default, Deephaven is free to parallelize column calculations and filter evaluation across multiple CPU cores when they are eligible for it — eligibility depends on statelessness, table size, and available threads. Use the methods below when your formula or filter has side effects, or depends on row order, that make parallel execution unsafe.

## Methods

### `withSerial`

Forces the expression to never run concurrently with itself; its rows are evaluated sequentially, in row-set order. Use this when the formula or filter has side effects or depends on row order.

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Force serial execution - never concurrent, rows processed in row-set order
col = Selectable.parse("ID = counter.getAndIncrement()").withSerial()
result = emptyTable(10).update([col])
```

When an expression is serial, every row is evaluated in order (row 0, then row 1, then row 2, etc.), only one thread processes the expression at a time, and shared state updates happen sequentially without race conditions.

> [!NOTE]
> Not running concurrently is not the same guarantee `withSerial` provides — the engine may still evaluate a non-serial expression out of row-set order. Use `withSerial` any time your formula or filter depends on shared state or row order, not just when you expect concurrent execution.

### `withDeclaredBarriers`

Marks the expression as declaring the given [barrier](./Barrier.md) object(s). The declaring expression runs to completion — all of its rows — before any expression that respects the same barrier begins.

```groovy syntax
import io.deephaven.api.Selectable

barrier = new Object()
col = Selectable.parse("A = someFunction()").withDeclaredBarriers(barrier)
```

Each barrier can only be declared by one expression, and only within the same `select`, `update`, or `where` call as the expression that respects it — a constant-valued expression cannot declare one either. See [Barrier](./Barrier.md) for the full constraints and a complete worked example.

### `withRespectedBarriers`

Marks the expression as respecting the given [barrier](./Barrier.md) object(s). The respecting expression does not start until every expression that declares that barrier has finished.

```groovy syntax
import io.deephaven.api.Selectable

barrier = new Object()
col = Selectable.parse("B = someFunction()").withRespectedBarriers(barrier)
```

Multiple expressions can respect the same barrier, and one expression can respect more than one barrier. The respecting expression must come after the declaring expression, in left-to-right order within the same call — the engine raises an error otherwise. See [Barrier](./Barrier.md) for the full constraints and a complete worked example, including how to coordinate more than two expressions.

## `withSerial` vs. barriers

These solve different problems:

- **`withSerial`**: Rows _within one_ expression are processed sequentially (row 0, then row 1, etc.). For a **filter**, a serial filter also acts as an absolute ordering barrier against every other filter in the same `where` call — no filter can execute out of order around it. For a **selectable**, `withSerial` gives no such guarantee relative to other _independent_ expressions by default; two expressions that do not reference each other's output can still run at the same time unless you add an explicit barrier. (An expression that references another's result column is a different case — the engine already evaluates the referenced column first as an ordinary data dependency, barrier or not.)
- **Barriers**: _Between_ expressions, one finishes all its rows before another starts. Rows within each expression can still be parallelized.

When shared state is involved, you often need both: `withSerial` to protect row-level access to the shared state, and — especially for selectables — a barrier to ensure one expression is completely done before another starts.

## Related documentation

- [Barrier](./Barrier.md) — The marker object used with `withDeclaredBarriers` and `withRespectedBarriers`
- [Query table configuration](../../../conceptual/query-table-configuration.md) — Configuration properties that control default parallelization behavior
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
- [Selectable Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html)
- [Filter Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html)
