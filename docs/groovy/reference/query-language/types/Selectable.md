---
title: Selectable
---

A [`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) is a column definition: a formula such as `NewColumn = ExistingColumn * 2`, optionally with concurrency controls attached. [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) accept it in place of a [formula string](../../../how-to-guides/formulas.md). The controls force [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution) or order execution between columns with [Barriers](./Barrier.md).

## Creating a Selectable

Two methods create a `Selectable`. [`Selectable.parse`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) takes a complete column assignment as one string. [`Selectable.of`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) takes the column name and the expression as separate values.

### From a formula string

Use [`Selectable.parse`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) when the column assignment is a single [formula string](../../../how-to-guides/formulas.md). This is the most common approach.

```groovy syntax
import io.deephaven.api.Selectable

col = Selectable.parse("NewColumn = ExistingColumn * 2")
```

### From column name and expression

Use [`Selectable.of`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) with [`ColumnName`](https://deephaven.io/core/javadoc/io/deephaven/api/ColumnName.html) and [`RawString`](https://deephaven.io/core/javadoc/io/deephaven/api/RawString.html) when the column name and expression are separate values built programmatically, such as from application variables.

```groovy syntax
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString

col = Selectable.of(ColumnName.of("NewColumn"), RawString.of("ExistingColumn * 2"))
```

> [!WARNING]
> Both [`Selectable.of`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) and [`Selectable.parse`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) compile the expression as query code without validating it. [`RawString`](https://deephaven.io/core/javadoc/io/deephaven/api/RawString.html) stores the expression unparsed, and [`Selectable.parse`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) wraps everything after the `=` in a [`RawString`](https://deephaven.io/core/javadoc/io/deephaven/api/RawString.html) in the same way. Untrusted user input can inject arbitrary query code, so build the expression only from a trusted source, and validate or allowlist external input first.

## Methods

A `Selectable` has the three methods defined in [`ConcurrencyControl`](./ConcurrencyControl.md): [`withSerial`](./ConcurrencyControl.md#withserial), [`withDeclaredBarriers`](./ConcurrencyControl.md#withdeclaredbarriers), and [`withRespectedBarriers`](./ConcurrencyControl.md#withrespectedbarriers). This section describes how they apply to column calculations.

### `withSerial`

[`withSerial`](./ConcurrencyControl.md#withserial) returns a `Selectable` that is serial, as [`ConcurrencyControl`](./ConcurrencyControl.md) defines the term: the column calculation never runs concurrently with itself, and it evaluates its rows one at a time in row set order.

With default settings, Deephaven considers a column calculation for parallel evaluation once it processes about 4.2 million rows or more. That count is the whole table on initialization, or the added and modified rows of each cycle on a refreshing table.

The following example evaluates 10 rows, far below the parallelization threshold. It shows the ordering guarantee rather than a speedup.

```groovy order=result
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

col = Selectable.parse("ID = counter.getAndIncrement()").withSerial()
result = emptyTable(10).update([col])
```

### `withDeclaredBarriers` and `withRespectedBarriers`

These two methods work together to order columns. One column declares a [Barrier](./Barrier.md), and another column respects it.

A [Barrier](./Barrier.md) is a synchronization object shared between columns. In Groovy, any Java object can serve as a [Barrier](./Barrier.md). The two roles are:

- [`withDeclaredBarriers`](./ConcurrencyControl.md#withdeclaredbarriers): this column goes first. It computes all of its rows before any respecting column computes its rows.
- [`withRespectedBarriers`](./ConcurrencyControl.md#withrespectedbarriers): this column waits. It computes no rows until the declaring column has finished.

See [Barrier](./Barrier.md) and [`ConcurrencyControl`](./ConcurrencyControl.md) for the full constraints and worked examples, and [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

### Table operations that accept a Selectable

> [!IMPORTANT]
> All five operations accept `Selectable` objects, but they differ in how they treat the concurrency methods:
>
> - [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) honor all three methods.
> - [`view`](../../table-operations/select/view.md) and [`updateView`](../../table-operations/select/update-view.md) compute values on demand and cannot guarantee processing order. [`withRespectedBarriers`](./ConcurrencyControl.md#withrespectedbarriers) throws an error ("view and updateView cannot respect Barriers"). [`withSerial`](./ConcurrencyControl.md#withserial) throws an error under the default configuration ("A stateful column cannot safely be used in a view or updateView"). [`withDeclaredBarriers`](./ConcurrencyControl.md#withdeclaredbarriers) is accepted and has no effect on evaluation order.
> - [`lazyUpdate`](../../table-operations/select/lazy-update.md) accepts all three methods and they have no effect on evaluation order.
>
> The usual [Barrier](./Barrier.md) validation still applies to [`view`](../../table-operations/select/view.md), [`updateView`](../../table-operations/select/update-view.md), and [`lazyUpdate`](../../table-operations/select/lazy-update.md). A respected [Barrier](./Barrier.md) must be declared earlier in the same call, each [Barrier](./Barrier.md) is declared once, and a constant-valued column cannot declare or respect one.

## When to use Selectable

A `Selectable` adds concurrency controls to a column formula.

### Do you need a Selectable at all?

A [formula string](../../../how-to-guides/formulas.md) passed to [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) becomes a `Selectable` internally, and Deephaven parallelizes it by default. A string suffices for a formula that meets both of these conditions:

- It uses only values from the current row, such as `"Total = Price * Quantity"`.
- It has no side effects. It does not modify global variables, write to files, or depend on processing order.

Such a formula gains nothing from an explicit `Selectable` object.

### When you need explicit control

A formula is stateful when it reads or writes shared state that changes between rows. It is safe to run in parallel only if it produces the same result when multiple threads process the rows in any order. Parallel execution can produce incorrect results for a stateful formula, which then needs a `Selectable` with one of these controls:

- [`withSerial`](./ConcurrencyControl.md#withserial): the formula evaluates its rows in row set order, never in parallel. Typical cases are:
  - A counter or accumulator that increments for each row.
  - Logging or file writes that must happen sequentially.
  - A formula whose result for row N depends on what happened in row N-1.
- [Barriers](./Barrier.md): one column finishes all of its rows before another column starts. A [Barrier](./Barrier.md) does not make a column serial, so columns that share state use a [Barrier](./Barrier.md) together with [`withSerial`](./ConcurrencyControl.md#withserial) on each column. See [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

## Related documentation

- [Filter](./Filter.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Formulas](../../../how-to-guides/formulas.md)
- [`select`](../../table-operations/select/select.md)
- [`update`](../../table-operations/select/update.md)
- [Selectable Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html)
