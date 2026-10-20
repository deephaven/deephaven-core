---
title: Selectable
---

A [`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) is a column definition: a formula such as `NewColumn = ExistingColumn * 2`, optionally with concurrency controls attached. [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) accept it in place of a [formula string](../../../how-to-guides/formulas.md), and so do [`view`](../../table-operations/select/view.md), [`updateView`](../../table-operations/select/update-view.md), and [`lazyUpdate`](../../table-operations/select/lazy-update.md), with the [restrictions](#pass-a-selectable-to-a-table-operation) below. The controls force [serial execution](../../../conceptual/query-engine/parallelization.md#serial-execution) or order execution between columns with [Barriers](./Barrier.md).

## Create a Selectable

Two methods create a `Selectable` from a single column definition. [`Selectable.parse`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) takes a complete column assignment as one string. [`Selectable.of`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) takes the column name and the expression as separate values. [`Selectable.from`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) creates several at once from multiple strings.

### Create a Selectable from a formula string

[`Selectable.parse`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) applies when the column assignment is a single [formula string](../../../how-to-guides/formulas.md). This is the most common approach.

```groovy order=source,result
import io.deephaven.api.Selectable

source = emptyTable(5).update("ExistingColumn = i")

col = Selectable.parse("NewColumn = ExistingColumn * 2")
result = source.update([col])
```

### Create a Selectable from a column name and expression

[`Selectable.of`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) takes a [`ColumnName`](https://deephaven.io/core/javadoc/io/deephaven/api/ColumnName.html) and a [`RawString`](https://deephaven.io/core/javadoc/io/deephaven/api/RawString.html). It applies when the column name and expression are separate values built programmatically, such as from application variables.

```groovy order=source,result
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString

source = emptyTable(5).update("ExistingColumn = i")

col = Selectable.of(ColumnName.of("NewColumn"), RawString.of("ExistingColumn * 2"))
result = source.update([col])
```

> [!WARNING]
> Neither [`Selectable.of`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) nor [`Selectable.parse`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) validates the expression, which Deephaven compiles as query code. [`RawString`](https://deephaven.io/core/javadoc/io/deephaven/api/RawString.html) stores the expression unparsed, and [`Selectable.parse`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) wraps everything after the `=` in a [`RawString`](https://deephaven.io/core/javadoc/io/deephaven/api/RawString.html) in the same way. Untrusted user input can inject arbitrary query code. The expression must come from a trusted source, and external input must be validated or allowlisted first.

## Methods

A `Selectable` has the three methods defined in [`ConcurrencyControl`](./ConcurrencyControl.md): [`withSerial`](./ConcurrencyControl.md#withserial), [`withDeclaredBarriers`](./ConcurrencyControl.md#withdeclaredbarriers), and [`withRespectedBarriers`](./ConcurrencyControl.md#withrespectedbarriers). This section describes how they apply to column calculations.

### `withSerial`

[`withSerial`](./ConcurrencyControl.md#withserial) returns a `Selectable` that is serial, as [`ConcurrencyControl`](./ConcurrencyControl.md) defines the term: the column calculation never runs concurrently with itself, and it evaluates its rows one at a time in row set order.

Parallel evaluation follows these defaults:

- Deephaven considers a column calculation for parallel evaluation once it processes 4,194,304 rows or more, and when more than one worker thread is available. [`QueryTable.minimumParallelSelectRows`](../../../conceptual/query-table-configuration.md#parallel-processing-with-select) sets the row count. The count is the whole table on initialization, or the added and modified rows of each cycle on a refreshing table.

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

A [Barrier](./Barrier.md) orders columns relative to each other and does not make a column serial.

See [Barrier](./Barrier.md) and [`ConcurrencyControl`](./ConcurrencyControl.md) for the full constraints and worked examples, and [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

## Pass a Selectable to a table operation

All five operations accept `Selectable` objects, but they differ in how they treat the concurrency methods:

- [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md) honor all three methods.
- [`view`](../../table-operations/select/view.md) and [`updateView`](../../table-operations/select/update-view.md) compute values on demand and cannot guarantee processing order.
  - [`withRespectedBarriers`](./ConcurrencyControl.md#withrespectedbarriers) throws an error ("view and updateView cannot respect barriers").
  - [`withSerial`](./ConcurrencyControl.md#withserial) throws an error under the default configuration ("A stateful column cannot safely be used in a view or updateView."). The error applies when [`QueryTable.statelessSelectByDefault`](../../../conceptual/query-table-configuration.md#stateless-by-default) is `true`, which is the default.
  - [`withDeclaredBarriers`](./ConcurrencyControl.md#withdeclaredbarriers) is accepted and orders nothing, because no column in the same call can respect the [Barrier](./Barrier.md).
- [`lazyUpdate`](../../table-operations/select/lazy-update.md) accepts all three methods, and they have no effect on evaluation order.

The usual [Barrier](./Barrier.md) validation still applies to [`view`](../../table-operations/select/view.md), [`updateView`](../../table-operations/select/update-view.md), and [`lazyUpdate`](../../table-operations/select/lazy-update.md):

- Each [Barrier](./Barrier.md) is declared once.
- A constant-valued column cannot declare or respect a [Barrier](./Barrier.md).
- In [`lazyUpdate`](../../table-operations/select/lazy-update.md), a respected [Barrier](./Barrier.md) must be declared earlier in the same call. [`view`](../../table-operations/select/view.md) and [`updateView`](../../table-operations/select/update-view.md) reject any respected [Barrier](./Barrier.md) first.

## Decide when to use Selectable

A `Selectable` adds concurrency controls to a column formula.

### Decide whether a Selectable is needed

A [formula string](../../../how-to-guides/formulas.md) passed to [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) becomes a `Selectable` internally, and Deephaven [parallelizes](../../../conceptual/query-engine/parallelization.md) it by default. A string is enough for a formula that meets both of these conditions:

- It uses only values from the current row, such as `"Total = Price * Quantity"`.
- It has no side effects. It does not modify global variables, write to files, or depend on processing order.

Such a formula gains nothing from an explicit `Selectable` object.

### Add explicit control to a Selectable

A formula that reads or writes shared state that changes between rows is safe to run in parallel only if it produces the same result when multiple threads process the rows in any order. Parallel execution can produce incorrect results otherwise, so such a formula needs a `Selectable` with one of these controls:

- [`withSerial`](./ConcurrencyControl.md#withserial): the formula evaluates its rows in row set order, never in parallel. Typical cases are:
  - A counter or accumulator that increments for each row.
  - Logging or file writes that must happen sequentially.
  - A formula whose result for row N depends on what happened in row N-1.
- [Barriers](./Barrier.md): one column finishes all of its rows before another column starts. A [Barrier](./Barrier.md) does not make a column serial, so a column that writes state that is not thread-safe needs [`withSerial`](./ConcurrencyControl.md#withserial) in addition to the [Barrier](./Barrier.md). A column that only reads state that a Barrier has already finalized does not. See [Barriers](../../../conceptual/query-engine/parallelization.md#barriers) in the parallelization guide for broader context.

## Related documentation

- [Filter](./Filter.md)
- [Barrier](./Barrier.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Formulas](../../../how-to-guides/formulas.md)
- [`select`](../../table-operations/select/select.md)
- [`update`](../../table-operations/select/update.md)
- [`view`](../../table-operations/select/view.md)
- [`updateView`](../../table-operations/select/update-view.md)
- [`lazyUpdate`](../../table-operations/select/lazy-update.md)
- [Selectable Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html)
