---
title: Generate tables with Groovy functions
---

This guide shows you how to create and use function-generated tables. A function-generated table is a table whose contents come from a user-defined Groovy function. With a source table or a refresh interval, the result is a ticking table, one whose rows update over time. The [`create`](../reference/table-operations/create/create.md) method of [`FunctionGeneratedTableFactory`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableFactory.html) creates a function-generated table from a Groovy function.

Use a function-generated table to bring data from external sources into a ticking table. A refresh that produces a new table replaces the whole result, so when the input is already a Deephaven table, use regular table operations, many of which update incrementally.

## Usage

The basic syntax for [`create`](../reference/table-operations/create/create.md) is as follows:

```groovy syntax
create(tableGenerator, sourceTables...)
create(tableGenerator, refreshIntervalMs)
create(spec)
```

To use this method, define a function that returns a table, then pass it to [`create`](../reference/table-operations/create/create.md) with a trigger. The third form accepts a [`FunctionGeneratedTableSpec`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.html). A spec offers additional options, such as a supplier that can keep the previous result. See [Control the result with `FunctionGeneratedTableSpec`](#control-the-result-with-functiongeneratedtablespec).

The function runs once when the table is created. Pass either source tables or a refresh interval as the trigger, not both:

- With source tables, the function re-runs whenever any of them ticks. Every source table must be a refreshing (ticking) table.
- With a refresh interval, the function re-runs once per interval, given in milliseconds.

A refresh interval of zero or less produces a static result, as does passing no source tables. The function then runs once and never re-runs.

The user-defined `tableGenerator` function can source its data from anywhere. It must return a table with the same column names and types on every invocation. The result takes its columns from the first generated table, or from the spec's [`tableDefinition`](#specify-the-table-definition) when you build the table from a [`FunctionGeneratedTableSpec`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.html).

The function can read data from its source tables, but don't run further table operations on them inside the function:

- Deephaven can return a cached (memoized) result for such an operation.
- The function-generated table doesn't track that cached table as a dependency.
- The function can therefore run before the cached table finishes updating in the same [update cycle](../conceptual/table-update-model.md), and read inconsistent data.

List every ticking table the function depends on as a source table.

### Execution context

[`create`](../reference/table-operations/create/create.md) does not accept an [execution context](../conceptual/execution-context.md). When you call it from a console script, the `tableGenerator` function runs under a fallback execution context. That context keeps the console's update graph and operation initializer, but its query scope, query library, and query compiler are unusable. A `tableGenerator` that needs any of those three must open a context itself. For example, the [`update`](../reference/table-operations/select/update.md) formulas in the examples below need the query compiler. The examples below capture the console's context with `defaultCtx = ExecutionContext.getContext()` and open it inside the function with `try (SafeCloseable ignored = defaultCtx.open())`.

### Generate tables with a Groovy function

The following example defines a `tableGenerator` that opens the captured execution context and returns a five-row table of random values. It then creates two function-generated tables. One re-runs `tableGenerator` every 2000 ms, and the other re-runs it whenever the source table ticks.

```groovy ticking-table order=null reset
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory

// Capture the console's execution context
defaultCtx = ExecutionContext.getContext()

// Define the tableGenerator function
tableGenerator = { ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return emptyTable(5).update("X = randomInt(0, 10)", "Y = randomDouble(-50.0, 50.0)")
    }
}

// Create a time table
timeTable1 = timeTable("PT1S")

// Create a function-generated table that re-runs tableGenerator every 2000 ms
resultTime = FunctionGeneratedTableFactory.create(tableGenerator, 2000)

// Create a function-generated table that re-runs tableGenerator whenever timeTable1 ticks
resultTick = FunctionGeneratedTableFactory.create(tableGenerator, timeTable1)
```

## How the result updates

Every refresh in which the function produces a table replaces the result in full. Each [table update](../conceptual/table-update-model.md):

- Removes all of the previous rows.
- Adds all of the newly generated rows.
- Contains no modified rows and no shifts, even when the generated data is identical to the previous cycle's.

Downstream operations therefore reprocess the entire result on every refresh that produces a table, which is why this guide recommends regular table operations when the input is already a Deephaven table.

A [`FunctionGeneratedTableSpec`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.html) can use a [`retainingLastTableSupplier`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.Builder.html), which may decline to produce a table. A refresh in which it declines fires no update, unless the result is a [blink table](../conceptual/table-types.md#specialization-3-blink). See [Choose a table supplier](#choose-a-table-supplier).

## Control the result with `FunctionGeneratedTableSpec`

The [`create`](../reference/table-operations/create/create.md) overloads above cover the common cases. For finer control, build a [`FunctionGeneratedTableSpec`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.html) and pass it to `create`. The spec exposes every option in one place — how the table is generated, what triggers a refresh, and how the result is shaped.

Two independent options refine [how the result updates](#how-the-result-updates):

- [`copyData`](#copy-data-or-delegate-to-the-generated-table) controls where the result's data lives and what its row keys look like.
- [`blinkTable`](#present-the-result-as-a-blink-table) controls how downstream operations interpret each update.

```groovy syntax
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableSpec
import java.time.Duration

spec = FunctionGeneratedTableSpec.builder()
    .tableSupplier(tableGenerator)   // the function that produces the table
    .addDependencies(timeTable1)     // refresh when timeTable1 ticks...
    // .refreshInterval(Duration.ofSeconds(2))  // ...or on a wall-clock interval instead
    // .tableDefinition(definition)   // optional: fix the result's columns up front
    .copyData(true)                  // copy the generated data (the default)
    .blinkTable(false)               // true presents the result as a blink table (the default is false)
    .build()

result = FunctionGeneratedTableFactory.create(spec)
```

### Choose a table supplier

Give the [`FunctionGeneratedTableSpec`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.html) builder exactly one of two suppliers:

- [`tableSupplier`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.Builder.html) — a [`Supplier<Table>`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/function/Supplier.html) that produces a new table on every invocation.
- [`retainingLastTableSupplier`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.Builder.html) — a `Supplier<Optional<Table>>` that may return an empty [`Optional`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Optional.html) to decline producing a new table. When it declines, the result keeps the previous table for the next cycle. A [blink table](../conceptual/table-types.md#specialization-3-blink) result is cleared instead.

The following example uses a `retainingLastTableSupplier` that produces a table only once the trigger table has rows. Because the supplier produces no table at construction time, the spec also supplies a [table definition](#specify-the-table-definition).

```groovy ticking-table order=null
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory
import io.deephaven.engine.table.impl.util.FunctionGeneratedTableSpec
import io.deephaven.util.SafeCloseable

defaultCtx = ExecutionContext.getContext()
tt = timeTable("PT1S")

// Only produce a table once the trigger has rows; otherwise retain the previous result.
countSupplier = { ->
    if (tt.size() == 0) {
        return Optional.empty()
    }
    try (SafeCloseable ignored = defaultCtx.open()) {
        return Optional.of(newTable(intCol("Count", (int) tt.size())))
    }
}

spec = FunctionGeneratedTableSpec.builder()
    .retainingLastTableSupplier(countSupplier)
    .addDependencies(tt)
    .tableDefinition(TableDefinition.of(ColumnDefinition.ofInt("Count")))
    .build()

result = FunctionGeneratedTableFactory.create(spec)
```

### Choose a refresh trigger

Give the [`FunctionGeneratedTableSpec`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.html) builder at most one refresh trigger:

- [`addDependencies`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.Builder.html) or [`addAllDependencies`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.Builder.html) — re-run the supplier whenever any of the listed tables ticks. Every listed table must be refreshing.
- [`refreshInterval`](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableSpec.Builder.html) — re-run the supplier on a wall-clock [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html). The interval must be at least one millisecond and at most `Integer.MAX_VALUE` milliseconds.

If you provide neither, the supplier runs exactly once at construction and the result is static.

### Specify the table definition

When you supply a `tableDefinition`, it is authoritative. It defines the result's columns and their order. Every table the supplier produces must be [mutually compatible](/core/javadoc/io/deephaven/engine/table/TableDefinition.html#checkMutualCompatibility(io.deephaven.engine.table.TableDefinition)) with it. A definition is required when a `retainingLastTableSupplier` produces no table at construction time, because the columns must be known before the first table exists.

### Copy data or delegate to the generated table

By default (`copyData(true)`), the generated rows are copied into the result's own [`ColumnSource`s](../conceptual/table-update-model.md#describing-table-updates), and the result uses a flat, contiguous [`RowSet`](../conceptual/table-update-model.md#describing-table-updates) with row keys `0` through `size - 1`. The generated table itself is not retained.

With `copyData(false)`, the result skips the copy and delegates directly to the generated table. It:

- Uses the generated table's [`ColumnSource`s](../conceptual/table-update-model.md#describing-table-updates).
- Adopts the generated table's [`RowSet`](../conceptual/table-update-model.md#describing-table-updates) as-is.
- Reports the generated table's `RowSet` as each update's added rows and the previous cycle's `RowSet` as its removed rows.

Because the result holds the generated [`ColumnSource`s](../conceptual/table-update-model.md#describing-table-updates) across cycles, a refreshing generated table must expose immutable `ColumnSource`s. A ticking table keeps each column's previous-cycle values so downstream operations can see what changed. A generated table that changes values in place would corrupt those values, so [`create`](../reference/table-operations/create/create.md) rejects a refreshing generated table if any of its `ColumnSource`s is not immutable. A static table produced fresh on each refresh, such as one produced by [`snapshot`](../reference/table-operations/snapshot/snapshot.md), always meets this requirement.

### Present the result as a blink table

Set `blinkTable(true)` to present the result as a [blink table](../conceptual/table-types.md#specialization-3-blink), so downstream operations see only the rows generated during the current cycle. Each update is still a full replacement, as described in [How the result updates](#how-the-result-updates). The blink setting changes only how downstream operations interpret that update, not how the rows are copied or delegated.

A blink result behaves as follows:

- It requires a refresh trigger.
- Rows generated in one update cycle are removed on the next cycle, whether or not the supplier runs again.
- With a refresh interval longer than one cycle, the result is empty between refreshes.

## Related documentation

- [`create`](../reference/table-operations/create/create.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [Table types](../conceptual/table-types.md)
- [Execution Context](../conceptual/execution-context.md)
- [`FunctionGeneratedTableFactory` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/FunctionGeneratedTableFactory.html)
