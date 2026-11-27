---
title: DynamicTableWriter
---

[`DynamicTableWriter`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html) creates a writer and a new real-time, in-memory table that the writer appends rows to.

> [!NOTE]
> For most cases, Deephaven recommends [`TablePublisher`](./TablePublisher.md) for writing data to a live table. It has a more flexible and more performant API and produces a [blink table](../../../conceptual/table-types.md#specialization-3-blink). `DynamicTableWriter` produces an [append-only table](../../../conceptual/table-types.md#specialization-1-append-only) and suits simple cases that add a few rows at a time.

## Syntax

```groovy syntax
new DynamicTableWriter(header)
new DynamicTableWriter(header, constantValues)
new DynamicTableWriter(columnNames, columnTypes)
new DynamicTableWriter(columnNames, columnTypes, constantValues)
new DynamicTableWriter(definition)
new DynamicTableWriter(definition, constantValues)
```

## Parameters

<ParamTable>
<Param name="header" type="TableHeader">

The names and types of the columns in the output table, as an [`io.deephaven.qst.table.TableHeader`](/core/javadoc/io/deephaven/qst/table/TableHeader.html). Each [`logRow`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#logRow(java.lang.Object...)) call supplies values for these columns.

</Param>
<Param name="columnNames" type="String[]">

Column names. Used with `columnTypes`; each name pairs with the type at the same position in `columnTypes`.

</Param>
<Param name="columnTypes" type="Type[]">

Column data types, as [`io.deephaven.qst.type.Type`](/core/javadoc/io/deephaven/qst/type/Type.html) objects, such as `Type.intType()` or `Type.stringType()`. Used with `columnNames`; each type applies to the name at the same position.

</Param>
<Param name="definition" type="TableDefinition">

The table definition to create the `DynamicTableWriter` for.

</Param>
<Param name="constantValues" type="Map<String, Object>">

Optional. A map of column names to values. Each listed column holds that single value in every row. The writer does not write to these columns, so `logRow` and [`logRowPermissive`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#logRowPermissive(java.lang.Object...)) take values only for the other columns, and [`getSetter`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getSetter(java.lang.String)) throws an exception for a constant column.

> [!WARNING]
> In the current release, writing a row to a `DynamicTableWriter` created with `constantValues` throws a `NullPointerException`. Create the writer without `constantValues` and add constant columns afterward, for example with [`update`](../select/update.md).

</Param>
</ParamTable>

## Returns

A `DynamicTableWriter`, which implements [`TableWriter`](/core/javadoc/io/deephaven/tablelogger/TableWriter.html).

## Methods

`DynamicTableWriter` supports the following methods:

- [`close`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#close()) - Closes the `DynamicTableWriter`.
- [`flush`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#flush()) - Has no effect for this writer. Each committed row, such as one written with [`logRow`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#logRow(java.lang.Object...)), appears the next time the table refreshes during an [update graph cycle](../../../conceptual/table-update-model.md), without a flush. To control when rows are committed, see [Transactions](#transactions).
- [`getColumnNames`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getColumnNames()) - Returns an array of column names.
- [`getColumnTypes`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getColumnTypes()) - Returns an array of column types.
- [`getRowWriter`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getRowWriter()) - Returns a new [`Row`](/core/javadoc/io/deephaven/tablelogger/Row.html) object with its own setters and its own [`writeRow`](/core/javadoc/io/deephaven/tablelogger/Row.html#writeRow()) method. Several `Row` objects can be in use at once.
- [`getSetter(name)`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getSetter(java.lang.String)) - Returns a setter for the named column in the writer's current row. After setting each column, call `writeRow` before starting the next row. Do not mix calls to `getSetter` with calls to `getRowWriter`.
- [`getSetter(name, tClass)`](/core/javadoc/io/deephaven/tablelogger/TableWriter.html#getSetter(java.lang.String,java.lang.Class)) - Returns the same setter as `getSetter(name)`, typed as `RowSetter<T>`. Throws a `ClassCastException` if the column's type cannot be assigned to `tClass` or to its primitive form.
- [`getTable`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getTable()) - Returns the real-time, in-memory table that the `DynamicTableWriter` writes to.
- [`logRow(values...)`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#logRow(java.lang.Object...)) - Writes a row of values to the table. Each value must be castable to its column's type.
- [`logRow(Map values)`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#logRow(java.util.Map)) - Writes a row from a map of column names to values. The map must contain every column and no others.
- [`logRowPermissive(values...)`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#logRowPermissive(java.lang.Object...)) - Writes a row of values to the table. Each value must be convertible to its column's type. Numeric values are converted even when the conversion loses precision or overflows.
- [`logRowPermissive(Map values)`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#logRowPermissive(java.util.Map)) - Writes a row from a map of column names to values, with the same permissive conversion as `logRowPermissive(values...)`.
- [`setFlags(flags)`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#setFlags(io.deephaven.tablelogger.Row.Flags)) - Sets the transaction flag (`Row.Flags`) for every later row that the writer itself writes with `logRow`, `logRowPermissive`, or `writeRow`. `Row` objects from `getRowWriter` keep their own flags. See [Transactions](#transactions).
- [`size`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#size()) - Returns the number of rows in the table. The `Row.size` method this overrides is deprecated, so use `getTable().size()` instead.
- [`writeRow`](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#writeRow()) - Writes the current row built with `getSetter` and advances to the next row.

A `DynamicTableWriter` is not thread-safe. Call `logRow`, `logRowPermissive`, `getSetter`, `setFlags`, and `writeRow` from one thread at a time, or synchronize the calls yourself. Several threads can write at once only if each one uses its own `Row` from `getRowWriter`.

### Transactions

The default flag, `SingleRow`, commits each row on its own, so a row does not wait for any other row before it can appear in the table. A transaction is a group of rows that the writer commits together, so they all become visible at once. To write a transaction, call `setFlags` on the writer, or on a `Row` from `getRowWriter`, as follows:

1. Set `StartTransaction` before the first row.
2. Set `None` before each middle row.
3. Set `EndTransaction` before the last row. Writing this row commits the whole group.
4. Set `SingleRow` again.

Writing a `StartTransaction` or `SingleRow` row before the transaction ends discards the transaction's uncommitted rows.

Transactions are not isolated between `Row` objects. All `Row` objects from the same writer share the writer's transaction state, so a `StartTransaction` or `SingleRow` row written through one `Row` discards a transaction that another `Row` has open.

## Examples

The following two examples create the same table, with an `int` column `Numbers` and a `String` column `Words`. They differ only in which thread writes the rows.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.util.DynamicTableWriter

definition = TableDefinition.of(
    ColumnDefinition.ofInt("Numbers"),
    ColumnDefinition.ofString("Words")
)
tableWriter = new DynamicTableWriter(definition)

result = tableWriter.getTable()

// The logRow method adds a row to the table
tableWriter.logRow(1, "Testing")
sleep(3000)
tableWriter.logRow(2, "Dynamic")
sleep(3000)
tableWriter.logRow(3, "Table")
sleep(3000)
tableWriter.logRow(4, "Writer")
```

![The above `result` table](../../../assets/reference/create/DynamicTableWriter_ref1.png)

The example above writes data to `result` from the thread that runs the script. The script session holds the exclusive [update graph lock](../../../conceptual/query-engine/engine-locking.md#query-engine-locks) while the script runs, so no rows appear in `result` until the script finishes. Then all four rows appear at once. For details, see [Data appears on the next update cycle](../../../how-to-guides/table-publisher.md#data-appears-on-the-next-update-cycle).

The example below writes data to the table from a separate thread. The script finishes right away, and each row appears in `result` the next time the table refreshes during an [update graph cycle](../../../conceptual/table-update-model.md), usually the next cycle after the thread writes it.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.util.DynamicTableWriter

definition = TableDefinition.of(
    ColumnDefinition.ofInt("Numbers"),
    ColumnDefinition.ofString("Words")
)
tableWriter = new DynamicTableWriter(definition)

result = tableWriter.getTable()

// Run the thread that writes to the table
Thread.start {
    def strings = ["Testing", "Dynamic", "Table", "Writer"]

    for (int i = 0; i < 4; i++) {
        // The logRow method adds a row to the table
        tableWriter.logRow(i + 1, strings[i])
        sleep(3000)
    }
}
```

<LoopedVideo src='../../../assets/reference/create/DynamicTableWriter_ref2.mp4' />

## Related documentation

- [How to write data to a real-time in-memory table](../../../how-to-guides/table-publisher.md#dynamictablewriter)
- [Incremental update model](../../../conceptual/table-update-model.md)
- [Multithreading: Synchronization, locks, and snapshots](../../../conceptual/query-engine/engine-locking.md)
- [`TablePublisher`](./TablePublisher.md)
- [Table types](../../../conceptual/table-types.md)
- [`update`](../select/update.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html)
