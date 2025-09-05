---
title: DynamicTableWriter
---

`DynamicTableWriter` creates a `TableWriter` for writing data to a real-time, in-memory table.

<!-- TODO: https://github.com/deephaven/deephaven.io/issues/497 Add TableWriter javadoc link -->

## Syntax

```
DynamicTableWriter(header, constantValues)
DynamicTableWriter(header)
DynamicTableWriter(columnNames, columnTypes, constantValues)
DynamicTableWriter(columnNames, columnTypes)
DynamicTableWriter(definition)
DynamicTableWriter(definition, constantValues)
```

## Parameters

<ParamTable>
<Param name="columnNames" type="String[]">

Column names.

</Param>
<Param name="columnTypes" type="Class[]">

Column data types.

</Param>
<Param name="header" type="TableHeader">

The names and types of the columns in the output table (and our input).

</Param>
<Param name="constantValues" type="Map<String, Object>">

A Map of columns with constant values.

</Param>
<Param name="definition" type="TableDefinition">

The table definition to create the Dynamic Table Writer for.

</Param>
</ParamTable>

## Returns

A `TableWriter`. <!-- TODO: https://github.com/deephaven/deephaven.io/issues/497 Add TableWriter javadoc link -->

## Methods

`DynamicTableWriter` supports the following methods:

- [`close()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#close()) - Closes the `TableWriter`.
- [`flush()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#flush()) - Flushes the `TableWriter`.
- [`getColumnNames()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getColumnNames()) - Returns a list of column names.
- [`getColumnTypes()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getColumnTypes()) - Returns a list of column types.
- [`getTable()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#getTable()) - Returns a real-time, in-memory table.
- [`logRow(values...)`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#logRow(java.lang.Object...)) - Writes a row of values to the table.
- [`size()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html#size()) - Returns the number of rows in the table.

## Examples

In this example, `DynamicTableWriter` is used to create a table with two columns:

- The first contains the row number.
- The second contains a string.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.impl.util.DynamicTableWriter

columnNames = ["Numbers", "Words"] as String[]
columnTypes = [int.class, String.class] as Class[]
tableWriter = new DynamicTableWriter(columnNames, columnTypes)

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

The example above writes data to `result` from the main thread. As a result, the Deephaven web interface will not display the `result` table until the script finishes execution.

The example below uses a dedicated thread to write data to the table. The Deephaven web interface immediately updates to display all `result` table changes.

```groovy ticking-table order=null reset
import io.deephaven.engine.table.impl.util.DynamicTableWriter

columnNames = ["Numbers", "Words"] as String[]
columnTypes = [int.class, String.class] as Class[]
tableWriter = new DynamicTableWriter(columnNames, columnTypes)

result = tableWriter.getTable()

// Thread to log data to the dynamic table
def thread = Thread.start {
    def strings = ["Testing", "Dynamic", "Table", "Writer"]

    for(int i = 0; i < 4; i++) {
        // The logRow method adds a row to the table
        tableWriter.logRow(i + 1, strings[i])
        sleep(3000)
    }

    return
}
```

<LoopedVideo src='../../../assets/reference/create/DynamicTableWriter_ref2.mp4' />

## Related documentation

- [How to use write data to a real-time in-memory table](../../../how-to-guides/dynamic-table-writer.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/DynamicTableWriter.html)
