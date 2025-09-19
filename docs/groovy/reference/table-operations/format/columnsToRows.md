---
title: columnsToRows
---

The `columnsToRows` method is used to convert a table's columns into rows.

## Syntax

```
columnsToRows(source, labelColumn, valueColumns, labels, transposeColumns)
columnsToRows(source, labelColumn, valueColumn, transposeColumns...)
columnsToRows(source, labelColumn, valueColumn, labels, transposeColumns)
```

## Parameters

<ParamTable>
<Param name="source" type="Table">

The table with multiple value columns.

</Param>
<Param name="labelColumn" type="String">

The output column name for the label column.

</Param>
<Param name="valueColumns" type="String[]">

The output column names for the value columns.

For each output value column, all of the constituent input columns columns must have the same type. If the types are different, then an `IllegalArgumentException` is thrown. All value columns must have the same number of source columns.

</Param>
<Param name="valueColumn" type="String">

The output column name for the value column.

For each output value column, all of the constituent input columns columns must have the same type. If the types are different, then an `IllegalArgumentException` is thrown. All value columns must have the same number of source columns.

</Param>
<Param name="labels" type="String[]">

The labels for the transposed columns. Must be parallel to `transposeColumns`.

</Param>
<Param name="transposeColumns" type="String[][]">

An array parallel to `valueColumns`; each element is in turn an array of input column names that are constituents for the output column. The input columns within each element must be the same type, and the cardinality much match labels.

</Param>
<Param name="transposeColumns" type="String[]">

The input column names to transpose. Must be parallel to `labels`.

</Param>
<Param name="transposeColumns" type="String...">

The names of the columns to transpose. The label value is the name of the column.

</Param>
</ParamTable>

## Returns

The transformed table.

## Example

In this example, we start with a table that is very wide compared to how tall it is. The information in the table would be easier to read if we reorganize it, which we will do with the `columnsToRows` method.

```groovy order=result,source
import io.deephaven.engine.table.impl.util.ColumnsToRowsTransform

source = newTable(
    stringCol("Sym", "AAPL", "SPY"),
    intCol("Val1", 1, 2),
    doubleCol("D1", 7.7, 8.8),
    doubleCol("D2", 9.9, 10.1),
    intCol("Val2", 3, 4),
    intCol("Val3", 5, 6),
    doubleCol("D3", 11.11, 12.12)
)

result = ColumnsToRowsTransform.columnsToRows(
  source,
  "Name",
  new String[]{"IntVal", "DubVal"},
  new String[]{"A", "B", "C"},
  new String[][]{new String[]{"Val1", "Val2", "Val3"},
  new String[]{"D1", "D2", "D3"}}
)
```

## Related documentation

- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnsToRowsTransform.html)
