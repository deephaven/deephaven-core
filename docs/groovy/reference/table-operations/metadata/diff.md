---
title: diff
---

The `diff` method returns the differences between two provided tables as a string. If the two tables are the same, an empty string is returned.

This method starts by comparing the table sizes and then the schema of the two tables, such as the number of columns, column names, column types, and column orders. If the schemas differ, the comparison stops, and the differences are returned. If the schemas are the same, the method compares the table data. The method compares the table data column by column (not row by row) and only records the first difference found in each column.

Note that inexact comparison of floating numbers may sometimes be desirable due to their inherent imprecision.

## Syntax

```groovy syntax
diff(actualResult, expectedResult, maxDiffLines)
diff(actualResult, expectedResult, maxDiffLines, itemsToSkip)
```

## Parameters

<ParamTable>
<Param name="actualResult" type="Table">

The table to compare.

</Param>
<Param name="expectedResult" type="Table">

The table to compare `actualResult` against.

</Param>
<Param name="maxDiffLines" type="long">

The maximum number of differences to return.

</Param>
<Param name="itemsToSkip" type="EnumSet<TableDiff.DiffItems>">

EnumSet of checks not to perform.

- `ColumnsOrder`: Columns that exist in both tables but in different orders are not treated as different.
- `DoubleFraction`: Doubles and Floats are not treated as differences if they are within a factor of `TableDiff.DOUBLE_EXACT_THRESHOLD` or `TableDiff.FLOAT_EXACT_THRESHOLD`.
- `DoublesExaxt`: Doubles and Floats are not treated as differences if they are within `TableDiff.DOUBLE_EXACT_THRESHOLD` or `TableDiff.FLOAT_EXACT_THRESHOLD`.

</Param>
</ParamTable>

## Returns

The differences between `actualResult` and `expectedResult` as a string.

## Examples

In the following example, we use the `diff` method to compare two tables.

```groovy order=:log
t1 = emptyTable(10).update("A = i", "B = i", "C = i")
t2 = emptyTable(10).update("A = i", "C = i % 2 == 0? i: i + 1", "C = i % 2 == 0? i + 1: i")

d = diff(t1, t2, 10).split("\n")

println d
```

Here, we have two identical tables, except that table `t2` swaps columns `B` and `C`. We'll use the `itemsToSkip` parameter to ignore the column order.

```groovy order=:log
import io.deephaven.engine.util.TableDiff.DiffItems

t1 = emptyTable(10).update("A = i", "B = i", "C = i")
t2 = emptyTable(10).update("A = i", "C = i", "B = i")


d = diff(t1, t2, 10, EnumSet.of(DiffItems.valueOf("ColumnsOrder")))
d2 = diff(t1, t2, 10)

println "d:" + d
println "d2: \n" + d2
```

## Related documentation

- [Create an empty table](../../../how-to-guides/new-and-empty-table.md#emptytable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/TableTools.html#diff(io.deephaven.engine.table.Table,io.deephaven.engine.table.Table,long))
