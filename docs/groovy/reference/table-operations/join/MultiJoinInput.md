---
title: MultiJoinInput
---

A `MultiJoinInput` is an object that contains a table and a list of columns to match and add, and is used as an input for a [Multi-Join](./multijoin.md).

## Methods

```
MultiJoinInput.of(inputTable, columnsToMatch, columnsToAdd)
MultiJoinInput.of(inputTable, columnsToMatch...)
```

## Parameters

<ParamTable>
<Param name="inputTable" type="Table">

The table to add to the MultiJoin.

</Param>
<Param name="columnsToMatch" type="JoinMatch[]">

An array of [`JoinMatch`](/core/javadoc/io/deephaven/api/JoinMatch.html) specifying match conditions.

</Param>
<Param name="columnsToMatch" type="String[]">

The key columns, in string format (e.g., "ResultKey=SourceKey" or "KeyInBoth").

</Param>
<Param name="columnsToMatch" type="Collection<? extends JoinMatch>">

A collection of [`JoinMatch`](/core/javadoc/io/deephaven/api/JoinMatch.html) objects specifying the key columns.

</Param>
<Param name="columnsToMatch" type="String...">

The key columns, in string format (e.g., "ResultKey=SourceKey" or "KeyInBoth").

</Param>
<Param name="ColumnsToAdd" type="JoinAddition[]">

An array of [`JoinAddition`](/core/javadoc/io/deephaven/api/JoinAddition.html) objects specifying the columns to add.

</Param>
<Param name="ColumnsToAdd" type="String[]">

The columns to add, in string format (e.g., "ResultColumn=SourceColumn" or "SourceColumnToAddWithSameName"); empty for all columns.

</Param>
<Param name="ColumnsToAdd" type="Collection<? extends JoinAddition>">

A collection of [`JoinAddition`](/core/javadoc/io/deephaven/api/JoinAddition.html) objects specifying the columns to add.

</Param>
</ParamTable>

## Returns

A `MultiJoinInput` object.

## Examples

```groovy order=result,t1,t2,t3
// import multijoin classes
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinTable
import io.deephaven.engine.table.MultiJoinInput

// create tables
t1 = newTable(intCol("C1", 1, 2), intCol("C2", 1, 1), intCol("S1", 10, 11))
t2 = newTable(intCol("C1", 3123, 62364), intCol("C3", 56, 99), intCol("S2", 10, 11))
t3 = newTable(intCol("C1", 44, 3), intCol("C4", 182374, 1231), intCol("S3", 44, 2313))

// create MultiJoinInput objects
mj1 = MultiJoinInput.of(t1, "Key=C1")
mj2 = MultiJoinInput.of(t2, "Key=C1")
mj3 = MultiJoinInput.of(t3, "Key=C1")

// create a MultiJoinTable object
mjTable = MultiJoinFactory.of(mj1, mj2, mj3)

// retrieve the underlying table
result = mjTable.table()
```

## Related documentation

- [Multi-Join](./multijoin.md)
- [`MultiJoinTable`](./MultiJoinTable.md)
