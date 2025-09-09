---
title: Multi-Join
---

The Multi-Join feature joins the unique rows from a set of multiple tables onto a set of common keys.

Unlike most table operations, Multi-Join is not a constituent method of the `Table` class; instead, it is an external function that takes tables as arguments. Also, unlike most table operations, Multi-Join does not return a table -- it returns a [`MultiJoinTable`](/core/javadoc/io/deephaven/engine/table/MultiJoinTable.html) object, which in turn uses the `table()` method to return the underlying table.

## Syntax

```
MultiJoinFactory.of(keys, inputTables...)
MultiJoinFactory.of(columnsToMatch, inputTables...)
MultiJoinFactory.of(multiJoinInputs...)
```

## Parameters

<ParamTable>
<Param name="keys" type="String[]">

A String array containing key column names; e.g., `["key1", "key2"]`.

</Param>
<Param name="inputTables" type="Table...">

Any number of tables to be joined together.

</Param>
<Param name="columnsToMatch" type="String">

A list of key columns in string format (e.g., "ResultKey=SourceKey" or "KeyInBoth"). If there is more than one key, they must be separated with commas (e.g., `"key1, key2, key3"`).

> [!NOTE]
> Like [`natural_join`](./natural-join.md), `multi_join` will fail if there are duplicates in the key columns.

</Param>
<Param name="multiJoinInputs" type="MultiJoinInput...">

A list of [`MultiJoinInput`](./MultiJoinInput.md) objects.

</Param>
</ParamTable>

## Returns

A [`MultiJoinTable`](/core/javadoc/io/deephaven/engine/table/MultiJoinTable.html) object.

## Examples

In this example, we will create a `MultiJoinTable` using two `keys` and a list of three source tables.

```groovy order=result
// import multijoin classes
import io.deephaven.engine.table.MultiJoinFactory
import io.deephaven.engine.table.MultiJoinTable

// create tables
t1 = newTable(
    stringCol("Letter", "A", "B", "C", "D"),
    intCol("Number", 1, 2, 3, 4),
)

t2 = newTable(
    stringCol("Letter", "C", "B", "D", "A"),
    intCol("Number", 6, 7, 8, 9),
)

t3 = newTable(
    stringCol("Letter", "D", "C", "A", "B"),
    intCol("Number", 9, 8, 7, 6),
)

// create a MultiJoinTable object and join the three tables
MultiJoinTable mtTable = MultiJoinFactory.of("Letter, NumCol=Number", t1, t2, t3)

// access the multijoin object's internal table
result = mtTable.table()
```

In this example, we will create a `MultiJoinTable` using a list of `MultiJoinInput` objects.

```groovy order=result
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

result = mjTable.table()
```

## Related documentation

- [`newTable`](../create/newTable.md)
- [`MultiJoinInput`](./MultiJoinInput.md)
- [`MultiJoinTable`](./MultiJoinTable.md)
