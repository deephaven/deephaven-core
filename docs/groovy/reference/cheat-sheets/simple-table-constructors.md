---
title: Simple table constructors
---

This cheat sheet shows the basic ways to create a table:

- From column data
- With a set number of rows
- As a [time table](../../how-to-guides/time-table.md), which adds rows on a timer
- As a [ring table](../../how-to-guides/ring-table.md), which keeps the latest rows of another table
- As an [input table](../../how-to-guides/input-tables.md), which accepts new rows

It ends with how to add [formula](../../how-to-guides/formulas.md) columns to a new table.

## `newTable`

[`newTable`](../table-operations/create/newTable.md) creates a table from supplied column data.

Create columns with the following methods:

- [`booleanCol`](../table-operations/create/booleanCol.md)
- [`byteCol`](../table-operations/create/byteCol.md)
- [`charCol`](../table-operations/create/charCol.md)
- [`col`](../table-operations/create/col.md)
- [`doubleCol`](../table-operations/create/doubleCol.md)
- [`floatCol`](../table-operations/create/floatCol.md)
- [`instantCol`](../table-operations/create/instantCol.md)
- [`intCol`](../table-operations/create/intCol.md)
- [`longCol`](../table-operations/create/longCol.md)
- [`shortCol`](../table-operations/create/shortCol.md)
- [`stringCol`](../table-operations/create/stringCol.md)

```groovy order=result
result = newTable(
        intCol("Integers", 1, 2, 3),
        stringCol("Strings", "These", "are", "Strings"),
)
```

### Array columns

Of the methods in the list above, only [`col`](../table-operations/create/col.md) can create [array columns](../../how-to-guides/work-with-arrays.md), where each cell holds an array. Methods such as [`intCol`](../table-operations/create/intCol.md) and [`stringCol`](../table-operations/create/stringCol.md) cannot. To create one, pass [`col`](../table-operations/create/col.md) an array of arrays, with one inner array per row.

```groovy order=source
source = newTable(
        col("IntArrayCol", [[1, 2, 3] as int[], [4, 5] as int[]] as int[][])
)
```

## `emptyTable`

[`emptyTable`](../table-operations/create/emptyTable.md) creates a table with a given number of rows and no columns. Add columns to it with [`update`](../table-operations/select/update.md), as shown in [Add formula columns](#add-formula-columns).

```groovy order=result,result1
result = emptyTable(5)

result1 = result.update("X = 5")
```

## `timeTable`

[`timeTable`](../table-operations/create/timeTable.md) creates a table that adds a new row at a regular interval.

```groovy ticking-table order=null
result = timeTable("PT2S")
```

<LoopedVideo src='../../assets/tutorials/timetable.mp4' />

## `RingTableTools.of`

[`RingTableTools.of`](../table-operations/create/ringTable.md) creates a table that keeps only the latest rows added to a parent table, up to a given capacity. Ring tables are most useful with [blink tables](../../conceptual/table-types.md#specialization-3-blink), which keep only the rows from the latest update cycle.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

source = timeTable("PT1S")
result = RingTableTools.of(source, 3)
```

![The `source` time table and the `result` ring table ticking side by side in the Deephaven console](../../assets/how-to/ring-table-1.gif)

## `AppendOnlyArrayBackedInputTable.make`

An [input table](../table-operations/create/InputTable.md) is an in-memory table that users can add rows to. [`AppendOnlyArrayBackedInputTable.make`](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html) creates an append-only one: new rows go to the end, and rows cannot be edited or deleted. It starts from either an existing table or a [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html), which lists each column's name and type. Create each column entry with a [`ColumnDefinition`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) factory such as [`ColumnDefinition.ofInt`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html).

To create a [keyed input table](../../how-to-guides/input-tables.md#create-a-keyed-input-table), which adds, replaces, or deletes rows by key, use [`KeyedArrayBackedInputTable.make`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html) with the key column names.

```groovy order=source,result,result2
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

// from an existing table
source = emptyTable(10).update("X = i")

result = AppendOnlyArrayBackedInputTable.make(source)

// from a TableDefinition
tableDef = TableDefinition.of(
        ColumnDefinition.ofInt("Integers"),
        ColumnDefinition.ofDouble("Doubles"),
        ColumnDefinition.ofString("Strings"),
)

result2 = AppendOnlyArrayBackedInputTable.make(tableDef)
```

## Add formula columns

After creating a table, add columns calculated from its existing columns by passing a [formula](../../how-to-guides/formulas.md) to [`update`](../table-operations/select/update.md). A formula can combine columns, [built-in functions](../../how-to-guides/built-in-functions.md) such as [`sqrt`](../query-language/query-library/auto-imported/math.md), [variables](../../how-to-guides/query-scope.md), and [Groovy closures](../../how-to-guides/groovy-closures.md). A closure called in a formula returns [`Object`](../query-language/types/objects.md), whatever types it declares, so [cast](../../how-to-guides/casting.md#groovy-closures) its result to the type you need, as `(int)f(A, B)` does below.

```groovy order=source,result
var = 3

f = { a, b -> a + b }

source = newTable(intCol("A", 1, 2, 3, 4, 5), intCol("B", 10, 20, 30, 40, 50))

result = source.update("X = A + 3 * sqrt(B) + var + (int)f(A, B)")
```

## Related documentation

- [Create static tables](../../how-to-guides/new-and-empty-table.md)
- [Select and create columns](../../how-to-guides/use-select-view-update.md)
- [Formulas](../../how-to-guides/formulas.md)
- [Work with arrays](../../how-to-guides/work-with-arrays.md)
- [Built-in functions](../../how-to-guides/built-in-functions.md)
- [Query scope](../../how-to-guides/query-scope.md)
- [Groovy closures](../../how-to-guides/groovy-closures.md)
- [Casting](../../how-to-guides/casting.md)
- [Create a time table](../../how-to-guides/time-table.md)
- [Create a ring table](../../how-to-guides/ring-table.md)
- [Create and use input tables](../../how-to-guides/input-tables.md)
- [`col`](../table-operations/create/col.md)
- [`emptyTable`](../table-operations/create/emptyTable.md)
- [Input table](../table-operations/create/InputTable.md)
- [`intCol`](../table-operations/create/intCol.md)
- [`newTable`](../table-operations/create/newTable.md)
- [`RingTableTools.of`](../table-operations/create/ringTable.md)
- [`sqrt`](../query-language/query-library/auto-imported/math.md)
- [`stringCol`](../table-operations/create/stringCol.md)
- [`timeTable`](../table-operations/create/timeTable.md)
- [`update`](../table-operations/select/update.md)
- [`AppendOnlyArrayBackedInputTable` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html)
- [`KeyedArrayBackedInputTable` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html)
- [`ColumnDefinition` Javadoc](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html)
- [`TableDefinition` Javadoc](/core/javadoc/io/deephaven/engine/table/TableDefinition.html)
