---
title: Simple table constructors
---

## `newTable`

- [`newTable`](../table-operations/create/newTable.md)

Columns are created using the following methods:

- [`boolCol`](../table-operations/create/booleanCol.md)
- [`byteCol`](../table-operations/create/byteCol.md)
- [`charCol`](../table-operations/create/charCol.md)
- [`col`](../table-operations/create/col.md)
- [`instantCol`](../table-operations/create/instantCol.md)
- [`doubleCol`](../table-operations/create/doubleCol.md)
- [`floatCol`](../table-operations/create/floatCol.md)
- [`intCol`](../table-operations/create/intCol.md)
- [`longCol`](../table-operations/create/longCol.md)
- [`shortCol`](../table-operations/create/shortCol.md)
- [`stringCol`](../table-operations/create/stringCol.md)

```groovy syntax
result = newTable(
        intCol("Integers", 1, 2, 3),
        stringCol("Strings", "These", "are", "Strings"),
)
```

### Formula columns

```groovy order=source,result
var = 3

f = { a, b -> a + b }

source = newTable(intCol("A", 1, 2, 3, 4, 5), intCol("B", 10, 20, 30, 40, 50))

result = source.update("X = A + 3 * sqrt(B) + var + (int)f(A, B)")
```

### String columns

```groovy test-set=1 order=total,scores,average
scores = newTable(
        stringCol("Name", "James", "Lauren", "Zoey"),
        intCol("Math", 95, 72, 100),
        intCol("Science", 100, 78, 98),
        intCol("Art", 90, 92, 96),
)

total = scores.update("Total = Math + Science + Art")
average = scores.update("Average = (Math + Science + Art) / 3 ")
```

## `emptyTable`

- [`emptyTable`](../table-operations/create/emptyTable.md)

```groovy order=result,result1
result = emptyTable(5)

// Empty tables are often followed with a formula
result1 = result.update("X = 5")
```

## `timeTable`

- [`timeTable`](../table-operations/create/timeTable.md)

```groovy order=null
result = timeTable("PT2S")
```

<LoopedVideo src='../../assets/tutorials/timetable.mp4' />

## `RingTableTools.of`

- [`RingTableTools.of`](../table-operations/create/ringTable.md)

```groovy order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

source = timeTable("PT00:00:01")
result = RingTableTools.of(source, 3)
```

![The `source` time table and the `result` ring table ticking side-by-side in the Deephaven console](../../assets/how-to/ring-table-1.gif)

## `InputTable`

- [`InputTable`](../table-operations/create/InputTable.md)

```groovy order=source,result,result2
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

// from an existing table
source = emptyTable(10).update("X = i")

result = AppendOnlyArrayBackedInputTable.make(source)


// from scratch
tableDef = TableDefinition.of(ColumnDefinition.ofInt("Integers"), ColumnDefinition.ofDouble("Doubles"), ColumnDefinition.ofString("Srings"))

result2 = AppendOnlyArrayBackedInputTable.make(tableDef)
```

## Related documentation

- [Create static tables](../../how-to-guides/new-and-empty-table.md)
- [Select and create columns](../../how-to-guides/use-select-view-update.md)
- [`emptyTable`](../../reference/table-operations/create/emptyTable.md)
- [`InputTable`](../table-operations/create/InputTable.md)
- [`newTable`](../../reference/table-operations/create/newTable.md)
- [`RingTableTools.Of`](../table-operations/create/ringTable.md)