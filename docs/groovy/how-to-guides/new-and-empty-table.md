---
title: Create static tables
---

Deephaven often reads table data from Parquet, Kafka, or other external sources, but it can also generate tables from scratch. Static tables hold fixed data that does not change after creation. This guide covers two simple methods for creating them: [`emptyTable`](../reference/table-operations/create/emptyTable.md) and [`newTable`](../reference/table-operations/create/newTable.md). It shows how to use these methods to create static tables and columns, and how to add data to those tables. To create a table that updates in real time, see [Create a time table](./time-table.md), [Write data to an in-memory, real-time table](./table-publisher.md), [Create and use input tables](./input-tables.md), and [Replay data from static tables](./replay-data.md).

## `emptyTable`

The [`emptyTable`](../reference/table-operations/create/emptyTable.md) method takes a single argument — a `long` representing the number of rows in the new table. The resulting table has no columns and the specified number of rows. In the following example, we create a table with 10 rows and no columns:

```groovy order=table
table = emptyTable(10)
```

Calling [`emptyTable`](../reference/table-operations/create/emptyTable.md) on its own generates a table with no data. You can add columns and data with [`update`](../reference/table-operations/select/update.md) or another [selection method](./use-select-view-update.md), either in the same line that creates the table or at any time afterward.

In the following example, we create a table with 10 rows and a single column `X` with values 0 through 9 by using the [special variable `i`](../reference/query-language/variables/special-variables.md) to represent the row index. Then, we update the table again to add a column `Y` with values equal to `X` squared:

```groovy order=table
table = emptyTable(10).update("X = i")

table = table.update("Y = X * X")
```

The [Create new columns in a table](#create-new-columns-in-a-table) section shows more ways to add columns.

## `newTable`

Deephaven's [`newTable`](../reference/table-operations/create/newTable.md) method creates a new table that you populate with data. It accepts one or more [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html) objects as arguments.

A [`ColumnHolder`](/core/javadoc/io/deephaven/engine/table/impl/util/ColumnHolder.html) stores a column's name, type, and data. Column methods such as [`stringCol`](../reference/table-operations/create/stringCol.md) and [`intCol`](../reference/table-operations/create/intCol.md) create them. The [Column types](#column-types) section lists these methods.

The following query creates a new table with a `String` column and an `int` column:

```groovy order=result
result = newTable(
        stringCol("NameOfStringCol", "Data String 1", "Data String 2", "Data String 3"),
        intCol("NameOfIntCol", 4, 5, 6),
)
```

### Column types

The following methods create columns of common types:

| Data type           | Method                                                             |
| ------------------- | ------------------------------------------------------------------ |
| `Boolean`           | [`booleanCol`](../reference/table-operations/create/booleanCol.md) |
| `byte`              | [`byteCol`](../reference/table-operations/create/byteCol.md)       |
| `char`              | [`charCol`](../reference/table-operations/create/charCol.md)       |
| `java.lang.Object`  | [`col`](../reference/table-operations/create/col.md)               |
| `double`            | [`doubleCol`](../reference/table-operations/create/doubleCol.md)   |
| `float`             | [`floatCol`](../reference/table-operations/create/floatCol.md)     |
| `java.time.Instant` | [`instantCol`](../reference/table-operations/create/instantCol.md) |
| `int`               | [`intCol`](../reference/table-operations/create/intCol.md)         |
| `long`              | [`longCol`](../reference/table-operations/create/longCol.md)       |
| `short`             | [`shortCol`](../reference/table-operations/create/shortCol.md)     |
| `String`            | [`stringCol`](../reference/table-operations/create/stringCol.md)   |

When you pass [`col`](../reference/table-operations/create/col.md) individual values, it creates a `java.lang.Object` column. To create a column of a type not in this list, such as an array column, see [Array columns](#array-columns).

### Array columns

[`newTable`](../reference/table-operations/create/newTable.md) can also create array columns, where each cell holds an array. Typed methods such as [`intCol`](../reference/table-operations/create/intCol.md) and [`stringCol`](../reference/table-operations/create/stringCol.md) can't create these. Instead, pass [`col`](../reference/table-operations/create/col.md) an array of arrays, with one inner array per row.

The following example creates a new table with a single `int[]` column that has two rows:

```groovy order=source
source = newTable(
        col("IntArrayCol", [[1, 2, 3] as int[], [4, 5] as int[]] as int[][])
)
```

## Create new columns in a table

[Selection methods](./use-select-view-update.md) and [formulas](./formulas.md) work together to create new columns. The selection methods are [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md), [`update`](../reference/table-operations/select/update.md), [`updateView`](../reference/table-operations/select/update-view.md), and [`lazyUpdate`](../reference/table-operations/select/lazy-update.md). Selection methods and formulas each have their own job:

- The [selection method](./use-select-view-update.md) determines which columns appear in the output table. It also determines whether their values are computed immediately and stored in memory, or computed on demand when they are read.
- The [formulas](./formulas.md) are the recipes for computing the cell values.

In the following example, we use a table of student test results. Using [`update`](../reference/table-operations/select/update.md), we create a new `Total` column containing the sum of each student's math, science, and art scores, and an `Average` column computed from `Total`.

```groovy test-set=1 order=total,scores
scores = newTable(
        stringCol("Name", "James", "Lauren", "Zoey"),
        intCol("Math", 95, 72, 100),
        intCol("Science", 100, 78, 98),
        intCol("Art", 90, 92, 96),
)

total = scores.update("Total = Math + Science + Art", "Average = Total / 3")
```

A formula can use a column created by an earlier formula in the same call, as `Average` uses `Total` here. The output table also keeps every column from the source table.

[`select`](../reference/table-operations/select/select.md), by contrast, includes only the columns you list. The following example keeps the `Name` column and adds a `Total` column:

```groovy test-set=1 order=nameTotal
nameTotal = scores.select("Name", "Total = Math + Science + Art")
```

The formulas you pass to [`update`](../reference/table-operations/select/update.md) and the other selection methods can use the full Deephaven Query Language (DQL), including mathematical operations, built-in functions, comparison and conditional operators, Java functions, and user-defined closures. In the following example, we create a table with 100 rows, then create four columns:

```groovy order=source
source = emptyTable(100).update(
        // mathematical operations are supported
        "X = 0.1 * i",
        // many built-in functions are provided to cover common operations
        "SinX = sin(X)",
        // comparison operators are supported
        "PositiveSinX = SinX > 0",
        // and they can all be combined with the ternary (conditional) operator
        "TransformedX = PositiveSinX ? 5 * X : 0",
)
```

The following example creates a table with two integer columns. Then, it updates the table to add a new column `X` via a [formula](./formulas.md) that uses a [variable](./groovy-variables.md), a [user-defined closure](./groovy-closures.md), an [auto-imported Java function](../reference/query-language/query-library/auto-imported/index.md), and various [operators](./operators.md):

```groovy order=source,result
var = 3

f = { a, b -> a + b }
source = newTable(intCol("A", 1, 2, 3, 4, 5), intCol("B", 10, 20, 30, 40, 50))

result = source.update("X = A + 3 * sqrt(B) + var + (int)f(A, B)")
```

For more about DQL, see the [Query string overview](./query-string-overview.md) and [Select and create columns](./use-select-view-update.md).

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [Select and create columns](./use-select-view-update.md)
- [Query string overview](./query-string-overview.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`update`](../reference/table-operations/select/update.md)
