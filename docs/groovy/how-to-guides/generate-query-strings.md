---
title: Programmatically generate query strings with Groovy
---

The Deephaven Query Language lets you write powerful [query strings](./query-string-overview.md) that filter and modify tables of data. Consider the following query, which uses a [formula](./formulas.md) to add a new column to a table and a [filter](./filters.md) to filter the resulting table.

```groovy order=result,source
source = emptyTable(7).update("Value = i")

result = source.update("X = sqrt(Value) + Value").where("X > 2 && X < 8")
```

[Table operations](../conceptual/table-operations-overview.md) take query strings as ordinary [Groovy strings](https://groovy-lang.org/syntax.html#all-strings), so all of Groovy's power is available to build them. That helps most when a query is long or repetitive. Let's work through a few examples that Groovy-generated query strings simplify.

> [!NOTE]
> This guide assumes you are familiar with the use of [strings](https://groovy-lang.org/syntax.html#all-strings), [GStrings](https://groovy-lang.org/syntax.html#_string_interpolation), [loops](https://groovy-lang.org/semantics.html#_looping_structures), [list operations](https://groovy-lang.org/syntax.html#_lists), and the [spread operator](https://groovy-lang.org/operators.html#_spreading_method_arguments) in Groovy. If not, please refer to the Groovy documentation for more information.

## Many columns

In practice, queries may have a large number of inputs, making it inconvenient to type in each column name. Other times, user inputs determine the column names, so you don't know them when you write the query. In both situations, you can generate the query from a list of column names.

In the following example, a [GString](https://groovy-lang.org/syntax.html#_string_interpolation) and [`join`](https://groovy-lang.org/groovy-dev-kit.html#_working_with_collections) build a query string that adds the values of all the columns in each row and takes the square root. [`collect`](https://groovy-lang.org/groovy-dev-kit.html#_working_with_collections) makes one column holder per name. [`newTable`](../reference/table-operations/create/newTable.md) takes column holders as separate arguments, not as a list, so the [spread operator](https://groovy-lang.org/operators.html#_spreading_method_arguments) (`*`) in front of `cols.collect { ... }` passes each holder as its own argument.

```groovy order=result,source
cols = ["A", "B", "C", "D"]

source = newTable(*cols.collect { col -> intCol(col, 0, 1, 2, 3, 4, 5, 6) })

result = source.update("X = sqrt(sum(${cols.join(',')}))")
```

Add a column name to the list, and the generated query string includes it the next time the code runs:

```groovy order=result,source
cols = ["A", "B", "C", "D", "E"]

source = newTable(*cols.collect { col -> intCol(col, 0, 1, 2, 3, 4, 5, 6) })

result = source.update("X = sqrt(sum(${cols.join(',')}))")
```

## Repeated logic

Some queries repeat the same logic with minor tweaks. For example, a query may add columns containing data from 1, 5, and 10 rows before the current row. Generated query strings simplify these queries too.

In the following example, a [GString](https://groovy-lang.org/syntax.html#_string_interpolation) creates columns of data from 1, 5, and 10 rows before. Each formula uses the [underscore operator](./work-with-arrays.md#using-the-underscore-operator) to access column `X` as an array, and the built-in variable [`ii`](./built-in-variables.md) to index that array by row position.

```groovy order=result,source
source = emptyTable(100).update("X = ii")

offsets = [1, 5, 10]

result = source

for (offset in offsets) {
    result = result.update("X${offset} = X_[ii - ${offset}]")
}
```

`X1` is null in the first row, `X5` in the first 5 rows, and `X10` in the first 10 rows, because those rows have no row that far back.

Groovy's [`collect`](https://groovy-lang.org/groovy-dev-kit.html#_working_with_collections) method simplifies this further. [`update`](../reference/table-operations/select/update.md) takes formula strings as separate arguments, and its list form accepts only [`Selectable`](/core/javadoc/io/deephaven/api/Selectable.html) objects, so the [spread operator](https://groovy-lang.org/operators.html#_spreading_method_arguments) (`*`) passes each generated string as its own argument.

```groovy order=result,source
source = emptyTable(100).update("X = ii")

offsets = [1, 5, 10]
result = source.update(*offsets.collect { offset -> "X${offset} = X_[ii - ${offset}]" })
```

Generated strings are ordinary query strings, so the same code works on static and ticking tables. The constant offsets these examples generate, such as `X_[ii - 1]`, are a form the engine supports on refreshing tables. See [Refreshing table restrictions](./built-in-variables.md#refreshing-table-restrictions) for details.

## Time bins

Data analysis, particularly in finance, often groups data into time bins, and a single query may use several bin sizes. Here is a query that uses [`lowerBin`](../reference/time/datetime/lowerBin.md) to bin the timestamps at nine bin sizes, written out by hand:

```groovy order=result,source
source = emptyTable(100).update(
    "Timestamp = '2024-03-15T09:30:00 ET' + i * MINUTE",
    "Price = randomDouble(0, 100)",
    "Size = randomInt(0, 25)"
)

result = source.update(
    "Bin3Min = lowerBin(Timestamp, 3 * MINUTE)",
    "Bin5Min = lowerBin(Timestamp, 5 * MINUTE)",
    "Bin7Min = lowerBin(Timestamp, 7 * MINUTE)",
    "Bin10Min = lowerBin(Timestamp, 10 * MINUTE)",
    "Bin15Min = lowerBin(Timestamp, 15 * MINUTE)",
    "Bin20Min = lowerBin(Timestamp, 20 * MINUTE)",
    "Bin30Min = lowerBin(Timestamp, 30 * MINUTE)",
    "Bin45Min = lowerBin(Timestamp, 45 * MINUTE)",
    "Bin60Min = lowerBin(Timestamp, 60 * MINUTE)"
)
```

That query is tedious to write, long, and repetitive. It doesn't take advantage of Groovy's power.

The [GString](https://groovy-lang.org/syntax.html#_string_interpolation) and [`collect`](https://groovy-lang.org/groovy-dev-kit.html#_working_with_collections) pattern from [Repeated logic](#repeated-logic) does the same thing in a few lines:

```groovy order=result,source
source = emptyTable(100).update(
    "Timestamp = '2024-03-15T09:30:00 ET' + i * MINUTE",
    "Price = randomDouble(0, 100)",
    "Size = randomInt(0, 25)"
)

binSizes = [3, 5, 7, 10, 15, 20, 30, 45, 60]

result = source.update(
    *binSizes.collect { binSize -> "Bin${binSize}Min = lowerBin(Timestamp, ${binSize} * MINUTE)" }
)
```

This query is shorter, faster to write, and easier to read for a Groovy programmer. It also makes future updates easier: to change the bins, edit the `binSizes` list and re-run the query.

## Other table operations

Programmatically generated strings work as arguments to any [table operation](../conceptual/table-operations-overview.md) that takes strings, not just [`update`](../reference/table-operations/select/update.md). For example, the following query generates the arguments to each [`naturalJoin`](../reference/table-operations/join/natural-join.md).

```groovy order=result,source
source = emptyTable(100).update("X = ii", "Y = X", "Z = sqrt(X)")

offsets = [1, 5, 10]

result = source

for (offset in offsets) {
    result = result.naturalJoin(
        source.update("XOffset = X + ${offset}"),
        "X=XOffset",
        "Y${offset}=Y,Z${offset}=Z"
    )
}
```

Each pass of the loop runs one [`naturalJoin`](../reference/table-operations/join/natural-join.md). It matches every row to the row `offset` rows earlier, the one whose `X` is smaller by `offset`. It copies that row's `Y` and `Z` into new columns named for the offset, such as `Y1` and `Z1`. The first `offset` rows have no earlier row to match, so their new columns are null.

## Related documentation

- [Think like a Deephaven ninja](../conceptual/ninja.md)
- [Table operations overview](../conceptual/table-operations-overview.md)
- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Create a new table](./new-and-empty-table.md#newtable)
- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Query string overview](./query-string-overview.md)
- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Operators in query strings](./operators.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in query strings](./groovy-closures.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)
- [Work with arrays](./work-with-arrays.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`intCol`](../reference/table-operations/create/intCol.md)
- [`lowerBin`](../reference/time/datetime/lowerBin.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`Selectable` Javadoc](/core/javadoc/io/deephaven/api/Selectable.html)
