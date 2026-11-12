---
title: Programmatically generate query strings with Python
---

The Deephaven Query Language lets you write powerful [query strings](./query-string-overview.md) that filter and modify tables of data. Consider the following query, which uses a [formula](./formulas.md) to add a new column to a table and a [filter](./filters.md) to filter the resulting table.

```python order=result,source
from deephaven import empty_table

source = empty_table(7).update(["Value = i"])

result = source.update("X = sqrt(Value) + Value").where("X > 2 && X < 8")
```

[Table operations](../conceptual/table-operations-overview.md) take query strings as ordinary [Python strings](https://docs.python.org/3/library/stdtypes.html#text-sequence-type-str), so all of Python's power is available to build them. That helps most when a query is long or repetitive. Let's work through a few examples that Python-generated query strings simplify.

> [!NOTE]
> This guide assumes you are familiar with the use of [strings](https://docs.python.org/3/library/stdtypes.html#text-sequence-type-str), [f-strings](https://peps.python.org/pep-0498/), [loops](https://docs.python.org/3/tutorial/controlflow.html#for-statements), and [list comprehension](https://peps.python.org/pep-0202/) in Python. If not, please refer to the Python documentation for more information.

## Many columns

In practice, queries may have a large number of inputs, making it inconvenient to type in each column name. Other times, user inputs determine the column names, so you don't know them when you write the query. In both situations, you can generate the query from a list of column names.

In the following example, an [f-string](https://peps.python.org/pep-0498/) and [`str.join`](https://docs.python.org/3/library/stdtypes.html#str.join) build a query string that adds the values of all the columns in each row and takes the square root.

```python order=result,source
from deephaven import new_table
from deephaven.column import int_col

cols = ["A", "B", "C", "D"]

source = new_table([int_col(c, [0, 1, 2, 3, 4, 5, 6]) for c in cols])

result = source.update(f"X = sqrt(sum({','.join(cols)}))")
```

Add a column name to the list, and the generated query string includes it the next time the code runs:

```python order=result,source
from deephaven import new_table
from deephaven.column import int_col

cols = ["A", "B", "C", "D", "E"]

source = new_table([int_col(c, [0, 1, 2, 3, 4, 5, 6]) for c in cols])

result = source.update(f"X = sqrt(sum({','.join(cols)}))")
```

## Repeated logic

Some queries repeat the same logic with minor tweaks. For example, a query may add columns containing data from 1, 5, and 10 rows before the current row. Generated query strings simplify these queries too.

In the following example, an [f-string](https://peps.python.org/pep-0498/) creates columns of data from 1, 5, and 10 rows before. Each formula uses the [underscore operator](./work-with-arrays.md#using-the-underscore-operator) to access column `X` as an array, and the built-in variable [`ii`](./built-in-variables.md) to index that array by row position.

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update("X = ii")

offsets = [1, 5, 10]

result = source

for offset in offsets:
    result = result.update(f"X{offset} = X_[ii - {offset}]")
```

`X1` is null in the first row, `X5` in the first 5 rows, and `X10` in the first 10 rows, because those rows have no row that far back.

A [list comprehension](https://peps.python.org/pep-0202/) simplifies this further.

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update("X = ii")

offsets = [1, 5, 10]
result = source.update([f"X{offset} = X_[ii - {offset}]" for offset in offsets])
```

Generated strings are ordinary query strings, so the same code works on static and ticking tables. The constant offsets these examples generate, such as `X_[ii - 1]`, are a form the engine supports on refreshing tables. See [Refreshing table restrictions](./built-in-variables.md#refreshing-table-restrictions) for details.

## Time bins

Data analysis, particularly in finance, often groups data into time bins, and a single query may use several bin sizes. Here is a query that uses [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) to bin the timestamps at nine bin sizes, written out by hand:

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2024-03-15T09:30:00 ET' + i * MINUTE",
        "Price = randomDouble(0, 100)",
        "Size = randomInt(0, 25)",
    ]
)

result = source.update(
    [
        "Bin3Min = lowerBin(Timestamp, 3 * MINUTE)",
        "Bin5Min = lowerBin(Timestamp, 5 * MINUTE)",
        "Bin7Min = lowerBin(Timestamp, 7 * MINUTE)",
        "Bin10Min = lowerBin(Timestamp, 10 * MINUTE)",
        "Bin15Min = lowerBin(Timestamp, 15 * MINUTE)",
        "Bin20Min = lowerBin(Timestamp, 20 * MINUTE)",
        "Bin30Min = lowerBin(Timestamp, 30 * MINUTE)",
        "Bin45Min = lowerBin(Timestamp, 45 * MINUTE)",
        "Bin60Min = lowerBin(Timestamp, 60 * MINUTE)",
    ]
)
```

That query is tedious to write, long, and repetitive. It doesn't take advantage of Python's power.

The [f-string](https://peps.python.org/pep-0498/) and [list comprehension](https://peps.python.org/pep-0202/) pattern from [Repeated logic](#repeated-logic) does the same thing in a few lines:

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Timestamp = '2024-03-15T09:30:00 ET' + i * MINUTE",
        "Price = randomDouble(0, 100)",
        "Size = randomInt(0, 25)",
    ]
)

bin_sizes = [3, 5, 7, 10, 15, 20, 30, 45, 60]

result = source.update(
    [
        f"Bin{bin_size}Min = lowerBin(Timestamp, {bin_size} * MINUTE)"
        for bin_size in bin_sizes
    ]
)
```

This query is shorter, faster to write, and easier to read for a Python programmer. It also makes future updates easier: to change the bins, edit the `bin_sizes` list and re-run the query.

## Other table operations

Programmatically generated strings work as arguments to any [table operation](../conceptual/table-operations-overview.md) that takes strings, not just [`update`](../reference/table-operations/select/update.md). For example, the following query generates the arguments to each [`natural_join`](../reference/table-operations/join/natural-join.md).

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update(["X = ii", "Y = X", "Z = sqrt(X)"])

offsets = [1, 5, 10]

result = source

for offset in offsets:
    result = result.natural_join(
        source.update(f"XOffset = X + {offset}"),
        on="X=XOffset",
        joins=[f"Y{offset}=Y", f"Z{offset}=Z"],
    )
```

Each pass of the loop runs one [`natural_join`](../reference/table-operations/join/natural-join.md). It matches every row to the row `offset` rows earlier, the one whose `X` is smaller by `offset`. It copies that row's `Y` and `Z` into new columns named for the offset, such as `Y1` and `Z1`. The first `offset` rows have no earlier row to match, so their new columns are null.

## Related documentation

- [Think like a Deephaven ninja](../conceptual/ninja.md)
- [Table operations overview](../conceptual/table-operations-overview.md)
- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Create a new table](./new-and-empty-table.md#new_table)
- [Create an empty table](./new-and-empty-table.md#empty_table)
- [Query string overview](./query-string-overview.md)
- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Operators in query strings](./operators.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [Work with arrays](./work-with-arrays.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`int_col`](../reference/table-operations/create/intCol.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [Time functions](../reference/query-language/query-library/auto-imported/time.md)
- [`lowerBin` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long))
