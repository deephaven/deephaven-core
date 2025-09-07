---
title: Programmatically generate query strings with Python
sidebar_label: Generate query strings with Python
---

The Deephaven Query Language allows users to write very powerful queries to filter and modify tables of data. Consider the following query, which uses a [formula](./formulas.md) to add a new column to a table and a [filter](./filters.md) to filter the resulting table.

```python order=result,source
from deephaven import empty_table
from deephaven.column import int_col

source = empty_table(7).update(["Value = i"])

result = source.update("X = sqrt(Value) + Value").where("X > 2 && X < 8")
```

Deephaven query strings are passed into table operations as [Python strings](https://docs.python.org/3/library/string.html). As such, all of the power of Python can be used to generate query strings. This can be convenient when working with complex queries. Let's work though a few examples that are simplified by using Python to generate query strings.

> [!NOTE]
> This guide assumes you are familiar with the use of [strings](https://docs.python.org/3/library/string.html), [f-strings](https://peps.python.org/pep-0498/), [loops](https://www.learnpython.org/en/Loops), and [list comprehension](https://peps.python.org/pep-0202/) in Python. If not, please refer to the Python documentation for more information.

## Many columns

In practice, queries may have a large number of inputs, making it inconvenient to type in each column name. Other times, the input column names are determined by user inputs and are not known when the query is written. Both of these situations can be addressed by using a list of column names to generate queries.

In the following example, an [f-string](https://peps.python.org/pep-0498/) and [`str.join`](https://docs.python.org/3/library/stdtypes.html#str.join) are used to create a query string to sum up all of the columns and then take the square root.

```python order=result,source
from deephaven import new_table
from deephaven.column import int_col

cols = ["A", "B", "C", "D"]

source = new_table([int_col(c, [0, 1, 2, 3, 4, 5, 6]) for c in cols])

result = source.update(f"X = sqrt(sum({','.join(cols)}))")
```

If the list of columns changes, the query string programmatically adapts:

```python order=result,source
from deephaven import new_table
from deephaven.column import int_col

cols = ["A", "B", "C", "D", "E"]

source = new_table([int_col(c, [0, 1, 2, 3, 4, 5, 6]) for c in cols])

result = source.update(f"X = sqrt(sum({','.join(cols)}))")
```

## Repeated logic

Some queries repeat the same logic -- with minor tweaks. For example, a query may add columns containing data from 1, 5, and 10 minutes ago. Generated query strings can also help simplify these situations.

In the following example, an [f-string](https://peps.python.org/pep-0498/) is used to create columns of data from 1, 5, and 10 rows before.

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update("X = ii")

offsets = [1, 5, 10]

result = source

for offset in offsets:
    result = result.update(f"X{offset} = X_[ii-{offset}]")
```

This can be simplified further by using a [list comprehension](https://peps.python.org/pep-0202/).

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update("X = ii")

offsets = [1, 5, 10]
result = source.update([f"X{offset} = X_[ii-{offset}]" for offset in offsets])
```

Data analysis, particularly in finance, often involves binning data into time buckets for analysis. These queries rarely use a single time bucket to analyze the data - they often use several or more. Python's f-strings make queries shorter and more readable. Consider first, a query that places data into 9 different temporal buckets without f-strings:

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

Not only was that query tedious to write, but the formatting is long and repetitive, and doesn't take advantage of Python's power. Consider the following query, which does the same thing, but with f-strings and list comprehension.

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

This query is shorter, faster to write, and easier to read for a Python programmer. It also makes future updates easier to write, and changes to the `bin_sizes` list are automatically reflected in the `result` table.

## Be creative!

Programatically generating query strings works for all Deephaven operations, not just [`update`](../reference/table-operations/select/update.md). For example, this case uses multiple programatically generated query strings while performing a join.

```python order=result,source
from deephaven import empty_table

source = empty_table(100).update(["X = ii", "Y = X", "Z = sqrt(X)"])

offsets = [1, 5, 10]

result = source

for offset in offsets:
    result = result.natural_join(
        source.update(f"XOffset = X+{offset}"),
        on="X=XOffset",
        joins=[f"Y{offset}=Y", f"Z{offset}=Z"],
    )
```

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Create a new table](./new-and-empty-table.md#new_table)
- [Create an empty table](./new-and-empty-table.md#empty_table)
- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Operators in query strings](./operators.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes in query strings](./python-classes.md)
- [Think like a Deephaven ninja](../conceptual/ninja.md)
