---
title: Create static tables
sidebar_label: Static tables
---

Deephaven is often used to read table data from Parquet, Kafka, or other external sources, but it can also generate static or ticking tables from scratch. There are two functions for creating static tables: [`empty_table`](../reference/table-operations/create/emptyTable.md) and [`new_table`](../reference/table-operations/create/newTable.md). This guide will show you how to use these functions to create static tables and columns, and how to add data to those tables.

## `empty_table`

The [`empty_table`](../reference/table-operations/create/emptyTable.md) function takes a single argument - an `int` representing the number of rows in the new table. The resulting table has no columns and the specified number of rows. In the following example, we create a table with 10 rows and no columns:

```python order=table
from deephaven import empty_table

table = empty_table(10)
```

Calling [`empty_table`](../reference/table-operations/create/emptyTable.md) on its own generates a table with no data, but it can easily be populated with columns and data using [`update`](../reference/table-operations/select/update.md) or another [selection method](./use-select-view-update.md). This can be done in the same line that creates the table, or at any time afterward.

In the following example, we create a table with 10 rows and a single column `X` with values 0 through 9 by using the [special variable `i`](../reference/query-language/variables/special-variables.md) to represent the row index. Then, the table is updated again to add a column `Y` with values equal to `X` squared:

```python order=table
from deephaven import empty_table

table = empty_table(10).update("X = i")

table = table.update("Y = X * X")
```

Deephaven's [`update`](../reference/table-operations/select/update.md) and other selection methods can take user-defined functions as arguments and harness the power of the Deephaven Query Language to handle complex data transformations. For more information, see the [select, view, and update guide](./use-select-view-update.md).

DQL supports logical operators, Java functions, user-defined functions, and more. In the following example, we'll create a table with 100 rows, then create four columns:

```python order=source
from deephaven import empty_table

source = empty_table(100).update(
    formulas=[
        # mathematical operations are supported
        "X = 0.1 * i",
        # many built-in functions are provided to cover common operations
        "SinX = sin(X)",
        # in-line logical operations and comparison operators are supported
        "PositiveSinX = SinX > 0 ? true : false",
        # and they can all be combined
        "TransformedX = PositiveSinX == true ? 5 * X : 0",
    ]
)
```

DQL is a powerful, versatile tool for table transformations. For more information, see the [Query string overview](./query-string-overview.md) for more.

## `new_table`

Deephaven's [`new_table`](../reference/table-operations/create/newTable.md) function allows you to create a new table and manually populate it with data. [`new_table`](../reference/table-operations/create/newTable.md) accepts a list of [Deephaven column objects](#column-types). The following query creates a new table with a `string` column and an `int` column.

```python order=result
from deephaven import new_table

from deephaven.column import string_col, int_col

result = new_table(
    [
        string_col(
            "NameOfStringCol", ["Data String 1", "Data String 2", "Data String 3"]
        ),
        int_col("NameOfIntCol", [4, 5, 6]),
    ]
)
```

Here, we create an example with two integer columns. Then, we update the table to add a new column `X` via a [formula](./formulas.md) that uses a [variable](./python-variables.md), a [Python function](./python-functions.md), an [auto-imported Java function](../reference/query-language/query-library/auto-imported/index.md), and various [operators](./operators.md):

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

var = 3


def f(a, b) -> int:
    return a + b


source = new_table([int_col("A", [1, 2, 3, 4, 5]), int_col("B", [10, 20, 30, 40, 50])])

result = source.update(formulas=["X = A + 3 * sqrt(B) + var + f(A, B)"])
```

### Array columns

[`new_table`](../reference/table-operations/create/newTable.md) can also be used to create columns from Python Sequences. This cannot be done with methods like [`int_col`](../reference/table-operations/create/intCol.md) and [`string_col`](../reference/table-operations/create/stringCol.md). In these cases, you must use the [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn) class directly along with the [dtypes](../reference/python/deephaven-python-types.md) package.

The following example creates a new table with a single integer array column.

```python order=source
from deephaven.column import InputColumn
from deephaven import new_table
from deephaven import dtypes
import numpy as np

int_array = dtypes.array(dtypes.int32, np.array([1, 2, 3], dtype=np.int32))
int_array_col = InputColumn("IntArrayCol", dtypes.int32_array, input_data=[int_array])

source = new_table([int_array_col])
```

## Create new columns in a table

Here, we will go into detail on creating new columns in your tables.

[Selection methods](./use-select-view-update.md) -- such as [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md), [`update`](../reference/table-operations/select/update.md), [`update_view`](../reference/table-operations/select/update-view.md), and [`lazy_update`](../reference/table-operations/select/lazy-update.md) -- and [formulas](./formulas.md) are used to create new columns:

- The [selection method](./use-select-view-update.md) determines which columns will be in the output table and how the values are computed.
- The [formulas](./formulas.md) are the recipes for computing the cell values.

In the following examples, we use a table of student test results. Using [`update`](../reference/table-operations/select/update.md), we create a new `Total` column containing the sum of each student's math, science, and art scores. Notice that [`update`](../reference/table-operations/select/update.md) includes the columns from the source table in the output table.

```python test-set=1 order=total,scores
from deephaven import new_table
from deephaven.column import string_col, int_col

scores = new_table(
    [
        string_col("Name", ["James", "Lauren", "Zoey"]),
        int_col("Math", [95, 72, 100]),
        int_col("Science", [100, 78, 98]),
        int_col("Art", [90, 92, 96]),
    ]
)

total = scores.update(formulas=["Total = Math + Science + Art"])
```

Now we make the example a little more complicated by adding a column of average test scores.

```python test-set=1 order=average
average = scores.update(formulas=["Average = (Math + Science + Art) / 3 "])
```

For the next example, we have the students' test results in various subjects and the class averages. We want to see which students scored higher than the class average. We can use the [`select`](../reference/table-operations/select/select.md) method to create a table containing the `Name` and `Subject` columns from the source table, plus a new column indicating if the score is above average.

```python order=above_average,class_average
from deephaven import new_table
from deephaven.column import string_col, int_col

class_average = new_table(
    [
        string_col(
            "Name",
            [
                "James",
                "James",
                "James",
                "Lauren",
                "Lauren",
                "Lauren",
                "Zoey",
                "Zoey",
                "Zoey",
            ],
        ),
        string_col(
            "Subject",
            [
                "Math",
                "Science",
                "Art",
                "Math",
                "Science",
                "Art",
                "Math",
                "Science",
                "Art",
            ],
        ),
        int_col("StudentAverage", [95, 100, 90, 72, 78, 92, 100, 98, 96]),
        int_col("ClassAverage", [86, 90, 95, 86, 90, 95, 86, 90, 95]),
    ]
)

above_average = class_average.select(
    formulas=["Name", "Subject", "AboveAverage = StudentAverage > ClassAverage"]
)
```

## Column types

Deephaven supports the following column types:

| Data Type         | Method                                                                |
| ----------------- | --------------------------------------------------------------------- |
| boolean           | [`bool_col`](../reference/table-operations/create/boolCol.md)         |
| byte              | [`byte_col`](../reference/table-operations/create/byteCol.md)         |
| char              | [`char_col`](../reference/table-operations/create/charCol.md)         |
| java.time.Instant | [`datetime_col`](../reference/table-operations/create/dateTimeCol.md) |
| double            | [`double_col`](../reference/table-operations/create/doubleCol.md)     |
| float             | [`float_col`](../reference/table-operations/create/floatCol.md)       |
| int               | [`int_col`](../reference/table-operations/create/intCol.md)           |
| java.lang.Object  | [`jobj_col`](../reference/table-operations/create/jobj_col.md)        |
| long              | [`long_col`](../reference/table-operations/create/longCol.md)         |
| Python Object     | [`pyobj_col`](../reference/table-operations/create/pyobj_col.md)      |
| short             | [`short_col`](../reference/table-operations/create/shortCol.md)       |
| String            | [`string_col`](../reference/table-operations/create/stringCol.md)     |

As demonstrated above, Deephaven can handle types outside of this list by using [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn).

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [How to use select, view, and update](./use-select-view-update.md)
- [Query string overview](./query-string-overview.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`update`](../reference/table-operations/select/update.md)
