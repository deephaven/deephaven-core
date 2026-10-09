---
title: Create static tables
---

Deephaven often reads table data from Parquet, Kafka, or other external sources, but it can also generate tables from scratch. Static tables hold fixed data that does not change after creation. This guide covers two simple functions for creating them: [`empty_table`](../reference/table-operations/create/emptyTable.md) and [`new_table`](../reference/table-operations/create/newTable.md). It shows how to use these functions to create static tables and columns, and how to add data to those tables. To create a table that adds rows over time, see [Create a time table](./time-table.md).

## `empty_table`

The [`empty_table`](../reference/table-operations/create/emptyTable.md) function takes a single argument — an `int` representing the number of rows in the new table. The resulting table has no columns and the specified number of rows. In the following example, we create a table with 10 rows and no columns:

```python order=table
from deephaven import empty_table

table = empty_table(10)
```

Calling [`empty_table`](../reference/table-operations/create/emptyTable.md) on its own generates a table with no data. You can add columns and data with [`update`](../reference/table-operations/select/update.md) or another [selection method](./use-select-view-update.md), either in the same line that creates the table or at any time afterward.

In the following example, we create a table with 10 rows and a single column `X` with values 0 through 9 by using the [special variable `i`](../reference/query-language/variables/special-variables.md) to represent the row index. Then, we update the table again to add a column `Y` with values equal to `X` squared:

```python order=table
from deephaven import empty_table

table = empty_table(10).update("X = i")

table = table.update("Y = X * X")
```

The [Create new columns in a table](#create-new-columns-in-a-table) section shows more ways to add columns.

## `new_table`

Deephaven's [`new_table`](../reference/table-operations/create/newTable.md) function creates a new table that you populate with data. It accepts either a list of column objects or a dictionary that maps column names to data. This section covers column objects first, including the [column types](#column-types) they support and [array columns](#array-columns), then [creating a table from a dictionary](#create-a-table-from-a-dictionary).

A column object is an [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn), which stores a column's name, type, and data. Functions such as [`string_col`](../reference/table-operations/create/stringCol.md) and [`int_col`](../reference/table-operations/create/intCol.md) create them. The [Column types](#column-types) section lists these functions.

The following query creates a new table with a `String` column and an `int` column from a list of column objects:

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

### Column types

The following functions create columns of common types:

| Data type           | Function                                                              |
| ------------------- | --------------------------------------------------------------------- |
| `Boolean`           | [`bool_col`](../reference/table-operations/create/boolCol.md)         |
| `byte`              | [`byte_col`](../reference/table-operations/create/byteCol.md)         |
| `char`              | [`char_col`](../reference/table-operations/create/charCol.md)         |
| `java.time.Instant` | [`datetime_col`](../reference/table-operations/create/dateTimeCol.md) |
| `double`            | [`double_col`](../reference/table-operations/create/doubleCol.md)     |
| `float`             | [`float_col`](../reference/table-operations/create/floatCol.md)       |
| `int`               | [`int_col`](../reference/table-operations/create/intCol.md)           |
| `java.lang.Object`  | [`jobj_col`](../reference/table-operations/create/jobj_col.md)        |
| `long`              | [`long_col`](../reference/table-operations/create/longCol.md)         |
| Python Object       | [`pyobj_col`](../reference/table-operations/create/pyobj_col.md)      |
| `short`             | [`short_col`](../reference/table-operations/create/shortCol.md)       |
| `String`            | [`string_col`](../reference/table-operations/create/stringCol.md)     |

To create a column of a type not in this list, such as an array column, see [Array columns](#array-columns).

### Array columns

[`new_table`](../reference/table-operations/create/newTable.md) can also create array columns, where each cell holds an array. Typed functions such as [`int_col`](../reference/table-operations/create/intCol.md) and [`string_col`](../reference/table-operations/create/stringCol.md) can't create these. Instead, construct an [`InputColumn`](/core/pydoc/code/deephaven.column.html#deephaven.column.InputColumn) yourself with an array type from the [`deephaven.dtypes`](../reference/python/deephaven-python-types.md) module, such as `dtypes.int32_array`.

The following example creates a new table with a single integer array column. [`dtypes.array`](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.array) converts a NumPy array to a Java `int` array. The column's data is a list that holds that one array, so the table has one row.

```python order=source
from deephaven.column import InputColumn
from deephaven import new_table
from deephaven import dtypes
import numpy as np

int_array = dtypes.array(dtypes.int32, np.array([1, 2, 3], dtype=np.int32))
int_array_col = InputColumn("IntArrayCol", dtypes.int32_array, input_data=[int_array])

source = new_table([int_array_col])
```

### Create a table from a dictionary

When you pass a dictionary, [`new_table`](../reference/table-operations/create/newTable.md) first converts it to a pandas DataFrame, so pandas infers each column's type from its data. In the following example, the `Name` column is a `String` column, `Math` is a `long` column, and `Gpa` is a `double` column:

```python order=result
from deephaven import new_table

result = new_table(
    {
        "Name": ["James", "Lauren", "Zoey"],
        "Math": [95, 72, 100],
        "Gpa": [3.5, 3.1, 4.0],
    }
)
```

## Create new columns in a table

[Selection methods](./use-select-view-update.md) and [formulas](./formulas.md) work together to create new columns. The selection methods are [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md), [`update`](../reference/table-operations/select/update.md), [`update_view`](../reference/table-operations/select/update-view.md), and [`lazy_update`](../reference/table-operations/select/lazy-update.md). Selection methods and formulas each have their own job:

- The [selection method](./use-select-view-update.md) determines which columns appear in the output table. It also determines whether their values are computed immediately and stored in memory, or computed on demand when they are read.
- The [formulas](./formulas.md) are the recipes for computing the cell values.

In the following example, we use a table of student test results. Using [`update`](../reference/table-operations/select/update.md), we create a new `Total` column containing the sum of each student's math, science, and art scores, and an `Average` column computed from `Total`.

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

total = scores.update(formulas=["Total = Math + Science + Art", "Average = Total / 3"])
```

A formula can use a column created by an earlier formula in the same call, as `Average` uses `Total` here. The output table also keeps every column from the source table.

[`select`](../reference/table-operations/select/select.md), by contrast, includes only the columns you list. The following example keeps the `Name` column and adds a `Total` column:

```python test-set=1 order=name_total
name_total = scores.select(formulas=["Name", "Total = Math + Science + Art"])
```

The formulas you pass to [`update`](../reference/table-operations/select/update.md) and the other selection methods can use the full Deephaven Query Language (DQL), including mathematical operations, built-in functions, comparison and conditional operators, Java functions, and user-defined functions. In the following example, we create a table with 100 rows, then create four columns:

```python order=source
from deephaven import empty_table

source = empty_table(100).update(
    formulas=[
        # mathematical operations are supported
        "X = 0.1 * i",
        # many built-in functions are provided to cover common operations
        "SinX = sin(X)",
        # comparison operators are supported
        "PositiveSinX = SinX > 0",
        # and they can all be combined with the ternary (conditional) operator
        "TransformedX = PositiveSinX ? 5 * X : 0",
    ]
)
```

The following example creates a table with two integer columns. Then, it updates the table to add a new column `X` via a [formula](./formulas.md) that uses a [variable](./python-variables.md), a [Python function](./python-functions.md), an [auto-imported Java function](../reference/query-language/query-library/auto-imported/index.md), and various [operators](./operators.md):

```python order=source,result
from deephaven import new_table
from deephaven.column import int_col

var = 3


def f(a, b) -> int:
    return a + b


source = new_table([int_col("A", [1, 2, 3, 4, 5]), int_col("B", [10, 20, 30, 40, 50])])

result = source.update(formulas=["X = A + 3 * sqrt(B) + var + f(A, B)"])
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
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`update`](../reference/table-operations/select/update.md)
