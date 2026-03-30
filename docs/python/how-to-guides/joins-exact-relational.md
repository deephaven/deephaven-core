---
title: Exact and Relational Joins
---

This guide covers exact and relational joins in Deephaven. Exact and relational join operations combine data from two tables based on one or more related key columns.

- An exact join combines tables and only keeps rows where exact matches occur in the key column(s). The following table operations perform an exact join:
  - [`exact_join`](../reference/table-operations/join/exact-join.md)
  - [`natural_join`](../reference/table-operations/join/natural-join.md)
- A relational join primarily combines rows with exact matches across tables, but can also include rows where no exact match exists, depending on the type of join used. The following table operations exemplify different relational joins:
  - [`join`](../reference/table-operations/join/join.md)
  - [`left_outer_join`](../reference/table-operations/join/left-outer-join.md)
  - [`full_outer_join`](../reference/table-operations/join/full-outer-join.md)

Exact and relational joins differ from time-series and range joins. For a detailed guide, see [Joins: time-series and range](./joins-timeseries-range.md).

To join three or more table operations with one operation, see the [`multi_join`](../reference/table-operations/join/multi-join.md) operation documented in a [later section](#join-three-or-more-tables) of this article.

## Syntax

Following convention, the tables being joined together will be referred to as the "left table" and the "right table":

- The left table is the base table to which data is added.
- The right table is the source of data added to the left table.

One or more columns will be used as keys to match data between the left and right tables. This format is fundamental for writing join statements in Deephaven. However, the syntax can vary depending on the circumstances.

The basic syntax for [`join`](../reference/table-operations/join/join.md), [`exact_join`](../reference/table-operations/join/exact-join.md), and [`natural_join`](../reference/table-operations/join/natural-join.md) is as follows:

```python syntax
# Include all non-key columns
result = left_table.join_method(table=right_table, on=["ColumnsToMatch"])

# Include only some non-key columns (ColumnsToAdd)
result = left_table.join_method(
    table=right_table, on=["ColumnsToMatch"], joins=["ColumnsToAdd"]
)
```

Where `right_table` is the table to join with, and `on` and `joins` are the String names for columns to match and add, respectively.

The basic syntax for [`left_outer_join`](../reference/table-operations/join/left-outer-join.md) and [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) are as follows:

```python syntax
# Port all non-key columns
result = outer_join_method(
    l_table=left_table, r_table=right_table, on=["ColumnsToMatch"]
)

# Port only some non-key columns (ColumnsToAdd)
result = outer_join_method(
    l_table=left_table,
    r_table=right_table,
    on=["ColumnsToMatch"],
    joins=["ColumnsToAdd"],
)
```

> [!NOTE]
> [`left_outer_join`](../reference/table-operations/join/left-outer-join.md) and [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) are currently experimental. The API may change in the future.

Outside of the left and right tables, exact and relational joins take up to two more arguments. The first is required, while the second is optional:

- `on`: The key column(s) on which to look for exact matches. Columns of any data type can be used as key columns, but corresponding match columns in the left and right table _must_ be of the same data type.
- `joins` (Optional): The column(s) in the right table to join to the left table. If not specified, all columns are joined.

### Match columns with different names

When two tables can be joined, their match column(s) often don't have identical names. The syntax below joins `left_table` and `right_table` on `ColumnToMatchLeft` and `ColumnToMatchRight`:

```python syntax
result = left_table.join_method(
    table=right_table,
    on=["ColumnToMatchLeft=ColumnToMatchRight"],
    joins=["ColumnsToAdd"],
)
```

### Multiple match columns

Tables can be joined on more than one match column. The syntax below joins tables on two or more match columns:

```python syntax
result = left_table.join_method(
    table=right_table, on=["Column1", "Column2", "Column3Left = Column3Right", ...]
)
```

### Rename joined columns

Columns being joined from the right table that have the same name as existing columns in the left table will cause a name conflict error. To avoid this, the `joins` argument can be renamed as a column from the right table. The following example renames the right table's `OldColumnName` column to `NewColumnName`:

```python syntax
result = left_table.join_method(
    table=right_table,
    on=["ColumnToMatchLeft=ColumnToMatchRight"],
    joins=["NewColumnName=OldColumnName"],
)
```

## Exact joins

The output of an exact join operation appends columns to the left table (from the right table) for rows where an exact key match exists in the right table.

Exact matches fail if multiple matching keys are in the right table for any key in the left table.

There are two available operations to perform an exact match join. They differ based on:

- If all rows from the left table are included.
- How zero matches are handled.

### `exact_join`

`Exact_join` requires the distinct key set of the left table to be identical to the full set of key column values in the right table. If there is no matching key in the right table for any value in a left table key column, the `exact_join` will fail. Additionally, the operation will fail if multiple matching keys exist in the right table for any key in the left table.

```python order=result,left,right
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

left = new_table(
    [
        string_col("LastName", ["Rafferty", "Jones", "Steiner", "Robins", "Smith"]),
        int_col("DeptID", [31, 33, 33, 34, 34]),
        string_col(
            "Telephone",
            [
                "(303) 555-0162",
                "(303) 555-0149",
                "(303) 555-0184",
                "(303) 555-0125",
                "",
            ],
        ),
    ]
)

right = new_table(
    [
        int_col("DeptID", [31, 33, 34, 35]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical", "Marketing"]),
        string_col(
            "DeptTelephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)

result = left.exact_join(table=right, on=["DeptID"])
```

### `natural_join`

`Natural_join` allows for cases when there are no matching keys in the right table for particular values in the key column of the left table. If no matching key exists in the right table, appended column values are simply `NULL`. Similarly to `exact_join`, if there are multiple key matches in the right table, the operation will fail.

```python test-set=1 order=result,left,right
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

left = new_table(
    [
        string_col(
            "LastName",
            ["Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"],
        ),
        int_col("DeptID", [31, 33, 33, 34, 34, 36, NULL_INT]),
        string_col(
            "Telephone",
            [
                "(303) 555-0162",
                "(303) 555-0149",
                "(303) 555-0184",
                "(303) 555-0125",
                "",
                "",
                "(303) 555-0160",
            ],
        ),
    ]
)

right = new_table(
    [
        int_col("DeptID", [31, 33, 34, 35]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical", "Marketing"]),
        string_col(
            "DeptTelephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)

result = left.natural_join(table=right, on=["DeptID"])
```

## Relational joins

In contrast to exact joins, relational joins provide operations where multiple key matches in the right table will not result in an error.

The three relational join methods differ in how zero exact matches are handled.

### `join`

The output table from a `join` operation contains rows with matching values in both tables. Rows without matching values are not included in the result.

```python order=result,left,right
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

left = new_table(
    [
        string_col(
            "LastName",
            ["Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"],
        ),
        int_col("DeptID", [31, 33, 33, 34, 34, 36, NULL_INT]),
        string_col(
            "Telephone",
            [
                "(303) 555-0162",
                "(303) 555-0149",
                "(303) 555-0184",
                "(303) 555-0125",
                "",
                "",
                "(303) 555-0160",
            ],
        ),
    ]
)

right = new_table(
    [
        int_col("DeptID", [31, 33, 34, 35]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical", "Marketing"]),
        string_col(
            "DeptTelephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)

result = left.join(table=right, on=["DeptID"])
```

> [!TIP]
> [`join`](../reference/table-operations/join/join.md) computes the cross product of the left and right tables and subsets the rows based on the arguments. This means it is slow relative to [`natural_join`](../reference/table-operations/join/natural-join.md), so [`natural_join`](../reference/table-operations/join/natural-join.md) should be preferred in most places.

### `left_outer_join`

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

The output table from a [`left_outer_join`](../reference/table-operations/join/left-outer-join.md) operation contains _all_ rows from the left table as well as rows from the right table that have matching keys in the match column(s).

```python order=result,left,right
from deephaven.experimental.outer_joins import left_outer_join
from deephaven import empty_table

left = empty_table(5).update(["I = ii", "A = `left`"])
right = empty_table(5).update(["I = ii * 2", "B = `right`", "C = Math.sin(I)"])

result = left_outer_join(l_table=left, r_table=right, on=["I"])
```

### `full_outer_join`

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

The output table from a [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) operation contains all rows in the key identifier columns from both tables. Keys that exist in one table but not the other project null values into the respective non-key columns for the unmatched row.

```python order=result,left,right
from deephaven.experimental.outer_joins import full_outer_join
from deephaven import empty_table

left = empty_table(5).update(["I = ii", "A = `left`"])
right = empty_table(5).update(["I = ii * 2", "B = `right`", "C = Math.sin(I)"])

result = full_outer_join(l_table=left, r_table=right, on=["I"])
```

## Join three or more tables

The [`multi_join`](../reference/table-operations/join/multi-join.md) operation joins three or more tables. It was developed to improve the join speed by taking advantage of the potential to share a single hash table and exploit concurrency.

[`multi_join`](../reference/table-operations/join/multi-join.md) joins three or more tables together in the same way that [`natural_join`](../reference/table-operations/join/natural-join.md) joins two tables together. The result of [`multi_join`](../reference/table-operations/join/multi-join.md) is not a typical table, but rather a `MultiJoinTable` object, so calling the `table` method is necessary for most use cases.

There are two ways to use [`multi_join`](../reference/table-operations/join/multi-join.md): with constituent tables or with one or more `MultiJoinInput` objects.

### With constituent tables

Using constituent tables is syntactically simple. The syntax is as follows:

```python syntax
multi_table = multi_join(input=[t1, t2, t3], on="CommonKeyColumn")
multi_table = multi_join(input=[t1, t2, t3], on=["CommonKeyCol1", "CommonKeyCol2"])
```

- `input` is any number of tables to merge; for example, `table1, table2, table3`.
- `on` is a String or list of String key column names; for example, `["key1", "key2"]`.

Using constituent tables requires that all tables have identical key column names and that _all_ of the tables' output rows are desired.

The following example joins three tables that correspond to letter grades for students at three different grade levels.

```python order=result,grade5,grade6,grade7
from deephaven.table import multi_join
from deephaven import new_table
from deephaven.column import string_col

grade5 = new_table(
    [
        string_col("Name", ["Mark", "Austin", "Sandra", "Andy", "Caleb"]),
        string_col("Grade5", ["A", "A", "C", "B", "A"]),
    ]
)

grade6 = new_table(
    [
        string_col("Name", ["Sandra", "Andy", "Kathy", "June", "Caleb"]),
        string_col("Grade6", ["B", "C", "D", "A", "A"]),
    ]
)

grade7 = new_table(
    [
        string_col("Name", ["Austin", "Kathy", "Sandra", "Mark", "Caleb"]),
        string_col("Grade7", ["C", "B", "A", "C", "B"]),
    ]
)

# create a MultiJoinTable object and join the three tables
multijoin_table = multi_join(input=[grade5, grade6, grade7], on=["Name"])

# access the multijoin object's internal table
result = multijoin_table.table
```

### With `MultiJoinInput` objects

Using `MultiJoinInput` objects as inputs for [`multi_join`](../reference/table-operations/join/multi-join.md) is syntactically more complex than using [constituent tables](#with-constituent-tables), but allows for more flexibility. The syntax for creating a `MultiJoinInput` object is as follows:

```python syntax
multijoin_input = [
    MultiJoinInput(table=t1, on="KeyColumn"),  # All columns added
    MultiJoinInput(
        table=t2, on="KeyColumn", joins=["Column1", "Column2"]
    ),  # Specific columns added
]
```

Then, the syntax for using the `multijoin_input` object in a [`multi_join`](../reference/table-operations/join/multi-join.md) is simple:

```python syntax
multi_table = multi_join(input=multijoin_input)
```

The following example demonstrates the use of [`multi_join`](../reference/table-operations/join/multi-join.md) to join three tables via a `MultiJoinInput` object.

```python order=result,grade5,grade6,grade7
# import multijoin classes
from deephaven.table import MultiJoinInput, MultiJoinTable, multi_join
from deephaven import new_table
from deephaven.column import string_col

grade5 = new_table(
    [
        string_col("Name", ["Mark", "Austin", "Sandra", "Andy", "Caleb"]),
        string_col("Grade5", ["A", "A", "C", "B", "A"]),
    ]
)

grade6 = new_table(
    [
        string_col("Name", ["Sandra", "Andy", "Kathy", "June", "Caleb"]),
        string_col("Grade6", ["B", "C", "D", "A", "A"]),
    ]
)

grade7 = new_table(
    [
        string_col("Name", ["Austin", "Kathy", "Sandra", "Mark", "Caleb"]),
        string_col("Grade7", ["C", "B", "A", "C", "B"]),
    ]
)

# create a MultiJoinInput array
mji_arr = [
    MultiJoinInput(table=grade5, on="Key=Name"),
    MultiJoinInput(table=grade6, on="Key=Name"),
    MultiJoinInput(table=grade7, on="Key=Name"),
]

# create a MultiJoinTable object
multijoin_table = multi_join(input=mji_arr)

# retrieve the underlying table
result = multijoin_table.table
```

## Which method should you use?

Choosing the right join method can be tricky, so here are some things to consider when choosing between what's available:

- How should multiple exact matches be handled?
  - [`exact_join`](../reference/table-operations/join/exact-join.md) and [`natural_join`](../reference/table-operations/join/natural-join.md) raise an error.
  - [`join`](../reference/table-operations/join/join.md), [`left_outer_join`](../reference/table-operations/join/left-outer-join.md), and [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) do not raise an error.
- How should zero exact matches be handled?
  - [`exact_join`](../reference/table-operations/join/exact-join.md) raises an error.
  - [`natural_join`](../reference/table-operations/join/natural-join.md) joins a null value.
- What data should be included in the result?
  - [`join`](../reference/table-operations/join/join.md) includes only rows that have matching values in _both_ tables.
  - [`left_outer_join`](../reference/table-operations/join/left-outer-join.md) includes all rows from the left table, and only rows from the right table that match those in the left table.
  - [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) includes all rows from _both_ tables.

For help in choosing a method that uses inexact matches to join tables, see [here](./joins-timeseries-range.md).

The following figure presents a flowchart to help choose the right join method for your query.

<Svg src='../assets/conceptual/joins3.svg' style={{height: 'auto', maxWidth: '100%'}} />

## Related documentation

- [Time series and range joins](./joins-timeseries-range.md)
- [`exact_join`](../reference/table-operations/join/exact-join.md)
- [`full_outer_join`](../reference/table-operations/join/full-outer-join.md)
- [`join`](../reference/table-operations/join/join.md)
- [`left_outer_join`](../reference/table-operations/join/left-outer-join.md)
- [`multi_join`](../reference/table-operations/join/multi-join.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
