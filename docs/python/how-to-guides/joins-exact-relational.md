---
title: Exact and relational joins
---

This guide covers exact and relational joins in Deephaven. Both kinds of join combine data from two tables by matching values in one or more key columns. This guide calls the table that receives data the _left table_ and the table that supplies it the _right table_.

- An exact join adds columns from the right table to every row of the left table, using at most one matching right row per key. The result has the same rows as the left table. These table operations perform an exact join:
  - [`exact_join`](../reference/table-operations/join/exact-join.md)
  - [`natural_join`](../reference/table-operations/join/natural-join.md)
- A relational join pairs each left row with every matching right row, so one key can produce several result rows. Depending on the operation, the result can also include rows that have no match. These table operations perform a relational join:
  - [`join`](../reference/table-operations/join/join.md)
  - [`left_outer_join`](../reference/table-operations/join/left-outer-join.md)
  - [`full_outer_join`](../reference/table-operations/join/full-outer-join.md)

To join three or more tables on matching key values in one operation, use [`multi_join`](../reference/table-operations/join/multi-join.md), described in [Join three or more tables](#join-three-or-more-tables). The key columns can have different names in each table.

Exact and relational joins match key values exactly. To match on the nearest value or on a range of values, see [Inexact, time-series, and range joins](./joins-timeseries-range.md).

## Which method should you use?

Answer these questions to choose a join method:

- What should happen when the right table has more than one match for a key?
  - [`exact_join`](../reference/table-operations/join/exact-join.md) raises an error.
  - [`natural_join`](../reference/table-operations/join/natural-join.md) raises an error by default. It can instead [keep the first or last matching row](#natural_join).
  - [`join`](../reference/table-operations/join/join.md), [`left_outer_join`](../reference/table-operations/join/left-outer-join.md), and [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) include a result row for every match.
- What should happen to a left-table row with no match?
  - [`exact_join`](../reference/table-operations/join/exact-join.md) raises an error.
  - [`natural_join`](../reference/table-operations/join/natural-join.md), [`left_outer_join`](../reference/table-operations/join/left-outer-join.md), and [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) keep the row and fill the right table's columns with null values.
  - [`join`](../reference/table-operations/join/join.md) leaves the row out of the result.
- Should the result include right-table rows that match nothing in the left table?
  - Only [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) includes them, with null values in the left table's columns.
- Are you joining three or more tables on the same keys, even if the key column names differ, with at most one row per key in each table?
  - Use [`multi_join`](../reference/table-operations/join/multi-join.md).

The following flowchart walks through the same choices for two tables, including the inexact joins described in [Inexact, time-series, and range joins](./joins-timeseries-range.md).

<Svg src='../assets/conceptual/joins3.svg' style={{height: 'auto', maxWidth: '100%'}} />

## Syntax

[`join`](../reference/table-operations/join/join.md), [`exact_join`](../reference/table-operations/join/exact-join.md), and [`natural_join`](../reference/table-operations/join/natural-join.md) are methods of the left table:

```python syntax
# Include all non-key columns from the right table
result = left_table.join_method(table=right_table, on=["ColumnsToMatch"])

# Include only some non-key columns from the right table
result = left_table.join_method(
    table=right_table, on=["ColumnsToMatch"], joins=["ColumnsToAdd"]
)
```

[`left_outer_join`](../reference/table-operations/join/left-outer-join.md) and [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) are functions in the `deephaven.experimental.outer_joins` module that take both tables as arguments:

```python syntax
from deephaven.experimental.outer_joins import left_outer_join, full_outer_join

# Include all non-key columns from the right table
result = outer_join_function(
    l_table=left_table, r_table=right_table, on=["ColumnsToMatch"]
)

# Include only some non-key columns from the right table
result = outer_join_function(
    l_table=left_table,
    r_table=right_table,
    on=["ColumnsToMatch"],
    joins=["ColumnsToAdd"],
)
```

Besides the two tables, these operations take two main arguments. Each is a column name or expression, or a list of them:

- `on`: The key columns to match. Required for `exact_join` and `natural_join`. Optional for `join` and the outer joins, which pair every left row with every right row when `on` is omitted.
- `joins` (optional): The columns from the right table to add to the left table. If omitted, the join adds all non-key columns from the right table.

A key column can be of any data type, but each pair of matched columns in the left and right tables _must_ have the same data type.

### Match columns with different names

The key columns in two tables often have different names. The syntax below joins `left_table` and `right_table` on `ColumnToMatchLeft` and `ColumnToMatchRight`:

```python syntax
result = left_table.join_method(
    table=right_table,
    on=["ColumnToMatchLeft = ColumnToMatchRight"],
    joins=["ColumnsToAdd"],
)
```

### Multiple match columns

To join tables on more than one key column, list each one in `on`:

```python syntax
result = left_table.join_method(
    table=right_table, on=["Column1", "Column2", "Column3Left = Column3Right"]
)
```

### Rename joined columns

If a column added from the right table has the same name as a column in the left table, the join fails with a name conflict. To avoid this, rename the column in the `joins` argument. The following example adds the right table's `OldColumnName` column to the result as `NewColumnName`:

```python syntax
result = left_table.join_method(
    table=right_table,
    on=["ColumnToMatchLeft = ColumnToMatchRight"],
    joins=["NewColumnName = OldColumnName"],
)
```

## Example tables

The examples in the [Exact joins](#exact-joins) and [Relational joins](#relational-joins) sections use two tables. `employees` lists employees and the ID of the department each one works in. Rogers works in department 36, which doesn't exist, and DelaCruz has no department. `departments` lists departments by ID. No employee works in Marketing (department 35).

```python test-set=1 order=employees,departments
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

employees = new_table(
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

departments = new_table(
    [
        int_col("DeptID", [31, 33, 34, 35]),
        string_col("DeptName", ["Sales", "Engineering", "Clerical", "Marketing"]),
        string_col(
            "DeptTelephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)
```

## Exact joins

An exact join keeps every row of the left table and appends columns from the matching row of the right table. [`exact_join`](../reference/table-operations/join/exact-join.md) and [`natural_join`](../reference/table-operations/join/natural-join.md) differ in how they handle a left-table row with no match, and in whether they can keep one of several matching right rows.

### `exact_join`

[`exact_join`](../reference/table-operations/join/exact-join.md) requires every row in the left table to have exactly one matching row in the right table. The operation fails if a left-table key has no match or more than one match in the right table. The operation ignores right-table keys that have no match in the left table.

Calling `exact_join` on the whole [`employees`](#example-tables) table fails because Rogers and DelaCruz have no matching department. This example first keeps only the employees whose department exists, and then adds the department columns:

```python test-set=1 order=result,assigned
assigned = employees.where("DeptID in 31, 33, 34")

result = assigned.exact_join(table=departments, on=["DeptID"])
```

### `natural_join`

[`natural_join`](../reference/table-operations/join/natural-join.md) allows left-table rows that have no match in the right table. The appended columns in those rows are null. In this example, Rogers and DelaCruz get null department columns:

```python test-set=1 order=result
result = employees.natural_join(table=departments, on=["DeptID"])
```

By default, `natural_join` fails if the right table has more than one row for a key. To keep one of the matching rows instead, set `type` to [`NaturalJoinType`](/core/pydoc/code/deephaven.table.html#deephaven.table.NaturalJoinType) `.FIRST_MATCH` or `NaturalJoinType.LAST_MATCH`. The following example swaps the tables, using `departments` as the left table, and adds the first matching employee to each department. Departments 33 and 34 each have two employees, so the default join type would fail. Marketing has no employees, so its employee columns are null:

```python test-set=1 order=result
from deephaven.table import NaturalJoinType

result = departments.natural_join(
    table=employees, on=["DeptID"], type=NaturalJoinType.FIRST_MATCH
)
```

## Relational joins

Unlike exact joins, which use at most one matching right row per key, relational joins keep every matching right row. Each left row appears in the result once for every matching right row. The three relational joins differ in which unmatched rows they keep. The examples in this section use `departments` as the left table and `employees` as the right table, so a department with several employees produces several result rows.

### `join`

The output table from a [`join`](../reference/table-operations/join/join.md) contains a row for every pair of matching left and right rows. The result leaves out rows that have no match in the other table. In this example, the Engineering and Clerical departments each appear twice, once per employee. Marketing doesn't appear because no employee works there:

```python test-set=1 order=result
result = departments.join(table=employees, on=["DeptID"])
```

> [!TIP]
> Because [`join`](../reference/table-operations/join/join.md) includes every matching combination of left and right rows, its output can be much larger than either input. A large result also costs more to maintain on [ticking tables](../conceptual/table-update-model.md), whose rows change over time. If each left row needs at most one right match, use [`natural_join`](../reference/table-operations/join/natural-join.md) instead. It is faster, and its result has the same number of rows as the left table.

### `left_outer_join`

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

The output table from a [`left_outer_join`](../reference/table-operations/join/left-outer-join.md) contains every row that `join` would produce, plus each left-table row that has no match, with null values in the right table's columns. In this example, Marketing appears with null employee columns:

```python test-set=1 order=result
from deephaven.experimental.outer_joins import left_outer_join

result = left_outer_join(l_table=departments, r_table=employees, on=["DeptID"])
```

### `full_outer_join`

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

The output table from a [`full_outer_join`](../reference/table-operations/join/full-outer-join.md) contains every row that `left_outer_join` would produce, plus each right-table row that has no match, with null values in the left table's columns. In this example, Marketing appears with null employee columns, and Rogers and DelaCruz appear with null department names:

```python test-set=1 order=result
from deephaven.experimental.outer_joins import full_outer_join

result = full_outer_join(l_table=departments, r_table=employees, on=["DeptID"])
```

## Join three or more tables

[`multi_join`](../reference/table-operations/join/multi-join.md) joins any number of tables on a common set of key columns in a single operation. The result has one row for each distinct key found in any input table. Each input table adds its columns to that row. As with [`natural_join`](../reference/table-operations/join/natural-join.md) in its default mode, an input table can have at most one row per key, and `multi_join` fails if an input has duplicate keys. An input table with no row for a key contributes null values.

`multi_join` returns a [`MultiJoinTable`](/core/pydoc/code/deephaven.table.html#deephaven.table.MultiJoinTable) object rather than a table. To get the result table, use its `table` property.

There are two ways to call `multi_join`:

- **With constituent tables**: Pass the tables to join directly. These input tables are called _constituent tables_. Every constituent table must use the same key column names, and the result includes every non-key column from every constituent table.
- **With `MultiJoinInput` objects**: Pass a list of [`MultiJoinInput`](/core/pydoc/code/deephaven.table.html#deephaven.table.MultiJoinInput) objects. Each `MultiJoinInput` specifies one table, the mapping from its key columns to the result's key columns, and the columns to add from it. The columns to add are optional.

### With constituent tables

To use constituent tables, pass them as a list in `input`, and pass a key column name or a list of key column names in `on`:

```python syntax
multi_table = multi_join(input=[table1, table2, table3], on="CommonKeyColumn")
multi_table = multi_join(
    input=[table1, table2, table3], on=["CommonKeyCol1", "CommonKeyCol2"]
)
```

The following example joins three tables of letter grades for students in grades 5, 6, and 7. Not every student appears in every table, so some grades in the result are null:

```python test-set=2 order=result,grade5,grade6,grade7
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

multijoin_table = multi_join(input=[grade5, grade6, grade7], on=["Name"])

result = multijoin_table.table
```

### With `MultiJoinInput` objects

Use `MultiJoinInput` objects when the key columns have different names in different tables, or when you want only some of a table's columns in the result. Create one `MultiJoinInput` per table, and then pass the list to `multi_join` without the `on` argument:

```python syntax
from deephaven.table import MultiJoinInput, multi_join

multijoin_input = [
    # Include all non-key columns from t1
    MultiJoinInput(table=t1, on="KeyColumn"),
    # Match t2's OtherKey column to KeyColumn, and include only Column1 and Column2
    MultiJoinInput(table=t2, on="KeyColumn = OtherKey", joins=["Column1", "Column2"]),
]

multi_table = multi_join(input=multijoin_input)
```

The following example adds each student's club to the grades from the [previous example](#with-constituent-tables). The `clubs` table names its key column `Student` instead of `Name`, and the result includes only its `Club` column:

```python test-set=2 order=result,clubs
from deephaven.table import MultiJoinInput, multi_join
from deephaven import new_table
from deephaven.column import string_col

clubs = new_table(
    [
        string_col("Student", ["Andy", "Kathy", "Mark", "June"]),
        string_col("Club", ["Chess", "Drama", "Robotics", "Chess"]),
        string_col("Advisor", ["Lee", "Ortiz", "Patel", "Lee"]),
    ]
)

multijoin_table = multi_join(
    input=[
        MultiJoinInput(table=grade5, on="Name"),
        MultiJoinInput(table=grade6, on="Name"),
        MultiJoinInput(table=grade7, on="Name"),
        MultiJoinInput(table=clubs, on="Name = Student", joins="Club"),
    ]
)

result = multijoin_table.table
```

## Related documentation

- [Inexact, time-series, and range joins](./joins-timeseries-range.md)
- [`exact_join`](../reference/table-operations/join/exact-join.md)
- [`full_outer_join`](../reference/table-operations/join/full-outer-join.md)
- [`join`](../reference/table-operations/join/join.md)
- [`left_outer_join`](../reference/table-operations/join/left-outer-join.md)
- [`multi_join`](../reference/table-operations/join/multi-join.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
