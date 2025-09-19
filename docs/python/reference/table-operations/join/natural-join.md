---
title: natural_join
---

`natural_join` joins data from a pair of tables - a left and right table - based upon one or more match columns. The match columns establish key identifiers in the left table that will be used to find data in the right table. Any data type which implements Java equality and hashCode can be used as a key.

The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table. For columns appended to the left table, row values equal the row values from the right table where the key values in the left and right tables are equal. If there is no matching key in the right table, appended row values are `NULL`. If there are multiple matches, the operation will fail by default but can be configured to match either the first or last matching row.

## Syntax

```python syntax
left.natural_join(
    right: Table,
    on: Union[str, Sequence[str]],
    joins: Union[str, Sequence[str]] = None,
    type: NaturalJoinType = NaturalJoinType.ERROR_ON_DUPLICATE
) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The table data is added from (the right table).

</Param>
<Param name="on" type="Union[str, Sequence[str]]">

Columns from the left and right tables used to join on.

- `["A = B"]` will join when column `A` from the left table matches column `B` from the right table.
- `["X"]` will join on column `X` from both the left and right table. Equivalent to `"X = X"`.
- `["X, A = B"]` will join when column `X` matches from both the left and right tables, and when column `A` from the left table matches column `B` from the right table.

</Param>
<Param name="joins" type="Union[str, Sequence[str]]" optional>

Columns from the right table to be added to the left table based on key may be specified in this list:

- `[]` will add all columns from the right table to the left table (default).
- `["X"]` will add column `X` from the right table to the left table as column `X`.
- `["Y = X"]` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
<Param name="type" type="NaturalJoinType" optional>

How to handle duplicate keys in the right table. If omitted, the default is to error on duplicates (`ERROR_ON_DUPLICATE`).

The following options are available:

- `ERROR_ON_DUPLICATE`: Throw an error if a duplicate right table row is found.
- `FIRST_MATCH`: Match the first right table row and ignore later duplicates. This is equivalent to calling
  [`first_by()`](../group-and-aggregate/firstBy.md) on the right table's key columns before joining.
- `LAST_MATCH`: Match the last right table row and ignore earlier duplicates. This is equivalent to calling
  [`last_by()`](../group-and-aggregate/lastBy.md) on the right table's key columns before joining.
- `EXACTLY_ONE_MATCH`: Match exactly one right table row; throw an error if there are zero or more than one matches.
  This is equivalent to [`exact_join`](./exact-join.md).

</Param>
</ParamTable>

## Returns

A new table containing all of the rows and columns of the left table, plus additional columns containing data from the right table. For columns appended to the left table, row values equal the row values from the right table where the key values in the left and right tables are equal. If there is no matching key in the right table, appended row values are `NULL`.

## Examples

In the following example, the left and right tables are joined on a matching column named `DeptID`.

```python order=left,right,result
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

If the right table has columns that need renaming due to an initial name match, a new column name can be supplied in the third argument of the join. In the following example, `Telephone` in the right table is renamed to `DeptTelephone`.

```python order=left,right,result
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
            "Telephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)

result = left.natural_join(
    table=right, on=["DeptID"], joins=["DeptName, DeptTelephone = Telephone"]
)
```

In some cases, the matching columns have different names in the left and right table. Below, the left table has a column named `DeptNumber` that needs to be matched to the column `DeptID` in the right table. To perform this match, the second argument needs the name of each column in the left and right tables.

```python order=left,right,result
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

left = new_table(
    [
        string_col(
            "LastName",
            ["Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"],
        ),
        int_col("DeptNumber", [31, 33, 33, 34, 34, 36, NULL_INT]),
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
            "Telephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)

result = left.natural_join(
    table=right,
    on=["DeptNumber = DeptID"],
    joins=["DeptName, DeptTelephone = Telephone"],
)
```

Handling duplicate right table rows can be done by specifying the `type` parameter. In the following example, the `FIRST_MATCH` option is used to match the first right table rows and ignore later duplicates.

```python order=left,right,result
from deephaven import empty_table
from deephaven.table import NaturalJoinType

left = empty_table(5).update(["l_key = ii % 5", "l_index=ii"])
right = empty_table(10).update(["r_key = ii % 5", "r_index=ii"])

result = left.natural_join(
    table=right, on=["l_key = r_key"], type=NaturalJoinType.FIRST_MATCH
)
```

In the following example, the `LAST_MATCH` option is used to match the last right table rows and ignore earlier duplicates.

```python order=left,right,result
from deephaven import empty_table
from deephaven.table import NaturalJoinType

left = empty_table(5).update(["l_key = ii % 5", "l_index=ii"])
right = empty_table(10).update(["r_key = ii % 5", "r_index=ii"])

result = left.natural_join(
    table=right, on=["l_key = r_key"], type=NaturalJoinType.LAST_MATCH
)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Joins: Exact and Relational](../../../how-to-guides/joins-exact-relational.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#naturalJoin(TABLE,java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.natural_join)
