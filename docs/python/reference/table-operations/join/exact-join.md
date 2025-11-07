---
title: exact_join
---

`exact_join` joins data from a pair of tables - a left and right table - based upon a set of match columns. The match columns establish key identifiers in the left table that will be used to find data in the right table. Any data types can be chosen as keys, and keys can be constructed from multiple values.

The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table. For columns appended to the left table, row values equal the row values from the right table where the key values in the left and right tables are equal. If there are zero or multiple matches, the operation will fail.

## Syntax

```
left.exact_join(
    right: Table,
    on: Union[str, Sequence[str]],
    joins: Union[str, Sequence[str]] = None
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
</ParamTable>

## Returns

A new table containing all of the rows and columns of the left table plus additional columns containing data from the right table. For columns appended to the left table, row values equal the row values from the right table where the key values in the left and right tables are equal.

## Examples

In the following example, the left and right tables are joined by the matching column, `DeptID`.

```python order=left,right,result
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

If the right table has columns that need renaming due to an initial name match, a new column name can be supplied in the third argument of the join. In the following example, `Telephone` is renamed to `DeptTelephone`.

```python order=left,right,result
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
            "Telephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)

result = left.exact_join(
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
        string_col("LastName", ["Rafferty", "Jones", "Steiner", "Robins", "Smith"]),
        int_col("DeptNumber", [31, 33, 33, 34, 34]),
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
            "Telephone",
            ["(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171"],
        ),
    ]
)

result = left.exact_join(
    table=right,
    on=["DeptNumber = DeptID"],
    joins=["DeptName, DeptTelephone = Telephone"],
)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [Joins: Exact and Relational](../../../how-to-guides/joins-exact-relational.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#exactJoin(TABLE,java.lang.String))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.exact_join)
