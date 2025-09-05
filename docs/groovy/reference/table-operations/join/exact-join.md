---
title: exactJoin
---

`exactJoin` joins data from a pair of tables - a left and right table - based upon a set of match columns (`columnsToMatch`). The match columns establish key identifiers in the left table that will be used to find data in the right table. Any data types can be chosen as keys, and keys can be constructed from multiple values.

The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table. For columns appended to the left table (`columnsToAdd`), row values equal the row values from the right table where the key values in the left and right tables are equal. If there are zero or multiple matches, the operation will fail.

## Syntax

```
leftTable.exactJoin(rightTable, columnsToMatch)
leftTable.exactJoin(rightTable, columnsToMatch, columnsToAdd)
```

## Parameters

<ParamTable>
<Param name="rightTable" type="Table">

The table data is added from.

</Param>
<Param name="columnsToMatch" type="String">

Columns from the left and right tables used to join on.

- `"A = B"` will join when column `A` from the left table matches column `B` from the right table.
- `"X"` will join on column `X` from both the left and right table. Equivalent to `"X = X"`.
- `"X, A = B"` will join when column `X` matches from both the left and right tables, and when column `A` from the left table matches column `B` from the right table.

</Param>
<Param name="columnsToAdd" type="String">

The columns from the right table to be added to the left table based on key.

- `NULL` will add all columns from the right table to the left table.
- `"X"` will add column `X` from the right table to the left table as column `X`.
- `"Y = X"` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
</ParamTable>

## Returns

A new table containing all of the rows and columns of the left table plus additional columns containing data from the right table. For columns appended to the left table (`columnsToAdd`), row values equal the row values from the right table where the key values in the left and right tables are equal.

## Examples

In the following example, the left and right tables are joined by the matching column, `DeptID`.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith"),
    intCol("DeptID", 31, 33, 33, 34, 34),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", ""),
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)

result = left.exactJoin(right, "DeptID")
```

If the right table has columns that need renaming due to an initial name match, a new column name can be supplied in the third argument of the join. In the following example, `Telephone` is renamed to `DeptTelephone`.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith"),
    intCol("DeptID", 31, 33, 33, 34, 34),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", ""),
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("Telephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)


result = left.exactJoin(right, "DeptID", "DeptName, DeptTelephone = Telephone")
```

In some cases, the matching columns have different names in the left and right table. Below, the left table has a column named `DeptNumber` that needs to be matched to the column `DeptID` in the right table. To perform this match, the second argument needs the name of each column in the left and right tables.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith"),
    intCol("DeptNumber", 31, 33, 33, 34, 34),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", ""),
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("Telephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)

result = left.exactJoin(right, "DeptNumber = DeptID", "DeptName, DeptTelephone = Telephone")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose a join method](../../../how-to-guides/joins-exact-relational.md#which-method-should-you-use)
- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#exactJoin(TABLE,java.lang.String))
