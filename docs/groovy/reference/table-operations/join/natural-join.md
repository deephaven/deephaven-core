---
title: naturalJoin
---

`naturalJoin` joins data from a pair of tables - a left and right table - based upon one or more match columns (`columnsToMatch`). The match columns establish key identifiers in the left table that will be used to find data in the right table. Any data type which implements Java equality and hashCode can be used as a key.

The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table. For columns appended to the left table (`columnsToAdd`), row values equal the row values from the right table where the key values in the left and right tables are equal. If there is no matching key in the right table, appended row values are `NULL`. If there are multiple matches, the operation will fail by default but can be configured to match either the first or last matching row.

## Syntax

```
leftTable.naturalJoin(rightTable, columnsToMatch)
leftTable.naturalJoin(rightTable, columnsToMatch, columnsToAdd)
leftTable.naturalJoin(rightTable, columnsToMatch, columnsToAdd, joinType)
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

The columns from the right table to add to the left table based on key.

- `NULL` will add all columns from the right table to the left table.
- `"X"` will add column `X` from the right table to the left table as column `X`.
- `Y = X` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
<Param name="joinType" type="NaturalJoinType">

How to handle duplicate keys in the right table. If omitted, the default is to error on duplicates (`ERROR_ON_DUPLICATE`).

The following options are available:

- `ERROR_ON_DUPLICATE`: Throw an error if a duplicate right table row is found.
- `FIRST_MATCH`: Match the first right table row and ignore later duplicates. This is equivalent to calling
  [`firstBy()`](../group-and-aggregate/firstBy.md) on the right table before joining.
- `LAST_MATCH`: Match the last right table row and ignore earlier duplicates. This is equivalent to calling
  [`lastBy()`](../group-and-aggregate/lastBy.md) on the right table before joining.
- `EXACTLY_ONE_MATCH`: Match exactly one right table row; throw an error if there are zero or more than one matches.
  This is equivalent to [`exactJoin`](./exact-join.md).

</Param>
</ParamTable>

## Returns

A new table containing all of the rows and columns of the left table, plus additional columns containing data from the right table. For columns appended to the left table (`columnsToAdd`), row values equal the row values from the right table where the key values in the left and right tables are equal. If there is no matching key in the right table, appended row values are `NULL`.

## Examples

In the following example, the left and right tables are joined on a matching column named `DeptID`.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptID", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160"),
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone","(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)

result = left.naturalJoin(right, "DeptID")
```

If the right table has columns that need renaming due to an initial name match, a new column name can be supplied in the third argument of the join. In the following example, `Telephone` is renamed to `DeptTelephone`.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptID", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160")
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("Telephone","(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)

result = left.naturalJoin(right, "DeptID", "DeptName, DeptTelephone = Telephone")
```

In some cases, the matching columns have different names in the left and right table. Below, the left table has a column named `DeptNumber` that needs to be matched to the column `DeptID` in the right table. To perform this match, the second argument needs the name of each column in the left and right tables.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptNumber", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160")
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("Telephone","(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)

result = left.naturalJoin(right, "DeptNumber = DeptID", "DeptName, DeptTelephone = Telephone")
```

Handling duplicate right rows can be done by specifying the `joinType` parameter. In the following example, the `FIRST_MATCH` option is used to match the first right table row and ignore later duplicates.

```groovy order=left,right,result
import io.deephaven.api.NaturalJoinType

left = emptyTable(5).update("l_key = ii % 5", "l_index=ii")
right = emptyTable(10).update("r_key = ii % 5", "r_index=ii")

result = left.naturalJoin(right, "l_key = r_key", NaturalJoinType.FIRST_MATCH)
```

In the following example, the `LAST_MATCH` option is used to match the last right table row and ignore earlier duplicates.

```groovy order=left,right,result
import io.deephaven.api.NaturalJoinType

left = emptyTable(5).update("l_key = ii % 5", "l_index=ii")
right = emptyTable(10).update("r_key = ii % 5", "r_index=ii")

result = left.naturalJoin(right, "l_key = r_key", NaturalJoinType.LAST_MATCH)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose a join method](../../../how-to-guides/joins-exact-relational.md#which-method-should-you-use)
- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#naturalJoin(TABLE,java.lang.String))
