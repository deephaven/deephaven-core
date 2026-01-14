---
title: join
slug: ./join
---

`join` joins data from a pair of tables - a left and right table - based upon a set of match columns (`columnsToMatch`). The match columns establish key identifiers in the left table that will be used to find data in the right table. Any data types can be chosen as keys, and keys can be constructed from multiple values.

The output table contains rows that have matching values in both tables. Rows that do not have matching criteria will not be included in the result. If there are multiple matches between a row from the left table and rows from the right table, all matching combinations will be included. If no `columnsToMatch` are specified (`""`), every combination of left and right table rows is included.

## Syntax

```
leftTable.join(rightTable, columnsToMatch)
leftTable.join(rightTable, columnsToMatch, columnsToAdd)
leftTable.join(rightTable, columnsToMatch, columnsToAdd, reserveBits)
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
- If this argument is left empty (`""`), a cross-join is performed. The result is a table with every possible combination of rows from the two tables.

</Param>
<Param name="columnsToAdd" type="String">

The columns from the right table to be added to the left table based on key.

- `NULL` will add all columns from the right table to the left table.
- `"X"` will add column `X` from the right table to the left table as column `X`.
- `"Y = X"` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
<Param name="reserveBits" type="int">

The number of bits to reserve for rightTable groups. If the maximum size of a right table's group is small, reserve fewer bits by setting reserveBits on initialization.

If this parameter is used, the `columnsToMatch` and `columnsToAdd` parameters must be a `JoinMatch` and `JoinAddition` respectively.

</Param>
</ParamTable>

## Returns

A new table containing rows that have matching values in both tables. Rows that do not have matching criteria will not be included in the result. If there are multiple matches between a row from the left table and rows from the right table, all matching combinations will be included. If no `columnsToMatch` are specified (`""`), every combination of left and right table rows is included.

## Examples

In the following example, the left and right tables are joined on a matching column named `DeptID`.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptID", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160")
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone","(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)

result = left.join(right, "DeptID")
```

The `left` table has seven rows of data and the `right` table has four rows of data, but the `result` table has five rows. This is because the last two rows of the `left` table and the last row of the `right` table have no matches in the `DeptID` column, so they are not included in the resulting table.

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
    stringCol("Telephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)


result = left.join(right, "DeptID", "DeptName, DeptTelephone = Telephone")
```

In the following example, the left and right tables have multiple matches. The result is the cross product of possible outcomes.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptID", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160"),
)

right = newTable(
    intCol("DeptID", 31, 31, 33, 34, 35, NULL_INT),
    stringCol("DeptName", "Sales", "Support", "Engineering", "Clerical", "Marketing", "Safety"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0187", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171", "(303) 555-0145",)
)

result = left.join(right, "DeptID")
```

In some cases, the matching columns have different names in the left and right table. Below, the left table has a column name `DeptNumber` that we want to match to the colomn `DeptID` in the right table. To perform this match, the second argument needs the name of each column in the left and right tables. We will also set `reserveBits` to keep the table size small.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptNumber", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160")
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("Telephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)


result = left.join(right, "DeptNumber = DeptID", "DeptName")
```

This example is identical to the previous one, but we will set the `reserveBits` parameter to ensure that our table object does not take up much memory. Consequently, we must use a `JoinMatch` and `JoinAddition` for the `columnsToMatch` and `columnsToAdd` parameters rather than strings.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptNumber", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160")
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("Telephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)


result = left.join(right, JoinMatch.from("DeptNumber = DeptID"), JoinAddition.from("DeptName"), 200)
```

In some cases, the matching columns argument is absent. As a result all possible matches are joined.

```groovy order=left,right,result
left = newTable(
    stringCol("LastName", "Rafferty", "Jones", "Steiner", "Robins", "Smith", "Rogers", "DelaCruz"),
    intCol("DeptNumber", 31, 33, 33, 34, 34, 36, NULL_INT),
    stringCol("Telephone", "(303) 555-0162", "(303) 555-0149", "(303) 555-0184", "(303) 555-0125", "", "", "(303) 555-0160")
)

right = newTable(
    intCol("DeptID", 31, 33, 34, 35),
    stringCol("DeptName", "Sales", "Engineering", "Clerical", "Marketing"),
    stringCol("DeptTelephone", "(303) 555-0136", "(303) 555-0162", "(303) 555-0175", "(303) 555-0171")
)


result = left.join(right, "")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose a join method](../../../how-to-guides/joins-exact-relational.md#which-method-should-you-use)
- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#join(TABLE))
