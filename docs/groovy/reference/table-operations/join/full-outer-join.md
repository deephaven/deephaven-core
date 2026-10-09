---
title: fullOuterJoin
---

`fullOuterJoin` joins data from a pair of tables - a left table and a right table - based upon one or more match columns (`columnsToMatch`). The match columns establish key identifiers in the source tables that are joined. Any data type can be used as keys (custom objects must implement consistent equality and `hashCode`).

The resultant table contains all rows from both tables that exist in the key identifier columns. Cells that exist in one table but not the other are filled with null values in the result.

> [!NOTE]
> This table operation is currently experimental. The API may change in the future.

## Syntax

```
fullOuterJoin(leftTable, rightTable, columnsToMatch)
fullOuterJoin(leftTable, rightTable, columnsToMatch, columnsToAdd)
```

## Parameters

<ParamTable>
<Param name="leftTable" type="Table">

The left table from which data is joined.

</Param>
<Param name="rightTable" type="Table">

The right table from which data is joined.

</Param>
<Param name="columnsToMatch" type="String">

Columns from the left and right tables used to join on.

- `"A = B"` will join when column `A` from the left table matches column `B` from the right table.
- `"X"` will join on column `X` from both the left and right table. Equivalent to `"X = X"`.
- `"X, A = B"` will join when column `X` matches from both the left and right tables, and when column `A` from the left table matches column `B` from the right table.

</Param>
<Param name="columnsToMatch" type="Collection<String>">

Columns from the left and right tables used to join on. Each element of the collection is a single match.

- `"A = B"` will join when column `A` from the left table matches column `B` from the right table.
- `"X"` will join on column `X` from both the left and right table. Equivalent to `"X = X"`.
- `["X", "A = B"]` will join when column `X` matches from both the left and right tables, and when column `A` from the left table matches column `B` from the right table.

</Param>
<Param name="columnsToAdd" type="String">

The columns from the right table to add to the left table based on key.

- If this argument is omitted or left empty (`""`), all columns from the right table are added to the left table, except right table columns whose names match a left table column in `columnsToMatch`.
- `"X"` will add column `X` from the right table to the left table as column `X`.
- `Y = X` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
<Param name="columnsToAdd" type="Collection<String>">

The columns from the right table to add to the left table based on key.

- An empty collection adds all columns from the right table to the left table, except right table columns whose names match a left table column in `columnsToMatch`.
- `"X"` will add column `X` from the right table to the left table as column `X`.
- `Y = X` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
</ParamTable>

## Returns

A new table containing all rows from the left and right table. A row that has no match in the other table is included once, with null values in the columns from the other table. If there are multiple matches between a row from the left table and rows from the right table, all matching combinations will be included. If no match columns are specified, every combination of left and right table rows is included; if one table is empty, each row of the other table is included once with null values in the columns from the empty table.

## Examples

The following example creates two source tables and performs a `fullOuterJoin` on them. It gives no `columnsToAdd`, so all columns from the `right` table appear in the `result` table.

```groovy order=left,right,result
import io.deephaven.engine.util.TableTools
import io.deephaven.engine.util.OuterJoinTools

left = TableTools.emptyTable(10).update("I = ii % 5 + 1", "A = `left` + ii")
right = TableTools.emptyTable(10).update("I = ii % 3", "B = `right` + ii", "C = Math.sin(I)")

result = OuterJoinTools.fullOuterJoin(left, right, "I")
```

The following example creates two source tables and performs a `fullOuterJoin` on them. It specifies `C` as the only `columnsToAdd`, so `C` is the only column from the `right` table that gets added to `result`.

```groovy order=left,right,result
import io.deephaven.engine.util.TableTools
import io.deephaven.engine.util.OuterJoinTools

left = TableTools.emptyTable(10).update("I = ii % 5 + 1", "A = `left` + ii")
right = TableTools.emptyTable(10).update("I = ii % 3", "B = `right` + ii", "C = Math.sin(I)")

result = OuterJoinTools.fullOuterJoin(left, right, "I", "C")
```

The example below shows how to [join tables on match columns with different names](../../../how-to-guides/joins-exact-relational.md#match-columns-with-different-names) and [rename appended columns](../../../how-to-guides/joins-exact-relational.md#rename-joined-columns) when performing a `fullOuterJoin`.

```groovy order=left,right,result
import io.deephaven.engine.util.TableTools
import io.deephaven.engine.util.OuterJoinTools

left = TableTools.emptyTable(10).update("X1 = ii % 5 + 1", "Y = Math.sin(X1)")
right = TableTools.emptyTable(10).update("X2 = ii % 3", "Y = Math.cos(X2)")

result = OuterJoinTools.fullOuterJoin(left, right, "X1 = X2", "Z = Y")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose a join method](../../../how-to-guides/joins-exact-relational.md#which-method-should-you-use)
- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/util/OuterJoinTools.html#fullOuterJoin(io.deephaven.engine.table.Table,io.deephaven.engine.table.Table,java.util.Collection))
