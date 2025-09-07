---
title: method
---

`method` does this. Returns this.

## Syntax

<!-- all the primary options -->

## Parameters

<ParamTable>
<Param name="rightTable" type="Table">

The table data is added from.

</Param>
<Param name="columnsToMatch" type="String">

<!-- ... for multiple types, e.g., String... -->
<!-- [] for an array of values, e.g., String[] -->

Columns from the left and right tables used to join on.

- `"A = B"` will join when column `A` from the left table matches column `B` from the right table.
- `"X"` will join on column `X` from both the left and right table. Equivalent to `"X = X"`.
- `"X, A = B"` will join when column `X` matches from both the left and right tables, and when column `A` from the left table matches column `B` from the right table.

The first `N-1` match columns are exactly matched. The last match column is used to find the key values from the right table that are closest to the values in the left table without going over.

</Param>
<Param name="columnsToAdd" type="String" optional>

<!-- note optional marker above -->

The columns from the right table to be added to the left table based on key:

- `NULL` will add all columns from the right table to the left table.
- `"X"` will add column `X` from the right table to the left table as column `X`.
- `"Y = X"` will add column `X` from right table to left table and rename it to be `Y`.

</Param>
</ParamTable>

## Returns

A new table containing all of the rows and columns of the left table, plus additional columns containing data from the right table. For columns appended to the left table (`columnsToAdd`), row values equal the row values from the right table where the keys from the left table most closely match the keys from the right table, as defined above. If there is no matching key in the right table, appended row values are `NULL`.

## Examples

<!-- Examples must be self-contained, using newTable or emptyTable. In other words, users do not have to do any special steps for them to run.

One example per syntax option.

Use the variables `source` and `result` for table names. Use camelCase when applicable.
 -->

The following example...

```groovy skip-test
```

<!-- example of an image path -->

![img](../../../assets/reference/join/aj4.png)

## Related documentation

<!-- include related how-tos, concept guides, reference articles, and Javadoc / Pydoc (in that order) -->

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Choose a join method](../how-to-guides/joins-exact-relational.md#which-method-should-you-use)
- [Exact and relational joins](../../../how-to-guides/joins-exact-relational.md)
- [Time series and range joins](../../../how-to-guides/joins-timeseries-range.md)
- [Exact Join](./exact-join.md)
- [Join](./join.md)
- [Left Join](./left-join.md)
- [Natural Join](./natural-join.md)
- [Reverse As-of Join](./raj.md)
- [Javadoc](/core/javadoc/)
- [Pydoc](/core/pydoc/)
