---
title: renameColumns
---

The `renameColumns` method creates a new table with specified columns renamed.

## Syntax

```
table.renameColumns(pairs...)
table.renameColumns(columns...)
```

## Parameters

<ParamTable>
<Param name="columns" type="String...">

Columns that will be renamed in the new table.

- `"X = Y"` will rename source column `Y` to `X`.

</Param>
<Param name="columns" type="Collection<String>">

Columns that will be renamed in the new table.

- `"X = Y"` will rename source column `Y` to `X`.

</Param>
<Param name="pairs" type="MatchPair...">

Columns that will be renamed in the new table.

- `"X = Y"` will rename source column `Y` to `X`.

</Param>
</ParamTable>

> [!IMPORTANT]
> If the new column name conflicts with an existing column name in the table, the existing column is silently replaced.

## Returns

A new table that renames the specified columns.

## Examples

The following example renames columns `A` and `C`:

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3),
)

result = source.renameColumns("Fruit = A", "Type = C")
```

The following example renames column `C` to `A`.  Because `A` already exists, it is silently replaced with the renamed `C` column:

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "plum"),
    intCol("B", 1, 1, 2, 2, 3, 3),
    stringCol("C", "Macoun", "Opal", "Navel", "Cara Cara ", "Greengage", "Mirabelle"),
    intCol("D", 1, 2, 12, 3, 2, 3),
)

result = source.renameColumns("A = C")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#renameColumns(java.lang.String...))
