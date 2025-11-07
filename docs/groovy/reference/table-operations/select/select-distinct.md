---
title: selectDistinct
---

The `selectDistinct` method creates a new table containing all of the unique values for a set of key columns.

When the `selectDistinct` method is used on multiple columns, it looks for distinct sets of values in the selected columns.

## Syntax

```
table.selectDistinct()
table.selectDistinct(columns...)
```

## Parameters

<ParamTable>
<Param name="columns" type="String...">

Key columns from which distinct values will be selected.

</Param>
<Param name="columns" type="Selectable...">

Key columns from which distinct values will be selected.

</Param>
<Param name="columns" type="Collection<? extends Selectable>">

Key columns from which distinct values will be selected.

</Param>
</ParamTable>

## Returns

A new table containing all of the unique values for a set of key columns.

## Examples

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "grape"),
    intCol("B", 1, 1, 2, 2, 3, 3),
)

result = source.selectDistinct("A")
```

```groovy order=source,result
source = newTable(
    stringCol("A", "apple", "apple", "orange", "orange", "plum", "grape"),
    intCol("B", 1, 1, 2, 2, 3, 3),
)

result = source.selectDistinct("A", "B")
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#selectDistinct(java.lang.String...))
