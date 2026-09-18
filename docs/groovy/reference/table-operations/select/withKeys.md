---
title: withKeys
---

The `withKeys` method returns a table that shares the underlying data and schema with the source table, but with the specified columns marked as its key columns.

Key columns are metadata used by consumers such as the Deephaven UI to identify which rows represent the same real-world entity. They do not affect the table's data. Multiple rows may share the same key column values. To additionally assert that each combination of key values identifies exactly one row, use [`withUniqueKeys`](./withUniqueKeys.md) instead.

## Syntax

```groovy syntax
table.withKeys(columns...)
```

## Parameters

<ParamTable>
<Param name="columns" type="String...">

The key column name(s). Must name at least one existing column in the source table.

</Param>
</ParamTable>

## Returns

A table that shares the underlying data and schema with the source table, with the `keyColumns` attribute set to the specified column names. If the source table already has this exact set of key columns, the source table itself may be returned.

> [!NOTE]
> `withKeys` only sets `keyColumns`; it does not clear an existing `uniqueKeys` attribute. If the source table was previously marked with [`withUniqueKeys`](./withUniqueKeys.md), `uniqueKeys` remains `true` after calling `withKeys`, even though the new key columns may no longer be unique. To fully convert such a table, also remove the attribute: `table.withKeys(columns).withoutAttributes(["uniqueKeys"])`.

## Preserved through table operations

The `keyColumns` and `uniqueKeys` attributes are preserved by:

- [`where`](../filter/where.md), [`whereIn`](../filter/where-in.md), and [`whereNotIn`](../filter/where-not-in.md)
- [`sort`](../sort/sort.md) and [`sortDescending`](../sort/sort-descending.md)
- [`reverse`](../sort/reverse.md)
- [`flatten`](../create/flatten.md)
- [`updateView`](./update-view.md) and [`lazyUpdate`](./lazy-update.md)
- [`naturalJoin`](../join/natural-join.md) and [`exactJoin`](../join/exact-join.md)

Every other operation, including [`select`](./select.md), [`update`](./update.md), [`view`](./view.md), [`join`](../join/join.md), and [`dropColumns`](./drop-columns.md), clears both attributes. Call `withKeys` or `withUniqueKeys` again on the result to restore them.

## Examples

In this example, `Key1` and `Key2` together form the key column set for `keyedTable`. Because the same combination of values repeats across rows, selecting one of them in the Deephaven UI selects every row that shares that combination.

```groovy order=null
notKeyed = emptyTable(100).update("Key1=i%3", "Key2=(i+1)%3", "Value=i")
keyedTable = notKeyed.withKeys("Key1", "Key2")
```

![Keyed row selection](../../../assets/how-to/keyed-row-selection.png)

## Related documentation

- [Keyed row selection](../../../how-to-guides/keyed-row-selection.md)
- [`withUniqueKeys`](./withUniqueKeys.md)
- [`withAttributes`](./withAttributes.md)
- [`getAttributes`](../metadata/getAttributes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#withKeys(java.lang.String...))
