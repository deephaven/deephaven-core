---
title: withUniqueKeys
---

The `withUniqueKeys` method returns a table that shares the underlying data and schema with the source table, but with the specified columns marked as its key columns, additionally indicating that each combination of key values identifies exactly one row.

Key columns are metadata used by consumers such as the Deephaven UI to identify which rows represent the same real-world entity. They do not affect the table's data. Use `withUniqueKeys` when the key columns form a true primary key, so that selecting a row in the Deephaven UI never selects any other row. If more than one row can share the same key values, use [`withKeys`](./withKeys.md) instead.

`withUniqueKeys` does not verify that the key values are actually unique; it only records that assumption for consumers of the table.

## Syntax

```groovy syntax
table.withUniqueKeys(columns...)
```

## Parameters

<ParamTable>
<Param name="columns" type="String...">

The key column name(s). Must name at least one existing column in the source table.

</Param>
</ParamTable>

## Returns

A table that shares the underlying data and schema with the source table, with the `keyColumns` attribute set to the specified column names and the `uniqueKeys` attribute set to `true`. If the source table already has this exact set of unique key columns, the source table itself may be returned.

## Examples

In this example, every row of `uniqueKeyedTable` has a distinct `Key1` value, so selecting a row in the Deephaven UI tracks only that row.

```groovy order=null
notKeyed = emptyTable(100).update("Key1=i", "Key2=i+1", "Value=i*2")
uniqueKeyedTable = notKeyed.withUniqueKeys("Key1", "Key2")
```

![Unique keyed row selection](../../../assets/how-to/keyed-row-selection-unique.png)

## Related documentation

- [Keyed row selection](../../../how-to-guides/keyed-row-selection.md)
- [`withKeys`](./withKeys.md)
- [`withAttributes`](./withAttributes.md)
- [`getAttributes`](../metadata/getAttributes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#withUniqueKeys(java.lang.String...))
