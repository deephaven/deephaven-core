---
title: with_unique_keys
---

The `with_unique_keys` method returns a table that shares the underlying data and schema with the source table, but with the specified columns marked as its key columns, additionally indicating that each combination of key values identifies exactly one row.

Key columns are metadata used by consumers such as the Deephaven UI to identify which rows represent the same real-world entity. They do not affect the table's data. Use `with_unique_keys` when the key columns form a true primary key, so that selecting a row in the Deephaven UI never selects any other row. If more than one row can share the same key values, use [`with_keys`](./withKeys.md) instead.

`with_unique_keys` does not verify that the key values are actually unique; it only records that assumption for consumers of the table.

## Syntax

```
with_unique_keys(cols: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]">

The key column name(s). Must name at least one existing column in the source table.

</Param>
</ParamTable>

## Returns

A table that shares the underlying data and schema with the source table, with the `keyColumns` attribute set to the specified column names and the `uniqueKeys` attribute set to `True`. If the source table already has this exact set of unique key columns, the source table itself may be returned.

## Examples

In this example, every row of `unique_keyed_table` has a distinct `Key1` value, so selecting a row in the Deephaven UI tracks only that row.

```python order=null
from deephaven import empty_table

not_keyed = empty_table(100).update(["Key1=i", "Key2=i+1", "Value=i*2"])
unique_keyed_table = not_keyed.with_unique_keys(["Key1", "Key2"])
```

![Unique keyed row selection](../../../assets/how-to/keyed-row-selection-unique.png)

## Related documentation

- [Keyed row selection](../../../how-to-guides/keyed-row-selection.md)
- [`with_keys`](./withKeys.md)
- [`with_attributes`](./withAttributes.md)
- [`attributes`](../metadata/attributes.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_unique_keys)
