---
title: with_keys
---

The `with_keys` method returns a table that shares the underlying data and schema with the source table, but with the specified columns marked as its key columns.

Key columns are metadata used by consumers such as the Deephaven UI to identify which rows represent the same real-world entity. They do not affect the table's data. Multiple rows may share the same key column values. To additionally assert that each combination of key values identifies exactly one row, use [`with_unique_keys`](./withUniqueKeys.md) instead.

## Syntax

```
with_keys(cols: Union[str, Sequence[str]]) -> Table
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]">

The key column name(s). Must name at least one existing column in the source table.

</Param>
</ParamTable>

## Returns

A table that shares the underlying data and schema with the source table, with the `keyColumns` attribute set to the specified column names. If the source table already has this exact set of key columns, the source table itself may be returned.

## Examples

In this example, `Key1` and `Key2` together form the key column set for `keyed_table`. Because the same combination of values repeats across rows, selecting one of them in the Deephaven UI selects every row that shares that combination.

```python order=null
from deephaven import empty_table

not_keyed = empty_table(100).update(["Key1=i%3", "Key2=(i+1)%3", "Value=i"])
keyed_table = not_keyed.with_keys(["Key1", "Key2"])
```

![Keyed row selection](../../../assets/how-to/keyed-row-selection.png)

## Related documentation

- [Keyed row selection](../../../how-to-guides/keyed-row-selection.md)
- [`with_unique_keys`](./withUniqueKeys.md)
- [`with_attributes`](./withAttributes.md)
- [`attributes`](../metadata/attributes.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.with_keys)
