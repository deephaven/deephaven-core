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

> [!NOTE]
> `with_keys` only sets `keyColumns`; it does not clear an existing `uniqueKeys` attribute. If the source table was previously marked with [`with_unique_keys`](./withUniqueKeys.md), `uniqueKeys` remains `True` after calling `with_keys`, even though the new key columns may no longer be unique. To fully convert such a table, also remove the attribute with [`without_attributes`](./withoutAttributes.md): `table.with_keys(cols).without_attributes("uniqueKeys")`.

## Preserved through table operations

The `keyColumns` and `uniqueKeys` attributes are preserved by:

- [`where`](../filter/where.md), [`where_in`](../filter/where-in.md), [`where_not_in`](../filter/where-not-in.md), and [`where_one_of`](../filter/where-one-of.md)
- [`sort`](../sort/sort.md) and [`sort_descending`](../sort/sort-descending.md)
- [`reverse`](../sort/reverse.md)
- [`flatten`](../select/flatten.md)
- [`update_view`](../select/update-view.md) and [`lazy_update`](../select/lazy-update.md)
- [`natural_join`](../join/natural-join.md) and [`exact_join`](../join/exact-join.md)

Other operations that build a new result from the table's data — including [`select`](../select/select.md), [`update`](../select/update.md), [`view`](../select/view.md), [`join`](../join/join.md), and [`drop_columns`](../select/drop-columns.md) — clear both attributes. Call `with_keys` or `with_unique_keys` again on the result to restore them.

[`with_attributes`](./withAttributes.md) and [`without_attributes`](./withoutAttributes.md) are a separate case: they preserve every attribute they aren't explicitly asked to add or remove, so `keyColumns` and `uniqueKeys` survive them unless you target those specific keys.

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
