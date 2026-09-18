---
title: with_unique_keys
---

The `with_unique_keys` method returns a table that shares the underlying data and schema with the source table, but with the specified columns marked as its key columns, additionally indicating that each combination of key values identifies exactly one row.

Key columns are metadata used by consumers such as the Deephaven UI to identify which rows represent the same real-world entity. They do not affect the table's data. Use `with_unique_keys` when the key columns form a true primary key, so that selecting a row in the Deephaven UI never selects any other row. If more than one row can share the same key values, use [`with_keys`](./withKeys.md) instead — but note that `with_keys` does not clear the `uniqueKeys` attribute on its own; see the note on [`with_keys`](./withKeys.md) for converting a table that already has it set.

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

## Preserved through table operations

The `keyColumns` and `uniqueKeys` attributes are preserved by:

- [`where`](../filter/where.md), [`where_in`](../filter/where-in.md), [`where_not_in`](../filter/where-not-in.md), and [`where_one_of`](../filter/where-one-of.md)
- [`sort`](../sort/sort.md) and [`sort_descending`](../sort/sort-descending.md)
- [`reverse`](../sort/reverse.md)
- [`flatten`](../select/flatten.md)
- [`update_view`](../select/update-view.md) and [`lazy_update`](../select/lazy-update.md)
- [`natural_join`](../join/natural-join.md) and [`exact_join`](../join/exact-join.md)

Every other operation, including [`select`](../select/select.md), [`update`](../select/update.md), [`view`](../select/view.md), [`join`](../join/join.md), and [`drop_columns`](../select/drop-columns.md), clears both attributes. Call `with_keys` or `with_unique_keys` again on the result to restore them.

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
