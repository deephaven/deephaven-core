---
title: Keyed row selection
sidebar_label: Keyed row selection
---

This guide shows you how to control what happens when a user selects a row in the Deephaven UI: whether the selection follows just that one row, or every row that shares its identity.

By default, Deephaven has no notion of which column or columns make a row unique, so the UI can't tell when two rows represent the same real-world entity. Marking one or more columns as _key columns_ closes that gap. Key columns are metadata: setting them doesn't change any data in the table, only how the UI interprets a row selection.

## Select every row with a matching key

Use `with_keys` when rows can legitimately share the same key value, such as several rows that belong to the same group, and you want selecting one of them to select all of them.

```python test-set=1 order=null
from deephaven import empty_table

not_keyed = empty_table(100).update(["Key1=i%3", "Key2=(i+1)%3", "Value=i"])
keyed_table = not_keyed.with_keys(["Key1", "Key2"])
```

`Key1` and `Key2` together form the key. Because the combination `Key1=0, Key2=1` repeats across many rows, selecting any one of them in the Deephaven UI selects every row that shares that combination:

![Keyed row selection](../assets/how-to/keyed-row-selection.png)

`with_keys` records the key columns as a table attribute; it doesn't change the table's data:

```python test-set=1 order=:log
print(keyed_table.attributes())
```

See [`with_keys`](../reference/table-operations/create/withKeys.md) for the full syntax and parameter reference.

## Select a single row

If your key columns identify exactly one row apiece, a true primary key, use `with_unique_keys` instead. It sets the same key-column metadata as `with_keys`, but also tells the UI that no two rows share a key, so selecting a row never pulls in any others.

```python test-set=2 order=null
from deephaven import empty_table

not_keyed = empty_table(100).update(["Key1=i", "Key2=i+1", "Value=i*2"])
unique_keyed_table = not_keyed.with_unique_keys(["Key1", "Key2"])
```

Every row here has a distinct `Key1` value, so selecting a row tracks only that row:

![Unique keyed row selection](../assets/how-to/keyed-row-selection-unique.png)

```python test-set=2 order=:log
print(unique_keyed_table.attributes())
```

See [`with_unique_keys`](../reference/table-operations/create/withUniqueKeys.md) for the full syntax and parameter reference.

> [!NOTE]
> `with_unique_keys` doesn't verify that the key values are actually unique; it only records that assumption. If two rows do end up sharing a key, the UI treats them the same way `with_keys` would.

## Keep key columns through later operations

Key columns are just table attributes, so only specific operations carry them forward automatically. [`where`](../reference/table-operations/filter/where.md), [`sort`](../reference/table-operations/sort/sort.md), [`reverse`](../reference/table-operations/sort/reverse.md), [`flatten`](../reference/table-operations/select/flatten.md), [`update_view`](../reference/table-operations/select/update-view.md), [`natural_join`](../reference/table-operations/join/natural-join.md), and [`exact_join`](../reference/table-operations/join/exact-join.md) all preserve them. Most other operations, including [`select`](../reference/table-operations/select/select.md), [`update`](../reference/table-operations/select/update.md), [`join`](../reference/table-operations/join/join.md), and [`drop_columns`](../reference/table-operations/select/drop-columns.md), do not.

[`view`](../reference/table-operations/select/view.md) is an easy one to trip over: it looks like `update_view`'s sibling, but it does _not_ preserve key columns, while `update_view` does.

If an operation you need drops the key columns, call `with_keys` or `with_unique_keys` again on the result:

```python order=:log
from deephaven import empty_table

source = empty_table(10).update(["Key=i", "Value=i*2"]).with_unique_keys("Key")
print(source.attributes())

after_select = source.select(["Key", "Value"])
print(after_select.attributes())

restored = after_select.with_unique_keys("Key")
print(restored.attributes())
```

See [`with_keys`](../reference/table-operations/create/withKeys.md) and [`with_unique_keys`](../reference/table-operations/create/withUniqueKeys.md) for the exhaustive list of operations that preserve these attributes.

## Related documentation

- [Access table metadata](./metadata.md)
- [`with_attributes`](../reference/table-operations/create/withAttributes.md)
- [`with_keys`](../reference/table-operations/create/withKeys.md)
- [`with_unique_keys`](../reference/table-operations/create/withUniqueKeys.md)
- [`attributes`](../reference/table-operations/metadata/attributes.md)
