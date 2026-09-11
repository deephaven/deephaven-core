---
title: Keyed row selection
sidebar_label: Keyed row selection
---

This guide shows you how to use key columns to enable row-level selection in the Deephaven UI. Key columns annotate a table so that the UI knows which columns uniquely (or non-uniquely) identify each row. This allows users to select and track individual rows as the table updates.

Key columns are metadata — they do not change the data in the table, only how the UI interacts with it.

## `with_keys`

`with_keys` sets one or more columns as the key columns for a table. Multiple rows may share the same key values. When a user selects a row, the UI tracks _all_ rows with the same key column values.

### Syntax

```python syntax
table.with_keys(cols)
```

### Parameters

| Parameter | Type                        | Description                                     |
| --------- | --------------------------- | ----------------------------------------------- |
| `cols`    | `Union[str, Sequence[str]]` | One or more column names to use as key columns. |

### Returns

A copy of the table with the key columns attribute set.

### Example

```python
from deephaven import empty_table

not_keyed = empty_table(100).update(["Key1=i%3", "Key2=(i+1)%3", "Value=i"])
keyed_table = not_keyed.with_keys(["Key1", "Key2"])
```

In this example, `Key1` and `Key2` together form the key. Because values repeat (e.g., `Key1=0, Key2=1` appears many times), selecting any one row highlights all rows sharing that key combination.

![Keyed row selection](../assets/how-to/keyed-row-selection.png)

## `with_unique_keys`

`with_unique_keys` sets one or more columns as key columns _and_ declares that each combination of key values identifies exactly one row. When a user selects a row, only that single row is tracked.

Use `with_unique_keys` when your key columns form a true primary key — i.e., no two rows share the same key values.

### Syntax

```python syntax
table.with_unique_keys(cols)
```

### Parameters

| Parameter | Type                        | Description                                            |
| --------- | --------------------------- | ------------------------------------------------------ |
| `cols`    | `Union[str, Sequence[str]]` | One or more column names to use as unique key columns. |

### Returns

A copy of the table with the key columns and unique keys attributes set.

### Example

```python
from deephaven import empty_table

not_keyed = empty_table(100).update(["Key1=i", "Key2=i+1", "Value=i*2"])
unique_keyed_table = not_keyed.with_unique_keys(["Key1", "Key2"])
```

In this example, every row has a distinct `Key1` value, so selecting a row tracks only that row.

![Unique keyed row selection](../assets/how-to/keyed-row-selection-unique.png)

## Key column attributes

Under the hood, `with_keys` and `with_unique_keys` set table attributes:

- `"keyColumns"` — a comma-separated list of key column names.
- `"uniqueKeys"` — set to `True` by `with_unique_keys` to signal that keys are unique.

These attributes are preserved through the following operations:

- `filter`
- `sort`
- `reverse`
- `flatten`
- `update_view`
- `natural_join`
- `would_match`

Any other operation (e.g., `select`, `update`, `join`, `drop_columns`) will drop the key column attributes. If you need key columns after such an operation, call `with_keys` or `with_unique_keys` again on the result.

## Related documentation

- [`with_attributes`](../reference/table-operations/create/withAttributes.md)
- [`attributes`](../reference/table-operations/metadata/attributes.md)
