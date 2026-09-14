---
title: Keyed row selection
sidebar_label: Keyed row selection
---

This guide shows you how to use key columns to enable row-level selection in the Deephaven UI. Key columns annotate a table so that the UI knows which columns uniquely (or non-uniquely) identify each row. This allows users to select and track individual rows as the table updates.

Key columns are metadata — they do not change the data in the table, only how the UI interacts with it.

## `withKeys`

`withKeys` sets one or more columns as the key columns for a table. Multiple rows may share the same key values. When a user selects a row, the UI tracks _all_ rows with the same key column values.

### Syntax

```groovy syntax
table.withKeys(columns...)
```

### Parameters

| Parameter | Type     | Description                                     |
| --------- | -------- | ----------------------------------------------- |
| `columns` | `String` | One or more column names to use as key columns. |

### Returns

A copy of the table with the key columns attribute set.

### Example

```groovy order=null
notKeyed = emptyTable(100).update("Key1=i%3", "Key2=(i+1)%3", "Value=i")
keyedTable = notKeyed.withKeys("Key1", "Key2")
```

In this example, `Key1` and `Key2` together form the key. Because values repeat (e.g., `Key1=0, Key2=1` appears many times), selecting any one row highlights all rows sharing that key combination.

![Keyed row selection](../assets/how-to/keyed-row-selection.png)

## `withUniqueKeys`

`withUniqueKeys` sets one or more columns as key columns _and_ declares that each combination of key values identifies exactly one row. When a user selects a row, only that single row is tracked.

Use `withUniqueKeys` when your key columns form a true primary key — i.e., no two rows share the same key values.

### Syntax

```groovy syntax
table.withUniqueKeys(columns...)
```

### Parameters

| Parameter | Type     | Description                                            |
| --------- | -------- | ------------------------------------------------------ |
| `columns` | `String` | One or more column names to use as unique key columns. |

### Returns

A copy of the table with the key columns and unique keys attributes set.

### Example

```groovy order=null
notKeyed = emptyTable(100).update("Key1=i", "Key2=i+1", "Value=i*2")
uniqueKeyedTable = notKeyed.withUniqueKeys("Key1", "Key2")
```

In this example, every row has a distinct `Key1` value, so selecting a row tracks only that row.

![Unique keyed row selection](../assets/how-to/keyed-row-selection-unique.png)

## Key column attributes

Under the hood, `withKeys` and `withUniqueKeys` set table attributes:

- `KEY_COLUMNS_ATTRIBUTE` — a comma-separated list of key column names.
- `UNIQUE_KEYS_ATTRIBUTE` — set to `true` by `withUniqueKeys` to signal that keys are unique.

These attributes are preserved through the following operations:

- `filter`
- `sort`
- `reverse`
- `flatten`
- `updateView`
- `naturalJoin`
- `wouldMatch`

Any other operation (e.g., `select`, `update`, `join`, `dropColumns`) will drop the key column attributes. If you need key columns after such an operation, call `withKeys` or `withUniqueKeys` again on the result.

## Related documentation

- [`withAttributes`](../reference/table-operations/select/withAttributes.md)
- [`getAttribute`](../reference/table-operations/metadata/getAttribute.md)
- [`getAttributes`](../reference/table-operations/metadata/getAttributes.md)
