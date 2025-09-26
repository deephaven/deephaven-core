---
title: has_data_index
---

`has_data_index` checks if a table has a [`DataIndex`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex) for the given key column(s).

## Syntax

```python syntax
has_data_index(table: Table, key_cols: List[str]) -> bool
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The table to check.

</Param>
<Param name="key_cols" type="List[str]">

The names of the key column(s) indexed.

</Param>
</ParamTable>

## Returns

A `bool` indicating whether or not the table has a `DataIndex` for the given key column(s).

## Examples

The following example checks if a table has an index for one or two key columns.

```python order=:log,source
from deephaven.experimental.data_index import data_index, has_data_index
from deephaven import empty_table

source = empty_table(10).update(["Key1 = i % 3", "Key2 = i % 2", "Value = i"])

index = data_index(source, ["Key1"])

print(has_data_index(source, ["Key1"]))
print(has_data_index(source, ["Key1", "Key2"]))
```

## Related documentation

- [`data_index`](./data-index.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.has_data_index)
