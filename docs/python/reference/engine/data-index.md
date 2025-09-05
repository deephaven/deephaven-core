---
title: data_index
---

`data_index` gets the [`DataIndex`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex) for the given key columns on a table. It can also create a [`DataIndex`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex) when none is present. A [`DataIndex`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex) is an index that can make it faster for the engine to locate data by key. Some table operations have improved performance when a data index is present.

> [!NOTE]
> When a new data index is created, it is not immediately computed. The index is computed when a table operation first uses it, or when its `table` attribute is called. This is an important detail for performance considerations.

> [!NOTE]
> The Deephaven engine will only use a [`DataIndex`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex) when the keys _exactly_ match what is needed for an operation. For example, if a data index is present for the columns `X` and `Y`, it will not be used if the engine only needs an index for column `X`.

## Syntax

```python syntax
data_index(
    table: Table,
    key_cols: List[str],
    create_if_absent: bool = True
) -> Optional[DataIndex]
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The table to index.

</Param>
<Param name="key_cols" type="List[str]">

The names of the key column(s) to index.

</Param>
<Param name="create_if_absent" type="bool" optional>

If `True`, create the index if it does not already exist. If `False`, return `None` if the index does not exist. Creating an index does not compute the index. The index is computed when first used by a table operation, or when the `table` attribute is called on the index. Default is `True`.

</Param>
</ParamTable>

## Returns

A [`DataIndex`](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.DataIndex) or `None`.

## Examples

The following example demonstrates creating a data index on a table using a single key column.

```python order=source_index,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(10).update("X = randomInt(0, 10)")

index = data_index(table=source, key_cols="X")

source_index = index.table
```

The following example demonstrates creating a data index on a table using multiple key columns.

```python order=source_index,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(50).update(["Key1 = randomInt(0, 5)", "Key2 = randomInt(5, 8)"])

index = data_index(table=source, key_cols=["Key1", "Key2"])

source_index = index.table
```

The following example sets `create_if_absent` to `False`, so the operation returns `None` since the index does not yet exist.

```python order=:log,source
from deephaven.experimental.data_index import data_index
from deephaven import empty_table

source = empty_table(10).update("X = randomInt(0, 10)")

index = data_index(table=source, key_cols="X", create_if_absent=False)

print(index)
```

## Related documentation

- [`has_data_index`](./has-data-index.md)
- [Pydoc](/core/pydoc/code/deephaven.experimental.data_index.html#deephaven.experimental.data_index.data_index)
