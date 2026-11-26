---
title: is_blink
---

The `is_blink` property is `True` if the table is a [blink table](../../../conceptual/table-types.md#specialization-3-blink), and `False` otherwise.

## Syntax

```python syntax
source.is_blink -> bool
```

## Parameters

`is_blink` is a property and takes no arguments.

## Returns

A boolean value that is `True` if the table is a blink table or `False` if it is not.

## Example

The following example creates two [time tables](../create/timeTable.md), one with `blink_table` set to `True` and the other with `blink_table` set to `False`, and then reads the `is_blink` property of each table.

```python order=:log
from deephaven import time_table

tt1 = time_table(period="PT1S", blink_table=True)

tt2 = time_table(period="PT1S", blink_table=False)

print(tt1.is_blink)
print(tt2.is_blink)
```

## Related documentation

- [`time_table`](../create/timeTable.md)
- [`add_only_to_blink`](../create/add-only-to-blink.md)
- [`remove_blink`](../create/remove-blink.md)
- [`blink_to_append_only`](../create/blink-to-append-only.md)
- [Table types: Blink tables](../../../conceptual/table-types.md#specialization-3-blink)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.is_blink)
