---
title: blink_to_append_only
---

The `blink_to_append_only` method creates an append-only table from the supplied blink table.

## Syntax

```python syntax
from deephaven.stream import blink_to_append_only

blink_to_append_only(table: Table) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The blink table to convert to an append-only table.

</Param>
</ParamTable>

## Returns

An append-only table.

## Example

The following example creates a blink table with [`time_table`](./timeTable.md) and then converts the blink table to an append-only table with `blink_to_append_only`.

```python order=null
from deephaven.stream import blink_to_append_only
from deephaven import time_table

tt1 = time_table(period="PT1S", blink_table=True)

append_only_result = blink_to_append_only(tt1)
```

![The above `tt1` and `append_only_result` tables ticking side-by-side](../../../assets/reference/create/blink-to-append-only.gif)

## Related Documentation

- [`time_table`](./timeTable.md)
- [Pydoc](https://deephaven.io/core/pydoc/code/deephaven.stream.html#deephaven.stream.blink_to_append_only
