---
title: blink_to_append_only
---

The `blink_to_append_only` function creates an [append-only table](../../../conceptual/table-types.md#specialization-1-append-only) from the supplied [blink table](../../../conceptual/table-types.md#specialization-3-blink).

## Syntax

```python syntax
from deephaven.stream import blink_to_append_only

blink_to_append_only(table: Table) -> Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The blink table to convert to an append-only table. The table must be a blink table; otherwise, the function raises a `DHError`.

</Param>
</ParamTable>

## Returns

An in-memory, append-only table that contains the rows the blink table holds at the time of the call and every row the blink table adds after that. The table grows without bound as new rows arrive; this function has no parameter to limit the table's size. To keep only the first `n` rows, call [`head(n)`](../filter/head.md) on the blink table instead: on a blink table, `head` returns an append-only table that holds the first `n` rows the blink table delivers and ignores later rows. Unless the `QueryTable.memoizeResults` configuration property is `false`, a repeated call on the same blink table, while an earlier result is still in use, returns a table backed by that same underlying result table instead of creating a new one.

## Example

The following example creates a [blink time table](../../../how-to-guides/time-table.md#create-a-blink-time-table) with [`time_table`](./timeTable.md) and then converts the blink table to an append-only table with `blink_to_append_only`.

```python ticking-table order=null
from deephaven.stream import blink_to_append_only
from deephaven import time_table

tt1 = time_table(period="PT1S", blink_table=True)

append_only_result = blink_to_append_only(tt1)
```

![The above `tt1` and `append_only_result` tables ticking side by side](../../../assets/reference/create/blink-to-append-only.gif)

## Related documentation

- [`time_table`](./timeTable.md)
- [Create a time table](../../../how-to-guides/time-table.md)
- [`head`](../filter/head.md)
- [`is_blink`](../metadata/is_blink.md)
- [`remove_blink`](./remove-blink.md)
- [`add_only_to_blink`](./add-only-to-blink.md)
- [Table types: Specialized semantics for blink tables](../../../conceptual/table-types.md#specialized-semantics-for-blink-tables)
- [Pydoc](/core/pydoc/code/deephaven.stream.html#deephaven.stream.blink_to_append_only)
