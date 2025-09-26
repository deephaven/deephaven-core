---
title: tail
---

The `tail` method returns a table with a specific number of rows from the end of the source table.

## Syntax

```python syntax
table.tail(num_rows: int) -> Table
```

## Parameters

<ParamTable>
<Param name="num_rows" type="int">

The number of rows to return.

</Param>
</ParamTable>

## Returns

A new table with a specific number of rows from the end of the source table.

## Examples

The following example filters the table to the last two rows.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D", "A"]),
        int_col("Number", [NULL_INT, 2, 1, NULL_INT, 4, 5, 3]),
        string_col(
            "Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]
        ),
        int_col("Code", [12, 14, 11, NULL_INT, 16, 14, NULL_INT]),
    ]
)


result = source.tail(2)
```

The following example uses `tail` on a [blink](../../../conceptual/table-types.md#specialization-3-blink) table. Note that `tail` treats the blink table like an append-only table, showing the five rows that were most recently added to the table, regardless of what update cycle they are a part of.

```python order=null
from deephaven import time_table

source = time_table(period="PT0.5S", blink_table=True)

result = source.tail(5)
```

![The above `source` and `result` tables](../../../assets/reference/table-operations/tail_blink.gif)

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#tail(long))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.tail)
