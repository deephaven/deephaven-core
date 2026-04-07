---
title: tail
---

The `tail` method returns a table with a specific number of rows from the end of the source table.

## Syntax

```
table.tail(size)
```

## Parameters

<ParamTable>
<Param name="size" type="long">

The number of rows to return.

</Param>
</ParamTable>

## Returns

A new table with a specific number of rows from the end of the source table.

## Examples

The following example filters the table to the last two rows.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.tail(2)
```

The following example uses `tail` on a [blink](../../../conceptual/table-types.md#specialization-3-blink) table. Note that `tail` treats the blink table like an append-only table, showing the five rows that were most recently added to the table, regardless of what update cycle they are a part of.

```groovy order=null
source = timeTableBuilder().period("PT0.5S").blinkTable(true).build()

result = source.tail(5)
```

![The above `source` and `result` tables](../../../assets/reference/table-operations/tail_blink.gif)

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#tail(long))
