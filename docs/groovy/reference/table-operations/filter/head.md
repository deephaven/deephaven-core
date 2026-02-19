---
title: head
---

The `head` method returns a table with a specific number of rows from the beginning of the source table.

## Syntax

```
table.head(size)
```

## Parameters

<ParamTable>
<Param name="size" type="long">

The number of rows to return.

</Param>
</ParamTable>

## Returns

A new table with a specific number of rows from the beginning of the source table.

## Examples

The following example filters the table to the first two rows.

```groovy order=source,result
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.head(2)
```

The following example uses `head` on a [blink](../../../conceptual/table-types.md#specialization-3-blink) table. Note that `head` treats the blink table like an append-only table, saving the first five rows that were added to the table, and ignoring all following updates.

```groovy order=null
source = timeTableBuilder().period("PT0.10S").blinkTable(true).build()

result = source.head(5)
```

![The above `source` and `result` tables](../../../assets/reference/table-operations/head_blink.gif)

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#head(long))
