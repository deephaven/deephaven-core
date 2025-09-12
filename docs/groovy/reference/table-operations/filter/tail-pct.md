---
title: tailPct
---

The `tailPct` method returns a table with a specific percentage of rows from the end of the source table.

> [!CAUTION]
> Attempting to use `tailPct` on a [blink table](../../../conceptual/table-types.md#specialization-3-blink) will raise an error.

## Syntax

```
table.tailPct(percent)
```

## Parameters

<ParamTable>
<Param name="percent" type="double">

The percentage of rows to return. This value must be given as a floating-point number between 0 (0%) to 1 (100%).

</Param>
</ParamTable>

## Returns

A new table with a specific percentage of rows from the end of the source table.

## Examples

The following example filters the table to the last 40% and 33.33333333% of rows.

```groovy order=source,result,result1
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol("Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"),
    intCol("Code", 12, 13, 11, NULL_INT, 16, 14, NULL_INT),
)

result = source.tailPct(0.40)
result1 = source.tailPct(0.3333333333)
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use filters](../../../how-to-guides/filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#tailPct(double))
