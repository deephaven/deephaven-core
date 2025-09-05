---
title: slice_pct
---

The `slice_pct` method returns a table that is a subset of another table corresponding to the difference between the start and end row percentages. For example, for a table of size 10, `slice_pct(0.1, 0.7)` will return a subset from the second row to the seventh row. Similarly, `slice_pct(0, 1)` would return the entire table (because row positions run from 0 to size - 1). If the percentage arguments are outside of the range [0.0, 1.0], an error will occur.

> [!CAUTION]
> Attempting to use `slice_pct` on a [blink table](../../../conceptual/table-types.md#specialization-3-blink) will raise an error.

## Syntax

```python syntax
table.slice_pct(start_pct: float, end_pct: float) -> Table
```

## Parameters

<ParamTable>
<Param name="start_pct" type="float">

The starting percentage point (inclusive) for rows to include in the result. The percentage arguments must be in the range [0.0, 1.0].

</Param>
<Param name="end_pct" type="float">

The ending percentage point (exclusive) for rows to include in the result. The percentage arguments must be in the range [0.0, 1.0].

</Param>
</ParamTable>

## Returns

A new table that is a subset of the source table.

## Example

The following example filters the table to a subset between the tenth and 70th rows.

```python order=source,result
from deephaven import empty_table

source = empty_table(100).update(["X = i"])
result = source.slice_pct(0.1, 0.7)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to use filters](../../../how-to-guides/use-filters.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#slicePct(double,double))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.slice_pct)
