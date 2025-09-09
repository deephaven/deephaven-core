---
title: meta_table
---

`meta_table` is an attribute that can be used to create a new table containing metadata about a source table's columns.

## Syntax

```
source.meta_table
```

## Parameters

`meta_table` does not take any arguments.

## Returns

A table containing the metadata about the source table's columns.

## Example

The following example first creates a table of weather data for Miama over three dates in January, then [averages](../group-and-aggregate/avgBy.md) the high and low temperatures by day. Finally, the query creates a table of metadata, which shows the that the `Temp` column becomes a double column.

```python order=source,result,meta
from deephaven import new_table

from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("Day", ["Jan 1", "Jan 1", "Jan 2", "Jan 2", "Jan 3", "Jan 3"]),
        int_col("Temp", [45, 62, 48, 63, 39, 59]),
    ]
)

result = source.avg_by(by=["Day"])

meta = result.meta_table
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#new_table)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.meta_table)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#meta())
