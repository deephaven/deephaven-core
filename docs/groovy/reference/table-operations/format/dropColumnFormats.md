---
title: dropColumnFormats
---

The `dropColumnFormats` method removes color formatting from all columns of a table.

## Syntax

```groovy syntax
table.dropColumnFormats()
```

## Parameters

This method takes no parameters.

## Returns

A new table with all color formatting removed.

## Examples

```groovy order=result,source
source = newTable(
    intCol("X", 1,2,3,4,5),
    intCol("Y", 6,7,8,9,3),
).formatColumnWhere("Y", "X > 3", "RED")

result = source.dropColumnFormats()
```

## Related documentation

- [How to apply color formatting to columns in a table](../../../how-to-guides/format-columns.md)
- [How to `select`, `view`, and `update` data](../../../how-to-guides/use-select-view-update.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#dropColumnFormats())
