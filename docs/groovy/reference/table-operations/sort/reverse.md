---
title: reverse
---

`reverse` reverses the order of rows in a table.

## Syntax

```
table.reverse()
```

## Parameters

## Returns

A new table with all of the rows from the input table in reverse order.

## Examples

```groovy order=source,result
source = newTable(
    stringCol("Tool", "Paring", "Bread", "Steak", "Butter", "Teaspoon", "Spoon", "Slotted","Ladle"),
    stringCol("Type", "Knife", "Knife", "Knife", "Knife", "Spoon", "Spoon", "Spoon","Spoon"),
    intCol("Quantity", 5, 8, 11, 10, 24, 3, 2, 4),
)

result = source.reverse()
```

## Related documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#reverse())
