---
title: emptyTable
---

The `emptyTable` method creates an in-memory table with a specified number of rows and no columns.

It is commonly used as the starting point for generating data: add columns to the new table with [`update`](../select/update.md) or another selection method.

## Syntax

```groovy syntax
emptyTable(size)
```

## Parameters

<ParamTable>
<Param name="size" type="long">

The number of rows in the new table. Must be zero or greater.

</Param>
</ParamTable>

## Returns

A static table with `size` rows and no columns.

## Examples

The following example creates a table with five rows and no columns.

```groovy
result = emptyTable(5)
```

The following example creates a table with five rows and then uses [`update`](../select/update.md) to add two columns: `X`, which contains the value 5 in every row, and `Y`, which contains each row's index from the [special variable](../../query-language/variables/special-variables.md) `i`.

```groovy
result = emptyTable(5).update("X = 5", "Y = i")
```

## Related documentation

- [Create static tables](../../../how-to-guides/new-and-empty-table.md)
- [`newTable`](./newTable.md)
- [`update`](../select/update.md)
- [Special variables](../../query-language/variables/special-variables.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#emptyTable(long))
