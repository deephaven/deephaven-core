---
title: renameColumns
---

The `renameColumns` method creates a new table with the specified columns renamed.

## Syntax

```groovy syntax
table.renameColumns(pairs)
```

## Parameters

<ParamTable>
<Param name="pairs" type="Collection<Pair>">

The column rename expressions, e.g., `"X = Y"`.

</Param>
</ParamTable>

## Returns

A new table with the specified columns renamed.

## Examples

```groovy order=result,source
source = newTable(
    stringCol("A", "Some", "string", "values", "here"),
    intCol("B", 1, 2, 3, 4),
    stringCol("C", "more", "strings", "over", "here"),
)

result = source.renameColumns("X = A", "Y = B", "Z = C")
```

## Related documentation

- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/QueryTable.html#renameColumns(java.util.Collection))
