---
title: atFront
---

The `atFront` method is used within `setLayoutHints` to keep specified columns at the front of the table, after frozen columns. They will not be movable and will scroll off screen.

## Syntax

```
atFront(cols...)
```

## Parameters

<ParamTable>
<Param name="cols" type="String...">

The columns to place at the front of the table.

</Param>
<Param name="cols" type="Collection<String>">

The columns to place at the front of the table.

</Param>
</ParamTable>

## Returns

A new table with layout instructions for the UI.

## Examples

In the following example, the column `Odd` is moved to the front of the table.

```groovy order=source,result default=result
import io.deephaven.engine.util.LayoutHintBuilder

source = newTable(
    stringCol("A", "A", "a"),
    stringCol("B", "B", "b"),
    stringCol("C", "C", "c"),
    stringCol("D", "D", "d"),
    stringCol("E", "E", "e"),
    stringCol("Y", "Y", "y"),
    intCol("Even", 2, 4),
    intCol("Odd", 1, 3)
)

result = source.setLayoutHints(
    LayoutHintBuilder.get()
    .atFront("Odd")
    .build()
)
```

## Related documentation

- [Javadoc](/core/javadoc/io/deephaven/engine/util/LayoutHintBuilder.html)
