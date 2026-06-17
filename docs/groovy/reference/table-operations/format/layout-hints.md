---
title: setLayoutHints
---

The `setLayoutHints` method creates a new table with information about how the UI should layout the table.

## Syntax

```
table.setLayoutHints(hints)
```

## Parameters

<ParamTable>
<Param name="hints" type="String">

The string from `LayoutHintBuilder.build()`. Hints use the following methods:

- [`.freeze`](./freeze.md)
- [`.atFront`](./atFront.md)
- [`.atBack`](./atBack.md)
- [`.hide`](./hide.md)
- [`.columnGroup`](./columnGroup.md)

</Param>
</ParamTable>

## Returns

A new table with layout instructions for the UI.

## Examples

In the following example, column `Even` is frozen to the front of the table, `Odd` is moved to the front, `B` is moved to the back, `C` is hidden, and `A` and `E` form a column group called `Vowels`.

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
    .freeze("Even")
    .atFront("Odd")
    .atBack("B")
    .hide("C")
    .columnGroup("Vowels", ["A", "E"])
    .build()
)
```

## Related documentation

- [Javadoc](/core/javadoc/io/deephaven/engine/util/LayoutHintBuilder.html)
