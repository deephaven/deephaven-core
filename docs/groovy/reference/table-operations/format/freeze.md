---
title: freeze
---

The `freeze` method is used within [`setLayoutHints`](./layout-hints.md) to freeze columns in place at the front of the table. They will always be visible even when scrolling horizontally.


:::

[Freezing columns](/core/ui/docs/components/table/#column-order-and-visibility) can also be accomplished using the [`deephaven.ui`](/core/ui/docs/) Python package. 

:::

## Syntax

```
freeze(cols...)
```

## Parameters

<ParamTable>
<Param name="cols" type="String...">

The columns to freeze in place at the front of the table.

</Param>
<Param name="cols" type="Collection<String>">

Indicate the specified columns should be frozen (displayed as the first N, unmovable columns) upon display.

</Param>
</ParamTable>

## Returns

A new table with layout instructions for the UI.

## Examples

In the following example, the column `Even` is frozen to the front of the table.

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
    .build()
)
```

## Related documentation

- [Javadoc](/core/javadoc/io/deephaven/engine/util/LayoutHintBuilder.html)
