---
title: hide
---

The `hide` method is used within [`setLayoutHints`](./layout-hints.md) to hide columns in the table by default.

:::

[Hiding columns](/core/ui/docs/components/table/#column-order-and-visibility) can also be accomplished using the [`deephaven.ui`](/core/ui/docs/) Python package.

:::

## Syntax

```
hide(cols...)
```

## Parameters

<ParamTable>
<Param name="cols" type="String">

The columns to hide in the table.

</Param>
<Param name="cols" type="Collection<String>">

The columns to hide in the table.

</Param>
</ParamTable>

## Returns

A new table with layout instructions for the UI.

## Examples

In the following example, the column `Even` is hidden in the result table.

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
    .hide("Even")
    .build()
)
```

## Related documentation

- [Javadoc](/core/javadoc/io/deephaven/engine/util/LayoutHintBuilder.html)
