---
title: columnGroup
---

The `columnGroup` method is used within [`setLayoutHints`](./layout-hints.md) create column groups, which will be moved so their children are adjacent in the UI.

## Syntax

```
columnGroup(name, children)
columnGroup(name, children, color)
```

## Parameters

<ParamTable>
<Param name="name" type="String">

The column group name. Must be a valid Deephaven column name.

</Param>
<Param name="children" type="List<String>">

The columns and other groups belonging to this group.

</Param>
<Param name="color" type="Color">

The background color for the group in the UI.
See [Color](https://deephaven.io/core/javadoc/io/deephaven/gui/color/Color.html#color(java.lang.String)) for a comprehensive list of Color constants.

</Param>
<Param name="color" type="String">

String representing the group's background color in the UI. This can be a named [Color](https://deephaven.io/core/javadoc/io/deephaven/gui/color/Color.html#color(java.lang.String)) constant, or a hex value (e.g., #001122).
Hex values will be parsed as follows:

- The first two digits set the Red component.
- The second two digits set the Blue component.
- The final two digits set the Green component.
  Hex values must be preceded by a `#`.

</Param>
</ParamTable>

## Returns

A new table with layout instructions for the UI.

## Examples

In the following example, `A` and `E` form a column group called `Vowels`.

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
    .columnGroup("Vowels", ["A", "E"])
    .build()
)
```

## Related documentation

- [Javadoc](/core/javadoc/io/deephaven/engine/util/LayoutHintBuilder.html)
