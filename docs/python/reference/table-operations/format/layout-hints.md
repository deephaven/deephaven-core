---
title: layout_hints
---

The `layout_hints` method creates a new table with information about how the UI should layout the table.

> [!NOTE]
> You can also [show, hide, freeze, group, and reorder](/core/ui/docs/components/table/#column-order-and-visibility) columns using the [`deephaven.ui`](/core/ui/docs/) Python package.

## Syntax

```
layout_hints(
    front: Union[str, list[str]],
    back: Union[str, list[str]],
    freeze: Union[str, list[str]],
    hide: Union[str, list[str]],
    column_groups: List[Dict],
    search_display_mode: SearchDisplayMode
)
```

## Parameters

<ParamTable>
<Param name="front" type="Union[str, list[str]]">

The columns to show at the front.

</Param>
<Param name="back" type="Union[str, list[str]]">

The columns to show at the back.

</Param>
<Param name="freeze" type="Union[str, list[str]]">

The columns to freeze.

</Param>
<Param name="hide" type="Union[str, list[str]]">

The columns to hide.

</Param>
<Param name="column_groups" type="List[Dict]">

A list of dicts specifying which columns should be grouped in the UI. The dicts can specify the following:

- `name (str)`: The group name.
- `children (List[str])`: The column names in the group.
- `color (Optional[str])`: The hex color string or Deephaven color name.

</Param>
<Param name="search_display_mode" type="SearchDisplayMode">

Sets the search bar to either be explicitly accessible or inaccessible, or to use the system default.

- `SearchDisplayMode.SHOW`: show the search bar.
- `SearchDisplayMode.HIDE`: hide the search bar.
- `SearchDisplayMode.DEFAULT`: use the system default.

</Param>
</ParamTable>

## Returns

A new table with layout instructions for the UI.

## Examples

In the following example, column `Even` is frozen to the front of the table, `Odd` is moved to the front, `B` is moved to the back, `C` is hidden, and `A` and `E` form a column group called `Vowels`.

```python order=source,result default=result
from deephaven import new_table
from deephaven.table import SearchDisplayMode
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["A", "a"]),
        string_col("B", ["B", "b"]),
        string_col("C", ["C", "c"]),
        string_col("D", ["D", "d"]),
        string_col("E", ["E", "e"]),
        string_col("Y", ["Y", "y"]),
        int_col("Even", [2, 4]),
        int_col("Odd", [1, 3]),
    ]
)

result = source.layout_hints(
    front=["Odd"],
    back=["B"],
    freeze=["Even"],
    hide=["C"],
    column_groups=[{"name": "Vowels", "children": ["A", "E"], "color": "RED"}],
    search_display_mode=SearchDisplayMode.SHOW,
)
```

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.layout_hints)
