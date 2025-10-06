---
title: to_html
---

The `to_html` method writes a Deephaven table to a string HTML table.

## Syntax

```python syntax
to_html(table: Table) -> str
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

The Deephaven table to convert to an HTML table.

</Param>
</ParamTable>

## Returns

A new HTML string.

## Examples

The `to_html` function requires only a single argument - the table to convert. Here, we'll create a simple table and convert it to HTML.

```python order=:log,table
from deephaven import new_table
from deephaven.column import string_col, int_col, datetime_col
from deephaven import html

table = new_table(
    [
        string_col("Letter", ["A", "B", "C"]),
        int_col("Num", [1, 2, 3]),
        datetime_col(
            "Datetime",
            [
                "2007-12-03T10:15:30.00Z",
                "2007-12-03T10:15:30.00Z",
                "2007-12-03T10:15:30.00Z",
            ],
        ),
    ]
)

html_table = html.to_html(table)

print(html_table)
```

> [!NOTE]
> Since HTML tables represent all data as plain, untyped text, all typing will be lost.

## Related documentation

- [Import HTML Files](../../../how-to-guides/data-import-export/html-import.md)
- [Export HTML Files](../../../how-to-guides/data-import-export/html-export.md)
- [Pydoc](/core/pydoc/code/deephaven.html.html)
