---
title: Export HTML files
---

Deephaven can convert tables to HTML table-formatted strings via the [`deephaven.html`](/core/pydoc/code/deephaven.html.html) module. This module has a single function, [`to_html`](../../reference/data-import-export/HTML/to-html.md), which converts a Deephaven table to a string HTML table.

## `to_html`

The [`to_html`](../../reference/data-import-export/HTML/to-html.md) function requires only a single argument - the table to convert. Here, we'll create a simple table and convert it to HTML.

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

- [Import HTML files](./html-import.md)
