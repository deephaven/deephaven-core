---
title: Convert tables to HTML strings
---

The [`to_html`](../../reference/data-import-export/HTML/to-html.md) function in the [`deephaven.html`](/core/pydoc/code/deephaven.html.html) module converts a Deephaven table to a string that contains an HTML table. It is the module's only function. This is handy when you want to drop a small table into an email, a report, or any other page that renders HTML.

## Convert a table to an HTML string

Pass the table to convert as the only argument. Let's create a simple table and convert it to HTML. The `Letter` column includes a value that contains a comma and a null value, so the output shows how [`to_html`](../../reference/data-import-export/HTML/to-html.md) writes them.

```python order=:log,table
from deephaven import new_table
from deephaven.column import string_col, int_col, datetime_col
from deephaven import html

table = new_table(
    [
        string_col("Letter", ["A", "B, C", None]),
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

The returned string is an HTML `<table>` element with a header row of column names and one row for each table row. [`to_html`](../../reference/data-import-export/HTML/to-html.md) writes each cell's value as untyped text, so the output carries no column type information. It writes values as follows:

- It escapes string values with CSV rules, not HTML rules. It wraps a value that contains a comma, a double quote, or a line break in double quotes and doubles any double quotes inside it. It writes characters such as `<` and `&` as-is.
- It formats `Instant` values, such as the `Datetime` column created with [`datetime_col`](../../reference/table-operations/create/dateTimeCol.md), in the server JVM's system default time zone, which can differ from the time zone the web UI uses to display the table.
- It writes null values as `(null)`.
- It writes other values, including other date-time types such as `ZonedDateTime` and `LocalDate`, with their Java `toString` form. For a Java array column, such as an `int[]` column, that form is an object reference like `[I@1b6d3586`, not the array's contents.

In this example's output, the `B, C` value is wrapped in double quotes, the null value is written as `(null)`, and the `Datetime` values are in the server's time zone (UTC here), while the table widget shows them in the web UI's time zone.

The string is a copy of the table's rows at the moment you call [`to_html`](../../reference/data-import-export/HTML/to-html.md). If the table is ticking, the string doesn't change as the table updates. Call [`to_html`](../../reference/data-import-export/HTML/to-html.md) again to get the latest rows.

> [!WARNING]
> [`to_html`](../../reference/data-import-export/HTML/to-html.md) builds the whole table into a single in-memory string, so use it only on small tables to avoid running out of memory.

## Related documentation

- [Read HTML files](./html-import.md)
- [Export CSV files](./csv-export.md)
- [`to_html`](../../reference/data-import-export/HTML/to-html.md)
