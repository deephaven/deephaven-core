---
title: to_html
---

The `to_html` function in the [`deephaven.html`](/core/pydoc/code/deephaven.html.html) module converts a Deephaven table to a string containing an HTML table.

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

A string containing an HTML `<table>` element with a header row (`<tr>`) of column names in `<th>` cells, followed by one row (`<tr>`) of `<td>` cells for each table row, in table order. The string is a copy of the table's rows at the moment of the call. If the table is [ticking](../../../conceptual/table-update-model.md), the string does not change. Call `to_html` again to get the latest rows.

The output differs from the source table in the following ways:

- HTML tables hold all data as untyped text, so the output does not preserve column types.
- `to_html` formats `Instant` values in the server JVM's default time zone, which can differ from the time zone the web UI uses to display the table.
- `to_html` writes null values as `(null)`.
- `to_html` writes all other values, including other date-time types such as `ZonedDateTime` and `LocalDate`, with their Java `toString` form. For a Java array column, such as an `int[]` column, that form is an object reference like `[I@1b6d3586`, not the array's contents.

> [!WARNING]
> `to_html` escapes string values with CSV rules, not HTML rules. It wraps a value that contains a comma, a double quote, or a line break in double quotes and doubles any double quotes inside it. It writes characters such as `<` and `&` as-is, so a string value that contains HTML markup becomes active markup in any page that displays the output. Convert only tables whose contents you trust, or HTML-escape untrusted string values before conversion.

> [!WARNING]
> `to_html` builds the whole table into a single in-memory string, so use it only on small tables to avoid running out of memory.

`to_html` does not acquire an [UpdateGraph lock](../../../conceptual/query-engine/engine-locking.md) itself. Deephaven holds the exclusive lock while it runs console commands, so the rows `to_html` reads there are consistent. On a thread that holds no lock, such as a background thread, the rows it reads from a ticking table can be inconsistent. In that case, convert a [snapshot](../../table-operations/snapshot/snapshot.md) of the table instead, for example `html.to_html(table.snapshot())`. Alternatively, call `to_html` while holding the shared lock, as described in [Locking from Python](../../../conceptual/query-engine/engine-locking.md#locking-from-python).

## Examples

The example below creates a table with a comma-containing string value, a null value, and an `Instant` column, then converts it to HTML. The printed output shows the CSV-style escaping, the `(null)` placeholder, and the time-zone formatting described in [Returns](#returns).

```python order=:log,table
from deephaven import new_table, html
from deephaven.column import string_col, int_col, datetime_col

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

## Related documentation

- [Convert tables to HTML strings](../../../how-to-guides/data-import-export/html-export.md)
- [Read HTML files](../../../how-to-guides/data-import-export/html-import.md)
- [`datetime_col`](../../table-operations/create/dateTimeCol.md)
- [`snapshot`](../../table-operations/snapshot/snapshot.md)
- [Engine locking](../../../conceptual/query-engine/engine-locking.md)
- [Table update model](../../../conceptual/table-update-model.md)
- [Pydoc](/core/pydoc/code/deephaven.html.html)
