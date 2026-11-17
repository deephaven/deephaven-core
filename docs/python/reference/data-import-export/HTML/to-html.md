---
title: to_html
---

The `to_html` function converts a Deephaven table to a string containing an HTML table.

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

A string containing an HTML `<table>` element. The string is a copy of the table's rows at the moment of the call. If the table is ticking, the string does not update. `to_html` reads the table without taking a snapshot or a lock. Called from the console or an [Application Mode](../../app-mode/application-mode-config.md) script, which run under the update graph's [exclusive lock](../../../conceptual/query-engine/engine-locking.md#exclusive-lock), it reads a table that cannot change during the call. Called from code that runs without the lock, such as a separate thread, the copy can mix values from before and after an update; there, call `to_html(table.snapshot())` for a consistent copy, and call it again to get the latest rows.

## Examples

The `to_html` function takes one argument: the table to convert. The example below creates a table with a comma-containing string value and a null value, then converts it to HTML.

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

> [!WARNING]
> `to_html` builds the whole table into a single in-memory string, so use it only on small tables to avoid running out of memory.

> [!NOTE]
> HTML tables represent all data as plain, untyped text, so the output carries no column type information. `to_html` escapes string values with CSV rules, not HTML rules, so a value that contains HTML markup becomes active markup in any page that displays the output. `to_html` formats `Instant` values in the server JVM's default time zone and writes null values as `(null)`.

## Related documentation

- [Read HTML files](../../../how-to-guides/data-import-export/html-import.md)
- [Convert tables to HTML strings](../../../how-to-guides/data-import-export/html-export.md)
- [Multithreading: Synchronization, locks, and snapshots](../../../conceptual/query-engine/engine-locking.md)
- [Application Mode configuration](../../app-mode/application-mode-config.md)
- [Pydoc](/core/pydoc/code/deephaven.html.html)
