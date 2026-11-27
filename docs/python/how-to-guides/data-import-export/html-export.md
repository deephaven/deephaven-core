---
title: Convert tables to HTML strings
---

The [`to_html`](../../reference/data-import-export/HTML/to-html.md) function in the [`deephaven.html`](/core/pydoc/code/deephaven.html.html) module converts a Deephaven table to a string that contains an HTML table. This is handy when you want to drop a small table into an email, a report, or any other page that renders HTML.

## Convert a table to an HTML string

Pass the table to convert as the only argument. Let's create a simple table and convert it to HTML. The `Letter` column includes a null value and a value that contains a comma, so the output shows how [`to_html`](../../reference/data-import-export/HTML/to-html.md) writes each one.

```python order=:log,table test-set=html-export
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
- It formats [`Instant`](../../reference/query-language/types/date-time.md) values, such as the `Datetime` column that [`datetime_col`](../../reference/table-operations/create/dateTimeCol.md) creates, in the server JVM's default time zone. That zone can differ from the [time zone](../set-date-time-format.md#time-zone) the web UI uses to display the table.
- It writes [null values](../../reference/query-language/types/nulls.md) as `(null)`.
- It writes every other value with the string that Java's `toString` method returns for it. This includes other date-time types such as `ZonedDateTime` and `LocalDate`. For a Java [array](../../reference/query-language/types/arrays.md) column, such as an `int[]` column, that string is an object reference like `[I@1b6d3586`, not the array's contents. A Deephaven vector column, such as one that [`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md) creates, writes its first 10 elements, like `[1,2,3]`, and ends with `, ...]` when it has more.

In this example's output, [`to_html`](../../reference/data-import-export/HTML/to-html.md) wraps `B, C` in double quotes and writes the null value as `(null)`. The `Datetime` values read `2007-12-03T10:15:30.000000000 Etc/UTC`, because the server that produced this output runs in UTC. The table widget shows the same instants in the web UI's [time zone](../set-date-time-format.md#time-zone).

## Save the HTML to a file

[`to_html`](../../reference/data-import-export/HTML/to-html.md) returns a string, so you can write it to a file with Python's built-in `open` function. Let's save the HTML table from the previous example to an `.html` file that any browser can open:

```python order=null test-set=html-export
with open("/data/table.html", "w") as f:
    f.write(html_table)
```

> [!NOTE]
> The Deephaven server resolves file paths. If you run Deephaven in Docker, the path refers to the container's file system. The default Docker Compose setup mounts the `./data` directory of your Deephaven deployment as the `/data` [Docker bind mount](../../conceptual/docker-data-volumes.md#the-data-mount-point) in the container, so the file appears at `./data/table.html` on your computer. If you run Deephaven without Docker, replace `/data` with a directory that exists on the server's machine.

The file holds the table's rows as they were when you called [`to_html`](../../reference/data-import-export/HTML/to-html.md). To save newer rows from a [ticking](../../conceptual/table-types.md#standard-streaming-tables) table, convert the table again and rewrite the file. See [Ticking tables](#ticking-tables) for how to get a consistent copy.

## Limitations

Keep these limitations of [`to_html`](../../reference/data-import-export/HTML/to-html.md) in mind when you convert a table.

### Untrusted string values

> [!WARNING]
> Because [`to_html`](../../reference/data-import-export/HTML/to-html.md) doesn't HTML-escape cell values, a value that contains HTML markup becomes active markup in any page that displays the output. Convert only tables whose contents you trust, or HTML-escape untrusted values before you convert the table. Escaping a `String` column doesn't cover strings inside other columns, such as vector or map columns, because [`to_html`](../../reference/data-import-export/HTML/to-html.md) writes their text form unchanged.

To HTML-escape a string column, call the `escapeHtml4` method of the Apache Commons Text `StringEscapeUtils` class in a query string, then convert the result. Let's escape the `Letter` column of the example table:

```python order=:log test-set=html-export
safe = table.update(
    "Letter = org.apache.commons.text.StringEscapeUtils.escapeHtml4(Letter)"
)

print(html.to_html(safe))
```

The example's `Letter` values contain no markup, so the output matches the earlier one. A value such as `<b>` becomes `&lt;b&gt;`, which a browser displays as text instead of markup.

### Ticking tables

[`to_html`](../../reference/data-import-export/HTML/to-html.md) returns a fixed copy of the table's rows. If the table is [ticking](../../conceptual/table-types.md#standard-streaming-tables), the string doesn't change as the table updates.

[`to_html`](../../reference/data-import-export/HTML/to-html.md) also reads the table cell by cell without taking a [snapshot](../../reference/table-operations/snapshot/snapshot.md) or a lock. When you call it from the console or an [Application Mode](../../reference/app-mode/application-mode-config.md) script, Deephaven already holds the [exclusive lock](../../conceptual/query-engine/engine-locking.md#exclusive-lock), so the table can't change during the read. From code that runs without the lock, such as a separate thread, the string can mix values from before and after an update. There, convert a snapshot of the table instead:

- For a consistent copy, call `html.to_html(table.snapshot())`.
- For the latest rows, run that expression again. Each call takes a new snapshot.

### Table size

> [!WARNING]
> [`to_html`](../../reference/data-import-export/HTML/to-html.md) builds the whole table into a single in-memory string, so use it only on small tables to avoid running out of memory.

## Related documentation

- [Table types](../../conceptual/table-types.md)
- [Read HTML files](./html-import.md)
- [Export CSV files](./csv-export.md)
- [Access your file system with Docker bind mounts](../../conceptual/docker-data-volumes.md)
- [Multithreading: Synchronization, locks, and snapshots](../../conceptual/query-engine/engine-locking.md)
- [Application Mode configuration](../../reference/app-mode/application-mode-config.md)
- [Set date-time format](../set-date-time-format.md)
- [Arrays](../../reference/query-language/types/arrays.md)
- [date-time](../../reference/query-language/types/date-time.md)
- [Nulls](../../reference/query-language/types/nulls.md)
- [`datetime_col`](../../reference/table-operations/create/dateTimeCol.md)
- [`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md)
- [`int_col`](../../reference/table-operations/create/intCol.md)
- [`new_table`](../../reference/table-operations/create/newTable.md)
- [`snapshot`](../../reference/table-operations/snapshot/snapshot.md)
- [`string_col`](../../reference/table-operations/create/stringCol.md)
- [`to_html`](../../reference/data-import-export/HTML/to-html.md)
- [`to_html` Pydoc](/core/pydoc/code/deephaven.html.html#deephaven.html.to_html)
