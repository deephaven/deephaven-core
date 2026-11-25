---
title: html
---

The `html` method in [`TableTools`](/core/javadoc/io/deephaven/engine/util/TableTools.html) converts a Deephaven table to a string containing an HTML table.

## Syntax

```groovy syntax
html(Table source)
```

## Parameters

<ParamTable>
<Param name="source" type="Table">

The Deephaven table to convert to an HTML table.

</Param>
</ParamTable>

## Returns

A string containing an HTML `<table>` element with a header row (`<tr>`) of column names in `<th>` cells, followed by one row (`<tr>`) of `<td>` cells for each table row, in table order. The string is a copy of the table's rows at the moment of the call. If the table is [ticking](../../../conceptual/table-update-model.md), the string does not change. Call `html` again to get the latest rows.

The output differs from the source table in the following ways:

- HTML tables hold all data as untyped text, so the output does not preserve column types.
- `html` formats `Instant` values in the server JVM's default time zone, which can differ from the time zone the web UI uses to display the table.
- `html` writes null values as `(null)`.
- `html` writes all other values, including other date-time types such as `ZonedDateTime` and `LocalDate`, with their Java `toString` form. For a Java array column, such as an `int[]` column, that form is an object reference like `[I@1b6d3586`, not the array's contents.

> [!WARNING]
> `html` escapes string values with CSV rules, not HTML rules. It wraps a value that contains a comma, a double quote, or a line break in double quotes and doubles any double quotes inside it. It writes characters such as `<` and `&` as-is, so a string value that contains HTML markup becomes active markup in any page that displays the output. Convert only tables whose contents you trust, or HTML-escape untrusted string values before conversion.

> [!WARNING]
> `html` builds the whole table into a single in-memory string, so use it only on small tables to avoid running out of memory.

`html` does not acquire an [UpdateGraph lock](../../../conceptual/query-engine/engine-locking.md) itself. Deephaven holds the exclusive lock while it runs console commands, so the rows `html` reads there are consistent. On a thread that holds no lock, such as a background thread, the rows it reads from a ticking table can be inconsistent. In that case, convert a [snapshot](../../table-operations/snapshot/snapshot.md) of the table instead, for example `html(table.snapshot())`. Alternatively, call `html` while holding the shared lock, as described in [Using locks](../../../conceptual/query-engine/engine-locking.md#using-locks).

## Examples

The example below creates a table with a comma-containing string value, a null value, and an `Instant` column, then converts it to HTML. The printed output shows the CSV-style escaping, the `(null)` placeholder, and the time-zone formatting described in [Returns](#returns).

```groovy order=:log,table
table = newTable(
    stringCol("Letter", "A", "B, C", null),
    intCol("Num", 1, 2, 3),
    instantCol("Datetime",
        parseInstant("2007-12-03T10:15:30.00Z"),
        parseInstant("2007-12-03T10:15:30.00Z"),
        parseInstant("2007-12-03T10:15:30.00Z"),
    )
)

htmlTable = html(table)

println(htmlTable)
```

## Related documentation

- [Convert tables to HTML strings](../../../how-to-guides/data-import-export/html-export.md)
- [Read HTML files](../../../how-to-guides/data-import-export/html-import.md)
- [`instantCol`](../../table-operations/create/instantCol.md)
- [`snapshot`](../../table-operations/snapshot/snapshot.md)
- [Engine locking](../../../conceptual/query-engine/engine-locking.md)
- [Table update model](../../../conceptual/table-update-model.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#html(io.deephaven.engine.table.Table))
