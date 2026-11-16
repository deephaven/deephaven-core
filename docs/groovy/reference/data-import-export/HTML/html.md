---
title: html
---

The `html` method converts a Deephaven table to a string containing an HTML table.

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

A string containing an HTML `<table>` element. The string is a copy of the table's rows at the moment of the call. If the table is ticking, the string does not update, and `html` reads the table without taking a snapshot, so the copy can mix values from before and after an update. Call `html(table.snapshot())` for a consistent copy, and call it again to get the latest rows.

## Examples

The `html` method takes one argument: the table to convert. The example below creates a table with a comma-containing string value and a null value, then converts it to HTML.

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

> [!WARNING]
> `html` builds the whole table into a single in-memory string, so use it only on small tables to avoid running out of memory.

> [!NOTE]
> HTML tables represent all data as plain, untyped text, so the output carries no column type information. `html` escapes string values with CSV rules, not HTML rules, so a value that contains HTML markup becomes active markup in any page that displays the output. `html` formats `Instant` values in the server JVM's default time zone and writes null values as `(null)`.

## Related documentation

- [Convert tables to HTML strings](../../../how-to-guides/data-import-export/html-export.md)
- [Read HTML files](../../../how-to-guides/data-import-export/html-import.md)
- [`html` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#html(io.deephaven.engine.table.Table))
