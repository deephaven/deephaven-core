---
title: Export HTML files
---

Deephaven can convert tables to HTML table-formatted strings via the [`html`](/core/javadoc/io/deephaven/engine/util/TableTools.html#html(io.deephaven.engine.table.Table)) method from [`TableTools`](/core/javadoc/io/deephaven/engine/util/TableTools.html). It converts a Deephaven table to a string HTML table.

## `html`

The [`html`](/core/javadoc/io/deephaven/engine/util/TableTools.html#html(io.deephaven.engine.table.Table)) method requires only a single argument — the table to convert. Here, we'll create a simple table and convert it to HTML.

```groovy order=:log,table
table = newTable(
    stringCol("Letter", "A", "B", "C"),
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

> [!NOTE]
> Since HTML tables represent all data as plain, untyped text, all typing will be lost.

## Related documentation

- [Read HTML files](./html-import.md)
- [Export CSV files](./csv-export.md)
