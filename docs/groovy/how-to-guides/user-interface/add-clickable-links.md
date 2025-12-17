---
title: Add clickable links to a Deephaven table
sidebar_label: Clickable links
---

This guide will show you how to add clickable links to a Deephaven table.

## String columns containing links

Any string column can contain links. To add clickable links to a Deephaven table, simply create a table with a [string column](../../reference/table-operations/create/stringCol.md) and add links. Any string that is properly formatted as a URL will appear as a clickable link.

```groovy order=result
result = newTable(
    stringCol(
        "Strings",
        "https://deephaven.io/",
        "link: https://deephaven.io/",
        "deephaven.io (invalid link)",
        "two links in one cell: https://deephaven.io / https://deephaven.io/core/docs/"
    )
)
```

## Add links to an InputTable

An [input table](../../reference/table-operations/create/InputTable.md) allows the user to type input directly into the table's cells. Just like with a standard Deephaven table, any string that is correctly formatted as a valid URL will appear as a clickable link.

```groovy order=result
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition

tableDef = TableDefinition.of(
    ColumnDefinition.ofString("Title"),
    ColumnDefinition.ofString("Link")
)

result = AppendOnlyArrayBackedInputTable.make(tableDef)
```

### Add links

A link acts like any input in the table. Simply type (or paste) a valid link into a cell. That's it! Click the **Commit** button that appears to finalize your changes.

![A user types a link into a cell](../../assets/how-to/ui/clickable-link-gif-groovy.gif)

### Invalid input

Note that valid links must be complete. See the example below:

![Valid links are highlighted in blue and are clickable](../../assets/how-to/ui/invalid_links.png)

## Related documentation

- [Input Table](../../reference/table-operations/create/InputTable.md)
- [How to create a new table](../new-and-empty-table.md#newtable)
- [`stringCol`](../../reference/table-operations/create/stringCol.md)
