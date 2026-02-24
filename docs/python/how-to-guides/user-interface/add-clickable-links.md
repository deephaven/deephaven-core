---
title: Add clickable links to a Deephaven table
sidebar_label: Clickable links
---

This guide will show you how to add clickable links to a Deephaven table.

## String columns containing links

Any string column can contain links. To add clickable links to a Deephaven table, simply create a table with a [string column](../../reference/table-operations/create/stringCol.md) and add links. Any string that is properly formatted as a URL will appear as a clickable link.

```python order=result
from deephaven import new_table
from deephaven.column import string_col

result = new_table(
    [
        string_col(
            "Strings",
            [
                "https://deephaven.io/",
                "link: https://deephaven.io/",
                "deephaven.io (invalid link)",
                "two links in one cell: https://deephaven.io / https://deephaven.io/core/docs/",
            ],
        )
    ]
)
```

## Add links to an InputTable

An [input table](../../reference/table-operations/create/input-table.md) allows the user to type input directly into the table's cells. Just like with a standard Deephaven table, any string that is correctly formatted as a valid URL will appear as a clickable link.

```python order=result
from deephaven import dtypes as dht, input_table

my_col_defs = {
    "Title": dht.string,
    "Link": dht.string,
}

result = input_table(col_defs=my_col_defs)
```

### Add links

A link acts like any input in the table. Simply type (or paste) a valid link into a cell. That's it! Click the **Commit** button that appears to finalize your changes.

![A user types a link into a cell](../../assets/how-to/ui/clickable_link_gif.gif)

### Invalid input

Note that valid links must be complete. See the example below:

![Valid links are highlighted in blue and are clickable](../../assets/how-to/ui/invalid_links.png)

## Related documentation

- [`input_table`](../../reference/table-operations/create/input-table.md)
- [How to create a new table](../new-and-empty-table.md#new_table)
- [`string_col`](../../reference/table-operations/create/stringCol.md)
