---
title: Add clickable links to a Deephaven table
---

Any [string column](../../reference/table-operations/create/stringCol.md) in a Deephaven table can hold clickable links. The Deephaven web UI turns the URLs in these columns into links. You don't need to turn on a setting.

- Any URL that starts with `http://` or `https://` becomes a link, wherever it appears in the string.
- Email addresses, such as `user@example.com`, also become links.

This guide shows how links appear in a table you create from code and in an input table.

## Add links to a table from code

Let's create a table with a [string column](../../reference/table-operations/create/stringCol.md) that contains URLs.

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

The UI renders a URL as a link only when it includes the scheme, `http://` or `https://`. A link can appear inside other text, and one cell can hold more than one link. In the `result` table:

- `https://deephaven.io/` is a link.
- `link: https://deephaven.io/` contains a link after the label text.
- `deephaven.io (invalid link)` is plain text, because `deephaven.io` has no scheme.
- The last cell contains two separate links.

Here's how clicking works in a cell that contains a link:

- Click a web link to open it in a new browser tab. Click an email address to open it in your default email app.
- To select the cell without opening the link, click an empty part of the cell, or click the link and hold the mouse button for one second.
- If the cell is editable, holding the link for one second also starts editing the cell.

## Add links to an input table

An [input table](../../reference/table-operations/create/input-table.md) lets you type values directly into its cells. Strings you type into an [input table](../input-tables.md) follow the same rule as strings in any other column: only URLs that start with `http://` or `https://` become links.

Let's create an empty input table with two string columns, `Title` and `Link`.

```python order=result
from deephaven import dtypes as dht, input_table

my_col_defs = {
    "Title": dht.string,
    "Link": dht.string,
}

result = input_table(col_defs=my_col_defs)
```

### Type a link into the input table

To add a link, type or paste a URL into a cell, then click **Commit** to save the row.

- To edit a link in a row you haven't committed yet, click the link and hold the mouse button for one second. A normal click opens the link instead.
- After you commit a row, you can't edit it. This input table is append-only because the example doesn't pass `key_cols`.

![A user types a link into an input table cell, commits it, and clicks it to open the site](../../assets/how-to/ui/clickable_link_gif.gif)

To edit links after you commit them, create a keyed input table by passing `key_cols` to [`input_table`](../../reference/table-operations/create/input-table.md), for example `key_cols="Title"`. You can then edit the non-key columns, such as `Link`, at any time.

As in the first example, the UI doesn't convert values without a scheme, such as `deephaven.io` or `www.google.com`, to links:

![URLs with https:// appear as blue links, while deephaven.io and www.google.com appear as plain text](../../assets/how-to/ui/invalid_links.png)

## Related documentation

- [Create and use input tables](../input-tables.md)
- [How to create a new table](../new-and-empty-table.md#new_table)
- [`input_table`](../../reference/table-operations/create/input-table.md)
- [`new_table`](../../reference/table-operations/create/newTable.md)
- [`string_col`](../../reference/table-operations/create/stringCol.md)
- [Deephaven Python dtypes](../../reference/python/deephaven-python-types.md)
- [`dtypes` Pydoc](/core/pydoc/code/deephaven.dtypes.html)
- [`input_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.input_table)
- [`new_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.new_table)
- [`string_col` Pydoc](/core/pydoc/code/deephaven.column.html#deephaven.column.string_col)
