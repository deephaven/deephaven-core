---
title: Upload a table from a file
---

Deephaven lets you import a CSV or other delimited file directly from the UI and make it immediately available as a table in your session.

## Upload a table from a file

1. Click the **Console Options** menu (⋮) next to your session name.
2. Select **Upload Table from File**.
3. In the dialog that opens, drop your file onto the drop zone or click **Select File** to browse for it.
4. Set a **Table name** — this is the variable name the table will have in your session.
5. Choose a **File format** from the dropdown (for example, **Default csv (trimmed)**).
6. Check or uncheck **First row is column headers** as appropriate.
7. Click **Upload**.

![Upload Table from File dialog](../../assets/how-to/upload_table.png)

The table appears immediately in your session and is listed in the **Tables** dropdown.

> [!NOTE]
> Uploaded tables are held in memory for the duration of your session. To persist data, write it to `/data/` using [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md) or another export method.

## Related documentation

- [Read CSV files](../data-import-export/csv-import.md)
- [Navigate the GUI](./navigating-the-ui.md)
- [Export CSV files](../data-import-export/csv-export.md)
