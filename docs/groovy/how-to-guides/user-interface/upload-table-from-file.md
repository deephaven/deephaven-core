---
title: Upload a table from a file
---

You can upload a CSV or other delimited file, or paste delimited values, from the [Deephaven GUI](./navigating-the-ui.md), and the data is immediately available as a table in your session. It's the quickest way to get a file from your own computer into Deephaven without writing any code.

Deephaven accepts files with the extensions `.csv`, `.tsv`, `.tab`, `.psv`, `.dsv`, `.txt`, or `.zip`. For a `.zip` file, Deephaven imports the first file in the archive that has one of the other extensions. Use lowercase extensions. Deephaven rejects a dropped file whose extension is uppercase or mixed case, and it unzips only a lowercase `.zip`.

![The upload area in the Console panel, with the Console Options menu highlighted](../../assets/how-to/upload_table.png)

To upload a file:

1. Click the **Console Options** menu (⋮) at the top right of the **Console** panel. Its tooltip reads **More Actions...**.
2. Select **Upload Table from File**.
3. In the upload area that opens over the **Console** panel, add your data in one of these ways:
   - Drop your file onto the drop zone.
   - Click **Select File...** to browse for it.
   - Paste delimited values. Press Ctrl+V (Cmd+V on macOS) while the upload area is selected, or right-click the upload area and select **Paste**.

4. Check the **Table name**. It becomes the table's variable name in your session, and you can change it.
   - For a file, Deephaven fills in the file name without its extension. Spaces and characters such as `-` become underscores, and characters such as parentheses are dropped. For example, `my data (1).csv` becomes `my_data_1`. A name that starts with a digit gets the prefix `table_`, so `2024 sales.csv` becomes `table_2024_sales`.
   - For pasted values, the field starts empty, so type a name.

   **Upload** stays disabled while the field is empty.

5. Review the **File format**. For a `.csv`, `.tsv`, `.tab`, or `.psv` file, Deephaven preselects the matching format. For other files and for pasted values, it preselects **autodetect**. To use a different format, choose it from the dropdown. For example, choose **; semi-colon sv** for a file whose values are separated by semicolons.
6. Uncheck **First row is column headers** if the file has no header row. The box is checked by default. Deephaven turns header names into legal column names, so a `Unit Price` header becomes the column `Unit_Price`. Without a header row, Deephaven names the columns `A`, `B`, `C`, and so on.
7. Click **Upload**.

You can also drag a file onto the **Console** panel. The upload area opens with the file loaded, so you start at step 4.

When the upload finishes, Deephaven assigns the table to the variable you named, opens it in a panel, and lists it in the **Panels** menu at the top right of the GUI. If a variable with that name already exists, the uploaded table replaces it.

The uploaded table is a static copy of the file, so it doesn't change if the file does.

Deephaven chooses each column's type by scanning all of its values. If the values in a column don't all fit one type, such as a number, a boolean, a date-time, or a time of day, the column becomes a string column.

> [!NOTE]
> Uploaded tables exist only in the server's memory, so a server restart discards them. To keep one, save it to the server's file system with [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md) or another export method. After the restart, read the file back as described in [Read CSV or other delimited files](../data-import-export/csv-import.md).
>
> In Docker deployments, write to `/data`. To keep the files after you remove the container, mount a directory from your computer at `/data`, for example with `-v "$(pwd)/data:/data"`. See [Docker data volumes](../../conceptual/docker-data-volumes.md).

## Related documentation

- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [Read CSV or other delimited files](../data-import-export/csv-import.md)
- [Navigate the GUI](./navigating-the-ui.md)
- [Export data to CSV or other delimited files](../data-import-export/csv-export.md)
- [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md)
- [Javadoc](/core/javadoc/io/deephaven/csv/CsvTools.html#writeCsv(io.deephaven.engine.table.Table,java.lang.String,java.lang.String...))
