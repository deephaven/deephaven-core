---
title: Export data to CSV or other delimited files
sidebar_label: Export CSV files
---

This guide discusses how to export table data to CSV (or other delimited) files by using [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md).

> [!TIP]
> CSV files can also be exported via **Table Options > Download CSV** in the Deephaven UI.

## `writeCsv`

The basic syntax for `writeCsv` follows:

```groovy skip-test
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(table, "/data/outputFile.csv")
```

> [!NOTE]
> Deephaven writes files to locations relative to the base of its Docker container. See [Docker data volumes](../../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

We'll create a table to export by using [`emptyTable`](../../reference/table-operations/create/emptyTable.md) and [`update`](../../reference/table-operations/select/update.md). The table contains 100 rows of trigonometric values.

```groovy test-set=1
source = emptyTable(100).update(
    "X = 0.1 * i",
    "SinX = sin(X)",
    "CosX = cos(X)",
    "TanX = tan(X)"
)
```

### Standard CSV files

The simplest way to use [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md) is to supply two input parameters:

- The Deephaven source table.
- The path of the output CSV file.

```groovy test-set=1
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(source, "/data/TrigFunctions.csv")
```

![The newly written CSV file](../../assets/how-to/TrigFunctions_basic.png)

#### The `/data` mount point

If you are using Docker-installed Deephaven, you can find a `/data` folder inside your Deephaven installation's main folder, on the same level as your `docker-compose.yml` file. This folder is mounted to the `/data` volume in the running Deephaven container. This means that if the Deephaven console is used to write data to `/data/abc/file.csv`, that file will be visible at `./data/abc/file.csv` on the local file system of your computer.

> [!NOTE]
> If the `./data` directory does not exist when Deephaven is launched, it will be created.

### Null values

Null values are common in tables. How are they handled when exporting data to a CSV? This depends on how you call [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md).

First, let's create a table with null values. The example below uses a function to fill the `SinX` column with a large number of nulls.

```groovy test-set=2
sourceWithNulls = emptyTable(100).update(
    "X = 0.1 * i",
    "SinX = X % 0.2 < 0.01 ? NULL_DOUBLE : sin(X)",
    "CosX = cos(X)",
    "TanX = tan(X)"
)
```

The `SinX` column contains many null cells. The example below writes this table to a CSV file called `TrigFunctionsWithNulls.csv`.

```groovy test-set=2
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(sourceWithNulls, "/data/TrigFunctionsWithNulls.csv")
```

![The newly written CSV file](../../assets/how-to/TrigFunctions_basicWithNulls.png)

### Column selection

In the event you don't want to write every column in the table to a CSV file, you can specify which columns to write. This is done by providing a list of column names, as shown below.

```groovy test-set=2
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(sourceWithNulls, "/data/Cosine.csv", "X", "CosX")
```

![The newly written CSV file](../../assets/how-to/TrigFunctions_NullsCosineOnly.png)

## Related documentation

- [Create an empty table](../../how-to-guides/new-and-empty-table.md#emptytable)
- [How to import CSV files](./csv-import.md)
- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md)
