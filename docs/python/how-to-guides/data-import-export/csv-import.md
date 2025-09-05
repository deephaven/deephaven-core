---
title: Import CSV or other delimited files
sidebar_label: Import CSV files
---

This guide will show you how to import data from CSV (and other delimited) files into Deephaven tables with the [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) method.

> [!TIP]
> CSV files can also be imported into Deephaven with drag-and-drop uploading in the UI.

## `read_csv`

The two most common ways to load a CSV file are using the file path or the file URL. The header of the CSV file determines the column names.

The basic syntax for `read_csv` follows:

```python syntax
from deephaven import read_csv

read_csv(path: str) -> Table
```

Let's dive into some simple examples. So that we don't have to create a new CSV file from scratch, we'll use some CSV files from [Deephaven's examples repository](https://github.com/deephaven/examples). We encourage you to use your own files by replacing the file paths in our queries.

### `read_csv` with a file path

The `read_csv` method can be used to import a CSV file from a file path. In this example, we will import a CSV file containing R.A. Fisher's classic iris flower dataset commonly used in machine learning applications.

```python order=iris
from deephaven import read_csv

iris = read_csv("/data/examples/Iris/csv/iris.csv")
```

#### The `/data` mount point

If you are using Docker-installed Deephaven, you can find a `/data` folder inside your Deephaven installation's main folder, on the same level as your `docker-compose.yml` file. This folder is mounted to the `/data` volume in the running Deephaven container. This means that if the Deephaven console is used to write data to `/data/abc/file.csv`, that file will be visible at `./data/abc/file.csv` on the local file system of your computer.

> [!NOTE]
> If the `./data` directory does not exist when Deephaven is launched, it will be created.

See [Docker data volumes](../../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

> [!NOTE]
> If you're using our files, follow the directions in the [README](https://github.com/deephaven/examples/blob/main/README.md) to mount the content from [Deephaven's examples repository](https://github.com/deephaven/examples) onto `/data` in the Deephaven Docker container.

### `read_csv` with a URL

Now, we will import the same CSV file, but this time we will use a URL instead of a file path.

```python order=iris
from deephaven import read_csv

iris = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
```

> [!NOTE]
> This method works with any public URL that points to a CSV file.

### Headerless CSV files

CSV files don't always have headers. The example below uses the headerless [DeNiro CSV](https://github.com/deephaven/examples/tree/main/DeNiro/csv/deniro_headerless.csv) and includes an additional `headless` argument.

```python order=deniro
from deephaven import read_csv

deniro = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    headless=True,
)
```

Because no column names are provided, the table will produce default column names (`Column1`, `Column2`, etc.). You can explicitly set the column names, as shown below.

```python order=deniro
from deephaven import read_csv
import deephaven.dtypes as dht

header = {"Year": dht.int64, "Score": dht.int64, "Title": dht.string}
deniro = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    header=header,
    headless=True,
)
```

### Other formats

#### Tab-delimited data

Deephaven allows you to specify other delimiters as a second argument if your file is not comma-delimited. In the example below, we import a tab-delimited file, which requires a second argument.

```python order=deniro_tsv
from deephaven import read_csv

deniro_tsv = read_csv(
    "https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv",
    delimiter="\t",
)
```

#### Pipe-delimited data

Any character can be used as a delimiter. The pipe character (`|`) is common. In the example below, we supply the delimiter `|` as the second argument.

```python order=deniro_psv
from deephaven import read_csv

deniro_psv = read_csv(
    "https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv",
    delimiter="|",
)
```

#### Trim

By default, quoted values that have leading and trailing white space include the white space when reading the CSV file. For example, if `" Taxi Driver "` is in the CSV file, it will be read as `Taxi Driver`.

By setting `trim` to `true` when reading the CSV file, these leading and trailing white space will be removed. So `" Taxi Driver "` will be read as `Taxi Driver`.

### Using optional arguments

In the following example, we want to read in the `deniro_poorly_formatted.csv` file from the Deephaven Examples repo. This file has a number of issues that we need to address, but `read_csv` can handle them if we give the right input parameters:

```python order=deniro
from deephaven import read_csv, input_table
import deephaven.dtypes as dht

my_dict = {"ReleaseYear": dht.int16, "Rating": dht.int16, "Movie Title": dht.string}

deniro = read_csv(
    path="/data/examples/DeNiro/csv/deniro_poorly_formatted.csv",
    header=my_dict,
    header_row=1,
    skip_rows=5,
    num_rows=20,
    ignore_empty_lines=True,
    allow_missing_columns=True,
    ignore_excess_columns=True,
    trim=True,
)
```

In the example above, we:

- Use `header` to override and replace the files's header row, specified column names, and data types.
- Set `header_row` to 1 to indicate that the row in position 1 of the file is the header row.
- Use `skip_rows` to omit the first 5 rows of the CSV file.
- Set `num_rows` to 20 to limit our table to 20 rows.
- Set `ignore_empty_lines` to `True` avoid throwing an exception due to an empty line in the CSV file.
- Set `allow_missing_columns` to `True` to avoid throwing an exception due to a missing column in the CSV file. Note that the 1983 entry in our table has `null` in the missing column.
- Set `ignore_excess_columns` to `True` to avoid throwing an exception due to an extra column in the CSV file - 1984's _Brazil_ entry has an extra column containing a brief review that we don't need in our table.
- Set `trim` to `True` to remove leading and trailing white space from inside quoted strings - such as our movie titles.

See the [`read_csv` reference documentation](../../reference/data-import-export/CSV/readCsv.md) for a complete list of optional arguments.

## Related documentation

- [How to export CSV files](./csv-export.md)
- [`read_csv`](../../reference/data-import-export/CSV/readCsv.md)
