---
title: Read CSV or other delimited files
---

This guide shows you how to read data from CSV (and other delimited) files into in-memory Deephaven tables with the [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) method. The result is a static table that holds the file's contents when you read it. To work with data that keeps arriving, see [Kafka](./kafka-stream.md).

> [!TIP]
> To upload a CSV without writing code, see [Upload a table from a file](../user-interface/upload-table-from-file.md).

## `read_csv`

The two most common ways to load a CSV file are using the file path or the file URL. The header row of the CSV file supplies the column names. [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) rewrites names that aren't valid Deephaven column names, such as names that contain spaces or hyphens, into valid ones.

The full signature of [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) follows. Only `path` is required:

```python syntax
from deephaven import read_csv

read_csv(
    path: str,
    header: Optional[dict[str, DType]] = None,
    headless: bool = False,
    header_row: int = 0,
    skip_rows: int = 0,
    num_rows: int = MAX_LONG,
    ignore_empty_lines: bool = False,
    allow_missing_columns: bool = False,
    ignore_excess_columns: bool = False,
    delimiter: str = ",",
    quote: str = '"',
    ignore_surrounding_spaces: bool = True,
    trim: bool = False,
) -> Table
```

Let's dive into some simple examples. These examples use CSV files from [Deephaven's examples repository](https://github.com/deephaven/examples), so you don't have to create your own. We encourage you to use your own files by replacing the file paths in our queries.

The URL examples need no setup. The examples that read from a file path expect those files under `/data/examples`. To set that up, follow the directions in the [examples repository README](https://github.com/deephaven/examples/blob/main/README.md) and see [The `/data` mount point](#the-data-mount-point).

### `read_csv` with a file path

Use [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) to read a CSV file from a file path. In this example, we read a CSV file that contains R.A. Fisher's classic iris flower dataset, which is commonly used in machine learning.

```python order=iris
from deephaven import read_csv

iris = read_csv("/data/examples/Iris/csv/iris.csv")
```

#### The `/data` mount point

The Compose examples in the [Docker install guide](../../getting-started/docker-install.md) mount `./data:/data` in `docker-compose.yml`. With that mount, Docker shares the `data` folder next to your `docker-compose.yml` file with the running Deephaven container at `/data`. See [Docker data volumes](../../conceptual/docker-data-volumes.md) for more about how container paths map to your computer. A file you save at `./data/abc/file.csv` on your computer is readable in Deephaven at `/data/abc/file.csv`. A container started with `docker run` and no `-v` option still has a `/data` directory, but Docker doesn't share it with your computer.

> [!NOTE]
> With that mount, if the `./data` directory does not exist when you launch Deephaven, Docker creates it.

### `read_csv` with a URL

Now, let's read the same iris dataset, but this time we pass [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) a URL to the file in Deephaven's examples repository on GitHub instead of a file path.

```python order=iris
from deephaven import read_csv

iris = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
```

> [!NOTE]
> This works with a publicly accessible `http` or `https` URL that points to an uncompressed CSV file. [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) decompresses a compressed file only when it reads the file from a file path or a `file:` URL.

### Headerless CSV files

CSV files don't always have headers. The example below uses the headerless [DeNiro CSV](https://github.com/deephaven/examples/blob/main/DeNiro/csv/deniro_headerless.csv) and sets `headless=True` so that [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) treats the first row as data.

```python order=deniro
from deephaven import read_csv

deniro = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    headless=True,
)
```

Because no column names are provided, the table uses default column names (`Column1`, `Column2`, etc.). To name the columns, pass `header` a dict that maps each column name to its type. Keys are applied by position, so list them in the file's column order.

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

### Other delimiters

Deephaven lets you specify other delimiters if your file is not comma-delimited. Use the `delimiter` argument, which accepts any single 7-bit ASCII character.

#### Tab-delimited data

The example below reads a tab-delimited file with `delimiter="\t"`.

```python order=deniro_tsv
from deephaven import read_csv

deniro_tsv = read_csv(
    "https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv",
    delimiter="\t",
)
```

#### Pipe-delimited data

The pipe character (`|`) is a common delimiter. In the example below, we set `delimiter="|"`.

```python order=deniro_psv
from deephaven import read_csv

deniro_psv = read_csv(
    "https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv",
    delimiter="|",
)
```

### Read a poorly formatted file

In the following example, we read the `deniro_poorly_formatted.csv` file from Deephaven's examples repository. This file has a number of issues that we need to address, but [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) can handle them if we give the right input parameters:

```python order=deniro
from deephaven import read_csv
import deephaven.dtypes as dht

my_dict = {"ReleaseYear": dht.int16, "Rating": dht.int16, "MovieTitle": dht.string}

deniro = read_csv(
    path="/data/examples/DeNiro/csv/deniro_poorly_formatted.csv",
    header=my_dict,
    skip_rows=5,
    num_rows=20,
    ignore_empty_lines=True,
    allow_missing_columns=True,
    ignore_excess_columns=True,
    trim=True,
)
```

In the example above, we:

- Use `header` to replace the file's column names and set each column's type.
- Use `skip_rows` to skip the first 5 data rows after the header row.
- Set `num_rows` to 20 to read at most 20 lines after the skipped rows.
- Set `ignore_empty_lines` to `True` to skip the empty line in the CSV file. The skipped line still counts toward the `num_rows` limit, so the table has 19 rows. Without `ignore_empty_lines`, [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) treats an empty line as a row with missing columns. It throws an exception, or it reads a row of `null` values when `allow_missing_columns` is `True`.
- Set `allow_missing_columns` to `True` to avoid throwing an exception due to a missing column in the CSV file. Note that the 1983 entry in our table has `null` in the missing column.
- Set `ignore_excess_columns` to `True` to avoid throwing an exception due to an extra column in the CSV file. The entry for 1985's _Brazil_ has an extra column with a brief review that we don't need in our table.
- Set `trim` to `True` to remove leading and trailing white space from inside quoted strings, such as some of our movie titles. By default, [`read_csv`](../../reference/data-import-export/CSV/readCsv.md) keeps this white space and reads `" The Mission   "` with its leading and trailing spaces.

To skip lines that come before the header row, use `header_row` instead of `skip_rows`.

See the [`read_csv` reference documentation](../../reference/data-import-export/CSV/readCsv.md) for a complete list of optional arguments.

## Related documentation

- [Export data to CSV or other delimited files](./csv-export.md)
- [`read_csv`](../../reference/data-import-export/CSV/readCsv.md)
- [Navigate the GUI](../user-interface/navigating-the-ui.md)
