---
title: read_csv
---

The `read_csv` method reads a CSV file into an in-memory table.

## Syntax

```python syntax
read_csv(
    path: str,
    header: dict[str, DType] | None = None,
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

> [!NOTE]
> `read_csv` is the `deephaven.csv.read` function, re-exported from the top-level `deephaven` package under a name that distinguishes it from [`deephaven.parquet.read`](../Parquet/readTable.md).

## Parameters

<ParamTable>
<Param name="path" type="str">

The file path or URL to load into a table. `read_csv` accepts compressed files: it decompresses local files that end in `.tar.zip`, `.tar.bz2`, `.tar.gz`, `.tar.7z`, `.tar.zst`, `.zip`, `.bz2`, `.gz`, `.7z`, `.zst`, or `.tar` before reading them.

</Param>
<Param name="header" type="dict" optional>

A `dict[str, DType]` that defines the table's columns. Each key is a column name, and each value is a data type from the `deephaven.dtypes` module. This page imports that module as `dht`. See [Deephaven Python data types](../../python/deephaven-python-types.md). Default is `None`, in which case `read_csv` takes column names from the header row and infers each column's type from the data.

`read_csv` rewrites column names that are not valid Deephaven column names, such as names that contain spaces, into valid ones.

Keys apply by position: the first key names and types the first column of the file, the second key the second column, and so on. If the file has a header row, these names replace the names in that row. `read_csv` does not match the keys against the header row, so list the columns in the file's order.

Supported types are `dht.bool_`, `dht.byte`, `dht.char`, `dht.short`, `dht.int32`, `dht.long`, `dht.float32`, `dht.double`, `dht.string`, and `dht.Instant`, plus their aliases, such as `dht.int64`. Any other type raises an error.

</Param>
<Param name="headless" type="bool" optional>

- `False` (default) — The file has a header row. `header_row` sets which row it is.
- `True` — The first row is data. Unless `header` supplies names, the columns are named `Column1`, `Column2`, and so on.

</Param>
<Param name="header_row" type="int" optional>

The 0-based number of the header row. `read_csv` skips all rows before it. Default is 0.

If `headless` is `True`, `header_row` must be 0, or `read_csv` raises an exception.

</Param>
<Param name="skip_rows" type="int" optional>

The number of data rows to skip before processing data. Empty lines count toward this number. Default is 0.

</Param>
<Param name="num_rows" type="int" optional>

The maximum number of rows to process. Empty lines that `ignore_empty_lines` skips count toward this limit. Default is all rows in the file.

</Param>
<Param name="ignore_empty_lines" type="bool" optional>

- `False` (default) — `read_csv` reads an empty line as a row whose first value is null and whose other columns are missing. If the file has more than one column, `read_csv` raises an error unless `allow_missing_columns` is `True`, in which case it fills the missing columns with null values.
- `True` — `read_csv` skips empty lines.

</Param>
<Param name="allow_missing_columns" type="bool" optional>

- `False` (default) — `read_csv` raises an error for missing columns.
- `True` — `read_csv` fills missing columns at the end of a short row with null values.

</Param>
<Param name="ignore_excess_columns" type="bool" optional>

- `False` (default) — `read_csv` raises an error for excess columns in a row.
- `True` — `read_csv` ignores excess columns in a row.

</Param>
<Param name="delimiter" type="str" optional>

The single 7-bit ASCII character that separates fields, such as `,`, `;`, `:`, `|`, or `\t`. Default is `,`.

</Param>
<Param name="quote" type="str" optional>

The single 7-bit ASCII character that encloses a field so that delimiters and line breaks inside it are read as literal text. Default is `"`.

</Param>
<Param name="ignore_surrounding_spaces" type="bool" optional>

Whether to trim leading and trailing spaces and tabs from non-quoted values. Default is `True`.

</Param>
<Param name="trim" type="bool" optional>

Whether to trim leading and trailing spaces and tabs from inside quoted values. Default is `False`.

</Param>
</ParamTable>

## Returns

A new in-memory table containing the CSV data.

## Examples

> [!NOTE]
> The first two examples read and write files at paths inside the Docker container. See [Docker bind mounts](../../../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

In the following example, [`write_csv`](./writeCsv.md) writes the source table to `/data/file.csv`, and `read_csv` loads the file into a Deephaven table.

```python order=source,result
from deephaven import new_table, read_csv, write_csv
from deephaven.column import string_col, int_col
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col("X", ["A", "B", None, "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, NULL_INT, 3]),
        int_col("Z", [55, 76, 20, NULL_INT, 230, 50, 73, 137, 214]),
    ]
)

write_csv(source, "/data/file.csv")

result = read_csv("/data/file.csv")
```

`read_csv` infers each column's type from the data, so the single-letter values in `X` become a `char` column. To keep `X` as a string column, set its type with `header`, as the next example does.

In the following example, the file `/data/headless.csv` has no header row. `headless=True` tells `read_csv` that the first row is data, and `header` names the columns and sets their types. The keys apply by position, so the example lists them in the file's column order.

```python order=result
from deephaven import read_csv
import deephaven.dtypes as dht

with open("/data/headless.csv", "w") as f:
    f.write("A,2,55\nB,4,76\nC,1,20\n")

result = read_csv(
    "/data/headless.csv",
    header={"X": dht.string, "Y": dht.int32, "Z": dht.long},
    headless=True,
)
```

`read_csv` also accepts a URL. In the following example, `read_csv` loads the file [DeNiro CSV](https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv) directly from [Deephaven's example repository](https://github.com/deephaven/examples).

```python
from deephaven import read_csv

result = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv"
)
```

Pipe (`|`) and tab (`\t`) are common delimiters. In the following example, `read_csv` uses the `delimiter` parameter to read pipe- and tab-delimited files into memory.

```python order=result_psv,result_tsv
from deephaven import read_csv

result_psv = read_csv(
    "https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv",
    delimiter="|",
)
result_tsv = read_csv(
    "https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv",
    delimiter="\t",
)
```

## Related documentation

- [Read CSV or other delimited files](../../../how-to-guides/data-import-export/csv-import.md)
- [Export data to CSV or other delimited files](../../../how-to-guides/data-import-export/csv-export.md)
- [`write_csv`](./writeCsv.md)
- [`read` (Parquet)](../Parquet/readTable.md)
- [Docker bind mounts](../../../conceptual/docker-data-volumes.md)
- [Deephaven Python data types](../../python/deephaven-python-types.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#readCsv(java.lang.String,io.deephaven.csv.CsvSpecs))
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.read_csv)
