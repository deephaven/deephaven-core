---
title: read_csv
---

The `read_csv` method will read a CSV file into an in-memory table.

## Syntax

```python syntax
read_csv(
    path: str,
    header: Sequence[str] | dict[str, dht.DType] = None,
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
    col_types: dict[str, dht.DType] = None,
) -> Table:
```

> [!NOTE]
> The `deephaven` package's `read_csv` method is identical in function to `deephaven.csv.read`; however, `read_csv` is the preferred method as it differentiates the method from [`deephaven.parquet.read`](../Parquet/readTable.md).

## Parameters

The column names come from the file's header row, from `header`, or are `Column1`, `Column2`, and so on for a headless file. Deephaven turns names that aren't legal column names into legal ones; for example, `my venue` becomes `my_venue`. The column types are inferred from the data, unless a `header` dict or `col_types` sets them.

`header` applies names by position. The first name goes to the first column, the second to the second, and so on. The names replace the file's header names and are not matched against them. `col_types` matches by name, in any order, and uses the names before Deephaven makes them legal.

<ParamTable>
<Param name="path" type="str">

The file to load into a table. Note that compressed files _are_ accepted: paths ending in ".tar.zip", ".tar.bz2", ".tar.gz", ".tar.7z", ".tar.zst", ".zip", ".bz2", ".gz", ".7z", ".zst", or ".tar" will automatically be decompressed before they are read.

</Param>
<Param name="header" type="Sequence[str] | dict" optional>

The column names, applied by position. The number of names sets the number of columns. A row with more cells is an error, unless `ignore_excess_columns` is `True`. Default is `None`, which takes the names from the file.

- A sequence of names only renames the columns.
- A dict also sets each column's type, so its keys must be in the file's column order, or the names and types go to the wrong columns. To set types by name, use `col_types`. A dict can't be combined with `col_types`.

</Param>
<Param name="headless" type="bool" optional>

- `False` (default) - first row contains header information.
- `True` - first row is included in your dataset.

</Param>
<Param name="header_row" type="int" optional>

The header row number:

- All the rows before it will be skipped.
- The default is 0.
- Must be 0 if headless is `True`, otherwise an exception will be raised

</Param>
<Param name="skip_rows" type="int" optional>

The number of rows to skip before processing data. Default is 0.

</Param>
<Param name="num_rows" type="int" optional>

The maximum number of rows to process. Default is all rows in a file.

</Param>
<Param name="ignore_empty_lines" type="bool" optional>

- `False` (default) - Empty lines are treated as errors.
- `True` - Empty lines in the CSV file are ignored.

</Param>
<Param name="allow_missing_columns" type="bool" optional>

- `False` (default) - Missing columns are treated as errors.
- `True` - Missing columns in rows are treated as empty strings.

</Param>
<Param name="ignore_excess_columns" type="bool" optional>

- `False` (default) - Extra columns in rows are treated as errors.
- `True` - Excess columns in rows are ignored.

</Param>
<Param name="delimiter" type="char" optional>

The delimiter for the file.

- `<delimiter>` is the delimiter being used by the text file. Any non-newline string can be specified (i.e.,`,`, `;`, `:`, `\`, `|`, etc.).
- The default is `,`.

</Param>
<Param name="quote" type="char" optional>

The char surrounding a string value. Default is `\"`.

</Param>
<Param name="ignore_surrounding_spaces" type="bool" optional>

Trim leading and trailing blanks from non-quoted values. Default is `True`.

</Param>
<Param name="trim" type="bool" optional>

Trim leading and trailing blanks from inside quoted values. Default is `False`.

</Param>
<Param name="col_types" type="dict" optional>

The data types of some or all columns, matched by name: `dict[str, DType]`. A name that matches no column raises an error. Default is `None`, which infers the types, except those a `header` dict sets.

</Param>
</ParamTable>

## Returns

A new in-memory table from a CSV file.

## Examples

> [!NOTE]
> In this guide, we read data from locations relative to the base of the Docker container. See [Docker bind mounts](../../../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

In the following example, [`write_csv`](./writeCsv.md) writes the source table to `/data/file.csv`, and `read_csv` loads the file into a Deephaven table.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import read_csv, write_csv
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

> [!NOTE]
> In the following examples, the example data found in [Deephaven's example repository](https://github.com/deephaven/examples) will be used. Follow the instructions in the [README](https://github.com/deephaven/examples/blob/main/README.md) to download the data to the proper location for use with Deephaven.

In the following example, `read_csv` is used to load the file [DeNiro CSV](https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv) into a Deephaven table.

```python
from deephaven import read_csv

result = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv"
)
```

In the following example, `col_types` sets the types of two columns by name. The keys can be in any order, and the `Year` column, which is left out, keeps its inferred type.

```python
from deephaven import read_csv
import deephaven.dtypes as dht

result = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv",
    col_types={"Title": dht.string, "Score": dht.double},
)
```

Any character can be used as a delimiter. The pipe and tab characters (`|` and `\t`) are common. In the following example, the second input parameter is used to read pipe- and tab-delimited files into memory.

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
- [Docker bind mounts](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#readCsv(java.nio.file.Path))
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.read_csv)
