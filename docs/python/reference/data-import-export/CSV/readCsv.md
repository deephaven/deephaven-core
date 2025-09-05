---
title: read_csv
---

The `read_csv` method will read a CSV file into an in-memory table.

## Syntax

```python syntax
read_csv(
    path: str,
    header: dict[str, dht.DType] = None,
    headless: bool = False,
    header_row: int = 0
    skip_rows: int = 0,
    num_rows: int = MAX_LONG,
    ignore_empty_lines: bool = False,
    allow_missing_columns: bool = False,
    ignore_excess_columns: bool = False,
    delimiter: str = ",",
    quote: str = '"',
    ignore_surrounding_spaces: bool = True,
    trim: bool = False,
) -> Table:
```

> [!NOTE]
> The `deephaven` package's `read_csv` method is identical in function to `deephaven.csv.read`; however, `read_csv` is the preferred method as it differentiates the method from [`deephaven.parquet.read`](../Parquet/readTable.md).

## Parameters

<ParamTable>
<Param name="path" type="str">

The file to load into a table. Note that compressed files _are_ accepted: paths ending in ".tar.zip", ".tar.bz2", ".tar.gz", ".tar.7z", ".tar.zst", ".zip", ".bz2", ".gz", ".7z", ".zst", or ".tar" will automatically be decompressed before they are read.

</Param>
<Param name="header" type="dict" optional>

Define a dictionary for the header and the data type: `[str, DataType]`. Default is `None`.

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

The number of rows to skip before processing data. Default is none.

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
<Param name="charset" type="str" optional>

The character set. Default is `utf-8`.

</Param>
<Param name="csvSpecs" type="CsvSpecs" optional>

Specifications for how to load the CSV file.

</Param>
</ParamTable>

> [!NOTE]
> Only one format parameter can be used at a time.

## Returns

A new in-memory table from a CSV file.

## Examples

> [!NOTE]
> In this guide, we read data from locations relative to the base of the Docker container. See [Docker data volumes](../../../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

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

- [CSV import via query](../../../how-to-guides/data-import-export/csv-import.md)
- [CSV export via query](../../../how-to-guides/data-import-export/csv-export.md)
- [`write_csv`](./writeCsv.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#readCsv(java.nio.file.Path))
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.read_csv)
