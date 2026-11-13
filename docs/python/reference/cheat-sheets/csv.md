---
title: CSV cheat sheet
---

This cheat sheet shows common ways to read and write CSV files with [`read_csv`](../data-import-export/CSV/readCsv.md) and [`write_csv`](../data-import-export/CSV/writeCsv.md). The examples write files to `/data`, the data directory in Deephaven's Docker images, and read them back from there or from a URL. To map it to a directory on your computer, see [Docker bind mounts](../../conceptual/docker-data-volumes.md). In other deployments, use any path the server can read and write.

## Create a table

The examples on this page write and read this sample table. Its `SinX` column is null in every other row, so the write examples can show how null cells are written.

```python test-set=1 order=source
from deephaven import empty_table

source = empty_table(100).update(
    formulas=[
        "X = 0.1 * i",
        "SinX = i % 2 == 0 ? NULL_DOUBLE : sin(X)",
        "CosX = cos(X)",
        "TanX = tan(X)",
    ]
)
```

## Write CSV files

[`write_csv`](../data-import-export/CSV/writeCsv.md) writes a table, or the columns you list, to a CSV file on the server.

```python test-set=1 order=null
from deephaven import write_csv

# Write the whole table to a file on the server
# Null cells are written as empty fields
write_csv(source, "/data/TrigFunctions.csv")

# Write a subset of data
write_csv(source, "/data/Cosine.csv", cols=["X", "CosX"])
```

## Read CSV files

[`read_csv`](../data-import-export/CSV/readCsv.md) reads a CSV file from a server path or a URL. Options handle files with no header row and other delimiters.

```python test-set=1 order=result,iris,deniro_default,deniro_header,deniro_tsv,deniro_psv
from deephaven import read_csv

# Read the file written in the previous section
result = read_csv("/data/TrigFunctions.csv")

# Read a CSV file from a URL
iris = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

# Read a file with no header row, using default column names
deniro_default = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    headless=True,
)

# Read a file with no header row, providing column names and types in file column order
import deephaven.dtypes as dht

header = {"Year": dht.int64, "Score": dht.int64, "Title": dht.string}
deniro_header = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    header=header,
    headless=True,
)

# Read files with other delimiters
deniro_tsv = read_csv(
    "https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv",
    delimiter="\t",
)
deniro_psv = read_csv(
    "https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv",
    delimiter="|",
)
```

## Related documentation

- [Docker bind mounts](../../conceptual/docker-data-volumes.md)
- [Read CSV or other delimited files](../../how-to-guides/data-import-export/csv-import.md)
- [Export data to CSV or other delimited files](../../how-to-guides/data-import-export/csv-export.md)
- [`empty_table`](../table-operations/create/emptyTable.md)
- [`read_csv`](../data-import-export/CSV/readCsv.md)
- [`update`](../table-operations/select/update.md)
- [`write_csv`](../data-import-export/CSV/writeCsv.md)
- [Python data types (`deephaven.dtypes`)](../python/deephaven-python-types.md)
- [`read_csv` Pydoc](/core/pydoc/code/deephaven.html#deephaven.read_csv)
- [`write_csv` Pydoc](/core/pydoc/code/deephaven.html#deephaven.write_csv)
