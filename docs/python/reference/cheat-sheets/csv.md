---
title: CSV Cheat Sheet
sidebar_label: CSV
---

- [`read_csv`](../data-import-export/CSV/readCsv.md)
- [`write_csv`](../data-import-export/CSV/writeCsv.md)

```python order=source,result,iris,deniro_default,deniro_header,deniro_tsv,deniro_psv
# Create a table
from deephaven import empty_table

source = empty_table(100).update(
    formulas=[
        "X = 0.1 * i",
        "SinX = X % 0.2 < 0.01 ? NULL_DOUBLE : sin(X)",
        "CosX = cos(X)",
        "TanX = tan(X)",
    ]
)

# Write to a local file
# `(null)` for null cells
from deephaven import write_csv

write_csv(source, "/data/TrigFunctions.csv")

# Write a subset of data
write_csv(source, "/data/Cosine.csv", ["X", "SinX"])

# Read from a local file
from deephaven import read_csv

result = read_csv("/data/TrigFunctions.csv")

# Read from a csv from URL
iris = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

# Read headerless, default column names
deniro_default = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    headless=True,
)

# Read headerless, provide names for header
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

- [CSV import via query](../../how-to-guides/data-import-export/csv-import.md)
- [CSV export via query](../../how-to-guides/data-import-export/csv-export.md)
- [`read_csv`](../data-import-export/CSV/readCsv.md)
- [`write_csv`](../data-import-export/CSV/writeCsv.md)
- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#readCsv(java.nio.file.Path))
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.read_csv)
