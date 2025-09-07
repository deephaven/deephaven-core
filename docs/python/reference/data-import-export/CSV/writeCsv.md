---
title: write_csv
---

The `write_csv` method will write a table to a standard CSV file.

## Syntax

```python syntax
write(table: Table, path: str, cols: list[str])
```

> [!NOTE]
> The `deephaven` package's `write_csv` method is identical in function to `deephaven.csv.write`; however, `write_csv` is the preferred method as it differentiates the method from [`deephaven.parquet.write`](../Parquet/writeTable.md).

## Parameters

<ParamTable>
<Param name="table" type="Table">

The table to write to file.

</Param>
<Param name="path" type="str">

Path name of the file where the table will be stored.

</Param>

<Param name="cols" type="list[str]" optional>

The source column(s) to include in the CSV file. The default value is all source columns.

</Param>
</ParamTable>

## Returns

A CSV file located in the specified path.

## Examples

> [!NOTE]
> Deephaven writes files to locations relative to the base of its Docker container. See [Docker data volumes](../../../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

In the following example, `write_csv` writes the source table to `/data/output.csv`. All columns are included.

```python
from deephaven import new_table

from deephaven.column import string_col, int_col
from deephaven import write_csv
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col("X", ["A", "B", None, "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, NULL_INT, 3]),
        int_col("Z", [55, 76, 20, NULL_INT, 230, 50, 73, 137, 214]),
    ]
)

write_csv(source, "/data/output.csv")
```

In the following example, only the columns `X` and `Z` are written to the output file.

```python
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import write_csv
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col("X", ["A", "B", None, "C", "B", "A", "B", "B", "C"]),
        int_col("Y", [2, 4, 2, 1, 2, 3, 4, NULL_INT, 3]),
        int_col("Z", [55, 76, 20, NULL_INT, 230, 50, 73, 137, 214]),
    ]
)

write_csv(source, "/data/output.csv", cols=["X", "Z"])
```

## Related documentation

- [CSV import via query](../../../how-to-guides/data-import-export/csv-import.md)
- [CSV export via query](../../../how-to-guides/data-import-export/csv-export.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#writeCsv(io.deephaven.engine.table.Table,java.lang.String,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.html#deephaven.write_csv)
