---
title: write_csv
---

The `write_csv` function writes a table to a standard CSV file. `write_csv` writes null values as empty fields, separates fields with commas, and writes `Instant` values in the Deephaven server's system default time zone, followed by their UTC offset (for example, `2021-07-04T12:00Z` in UTC).

> [!NOTE]
> `write_csv` writes the rows the table holds when the call runs, and reads them without taking a snapshot. If the table ticks during the write, the file can be inconsistent. To write a ticking table, write a [`snapshot`](../../table-operations/snapshot/snapshot.md) of it instead, as shown in [Write a ticking table](../../../how-to-guides/data-import-export/csv-export.md#write-a-ticking-table).

## Syntax

```python syntax
write_csv(table: Table, path: str, cols: Sequence[str] = []) -> None
```

> [!NOTE]
> The `write_csv` function in the `deephaven` package is the same function as [`deephaven.csv.write`](/core/pydoc/code/deephaven.csv.html#deephaven.csv.write). Use `write_csv` because its name distinguishes it from [`deephaven.parquet.write`](../Parquet/writeTable.md).

## Parameters

<ParamTable>
<Param name="table" type="Table">

The table to write to file.

</Param>
<Param name="path" type="str">

The path of the file to write the table to. If the file exists, `write_csv` overwrites it. The parent directory must already exist.

</Param>

<Param name="cols" type="Sequence[str]" optional>

The source column(s) to include in the CSV file. The default value is all source columns.

</Param>
</ParamTable>

## Returns

None. `write_csv` writes the table as a CSV file at `path`.

## Examples

> [!NOTE]
> The path is resolved on the Deephaven server's file system. When Deephaven runs in Docker, that is the container's file system. See [Docker bind mounts](../../../conceptual/docker-data-volumes.md) to learn how container paths map to your local file system.

In the following example, `write_csv` writes all columns of the source table to `/data/output.csv`.

```python
from deephaven import new_table, write_csv
from deephaven.column import string_col, int_col
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

In the following example, `write_csv` writes only the columns `X` and `Z` to the output file.

```python
from deephaven import new_table, write_csv
from deephaven.column import string_col, int_col
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

- [Read CSV or other delimited files](../../../how-to-guides/data-import-export/csv-import.md)
- [Export data to CSV or other delimited files](../../../how-to-guides/data-import-export/csv-export.md)
- [Docker bind mounts](../../../conceptual/docker-data-volumes.md)
- [`snapshot`](../../table-operations/snapshot/snapshot.md)
- [`parquet.write`](../Parquet/writeTable.md)
- [Pydoc: `deephaven.write_csv`](/core/pydoc/code/deephaven.html#deephaven.write_csv)
- [Pydoc: `deephaven.csv.write`](/core/pydoc/code/deephaven.csv.html#deephaven.csv.write)
