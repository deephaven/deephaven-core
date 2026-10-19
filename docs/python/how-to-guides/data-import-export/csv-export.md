---
title: Export data to CSV or other delimited files
---

This guide shows how to export table data from Deephaven to CSV files with [`write_csv`](../../reference/data-import-export/CSV/writeCsv.md), and to other delimited files with [pandas](../use-pandas.md).

> [!TIP]
> You can also export a table to a CSV file from the Deephaven UI with **Table Options > Download CSV**.

## `write_csv`

> [!NOTE]
> The Deephaven server resolves file paths. If you run Deephaven in Docker, the path refers to the container's file system. The default Docker Compose setup mounts the `./data` directory of your Deephaven deployment as the `/data` [Docker data volume](../../conceptual/docker-data-volumes.md#the-data-mount-point) in the container. A file written to `/data/TrigFunctions.csv` appears at `./data/TrigFunctions.csv` on your computer. If you run Deephaven without Docker, replace `/data` with a directory that exists on the server's machine.

### Write a table to a CSV file

Let's create a table to export. It contains 100 rows of trigonometric values, made with [`empty_table`](../../reference/table-operations/create/emptyTable.md) and [`update`](../../reference/table-operations/select/update.md):

```python test-set=1
from deephaven import empty_table

source = empty_table(100).update(
    formulas=["X = 0.1 * i", "SinX = sin(X)", "CosX = cos(X)", "TanX = tan(X)"]
)
```

The simplest way to use [`write_csv`](../../reference/data-import-export/CSV/writeCsv.md) is to pass two arguments:

- The Deephaven source table.
- The path of the output CSV file.

```python test-set=1
from deephaven import write_csv

write_csv(source, "/data/TrigFunctions.csv")
```

The first lines of `TrigFunctions.csv` look like this:

```text
X,SinX,CosX,TanX
0.0,0.0,1.0,0.0
0.1,0.09983341664682815,0.9950041652780258,0.10033467208545055
0.2,0.19866933079506122,0.9800665778412416,0.2027100355086725
0.30000000000000004,0.2955202066613396,0.955336489125606,0.3093362496096233
0.4,0.3894183423086505,0.9210609940028851,0.4227932187381618
```

### Null values

Null values are common in tables. [`write_csv`](../../reference/data-import-export/CSV/writeCsv.md) writes them as empty fields.

To see this, let's create a table with null values. The example below uses the [ternary conditional operator](../ternary-if-how-to.md) to fill the `SinX` column with a large number of nulls.

```python test-set=2
from deephaven import empty_table

source_with_nulls = empty_table(100).update(
    formulas=[
        "X = 0.1 * i",
        "SinX = X % 0.2 < 0.01 ? NULL_DOUBLE : sin(X)",
        "CosX = cos(X)",
        "TanX = tan(X)",
    ]
)
```

The `SinX` column contains many null cells. The example below writes this table to a CSV file called `TrigFunctionsWithNulls.csv`.

```python test-set=2
from deephaven import write_csv

write_csv(source_with_nulls, "/data/TrigFunctionsWithNulls.csv")
```

The first lines of `TrigFunctionsWithNulls.csv` show the null `SinX` values as empty fields:

```text
X,SinX,CosX,TanX
0.0,,1.0,0.0
0.1,0.09983341664682815,0.9950041652780258,0.10033467208545055
0.2,,0.9800665778412416,0.2027100355086725
0.30000000000000004,0.2955202066613396,0.955336489125606,0.3093362496096233
0.4,,0.9210609940028851,0.4227932187381618
```

### Column selection

To write only some columns, pass their names in the `cols` parameter of [`write_csv`](../../reference/data-import-export/CSV/writeCsv.md). The example below writes the `X` and `CosX` columns of the `source_with_nulls` table created in [Null values](#null-values).

```python test-set=2
from deephaven import write_csv

write_csv(source_with_nulls, "/data/Cosine.csv", cols=["X", "CosX"])
```

`Cosine.csv` contains only the `X` and `CosX` columns:

```text
X,CosX
0.0,1.0
0.1,0.9950041652780258
0.2,0.9800665778412416
0.30000000000000004,0.955336489125606
0.4,0.9210609940028851
```

### Write a ticking table

Deephaven tables can tick, updating in real time as new data arrives. [`write_csv`](../../reference/data-import-export/CSV/writeCsv.md) writes the rows that a table holds when the call runs, so the file is a point-in-time copy of a live table. It reads those rows directly, without taking a snapshot, so a table that updates during the write can produce an inconsistent file. To write a consistent copy, take a [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) of the table first and write the snapshot.

The example below creates a ticking table with [`time_table`](../../reference/table-operations/create/timeTable.md) and writes a snapshot of it:

```python test-set=3 ticking-table order=null
from deephaven import time_table, write_csv

live = time_table("PT1s").update(formulas=["X = i"])

write_csv(live.snapshot(), "/data/LiveSnapshot.csv")
```

To keep a file up to date as a table ticks, see [How can I periodically write a ticking table to disk?](../../reference/community-questions/write-ticking-table-periodically.md).

## Write other delimited files with pandas

[`write_csv`](../../reference/data-import-export/CSV/writeCsv.md) always separates fields with commas. It has no parameter for a different delimiter.

To write a file with a different delimiter, convert the table to a pandas DataFrame with [`to_pandas`](../../reference/pandas/to-pandas.md) and call the DataFrame's [`to_csv`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html) method with the `sep` argument. The example below writes the `source` table created in [Write a table to a CSV file](#write-a-table-to-a-csv-file) as a tab-separated file:

```python test-set=1
from deephaven.pandas import to_pandas

to_pandas(source).to_csv("/data/TrigFunctions.tsv", sep="\t", index=False)
```

[`to_pandas`](../../reference/pandas/to-pandas.md) copies the whole table into memory, so for a large table, select only the rows and columns you need first.

## Related documentation

- [Create an empty table with `empty_table`](../new-and-empty-table.md#empty_table)
- [Read CSV or other delimited files](./csv-import.md)
- [Use pandas with Deephaven](../use-pandas.md)
- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [`write_csv`](../../reference/data-import-export/CSV/writeCsv.md)
