---
title: Export data to CSV or other delimited files
---

This guide shows how to export table data from Deephaven to CSV and other delimited files with [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md).

> [!TIP]
> You can also export a table to a CSV file from the Deephaven UI with **Table Options > Download CSV**.

## `writeCsv`

> [!NOTE]
> The Deephaven server resolves file paths. If you run Deephaven in Docker, the path refers to the container's file system. The default Docker Compose setup mounts the `./data` directory of your Deephaven deployment as the `/data` [Docker data volume](../../conceptual/docker-data-volumes.md#the-data-mount-point) in the container. A file written to `/data/TrigFunctions.csv` appears at `./data/TrigFunctions.csv` on your computer. If you run Deephaven without Docker, replace `/data` with a directory that exists on the server's machine.

### Write a table to a CSV file

Let's create a table to export. It contains 100 rows of trigonometric values, made with [`emptyTable`](../../reference/table-operations/create/emptyTable.md) and [`update`](../../reference/table-operations/select/update.md):

```groovy test-set=1
source = emptyTable(100).update(
    "X = 0.1 * i",
    "SinX = sin(X)",
    "CosX = cos(X)",
    "TanX = tan(X)"
)
```

The simplest way to use [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md) is to pass two arguments:

- The Deephaven source table.
- The path of the output CSV file.

```groovy test-set=1
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(source, "/data/TrigFunctions.csv")
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

Null values are common in tables. By default, [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md) writes them as empty fields.

To see this, let's create a table with null values. The example below uses the [ternary conditional operator](../ternary-if-how-to.md) to fill the `SinX` column with a large number of nulls.

```groovy test-set=2
sourceWithNulls = emptyTable(100).update(
    "X = 0.1 * i",
    "SinX = X % 0.2 < 0.01 ? NULL_DOUBLE : sin(X)",
    "CosX = cos(X)",
    "TanX = tan(X)"
)
```

The `SinX` column contains many null cells. The example below writes this table to a CSV file called `TrigFunctionsWithNulls.csv`.

```groovy test-set=2
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(sourceWithNulls, "/data/TrigFunctionsWithNulls.csv")
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

To write nulls as `(null)` instead, use the overload that takes a boolean `nullsAsEmpty` argument right after the file path, and pass `false`:

```groovy test-set=2
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(sourceWithNulls, "/data/TrigFunctionsWithNullStrings.csv", false)
```

The first lines of `TrigFunctionsWithNullStrings.csv` show `(null)` in place of each null `SinX` value:

```text
X,SinX,CosX,TanX
0.0,(null),1.0,0.0
0.1,0.09983341664682815,0.9950041652780258,0.10033467208545055
0.2,(null),0.9800665778412416,0.2027100355086725
```

### Column selection

To write only some columns, pass their names as extra arguments after the file path. The example below writes the `X` and `CosX` columns of the `sourceWithNulls` table created in [Null values](#null-values).

```groovy test-set=2
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(sourceWithNulls, "/data/Cosine.csv", "X", "CosX")
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

### Other delimiters, compression, and time zone

By default, [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md) separates fields with commas. To use a different delimiter, call the overload that takes a `separator` argument. The same overload also writes bz2-compressed files and writes `Instant` values in a time zone other than the system default. That overload has this signature:

```groovy syntax
writeCsv(source, destPath, compressed, timeZone, nullsAsEmpty, separator, columns...)
```

Between the file path and the separator, it takes these arguments, in order:

- `compressed`: whether to compress the file with bz2. When `true`, [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md) appends `.bz2` to the file path.
- `timeZone`: the time zone in which to write `Instant` values.
- `nullsAsEmpty`: whether to write nulls as empty fields.

In this overload, the boolean right after the path is `compressed`, not `nullsAsEmpty` as in [Null values](#null-values).

The example below writes the `sourceWithNulls` table created in [Null values](#null-values) as a tab-separated file, with the same defaults that the two-argument call uses: no compression, the system default time zone, and nulls as empty fields.

```groovy test-set=2
import static io.deephaven.csv.CsvTools.writeCsv

writeCsv(sourceWithNulls, "/data/TrigFunctionsWithNulls.tsv", false, timeZone(), true, '\t' as char)
```

### Write a ticking table

Deephaven tables can tick, updating in real time as new data arrives. [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md) writes the rows that a table holds when the call runs, so the file is a point-in-time copy of a live table. It reads those rows directly, without taking a snapshot, so a table that updates during the write can produce an inconsistent file. To write a consistent copy, take a [`snapshot`](../../reference/table-operations/snapshot/snapshot.md) of the table first and write the snapshot.

The example below creates a ticking table with [`timeTable`](../../reference/table-operations/create/timeTable.md) and writes a snapshot of it:

```groovy test-set=3 ticking-table order=null
import static io.deephaven.csv.CsvTools.writeCsv

live = timeTable("PT1s").update("X = i")

writeCsv(live.snapshot(), "/data/LiveSnapshot.csv")
```

To keep a file up to date as a table ticks, see [How can I periodically write a ticking table to disk?](../../reference/community-questions/write-ticking-table-periodically.md).

## Related documentation

- [Create an empty table with `emptyTable`](../new-and-empty-table.md#emptytable)
- [Read CSV or other delimited files](./csv-import.md)
- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [`writeCsv`](../../reference/data-import-export/CSV/writeCsv.md)
