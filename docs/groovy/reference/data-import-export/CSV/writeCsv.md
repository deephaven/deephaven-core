---
title: writeCsv
---

The `writeCsv` method writes one or more tables to a CSV file, or writes a table as CSV text to a `PrintStream` or `Writer`. By default, `writeCsv` writes null values as empty fields, separates fields with commas, and writes `Instant` values in the Deephaven server's system default time zone, followed by their UTC offset.

> [!NOTE]
> `writeCsv` writes the rows the table holds when the call runs, and reads them without taking a snapshot. If the table ticks during the write, the file can be inconsistent. To write a ticking table, write a [`snapshot`](../../table-operations/snapshot/snapshot.md) of it instead, as shown in [Write a ticking table](../../../how-to-guides/data-import-export/csv-export.md#write-a-ticking-table).

## Syntax

```groovy syntax
// Write one table to a file
writeCsv(source, destPath, columns...)
writeCsv(source, destPath, nullsAsEmpty, columns...)
writeCsv(source, compressed, destPath, columns...)
writeCsv(source, compressed, destPath, nullsAsEmpty, columns...)
writeCsv(source, destPath, compressed, timeZone, columns...)
writeCsv(source, destPath, compressed, timeZone, nullsAsEmpty, columns...)
writeCsv(source, destPath, compressed, timeZone, nullsAsEmpty, separator, columns...)
writeCsv(source, destPath, compressed, timeZone, progress, columns...)
writeCsv(source, destPath, compressed, timeZone, progress, nullsAsEmpty, columns...)
writeCsv(source, destPath, compressed, timeZone, progress, nullsAsEmpty, separator, columns...)

// Write one table to a PrintStream
writeCsv(source, out, columns...)
writeCsv(source, out, nullsAsEmpty, columns...)

// Write one table to a Writer
writeCsv(source, out, timeZone, progress, nullsAsEmpty, columns...)
writeCsv(source, out, timeZone, progress, nullsAsEmpty, separator, columns...)

// Write multiple tables to one file
writeCsv(sources, destPath, compressed, timeZone, tableSeparator, columns...)
writeCsv(sources, destPath, compressed, timeZone, tableSeparator, nullsAsEmpty, columns...)
writeCsv(sources, destPath, compressed, timeZone, tableSeparator, fieldSeparator, nullsAsEmpty, columns...)
```

## Parameters

<ParamTable>
<Param name="source" type="Table">

The table to write.

</Param>
<Param name="sources" type="Table[]">

An array of tables to write. `writeCsv` writes the tables to one file under a single header row.

</Param>
<Param name="destPath" type="String">

The path of the file to write the table or tables to. If the file exists, `writeCsv` overwrites it. The parent directory must already exist.

</Param>
<Param name="out" type="PrintStream or Writer">

The stream that `writeCsv` writes the CSV to. `writeCsv(source, out, columns...)` and `writeCsv(source, out, nullsAsEmpty, columns...)` take a `PrintStream`. The overloads that also take `timeZone` and `progress` take a `Writer`. `writeCsv` closes `out` after writing.

</Param>
<Param name="compressed" type="boolean">

Whether `writeCsv` compresses the output file with bz2. When `compressed` is `true`, `writeCsv` appends `.bz2` to `destPath`, so `destPath` should not include the suffix. For example, a `destPath` of `/data/output.csv.bz2` produces the file `/data/output.csv.bz2.bz2`.

</Param>
<Param name="timeZone" type="ZoneId">

The time zone in which `writeCsv` writes `Instant` values, such as one returned by [`timeZone`](../../time/datetime/timeZone.md). `writeCsv` writes each `Instant` as a local date-time in that time zone followed by its UTC offset (for example, `2021-07-04T08:00-04:00` in `ET`). `writeCsv` writes each `ZonedDateTime` value as its own local date-time followed by its own UTC offset, ignoring `timeZone`. For both types, `writeCsv` writes the UTC offset but not the zone ID. Overloads without this parameter use the system default time zone.

</Param>
<Param name="tableSeparator" type="String">

A string written immediately after the last value of each table, with no line break before it. To put the separator on its own line, start the string with a line break (for example, `"\n&"`).

</Param>
<Param name="progress" type="BiConsumer<Long, Long>">

A function that `writeCsv` calls after it writes each chunk of rows. `writeCsv` passes the number of rows written so far and the total number of rows.

</Param>
<Param name="nullsAsEmpty" type="boolean">

Whether to write null values as empty fields instead of `(null)`. Overloads without this parameter use `true`.

</Param>
<Param name="separator" type="char">

The delimiter for the CSV. Overloads without this parameter use a comma.

</Param>
<Param name="fieldSeparator" type="char">

The delimiter for the CSV file. This is the multi-table form of `separator`. Overloads without this parameter use a comma.

</Param>
<Param name="columns" type="String...">

The columns to include in the export. If `columns` is empty, `writeCsv` writes all columns of `source`, or all columns of the first table in `sources`. Every table in `sources` must have the columns that `writeCsv` writes.

</Param>
</ParamTable>

## Returns

None. `writeCsv` writes the table or tables as a CSV file at `destPath` (with `.bz2` appended when `compressed` is `true`), or as CSV text to `out`.

## Examples

> [!NOTE]
> The path is resolved on the Deephaven server's file system. When Deephaven runs in Docker, that is the container's file system. See [Docker bind mounts](../../../conceptual/docker-data-volumes.md) to learn how container paths map to your local file system.

In the following example, `writeCsv` writes all columns of the source table to `/data/output.csv`.

```groovy
import static io.deephaven.csv.CsvTools.writeCsv

source = newTable(
    stringCol("X", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("Y", 2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("Z", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

writeCsv(source, "/data/output.csv")
```

In the following example, `writeCsv` writes only the columns `X` and `Z` to the output file. Because `nullsAsEmpty` is `false`, `writeCsv` writes null values as `(null)`.

```groovy
import static io.deephaven.csv.CsvTools.writeCsv

source = newTable(
    stringCol("X", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("Y", 2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("Z", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

writeCsv(source, "/data/output.csv", false, "X", "Z")
```

The following example sets `compressed` to `true`, so `writeCsv` writes columns `X` and `Z` to `/data/output.csv.bz2` instead of an uncompressed CSV file.

```groovy
import static io.deephaven.csv.CsvTools.writeCsv

source = newTable(
    stringCol("X", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("Y", 2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("Z", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

writeCsv(source, true, "/data/output.csv", "X", "Z")
```

The following example writes the same `DateTimes` column to two uncompressed files (`compressed` is `false`). The `timeZone` argument sets the time zone of the written values: Mountain Time (`MT`) in the first file and Eastern Time (`ET`) in the second.

```groovy
import static io.deephaven.csv.CsvTools.writeCsv

tzMT = timeZone("MT")
tzET = timeZone("ET")

source = newTable(
    instantCol("DateTimes", parseInstant("2021-07-04T08:00:00 ET"), parseInstant("2021-07-04T08:00:00 MT"), parseInstant("2021-07-04T08:00:00 UTC"))
)

writeCsv(source, "/data/outputM_T.csv", false, tzMT, "DateTimes")
writeCsv(source, "/data/outputE_T.csv", false, tzET, "DateTimes")
```

`outputM_T.csv`:

```text
DateTimes
2021-07-04T06:00-06:00
2021-07-04T08:00-06:00
2021-07-04T02:00-06:00
```

`outputE_T.csv`:

```text
DateTimes
2021-07-04T08:00-04:00
2021-07-04T10:00-04:00
2021-07-04T04:00-04:00
```

The following example combines two tables into an array and writes their `A` and `B` columns to one file. `compressed` is `false` and `tableSeparator` is `"\n"`, so `writeCsv` writes an uncompressed file in which an empty line separates the two tables. The example then reads the file back with [`readCsv`](./readCsv.md), using `ignoreEmptyLines` to skip the empty line between the tables.

```groovy order=source,source2,result
import static io.deephaven.csv.CsvTools.writeCsv
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools

tz = timeZone()

source = newTable(
    stringCol("A", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("B", 2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("C", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

source2 = newTable(
    stringCol("A", "B", "C", "C", null, "B", "C", "A", "B", "C"),
    intCol("B", 5, 3, 5, 8, NULL_INT, 3, 1, 5, 3),
    intCol("C", 526, 76, NULL_INT, 967, 230, 44, 733, 154, 253),
)

Table[] sources = [source, source2]

writeCsv(sources, "/data/output.csv", false, tz, "\n", "A", "B")

result = readCsv("/data/output.csv", CsvTools.builder().ignoreEmptyLines(true).build())
```

The following example creates a [closure](https://groovy-lang.org/closures.html) called `progress`. A closure is a block of Groovy code that can be passed as an argument. The example passes the closure to `writeCsv` with `compressed` set to `false`, and Groovy converts the closure to a Java `BiConsumer`. `writeCsv` calls it after each chunk of rows it writes, so writing the 10,000-row table prints several progress lines to the log.

```groovy order=:log
import static io.deephaven.csv.CsvTools.writeCsv

tz = timeZone()

source = emptyTable(10_000).update("A = i", "B = A * 2")

progress = { current, max ->
    println(current * 100.0 / max + "% done")
}

writeCsv(source, "/data/output.csv", false, tz, progress, "A", "B")
```

## Related documentation

- [Read CSV or other delimited files](../../../how-to-guides/data-import-export/csv-import.md)
- [Export data to CSV or other delimited files](../../../how-to-guides/data-import-export/csv-export.md)
- [`readCsv`](./readCsv.md)
- [`snapshot`](../../table-operations/snapshot/snapshot.md)
- [`timeZone`](../../time/datetime/timeZone.md)
- [Docker bind mounts](../../../conceptual/docker-data-volumes.md)
- [Javadoc](/core/javadoc/io/deephaven/csv/CsvTools.html#writeCsv(io.deephaven.engine.table.Table,java.lang.String,java.lang.String...))
