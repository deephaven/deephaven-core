---
title: writeCsv
---

The `writeCsv` method will write a table to a standard CSV file.

## Syntax

```groovy skip-test
writeCsv(source, compressed, destPath, columns...)
writeCsv(source, compressed, destPath, nullsAsEmpty, columns...)
writeCsv(source, destPath, columns...)
writeCsv(source, destPath, nullsAsEmpty, columns...)
writeCsv(source, out, columns...)
writeCsv(source, out, nullsAsEmpty, columns...)
writeCsv(source, destPath, compressed, timeZone, columns...)
writeCsv(source, destPath, compressed, timeZone, nullsAsEmpty, columns...)
writeCsv(source, destPath, compressed, timeZone, nullsAsEmpty, separator, columns...)
writeCsv(sources, destPath, compressed, timeZone, tableSeparator, columns...)
writeCsv(sources, destPath, compressed, timeZone, tableSeparator, nullsAsEmpty, columns...)
writeCsv(sources, destPath, compressed, timeZone, tableSeparator, fieldSeparator, nullsAsEmpty, columns...)
writeCsv(source, destPath, compressed, timeZone, progress, columns...)
writeCsv(source, destPath, compressed, timeZone, progress, nullsAsEmpty, columns...)
writeCsv(source, destPath, compressed, timeZone, progress, nullsAsEmpty, separator, columns...)
writeCsv(source, out, timeZone, progress, nullsAsEmpty, columns...)
writeCsv(source, out, timeZone, progress, nullsAsEmpty, separator, columns...)
writeCsv(out, columns...)
```

## Parameters

<ParamTable>
<Param name="source" type="Table">

The table to write to file.

</Param>
<Param name="sources" type="Table[]">

An array of Deephaven Table objects to be exported.

</Param>
<Param name="columns" type="String...">

A list of columns to include in the export.

</Param>
<Param name="destPath" type="String">

Path name of the file where the table will be stored.

</Param>
<Param name="compressed" type="boolean">

Whether to compress (bz2) the file being written.

</Param>
<Param name="nullsAsEmpty" type="boolean">

Whether nulls should be written as blank instead of `(null)`.

</Param>
<Param name="out" type="Writer">

Writer used to write the CSV.

</Param>
<Param name="progress" type="BiConsumer<Long, Long>">

A procedure that implements [BiConsumer](https://docs.oracle.com/javase/8/docs/api/java/util/function/BiConsumer.html), and takes a progress Integer and a total size Integer to update progress.

</Param>
<Param name="timeZone" type="TimeZone">

A TimeZone constant relative to which DateTime data should be adjusted.

</Param>
<Param name="tableSeparator" type="String">

A String (normally a single character) to be used as the table delimiter.

</Param>
<Param name="fieldSeparator" type="char">

The delimiter for the CSV files.

</Param>
<Param name="separator" type="char">

The delimiter for the CSV.

</Param>
</ParamTable>

## Returns

A CSV file located in the specified path.

## Examples

> [!NOTE]
> Deephaven writes files to locations relative to the base of its Docker container. See [Docker data volumes](../../../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

In the following example, `write_csv` writes the source table to `/data/output.csv`. All columns are included.

```groovy
import static io.deephaven.csv.CsvTools.writeCsv

source = newTable(
    stringCol("X", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("Y",2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("Z", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

writeCsv(source, "/data/output.csv")
```

In the following example, only the columns `X` and `Z` are written to the output file. Null values will appear as "`(null)`" due to setting `nullsAsEmpty` to `false`.

```groovy
import static io.deephaven.csv.CsvTools.writeCsv

source = newTable(
    stringCol("X", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("Y",2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("Z", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

writeCsv(source, "/data/output.csv", false, "X", "Z")
```

In the following example, we set `compressed` to `true`, which means that our call to `writeCsv` will generate a `"/data/output.csv.bz2"` file rather than just a CSV.

> [!NOTE]
> It is unnecessary to include the `.bz2` suffix in an output file path; this will be appended automatically when the file is compressed. If `.bz2` is included in the `path`, the resulting file will be named `"/data/output.csv.bz2.bz2"`.

```groovy
import static io.deephaven.csv.CsvTools.writeCsv
import static io.deephaven.csv.CsvTools.readCsv

source = newTable(
    stringCol("X", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("Y",2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("Z", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

writeCsv(source, true, "/data/output.csv", "X", "Z")
```

In this example, we use the `timeZone` parameter so that when we call `writeCsv`, the resulting file will adjust the values in our `DateTimes` column accordingly.

```groovy
import static io.deephaven.csv.CsvTools.writeCsv
import static io.deephaven.csv.CsvTools.readCsv
import static io.deephaven.time.DateTimeUtils.timeZone

M_T = timeZone("MT")
E_T = timeZone("ET")

source = newTable(
    instantCol("DateTimes", parseInstant("2021-07-04T08:00:00 ET"), parseInstant("2021-07-04T08:00:00 MT"), parseInstant("2021-07-04T08:00:00 UTC"))
)

writeCsv(source, "/data/outputM_T.csv", false, M_T, "DateTimes")
writeCsv(source, "/data/outputE_T.csv", false, E_T, "DateTimes")
```

![The newly written .csv files](../../../assets/reference/data-import-export/write-csv-timezone.png)

In the following example, we create two tables and then combine them into an `array`. Then, we call `writeCsv` to write the `"A"` and `"B"` columns to our output CSV file.

```groovy order=source,source_2,result
import static io.deephaven.csv.CsvTools.writeCsv
import static io.deephaven.csv.CsvTools.readCsv
import static io.deephaven.time.DateTimeUtils.timeZone

tz = timeZone()

source = newTable(
    stringCol("A", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("B",2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("C", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

source_2 = newTable(
    stringCol("A", "B", "C", "C", null, "B", "C", "A", "B", "C"),
    intCol("B", 5, 3, 5, 8, NULL_INT, 3, 1, 5, 3),
    intCol("C", 526, 76, NULL_INT, 967, 230, 44, 733, 154, 253),
)

Table[] sources =  [source, source_2]

writeCsv(sources, "/data/output.csv", false, tz, "&", "A", "B")

result = readCsv("/data/output.csv")
```

In the following example, we create a `source` table, and then create a _closure_ called `progress` which Groovy will convert to a Java [`BiConsumer`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/function/BiConsumer.html) when we call the `writeCsv` method. With this method call, we will see output in the `log` telling us how far along our query is as it runs.

```groovy order=:log
import static io.deephaven.csv.CsvTools.writeCsv
import static io.deephaven.csv.CsvTools.readCsv
import static io.deephaven.time.DateTimeUtils.timeZone

tz = timeZone()

source = newTable(
    stringCol("A", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("B",2, 4, 2, 1, 2, 3, 4, NULL_INT, 39),
    intCol("C", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

progress = { current, max ->
  println(current * 100.0 / max + "% done")
}

writeCsv(source, "/data/output.csv", false, tz, progress, "A", "B")

result = readCsv("/data/output.csv")
```

## Related documentation

- [CSV import via query](../../../how-to-guides/csv-import.md)
- [CSV export via query](../../../how-to-guides/csv-export.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#writeCsv(io.deephaven.engine.table.Table,java.lang.String,java.lang.String...))
