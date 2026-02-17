---
title: readCsv
---

The `readCsv` method will read a CSV file into an in-memory table.

## Syntax

```groovy skip-test
import io.deephaven.csv.CsvTools

readCsv(path)
readCsv(stream)
readCsv(url)
readCsv(path, specs)
readCsv(stream, specs)
readCsv(url, specs)
```

## Parameters

<ParamTable>
<Param name="path" type="String">

The file to load into a table.

</Param>
<Param name="path" type="Path">

The file to load into a table. Note that compressed files _are_ accepted: paths ending in ".tar.zip", ".tar.bz2", ".tar.gz", ".tar.7z", ".tar.zst", ".zip", ".bz2", ".gz", ".7z", ".zst", or ".tar" will automatically be decompressed before they are read.

</Param>
<Param name="stream" type="InputStream">

An InputStream providing access to the CSV data.

</Param>
<Param name="url" type="URL">

The URL.

</Param>

<Param name="specs" type="io.deephaven.csv.CsvSpecs">

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

```groovy order=source,result
import static io.deephaven.csv.CsvTools.readCsv
import static io.deephaven.csv.CsvTools.writeCsv

source = newTable(
    stringCol("X", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("Y",2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("Z", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

writeCsv(source, "/data/file.csv")

result = readCsv("/data/file.csv")
```

> [!NOTE]
> In the following examples, the example data found in [Deephaven's example repository](https://github.com/deephaven/examples) will be used. Follow the instructions in the [README](https://github.com/deephaven/examples/blob/main/README.md) to download the data to the proper location for use with Deephaven.

In the following example, `read_csv` is used to load the file [DeNiro CSV](https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv) into a Deephaven table.

```groovy
import static io.deephaven.csv.CsvTools.readCsv

result = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv")
```

In the following example, `read_csv` is used to load the file [DeNiro TSV](https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv) into a Deephaven table. The second argument, `"\t"`, indicates that the file contains tab-separated values.

```groovy
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs

specs = CsvSpecs.tsv()

deniroTSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv", specs)
```

Any character can be used as a delimiter. The pipe character (`|`) is common. In the following example, the second input parameter is used to read a pipe-delimited file into memory.

```groovy
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs

specs = CsvSpecs.builder().delimiter('|' as char).build()

result = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv", specs)
```

## Related documentation

- [CSV import via query](../../../how-to-guides/data-import-export/csv-import.md)
- [CSV export via query](../../../how-to-guides/data-import-export/csv-export.md)
- [Docker data volumes](../../../conceptual/docker-data-volumes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#readCsv(java.nio.file.Path))
