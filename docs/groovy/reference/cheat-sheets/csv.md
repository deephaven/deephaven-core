---
title: CSV cheat sheet
---

This cheat sheet shows common ways to read and write CSV files with [`readCsv`](../data-import-export/CSV/readCsv.md) and [`writeCsv`](../data-import-export/CSV/writeCsv.md). The examples write files to `/data`, the data directory in Deephaven's Docker images, and read them back from there or from a URL. To map it to a directory on your computer, see [Docker bind mounts](../../conceptual/docker-data-volumes.md). In other deployments, use any path the server can read and write.

## Create a table

The examples on this page write and read this sample table. Its `SinX` column is null in every other row, so the write examples can show how null cells are written.

```groovy test-set=1 order=source
source = emptyTable(100).update(
    "X = 0.1 * i",
    "SinX = i % 2 == 0 ? NULL_DOUBLE : sin(X)",
    "CosX = cos(X)",
    "TanX = tan(X)"
)
```

## Write CSV files

[`writeCsv`](../data-import-export/CSV/writeCsv.md) writes a table, or the columns you list, to a CSV file on the server.

```groovy test-set=1 order=null
import static io.deephaven.csv.CsvTools.writeCsv

// Write the whole table to a file on the server
// Null cells are written as empty fields by default
writeCsv(source, "/data/TrigFunctions.csv")

// Pass false for nullsAsEmpty to write null cells as (null) instead
writeCsv(source, "/data/TrigFunctionsNulls.csv", false)

// Write a subset of data
writeCsv(source, "/data/Cosine.csv", "X", "CosX")
```

## Read CSV files

[`readCsv`](../data-import-export/CSV/readCsv.md) reads a CSV file from a server path or a URL. A [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) object handles files with no header row, column types, and other delimiters.

```groovy test-set=1 order=result,iris,deniroDefault,deniroHeader,deniroTSV,deniroPSV
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools
import io.deephaven.csv.parsers.Parsers

// Read the file written in the previous section
result = readCsv("/data/TrigFunctions.csv")

// Read a CSV file from a URL
iris = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv")

// Read a file with no header row, using default column names
// CsvTools.builder() returns a CsvSpecs builder that legalizes column names and parses Deephaven time zones
specs = CsvTools.builder().hasHeaderRow(false).build()
deniroDefault = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", specs)

// Read a file with no header row, providing column names (in file column order) and types
headers = List.of("Year", "Score", "Title")
specs = CsvTools.builder()
    .hasHeaderRow(false)
    .headers(headers)
    .putParserForName("Year", Parsers.LONG)
    .putParserForName("Score", Parsers.LONG)
    .putParserForName("Title", Parsers.STRING)
    .build()
deniroHeader = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", specs)

// Read files with other delimiters
specs = CsvTools.builder().delimiter('\t' as char).build()
deniroTSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv", specs)

specs = CsvTools.builder().delimiter('|' as char).build()
deniroPSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv", specs)
```

## Related documentation

- [Docker bind mounts](../../conceptual/docker-data-volumes.md)
- [Read CSV or other delimited files](../../how-to-guides/data-import-export/csv-import.md)
- [Export data to CSV or other delimited files](../../how-to-guides/data-import-export/csv-export.md)
- [Use the Deephaven CSV library](../../how-to-guides/csv-library.md)
- [`emptyTable`](../table-operations/create/emptyTable.md)
- [`readCsv`](../data-import-export/CSV/readCsv.md)
- [`update`](../table-operations/select/update.md)
- [`writeCsv`](../data-import-export/CSV/writeCsv.md)
- [`CsvTools` Javadoc](/core/javadoc/io/deephaven/csv/CsvTools.html)
- [`CsvSpecs` Javadoc](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html)
- [`Parsers` Javadoc](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/parsers/Parsers.html)
