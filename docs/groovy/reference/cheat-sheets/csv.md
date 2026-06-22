---
title: CSV Cheat Sheet
sidebar_label: CSV
---

- [`readCsv`](../data-import-export/CSV/readCsv.md)
- [`writeCsv`](../data-import-export/CSV/writeCsv.md)

```groovy order=source,result,iris,deniroDefault,deniroHeader,deniroTSV,deniroPSV
import static io.deephaven.csv.CsvTools.readCsv
import static io.deephaven.csv.CsvTools.writeCsv
import io.deephaven.csv.CsvSpecs

// Create a table
source = emptyTable(100).update(
    "X = 0.1 * i",
    "SinX = X % 0.2 < 0.01 ? NULL_DOUBLE : sin(X)",
    "CosX = cos(X)",
    "TanX = tan(X)"
)

// Write to a local file
// `(null)` for null cells
writeCsv(source, "/data/TrigFunctions.csv")

// Write a subset of data
writeCsv(source, "/data/Cosine.csv", "X", "SinX")

// Read from a local file
result = readCsv("/data/TrigFunctions.csv")

// Read from a csv from URL
iris = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv")

// Read headerless, default column names
specs = CsvSpecs.builder().hasHeaderRow(false).build()
deniroDefault = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", specs)


// Read files with other delimiters
specs = CsvSpecs.builder().delimiter('\t' as char).build()
deniroTSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv", specs)

specs = CsvSpecs.builder().delimiter('|' as char).build()
deniroPSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv", specs)

import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs
headers = List.of("Year", "Score", "Title")
specs = CsvSpecs.builder().hasHeaderRow(false).headers(headers).build()
deniroHeader = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", specs)
```

## Related documentation

- [CSV import via query](../../how-to-guides/data-import-export/csv-import.md)
- [CSV export via query](../../how-to-guides/data-import-export/csv-export.md)
- [`readCsv`](../data-import-export/CSV/readCsv.md)
- [`writeCsv`](../data-import-export/CSV/writeCsv.md)
- [Docker data volumes](../../conceptual/docker-data-volumes.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#readCsv(java.nio.file.Path))
