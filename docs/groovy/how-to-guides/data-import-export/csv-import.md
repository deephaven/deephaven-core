---
title: Read CSV or other delimited files
---

This guide shows you how to read data from CSV (and other delimited) files into in-memory Deephaven tables by using the [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) method. The result is a static table that holds the file's contents when you read it. To work with data that keeps arriving, see [Kafka](./kafka-stream.md).

> [!TIP]
> To upload a CSV without writing code, see [Upload a table from a file](../user-interface/upload-table-from-file.md).

## `readCsv`

The two most common ways to load a CSV file are using the file path or the file URL. The header row of the CSV file supplies the column names.

The basic syntax for [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) follows:

```groovy syntax
import static io.deephaven.csv.CsvTools.readCsv

readCsv(path)
readCsv(url)
readCsv(path, specs)
readCsv(url, specs)
```

The optional `specs` argument is a [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) object that holds read options such as the delimiter and header handling. To create one:

1. Call [`CsvTools.builder`](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#builder()).
2. Set the options you need. The [`readCsv` reference](../../reference/data-import-export/CSV/readCsv.md#csvspecs-builder-options) lists them all.
3. Call `build`.

The builder starts from the same defaults that [`readCsv(path)`](../../reference/data-import-export/CSV/readCsv.md) uses, such as rewriting headers that aren't valid Deephaven column names into valid ones. The examples from [Headerless CSV files](#headerless-csv-files) onward each build a [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) object.

Let's dive into some simple examples. These examples use CSV files from [Deephaven's examples repository](https://github.com/deephaven/examples), so you don't have to create your own. We encourage you to use your own files by replacing the file paths in our queries.

The URL examples need no setup. The examples that read from a file path expect those files under `/data/examples`. To set that up, follow the directions in the [examples repository README](https://github.com/deephaven/examples/blob/main/README.md) and see [The `/data` mount point](#the-data-mount-point).

### `readCsv` with a file path

Use [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) to read a CSV file from a file path. In this example, we read a CSV file that contains R.A. Fisher's classic iris flower dataset, which is commonly used in machine learning.

```groovy order=iris
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv("/data/examples/Iris/csv/iris.csv")
```

#### The `/data` mount point

The Compose examples in the [Docker install guide](../../getting-started/docker-install.md) mount `./data:/data` in `docker-compose.yml`. With that mount, Docker shares the `data` folder next to your `docker-compose.yml` file with the running Deephaven container at `/data`. See [Docker data volumes](../../conceptual/docker-data-volumes.md) for more about how container paths map to your computer. A file you save at `./data/abc/file.csv` on your computer is readable in Deephaven at `/data/abc/file.csv`. A container started with `docker run` and no `-v` option still has a `/data` directory, but Docker doesn't share it with your computer.

> [!NOTE]
> With that mount, if the `./data` directory does not exist when you launch Deephaven, Docker creates it.

### `readCsv` with a URL

Now, let's read the same iris dataset, but this time we pass [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) a URL to the file in Deephaven's examples repository on GitHub instead of a file path.

```groovy order=iris
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
```

> [!NOTE]
> This works with a publicly accessible `http` or `https` URL that points to an uncompressed CSV file. [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) decompresses a compressed file only when it reads the file from a file path or a `file:` URL.

### Headerless CSV files

CSV files don't always have headers. To read one, pass [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) a [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) whose `hasHeaderRow` option is set to `false`. The example below reads the headerless [DeNiro CSV](https://github.com/deephaven/examples/blob/main/DeNiro/csv/deniro_headerless.csv) this way.

```groovy order=deniro
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools

specs = CsvTools.builder().hasHeaderRow(false).build()

deniro = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", specs)
```

Because no column names are provided, the table uses default column names (`Column1`, `Column2`, etc.). To name the columns, pass the builder's `headers` option a list of names. Names are applied by position, so list them in the file's column order.

```groovy order=deniroHeader
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools

headers = List.of("Year", "Score", "Title")

specs = CsvTools.builder().hasHeaderRow(false).headers(headers).build()
deniroHeader = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", specs)
```

### Other delimiters

Deephaven lets you specify other delimiters if your file is not comma-delimited. Set the `delimiter` option of [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html), which accepts any single 7-bit ASCII character.

#### Tab-delimited data

The example below reads a tab-delimited file with `delimiter('\t' as char)`.

```groovy order=deniroTSV
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools

specs = CsvTools.builder().delimiter('\t' as char).build()

deniroTSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv", specs)
```

#### Pipe-delimited data

The pipe character (`|`) is a common delimiter. In the example below, we supply a [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) with the delimiter set to `|`.

```groovy order=deniroPSV
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools

specs = CsvTools.builder().delimiter('|' as char).build()

deniroPSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv", specs)
```

### Read a poorly formatted file

In the following example, we read the `deniro_poorly_formatted.csv` file from Deephaven's examples repository. This file has a number of issues that we need to address, but [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) can handle them if we give the right [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) options:

```groovy order=deniroFormatted
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools

headers = List.of("ReleaseYear", "Rating", "MovieTitle")

specs = CsvTools.builder()
    .headers(headers)
    .skipRows(5)
    .numRows(20)
    .ignoreEmptyLines(true)
    .allowMissingColumns(true)
    .ignoreExcessColumns(true)
    .trim(true)
    .build()

deniroFormatted = readCsv("/data/examples/DeNiro/csv/deniro_poorly_formatted.csv", specs)
```

In the example above, we:

- Use `headers` to replace the file's column names.
- Use `skipRows` to skip the first 5 data rows after the header row.
- Set `numRows` to 20 to read at most 20 lines after the skipped rows.
- Set `ignoreEmptyLines` to `true` to skip the empty line in the CSV file. The skipped line still counts toward the `numRows` limit, so the table has 19 rows. Without `ignoreEmptyLines`, [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) treats an empty line as a row with missing columns. It throws an exception, or it reads a row of `null` values when `allowMissingColumns` is `true`.
- Set `allowMissingColumns` to `true` to avoid throwing an exception due to a missing column in the CSV file. Note that the 1983 entry in our table has `null` in the missing column.
- Set `ignoreExcessColumns` to `true` to avoid throwing an exception due to an extra column in the CSV file. The entry for 1985's _Brazil_ has an extra column with a brief review that we don't need in our table.
- Set `trim` to `true` to remove leading and trailing white space from inside quoted strings, such as some of our movie titles. By default, [`readCsv`](../../reference/data-import-export/CSV/readCsv.md) keeps this white space and reads `" The Mission   "` with its leading and trailing spaces.

To skip lines that come before the header row, use `skipHeaderRows` instead of `skipRows`.

See the [`readCsv` reference documentation](../../reference/data-import-export/CSV/readCsv.md#csvspecs-builder-options) for every `CsvSpecs` builder option.

## Related documentation

- [Export data to CSV or other delimited files](./csv-export.md)
- [`readCsv`](../../reference/data-import-export/CSV/readCsv.md)
- [Use the Deephaven CSV library](../csv-library.md)
- [Navigate the GUI](../user-interface/navigating-the-ui.md)
