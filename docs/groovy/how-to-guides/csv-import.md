---
title: Import CSV or other delimited files
sidebar_label: Import CSV files
---

This guide will show you how to import data from CSV (and other delimited) files into Deephaven tables by using the [`readCsv`](../reference/data-import-export/CSV/readCsv.md) method.

> [!TIP]
> CSV files can also be imported into Deephaven with drag-and-drop uploading in the UI.

## `readCsv`

The two most common ways to load a CSV file are using the file path or the file URL. The header of the CSV file determines the column names.

The basic syntax for `readCsv` follows:

```groovy skip-test
import static io.deephaven.csv.CsvTools.readCsv

readCsv(path)
readCsv(url)
readCsv(path, csvSpecs)
readCsv(url, csvSpecs)
```

<!-- TODO: https://github.com/deephaven/deephaven.io/issues/489 Update CSV documentation when CsvHelpers goes live -->

Let's dive into some simple examples. So that we don't have to create a new CSV file from scratch, we'll use some CSV files from [Deephaven's examples repository](https://github.com/deephaven/examples). We encourage you to use your own files by replacing the file paths in our queries.

### `readCsv` with a file path

The `readCsv` method can be used to import a CSV file from a file path. In this example, we will import a CSV file containing R.A. Fisher's classic iris flower dataset commonly used in machine learning applications.

```groovy order=iris
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv("/data/examples/Iris/csv/iris.csv")
```

#### The `/data` mount point

If you are using Docker-installed Deephaven, you can find a `/data` folder inside your Deephaven installation's main folder, on the same level as your `docker-compose.yml` file. This folder is mounted to the `/data` volume in the running Deephaven container. This means that if the Deephaven console is used to write data to `/data/abc/file.csv`, that file will be visible at `./data/abc/file.csv` on the local file system of your computer.

> [!NOTE]
> If the `./data` directory does not exist when Deephaven is launched, it will be created.

See [Docker data volumes](../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

> [!NOTE]
> If you're using our files, follow the directions in the [README](https://github.com/deephaven/examples/blob/main/README.md) to mount the content from [Deephaven's examples repository](https://github.com/deephaven/examples) onto `/data` in the Deephaven Docker container.

### `readCsv` with a URL

Now, we will import the same CSV file, but this time we will use a URL instead of a file path.

```groovy order=iris
import static io.deephaven.csv.CsvTools.readCsv

iris = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
```

> [!NOTE]
> This method works with any public URL that points to a CSV file.

### Headerless CSV files

CSV files don't always have headers. The example below uses the headerless [DeNiro CSV](https://github.com/deephaven/examples/tree/main/DeNiro/csv/deniro_headerless.csv) and `CsvSpecs`' `hasHeaderRow` set to `false`.

```groovy order=deniro
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs

specs = CsvSpecs.builder().hasHeaderRow(false).build()

deniro = readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    specs,
)
```

Because no column names are provided, the table will produce default column names (`Column1`, `Column2`, etc.). You can explicitly set the column names, as shown below.

```groovy order=deniroHeader
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs

headers = List.of("Year", "Score", "Title")

specs = CsvSpecs.builder().hasHeaderRow(false).headers(headers).build()
deniroHeader = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", specs)
```

### Other formats

#### Tab-delimited data

Deephaven allows you to specify other delimiters if your file is not comma-delimited. In the example below, we import a tab-delimited file, which requires the use of `CsvSpecs.tsv()`.

```groovy order=deniroTSV
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs

specs = CsvSpecs.tsv()

deniroTSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv", specs)
```

#### Pipe-delimited data

Any character can be used as a delimiter. The pipe character (`|`) is common. In the example below, we supply a `CsvSpecs` with the delimiter set to `|`.

```groovy order=deniroPSV
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs

specs = CsvSpecs.builder().delimiter('|' as char).build()

deniroPSV = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv", specs)
```

#### Trim

By default, quoted values that have leading and trailing white space include the white space when reading the CSV file. For example, if `" Taxi Driver "` is in the CSV file, it will be read as `Taxi Driver`.

By setting `trim` to `true` when reading the CSV file, these leading and trailing white space will be removed. So `" Taxi Driver "` will be read as `Taxi Driver`.

### Using `CsvSpecs`

The `CsvSpecs` class allows you to specify additional options when reading a CSV file. The `CsvSpecs`' class methods are:

- `allowMissingColumns(boolean)`: Whether the library should allow missing columns in the input. If `true`, allows the CSV file to have fewer columns than specified in the header. If `false`, no missing columns will be permitted.
- `build()`: Returns the `CsvSpecs` object.
- `concurrent(boolean)`: Whether or not to run concurrently.
- `customDoubleParser()`: The custom double parser.
- `customTimeZoneParser()`: An optional low-level "timezone parser" that understands custom time zone strings.
- `delimiter(char)`: The field delimiter character (the character that separates one column from the next).
- `from(CsvSpecs)`: Copy all of the parameters from supplied CsvSpecs object into `this` builder.
- `hasHeaderRow(boolean)`: Whether the input file has a header row.
- `headerLegalizer(Function<String[],String[]>)`: An optional legalizer for column headers.
- `headers(Iterable<String>)`: Client-specified headers that can be used to override the existing headers in the input (if `CsvSpecs.hasHeaderRow()` is `true`), or to provide absent headers (if `CsvSpecs.hasHeaderRow()` is `false`).
- `headerValidator(Predicate<String>)`: An optional validator for column headers.
- `ignoreEmptyLines(boolean)`: Whether the library should skip over empty lines in the input.
- `ignoreExcessColumns(boolean)`: Whether the library should allow excess columns in the input.
- `ignoreSurroundingSpaces(boolean)`: Whether to trim leading and trailing blanks from non-quoted values.
- `nullParser(Parser<?>)`: The parser to use when all values in the column are null.
- `nullValueLiterals(Iterable<String>)`: The default collection of strings that means "null value" in the input.
- `numRows(long)`: Max number of rows to process.
- `parsers(Iterable<? extends Parser<?>>)`: The parsers that the user wants to participate in type inference.
- `putHeaderForIndex(int, String)`: Override a specific column header by 0-based column index.
- `putNullValueLiteralsForIndex(int, List<String>)`: The null value literal for specific columns, specified by 0-based column index.
- `putNullValueLiteralsForName(String, List<String>)`: The null value literal for specific columns, specified by column name.
- `putParserForIndex(int, Parser<?>)`: Used to force a specific parser for a specific column, specified by 0-based column index.
- `putParserForName(String, Parser<?>)`: Used to force a specific parser for a specific column, specified by column name.
- `quote(char)`: The quote character (used when you want field or line delimiters to be interpreted as literal text).
- `skipHeaderRows(long)`: Number of rows to skip before reading the header row from the input.
- `skipRows(long)`: Number of data rows to skip before processing data.
- `trim(boolean)`: Whether to trim leading and trailing blanks from inside quoted values.

In the following example, we want to read in the `deniro_poorly_formatted.csv` file from the Deephaven Examples repo. This file has a number of issues that we need to address, but `readCsv` can handle them if we give the right `CsvSpecs` parameters:

```groovy order=deniroHeader
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvSpecs

headers = List.of("Release Year", "Rating", "Movie Title")

specs = CsvSpecs.builder().headers(headers).hasHeaderRow(true).skipRows(5).numRows(20).ignoreEmptyLines(true).allowMissingColumns(true).ignoreExcessColumns(true).trim(true).build()

deniroHeader = readCsv("/data/examples/DeNiro/csv/deniro_poorly_formatted.csv", specs)
```

In the example above, we:

- Use `headers` to override and replace the files's header row.
- Set `hasHeaderRow` to `true` to indicate that the CSV file has a header row.
- Use `skipRows` to omit the first 5 rows of the CSV file.
- Set `numRows` to 20 to limit our table to 20 rows.
- Set `ignoreEmptyLines` to `true` to avoid throwing an exception due to an empty line in the CSV file.
- Set `allowMissingColumns` to `true` to avoid throwing an exception due to a missing column in the CSV file. Note that the 1983 entry in our table has `null` in the missing column.
- Set `ignoreExcessColumns` to `true` to avoid throwing an exception due to an extra column in the CSV file - 1984's _Brazil_ entry has an extra column containing a brief review that we don't need in our table.
- Set `trim` to `true` to remove leading and trailing white space from inside quoted strings - such as our movie titles.

For a complete list of optional arguments, see the [`readCsv` reference documentation](../reference/data-import-export/CSV/readCsv.md).

## Related documentation

- [How to export CSV files](./csv-export.md)
- [`readCsv`](../reference/data-import-export/CSV/readCsv.md)
