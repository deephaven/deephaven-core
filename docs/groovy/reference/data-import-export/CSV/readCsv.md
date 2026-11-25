---
title: readCsv
---

The `readCsv` method reads a CSV file into an in-memory table.

## Syntax

```groovy syntax
import static io.deephaven.csv.CsvTools.readCsv

readCsv(path)
readCsv(stream)
readCsv(url)
readCsv(path, specs)
readCsv(stream, specs)
readCsv(url, specs)
```

## Parameters

<ParamTable>
<Param name="path" type="String or Path">

The file path to load into a table. A `String` path can also be a URL. `readCsv` accepts compressed files: it decompresses local files that end in `.tar.zip`, `.tar.bz2`, `.tar.gz`, `.tar.7z`, `.tar.zst`, `.zip`, `.bz2`, `.gz`, `.7z`, `.zst`, or `.tar` before reading them.

</Param>
<Param name="stream" type="InputStream">

An `InputStream` providing access to the CSV data. `readCsv` closes the stream when it returns.

</Param>
<Param name="url" type="URL">

A URL to read CSV data from.

</Param>

<Param name="specs" type="io.deephaven.csv.CsvSpecs">

Specifications for how to load the CSV file. See [`CsvSpecs` builder options](#csvspecs-builder-options).

</Param>
</ParamTable>

## Returns

A new in-memory table containing the CSV data.

## `CsvSpecs` builder options

To create the `specs` argument, call [`CsvTools.builder`](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#builder()), set any of the options below, and then call `build` to get the configured [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html).

Start from `CsvTools.builder` rather than `CsvSpecs.builder`. `CsvTools.builder` sets Deephaven's column-name legalizer, column-name validator, and time zone parser. `readCsv` uses the same settings when you omit the `specs` argument.

- `allowMissingColumns(boolean)`: Whether the CSV file can have fewer columns than the header specifies. If `true`, `readCsv` fills the missing columns at the end of a short row with null values. The default is `false`.
- `concurrent(boolean)`: Whether to read the input and parse each column on separate threads. The default is `true`.
- `customDoubleParser(Tokenizer.CustomDoubleParser)`: The parser to use for values read as `double`. The default is a fast double parser if one is on the classpath, otherwise the JDK parser.
- `customTimeZoneParser(Tokenizer.CustomTimeZoneParser)`: A low-level parser for custom time zone strings. `CsvTools.builder` sets one that understands Deephaven time zone names.
- `delimiter(char)`: The field delimiter character (the character that separates one column from the next). It must be a single 7-bit ASCII character. The default is `,`.
- `escape(Character)`: The escape character, which must be a 7-bit ASCII character. Use it to read the field delimiter or the quote character as literal text, or to read the Java-style escape sequences `\b`, `\t`, `\n`, `\r`, and `\f`. Escaping an actual carriage return, a newline, or a non-ASCII character raises an error. It is unset by default.
- `fixedColumnWidths(Iterable<Integer>)`: The column widths of a fixed-width file, when `readCsv` should not determine them from the header row. Setting it when `hasFixedWidthColumns` is `false` raises an error. Each width must be at least 1. `readCsv` ignores the width given for the last column.
- `from(CsvSpecs)`: An existing `CsvSpecs` object to copy all options from into this builder.
- `hasFixedWidthColumns(boolean)`: Whether the input is organized into fixed-width columns rather than separated by a delimiter. The default is `false`. When it is `true`, changing `delimiter`, `quote`, or `trim` from its default raises an error.
- `hasHeaderRow(boolean)`: Whether the input file has a header row. The default is `true`. If it is `false` and `headers` is not set, the columns are named `Column1`, `Column2`, and so on.
- `headerLegalizer(Function<String[],String[]>)`: The function that rewrites column headers into legal names. `CsvTools.builder` sets Deephaven's column-name legalizer, and setting this option replaces it.
- `headers(Iterable<String>)`: Column headers you supply. If `hasHeaderRow` is `true`, these headers replace the headers in the input. If `hasHeaderRow` is `false`, they supply the missing headers.
- `headerValidator(Predicate<String>)`: The check that a column header is legal. `CsvTools.builder` sets Deephaven's column-name validator.
- `ignoreEmptyLines(boolean)`: Whether to skip empty lines in the input. If `false`, `readCsv` reads an empty line as a row whose first value is null and whose other columns are missing. If the file has more than one column, `readCsv` raises an error unless `allowMissingColumns` is `true`, in which case it fills the missing columns with null values. The default is `false`.
- `ignoreExcessColumns(boolean)`: Whether to allow excess columns in the input. The default is `false`.
- `ignoreSurroundingSpaces(boolean)`: Whether to trim leading and trailing spaces and tabs from non-quoted values. The default is `true`.
- `nullParser(Parser<?>)`: The parser to use when all values in the column are null. The default is `Parsers.STRING`.
- `nullValueLiterals(Iterable<String>)`: The strings that mean a null value in any column that has no null value literals of its own. The default is the empty string.
- `numRows(long)`: The maximum number of rows to process. Empty lines that `ignoreEmptyLines` skips count toward this limit. The default is all rows.
- `parsers(Iterable<? extends Parser<?>>)`: The parsers that participate in type inference. The default is `Parsers.DEFAULT`.
- `putHeaderForIndex(int, String)`: A column header that replaces the header at a 0-based column index.
- `putNullValueLiteralsForIndex(int, List<String>)`: The null value literals for the column at a 0-based column index.
- `putNullValueLiteralsForName(String, List<String>)`: The null value literals for the named column.
- `putParserForIndex(int, Parser<?>)`: A parser forced for the column at a 0-based column index.
- `putParserForName(String, Parser<?>)`: A parser forced for the named column, such as `Parsers.INT` from `io.deephaven.csv.parsers.Parsers`.
- `putParsersForIndex(int, List<Parser<?>>)`: A set of parsers forced for the column at a 0-based column index.
- `putParsersForName(String, List<Parser<?>>)`: A set of parsers forced for the named column.
- `quote(char)`: The quote character, which must be a 7-bit ASCII character. Enclose a field in it to read field or line delimiters inside the field as literal text. The default is `"`.
- `skipHeaderRows(long)`: The number of rows to skip before reading the header row from the input. The default is 0. A nonzero value raises an error when `hasHeaderRow` is `false`.
- `skipRows(long)`: The number of data rows to skip before processing data. Empty lines count toward this number. The default is 0.
- `threadShutdownTimeout(Duration)`: How long to wait for the reader threads to shut down if processing ends abnormally, such as after an exception. The default is 1 minute.
- `trim(boolean)`: Whether to trim leading and trailing spaces and tabs from inside quoted values. The default is `false`.
- `useUtf32CountingConvention(boolean)`: Whether to measure fixed-width column widths in Unicode code points. The default is `true`. If `false`, `readCsv` measures them in UTF-16 code units, which are Java `char` values. Setting it to `false` when `hasFixedWidthColumns` is `false` raises an error.

For the full description of each option, see the [`CsvSpecs.Builder` Javadoc](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.Builder.html).

## Examples

> [!NOTE]
> The first two examples read and write files at paths inside the Docker container. See [Docker bind mounts](../../../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

In the following example, [`writeCsv`](./writeCsv.md) writes the source table to `/data/file.csv`, and `readCsv` loads the file into a Deephaven table.

```groovy order=source,result
import static io.deephaven.csv.CsvTools.readCsv
import static io.deephaven.csv.CsvTools.writeCsv

source = newTable(
    stringCol("X", "A", "B", null, "C", "B", "A", "B", "B", "C"),
    intCol("Y", 2, 4, 2, 1, 2, 3, 4, NULL_INT, 3),
    intCol("Z", 55, 76, 20, NULL_INT, 230, 50, 73, 137, 214),
)

writeCsv(source, "/data/file.csv")

result = readCsv("/data/file.csv")
```

`readCsv` infers each column's type from the data, so the single-letter values in `X` become a `char` column. To keep `X` as a string column, set its parser with `putParserForName`, as the next example does.

In the following example, the file `/data/headless.csv` has no header row. The `specs` argument tells `readCsv` that the first row is data, names the columns with [`headers`](#csvspecs-builder-options), and sets column types with [`putParserForName`](#csvspecs-builder-options). Header names apply by position, so the example lists them in the file's column order.

```groovy order=result
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools
import io.deephaven.csv.parsers.Parsers

new File("/data/headless.csv").text = "A,2,55\nB,4,76\nC,1,20\n"

specs = CsvTools.builder()
    .hasHeaderRow(false)
    .headers(["X", "Y", "Z"])
    .putParserForName("X", Parsers.STRING)
    .putParserForName("Y", Parsers.INT)
    .putParserForName("Z", Parsers.LONG)
    .build()

result = readCsv("/data/headless.csv", specs)
```

A `String` path can also be a URL. In the following example, `readCsv` loads the file [DeNiro CSV](https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv) directly from [Deephaven's example repository](https://github.com/deephaven/examples).

```groovy
import static io.deephaven.csv.CsvTools.readCsv

result = readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv")
```

Pipe (`|`) and tab (`\t`) are common delimiters. In the following example, the `specs` argument sets the `delimiter` option so that `readCsv` reads pipe- and tab-delimited files into memory.

```groovy order=resultPsv,resultTsv
import static io.deephaven.csv.CsvTools.readCsv
import io.deephaven.csv.CsvTools

psvSpecs = CsvTools.builder().delimiter('|' as char).build()
tsvSpecs = CsvTools.builder().delimiter('\t' as char).build()

resultPsv = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.psv", psvSpecs)
resultTsv = readCsv("https://raw.githubusercontent.com/deephaven/examples/main/DeNiro/csv/deniro.tsv", tsvSpecs)
```

## Related documentation

- [Read CSV or other delimited files](../../../how-to-guides/data-import-export/csv-import.md)
- [Export data to CSV or other delimited files](../../../how-to-guides/data-import-export/csv-export.md)
- [`writeCsv`](./writeCsv.md)
- [Docker bind mounts](../../../conceptual/docker-data-volumes.md)
- [`CsvTools.readCsv` Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#readCsv(java.lang.String))
- [`CsvTools.builder` Javadoc](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#builder())
- [`CsvSpecs` Javadoc](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html)
- [`CsvSpecs.Builder` Javadoc](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.Builder.html)
