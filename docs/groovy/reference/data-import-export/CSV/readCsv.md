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

Specifications for how to load the CSV file. See [`CsvSpecs` builder options](#csvspecs-builder-options).

</Param>
</ParamTable>

> [!NOTE]
> Only one format parameter can be used at a time.

## Returns

A new in-memory table from a CSV file.

## `CsvSpecs` builder options

To create the `specs` argument, call [`CsvTools.builder`](https://deephaven.io/core/javadoc/io/deephaven/csv/CsvTools.html#builder()), set any of the options below, and then call `build` to get the configured [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html).

- `allowMissingColumns(boolean)`: Whether the CSV file can have fewer columns than the header specifies.
- `concurrent(boolean)`: Whether to read the input and parse each column on separate threads.
- `customDoubleParser(Tokenizer.CustomDoubleParser)`: The parser to use for values read as `double`.
- `customTimeZoneParser(Tokenizer.CustomTimeZoneParser)`: A low-level parser for custom time zone strings. `CsvTools.builder` sets one that understands Deephaven time zone names.
- `delimiter(char)`: The field delimiter character (the character that separates one column from the next).
- `escape(Character)`: The escape character. Use it to read field or line delimiters as literal text, or to read the Java-style escape sequences `\b`, `\t`, `\n`, `\r`, and `\f`. It is unset by default.
- `fixedColumnWidths(Iterable<Integer>)`: The column widths of a fixed-width file, when you don't want the library to determine them from the header row.
- `from(CsvSpecs)`: An existing `CsvSpecs` object whose options are copied into this builder.
- `hasFixedWidthColumns(boolean)`: Whether the input is organized into fixed-width columns rather than separated by a delimiter.
- `hasHeaderRow(boolean)`: Whether the input file has a header row.
- `headerLegalizer(Function<String[],String[]>)`: The function that rewrites column headers into legal names. `CsvTools.builder` sets Deephaven's column-name legalizer, and setting this option replaces it.
- `headers(Iterable<String>)`: Column headers you supply. If `hasHeaderRow` is `true`, these headers replace the headers in the input. If `hasHeaderRow` is `false`, they supply the missing headers.
- `headerValidator(Predicate<String>)`: The check that a column header is legal. `CsvTools.builder` sets Deephaven's column-name validator.
- `ignoreEmptyLines(boolean)`: Whether the library should skip over empty lines in the input.
- `ignoreExcessColumns(boolean)`: Whether the library should allow excess columns in the input.
- `ignoreSurroundingSpaces(boolean)`: Whether to trim leading and trailing white space from non-quoted values.
- `nullParser(Parser<?>)`: The parser to use when all values in the column are null.
- `nullValueLiterals(Iterable<String>)`: The strings that mean a null value in any column that has no null value literals of its own. The default is the empty string.
- `numRows(long)`: The maximum number of rows to process.
- `parsers(Iterable<? extends Parser<?>>)`: The parsers that participate in type inference.
- `putHeaderForIndex(int, String)`: A column header that replaces the header at a 0-based column index.
- `putNullValueLiteralsForIndex(int, List<String>)`: The null value literals for the column at a 0-based column index.
- `putNullValueLiteralsForName(String, List<String>)`: The null value literals for the named column.
- `putParserForIndex(int, Parser<?>)`: A parser forced for the column at a 0-based column index.
- `putParserForName(String, Parser<?>)`: A parser forced for the named column.
- `putParsersForIndex(int, List<Parser<?>>)`: A set of parsers forced for the column at a 0-based column index.
- `putParsersForName(String, List<Parser<?>>)`: A set of parsers forced for the named column.
- `quote(char)`: The quote character. Enclose a field in it to read field or line delimiters inside the field as literal text.
- `skipHeaderRows(long)`: The number of rows to skip before reading the header row from the input.
- `skipRows(long)`: The number of data rows to skip before processing data.
- `threadShutdownTimeout(Duration)`: How long to wait for the reader threads to shut down if processing ends abnormally, such as after an exception. The default is 1 minute.
- `trim(boolean)`: Whether to trim leading and trailing white space from inside quoted values.
- `useUtf32CountingConvention(boolean)`: Whether fixed-width column widths are measured in Unicode code points (`true`) or in UTF-16 code units, which are Java `char` values (`false`).

For the full description of each option, see the [`CsvSpecs.Builder` Javadoc](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.Builder.html).

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
