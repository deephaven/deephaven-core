//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.csv;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.csv.CsvSpecs.Builder;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.reading.CsvReader.ResultColumn;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.tokenization.Tokenizer.CustomTimeZoneParser;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.util.PathUtil;
import io.deephaven.io.streams.BzipFileOutputStream;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.deephaven.engine.util.TableTools.NULL_STRING;
import static io.deephaven.util.QueryConstants.*;

/**
 * Utilities for reading and writing CSV files to and from {@link Table}s
 */
public class CsvTools {

    // Public so it can be used from user scripts
    @SuppressWarnings("WeakerAccess")
    public final static int MAX_CSV_LINE_COUNT = 1000000;

    public final static boolean NULLS_AS_EMPTY_DEFAULT = true;

    /**
     * Creates a {@link Builder} with {@link CsvTools}-specific values. Sets {@link ColumnNameLegalizer#INSTANCE} as
     * {@link Builder#headerLegalizer(Function)} and {@link Builder#headerValidator(Predicate)}; sets a new instance of
     * {@link DeephavenTimeZoneParser} as {@link Builder#customTimeZoneParser(CustomTimeZoneParser)}.
     *
     * @return the builder
     */
    public static Builder builder() {
        return CsvSpecs.builder()
                .headerLegalizer(ColumnNameLegalizer.INSTANCE)
                .headerValidator(ColumnNameLegalizer.INSTANCE)
                .customTimeZoneParser(new DeephavenTimeZoneParser());
    }

    /**
     * Creates an in-memory table from {@code path} by importing CSV data.
     *
     * <p>
     * If {@code path} is a non-file {@link URL}, the CSV will be parsed via {@link #readCsv(URL, CsvSpecs)}.
     *
     * <p>
     * Otherwise, the {@code path} will be parsed via {@link #readCsv(Path, CsvSpecs)}, which will apply decompression
     * based on the {@code path}.
     *
     * @param path the path
     * @return the table
     * @see #readCsv(String, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(String path) throws CsvReaderException {
        return readCsv(path, builder().build());
    }

    /**
     * Creates an in-memory table from {@code stream} by importing CSV data. The {@code stream} will be closed upon
     * return.
     *
     * @param stream an InputStream providing access to the CSV data.
     * @return a Deephaven Table object
     * @see #readCsv(InputStream, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(InputStream stream) throws CsvReaderException {
        return readCsv(stream, builder().build());
    }

    /**
     * Creates an in-memory table from {@code url} by importing CSV data.
     *
     * @param url the url
     * @return the table
     * @see #readCsv(URL, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(URL url) throws CsvReaderException {
        return readCsv(url, builder().build());
    }

    /**
     * Creates an in-memory table from {@code path} by importing CSV data.
     *
     * <p>
     * Paths that end in ".tar.zip", ".tar.bz2", ".tar.gz", ".tar.7z", ".tar.zst", ".zip", ".bz2", ".gz", ".7z", ".zst",
     * or ".tar" will be decompressed.
     *
     * @param path the file path
     * @return the table
     * @see #readCsv(Path, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(Path path) throws CsvReaderException {
        return readCsv(path, builder().build());
    }

    /**
     * Creates an in-memory table from {@code path} by importing CSV data according to the {@code specs}.
     *
     * <p>
     * If {@code path} is a non-file {@link URL}, the CSV will be parsed via {@link #readCsv(URL, CsvSpecs)}.
     *
     * <p>
     * Otherwise, the {@code path} will be parsed via {@link #readCsv(Path, CsvSpecs)}, which will apply decompression
     * based on the {@code path}.
     *
     * @param path the path
     * @param specs the csv specs
     * @return the table
     * @see #readCsv(URL, CsvSpecs)
     * @see #readCsv(Path, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(String path, CsvSpecs specs) throws CsvReaderException {
        URL: {
            final URL url;
            try {
                url = new URL(path);
            } catch (MalformedURLException e) {
                break URL;
            }
            if (isStandardFile(url)) {
                return readCsv(Paths.get(url.getPath()), specs);
            }
            return readCsv(url, specs);
        }
        return readCsv(Paths.get(path), specs);
    }

    /**
     * Creates an in-memory table from {@code stream} by importing CSV data according to the {@code specs}. The
     * {@code stream} will be closed upon return.
     *
     * @param stream The stream
     * @param specs The CSV specs.
     * @return The table.
     * @throws CsvReaderException If some error occurs.
     */
    @ScriptApi
    public static Table readCsv(InputStream stream, CsvSpecs specs) throws CsvReaderException {
        final CsvReader.Result result = CsvReader.read(specs, stream, makeMySinkFactory());
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>(result.numCols());
        for (ResultColumn column : result) {
            columns.put(column.name(), (ColumnSource<?>) column.data());
        }
        final TableDefinition tableDef = TableDefinition.inferFrom(columns);
        final TrackingRowSet rowSet = RowSetFactory.flat(result.numRows()).toTracking();
        return InMemoryTable.from(tableDef, rowSet, columns);
    }

    /**
     * Creates an in-memory table from {@code url} by importing CSV data according to the {@code specs}.
     *
     * @param url the url
     * @param specs the csv specs
     * @return the table
     * @throws CsvReaderException If some CSV reading error occurs.
     */
    @ScriptApi
    public static Table readCsv(URL url, CsvSpecs specs) throws CsvReaderException {
        try {
            return readCsv(url.openStream(), specs);
        } catch (IOException inner) {
            throw new CsvReaderException("Caught exception", inner);
        }
    }

    /**
     * Creates an in-memory table from {@code path} by importing CSV data according to the {@code specs}.
     *
     * <p>
     * A {@code path} that ends in ".tar.zip", ".tar.bz2", ".tar.gz", ".tar.7z", ".tar.zst", ".zip", ".bz2", ".gz",
     * ".7z", ".zst", or ".tar" will be decompressed.
     *
     * @param path the path
     * @param specs the csv specs
     * @return the table
     * @throws CsvReaderException If some CSV reading error occurs.
     * @see PathUtil#open(Path)
     */
    @ScriptApi
    public static Table readCsv(Path path, CsvSpecs specs) throws CsvReaderException {
        try {
            return readCsv(PathUtil.open(path), specs);
        } catch (IOException inner) {
            throw new CsvReaderException("Caught exception", inner);
        }
    }

    /**
     * Convert an ordered collection of column names to use for a result table into a series of {@link Pair rename
     * pairs} to pass to {@link Table#renameColumns(Collection)}.
     *
     * @param columnNames The column names
     * @return A collection of {@link Pair rename columns}
     */
    public static Collection<Pair> renamesForHeaderless(Collection<String> columnNames) {
        int ci = 0;
        final List<Pair> out = new ArrayList<>(columnNames.size());
        for (String columnName : columnNames) {
            out.add(Pair.of(ColumnName.of(columnName), ColumnName.of(String.format("Column%d", ci + 1))));
            ++ci;
        }
        return out;
    }

    /**
     * Convert an array of column names to use for a result table into a series of {@link Pair rename pairs} to pass to
     * {@link Table#renameColumns(Collection)}.
     *
     * @param columnNames The column names
     * @return A collection of {@link Pair rename columns}
     */
    public static Collection<Pair> renamesForHeaderless(String... columnNames) {
        return renamesForHeaderless(Arrays.asList(columnNames));
    }

    /**
     * Equivalent to
     * {@code CsvTools.readCsv(filePath, CsvTools.builder().hasHeaderRow(false).build()).renameColumns(renamesForHeaderless(columnNames));}
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, Collection<String> columnNames) throws CsvReaderException {
        return readCsv(filePath, builder().hasHeaderRow(false).build())
                .renameColumns(renamesForHeaderless(columnNames));
    }

    /**
     * Equivalent to
     * {@code CsvTools.readCsv(filePath, CsvTools.builder().hasHeaderRow(false).build()).renameColumns(renamesForHeaderless(columnNames));}
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, String... columnNames) throws CsvReaderException {
        return readCsv(filePath, builder().hasHeaderRow(false).build())
                .renameColumns(renamesForHeaderless(columnNames));
    }

    /**
     * Creates an in-memory table by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @return a Deephaven Table object
     * @deprecated See {@link #readCsv(InputStream, CsvSpecs)}
     */
    @ScriptApi
    @Deprecated
    public static Table readCsv(InputStream is, final String format) throws CsvReaderException {
        final CsvSpecs specs = fromLegacyFormat(format);
        if (specs == null) {
            throw new IllegalArgumentException(String.format("Unable to map legacy format '%s' into CsvSpecs", format));
        }
        return readCsv(is, specs);
    }

    /**
     * Creates an in-memory table by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param separator a char to use as the delimiter value when parsing the file.
     * @return a Deephaven Table object
     * @deprecated See {@link #readCsv(InputStream, CsvSpecs)}
     */
    @ScriptApi
    @Deprecated
    public static Table readCsv(InputStream is, final char separator) throws CsvReaderException {
        return readCsv(is, builder().delimiter(separator).build());
    }

    private static boolean isStandardFile(URL url) {
        return "file".equals(url.getProtocol()) && url.getAuthority() == null && url.getQuery() == null
                && url.getRef() == null;
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, boolean compressed, String destPath, String... columns)
            throws IOException {
        writeCsv(source, compressed, destPath, NULLS_AS_EMPTY_DEFAULT, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, boolean compressed, String destPath, boolean nullsAsEmpty,
            String... columns) throws IOException {
        writeCsv(source, destPath, compressed, DateTimeUtils.timeZone(), nullsAsEmpty, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, String... columns) throws IOException {
        writeCsv(source, destPath, NULLS_AS_EMPTY_DEFAULT, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean nullsAsEmpty, String... columns)
            throws IOException {
        writeCsv(source, destPath, false, DateTimeUtils.timeZone(), nullsAsEmpty, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param out the stream to write to
     * @param columns a list of columns to include in the export
     * @throws IOException if there is a problem writing to the stream
     */
    @ScriptApi
    public static void writeCsv(Table source, PrintStream out, String... columns) throws IOException {
        writeCsv(source, out, NULLS_AS_EMPTY_DEFAULT, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param out the stream to write to
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if there is a problem writing to the stream
     */
    @ScriptApi
    public static void writeCsv(Table source, PrintStream out, boolean nullsAsEmpty, String... columns)
            throws IOException {
        final PrintWriter printWriter = new PrintWriter(out);
        final BufferedWriter bufferedWriter = new BufferedWriter(printWriter);
        writeCsv(source, bufferedWriter, DateTimeUtils.timeZone(), null, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, ZoneId timeZone,
            String... columns) throws IOException {
        writeCsv(source, destPath, compressed, timeZone, null, NULLS_AS_EMPTY_DEFAULT, ',', columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, ZoneId timeZone,
            boolean nullsAsEmpty, String... columns) throws IOException {
        writeCsv(source, destPath, compressed, timeZone, null, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, ZoneId timeZone,
            boolean nullsAsEmpty, char separator, String... columns) throws IOException {
        writeCsv(source, destPath, compressed, timeZone, null, nullsAsEmpty, separator, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed, ZoneId timeZone,
            String tableSeparator, String... columns) throws IOException {
        writeCsv(sources, destPath, compressed, timeZone, tableSeparator, NULLS_AS_EMPTY_DEFAULT, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed, ZoneId timeZone,
            String tableSeparator, boolean nullsAsEmpty, String... columns) throws IOException {
        writeCsv(sources, destPath, compressed, timeZone, tableSeparator, ',', nullsAsEmpty, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param fieldSeparator the delimiter for the CSV files
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed, ZoneId timeZone,
            String tableSeparator, char fieldSeparator, boolean nullsAsEmpty, String... columns) throws IOException {
        BufferedWriter out =
                (compressed ? new BufferedWriter(new OutputStreamWriter(new BzipFileOutputStream(destPath + ".bz2")))
                        : new BufferedWriter(new FileWriter(destPath)));

        if (columns.length == 0) {
            List<String> columnNames = sources[0].getDefinition().getColumnNames();
            columns = columnNames.toArray(String[]::new);
        }

        writeCsvHeader(out, fieldSeparator, columns);

        for (Table source : sources) {
            writeCsvContents(source, out, timeZone, null, nullsAsEmpty, fieldSeparator, columns);
            out.write(tableSeparator);
        }

        out.close();
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, ZoneId timeZone,
            @Nullable BiConsumer<Long, Long> progress, String... columns) throws IOException {
        writeCsv(source, destPath, compressed, timeZone, progress, NULLS_AS_EMPTY_DEFAULT, columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, ZoneId timeZone,
            @Nullable BiConsumer<Long, Long> progress, boolean nullsAsEmpty, String... columns)
            throws IOException {
        writeCsv(source, destPath, compressed, timeZone, progress, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, ZoneId timeZone,
            @Nullable BiConsumer<Long, Long> progress, boolean nullsAsEmpty, char separator, String... columns)
            throws IOException {
        final Writer out =
                (compressed ? new BufferedWriter(new OutputStreamWriter(new BzipFileOutputStream(destPath + ".bz2")))
                        : new BufferedWriter(new FileWriter(destPath)));
        writeCsv(source, out, timeZone, progress, nullsAsEmpty, separator, columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out Writer used to write the CSV
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, Writer out, ZoneId timeZone,
            @Nullable BiConsumer<Long, Long> progress, boolean nullsAsEmpty, String... columns)
            throws IOException {
        writeCsv(source, out, timeZone, progress, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out Writer used to write the CSV
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, Writer out, ZoneId timeZone,
            @Nullable BiConsumer<Long, Long> progress, boolean nullsAsEmpty, char separator, String... columns)
            throws IOException {

        if (columns == null || columns.length == 0) {
            List<String> columnNames = source.getDefinition().getColumnNames();
            columns = columnNames.toArray(String[]::new);
        }

        writeCsvHeader(out, separator, columns);
        writeCsvContents(source, out, timeZone, progress, nullsAsEmpty, separator, columns);
        out.write(System.lineSeparator());

        out.close();
    }

    /**
     * Writes the column name header row to a CSV file.
     *
     * @param out the Writer to which the header should be written
     * @param columns a list of column names to be written
     * @throws IOException if the Writer cannot be written to
     */
    @ScriptApi
    public static void writeCsvHeader(Writer out, String... columns) throws IOException {
        writeCsvHeader(out, ',', columns);
    }

    /**
     * Writes the column name header row to a CSV file.
     *
     * @param out the Writer to which the header should be written
     * @param separator a char to use as the delimiter value when writing out the header
     * @param columns a list of column names to be written
     * @throws IOException if the Writer cannot be written to
     */
    @ScriptApi
    public static void writeCsvHeader(Writer out, char separator, String... columns) throws IOException {
        final String separatorStr = String.valueOf(separator);
        for (int ci = 0; ci < columns.length; ci++) {
            String column = columns[ci];
            if (ci > 0) {
                out.write(separator);
            }
            out.write(separatorCsvEscape(column, separatorStr));
        }
    }

    /**
     * Writes a Deephaven table to one or more files, splitting it based on the MAX_CSV_LINE_COUNT setting.
     *
     * @param source a Deephaven table to be exported
     * @param destPath the path in which the CSV file(s) should be written
     * @param filename the base file name to use for the files. A dash and starting line number will be concatenated to
     *        each file.
     * @throws IOException if the destination files cannot be written
     */
    @ScriptApi
    public static void writeCsvPaginate(Table source, String destPath, String filename) throws IOException {
        writeCsvPaginate(source, destPath, filename, NULLS_AS_EMPTY_DEFAULT);
    }

    /**
     * Writes a Deephaven table to one or more files, splitting it based on the MAX_CSV_LINE_COUNT setting.
     *
     * @param source a Deephaven table to be exported
     * @param destPath the path in which the CSV file(s) should be written
     * @param filename the base file name to use for the files. A dash and starting line number will be concatenated to
     *        each file.
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @throws IOException if the destination files cannot be written
     */
    @ScriptApi
    public static void writeCsvPaginate(Table source, String destPath, String filename, boolean nullsAsEmpty)
            throws IOException {
        long fileCount = source.size() / MAX_CSV_LINE_COUNT;
        if (fileCount > 0) {
            for (long i = 0; i <= fileCount; i++) {
                writeToMultipleFiles(source, destPath, filename, i * MAX_CSV_LINE_COUNT, nullsAsEmpty);
            }
        } else {
            writeCsv(source, destPath + filename + ".csv", nullsAsEmpty);
        }
    }

    /**
     * Writes a subset of rows from a Deephaven table to a CSV file.
     *
     * @param table a Deephaven table from which rows should be exported
     * @param path the destination path in which the output CSV file should be created
     * @param filename the base file name to which a dash and starting line number will be concatenated for the file
     * @param startLine the starting line number from the table to export; the ending line number will be startLine +
     *        MAX_CSV_LINE_COUNT-1, or the end of the table
     * @throws IOException if the destination file cannot be written
     */
    @ScriptApi
    public static void writeToMultipleFiles(Table table, String path, String filename, long startLine)
            throws IOException {
        writeToMultipleFiles(table, path, filename, startLine, NULLS_AS_EMPTY_DEFAULT);
    }

    /**
     * Writes a subset of rows from a Deephaven table to a CSV file.
     *
     * @param table a Deephaven table from which rows should be exported
     * @param path the destination path in which the output CSV file should be created
     * @param filename the base file name to which a dash and starting line number will be concatenated for the file
     * @param startLine the starting line number from the table to export; the ending line number will be startLine +
     *        MAX_CSV_LINE_COUNT-1, or the end of the table
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @throws IOException if the destination file cannot be written
     */
    @ScriptApi
    public static void writeToMultipleFiles(Table table, String path, String filename, long startLine,
            boolean nullsAsEmpty) throws IOException {
        Table part = table.getSubTable(
                table.getRowSet().subSetByPositionRange(startLine, startLine + MAX_CSV_LINE_COUNT).toTracking());
        String partFilename = path + filename + "-" + startLine + ".csv";
        writeCsv(part, partFilename, nullsAsEmpty);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, ZoneId timeZone, String... colNames)
            throws IOException {
        writeCsvContents(source, out, timeZone, null, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, ZoneId timeZone, boolean nullsAsEmpty,
            String... colNames) throws IOException {
        writeCsvContents(source, out, timeZone, null, nullsAsEmpty, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, ZoneId timeZone,
            @Nullable BiConsumer<Long, Long> progress, String... colNames) throws IOException {
        writeCsvContents(source, out, timeZone, progress, NULLS_AS_EMPTY_DEFAULT, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, ZoneId timeZone,
            @Nullable BiConsumer<Long, Long> progress, boolean nullsAsEmpty, String... colNames)
            throws IOException {
        writeCsvContents(source, out, timeZone, progress, nullsAsEmpty, ',', colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, ZoneId timeZone,
            @Nullable BiConsumer<Long, Long> progress, boolean nullsAsEmpty, char separator, String... colNames)
            throws IOException {
        if (colNames.length == 0) {
            return;
        }
        final ColumnSource<?>[] cols =
                Arrays.stream(colNames).map(source::getColumnSource).toArray(ColumnSource[]::new);
        writeCsvContentsSeq(out, timeZone, source.getRowSet(), cols, nullsAsEmpty, separator, progress);
    }

    /**
     * Returns a String value for a CSV column's value. This String will be enclosed in double quotes if the value
     * includes a double quote, a newline, or the separator.
     *
     * @param str the String to be escaped
     * @param separator the delimiter for the CSV
     * @return the input String, enclosed in double quotes if the value contains a comma, newline or double quote
     */
    protected static String separatorCsvEscape(String str, String separator) {
        if (str.contains("\"") || str.contains("\n") || str.contains(separator)) {
            return '"' + str.replaceAll("\"", "\"\"") + '"';
        } else {
            return str;
        }
    }

    /**
     * Writes Deephaven columns out as a CSV file.
     *
     * @param out a Writer to which the header should be written
     * @param timeZone a time zone constant relative to which date time data should be adjusted
     * @param rows a RowSet containing the row keys to be written
     * @param cols an array of ColumnSources to be written
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param progress a procedure that implements BiConsumer, and takes a progress Integer and a total size Integer to
     *        update progress
     * @throws IOException if the target file cannot be written
     */
    private static void writeCsvContentsSeq(
            final Writer out,
            final ZoneId timeZone,
            final RowSet rows,
            final ColumnSource<?>[] cols,
            final boolean nullsAsEmpty,
            final char separator,
            @Nullable final BiConsumer<Long, Long> progress) throws IOException {
        if (rows.isEmpty()) {
            return;
        }
        try (final SafeCloseable ignored =
                QueryPerformanceRecorder.getInstance().getNugget("CsvTools.writeCsvContentsSeq()");
                final CsvRowFormatter formatter = new CsvRowFormatter(timeZone, nullsAsEmpty,
                        String.valueOf(separator), System.lineSeparator(), cols)) {
            formatter.writeRows(out, rows, progress);
        }
    }

    private static final class CsvRowFormatter implements Context {

        private static final int CHUNK_CAPACITY = ArrayBackedColumnSource.BLOCK_SIZE;

        private final ZoneId timeZone;
        private final String separator;
        private final String lineSeparator;
        private final String nullValue;
        private final SharedContext sharedContext;
        private final ColumnFormatter<?>[] columnFormatters;

        private CsvRowFormatter(
                @NotNull final ZoneId timeZone,
                final boolean nullsAsEmpty,
                final String separator,
                final String lineSeparator,
                @NotNull final ColumnSource<?>... columns) {
            this.timeZone = timeZone;
            this.separator = separator;
            this.lineSeparator = lineSeparator;
            nullValue = separatorCsvEscape(nullsAsEmpty ? "" : NULL_STRING, separator);
            sharedContext = columns.length > 1 ? SharedContext.makeSharedContext() : null;
            columnFormatters = Arrays.stream(columns).map(this::makeColumnFormatter).toArray(ColumnFormatter[]::new);
        }

        private synchronized void writeRows(
                @NotNull final Writer writer,
                @NotNull final RowSequence rows,
                @Nullable final BiConsumer<Long, Long> progress) throws IOException {
            final long totalSize = rows.size();
            long rowsWritten = 0;
            try (final RowSequence.Iterator rowsIterator = rows.getRowSequenceIterator()) {
                while (rowsIterator.hasMore()) {
                    final RowSequence sliceRows = rowsIterator.getNextRowSequenceWithLength(CHUNK_CAPACITY);
                    final int sliceSize = sliceRows.intSize();
                    for (final ColumnFormatter<?> columnFormatter : columnFormatters) {
                        columnFormatter.fill(sliceRows);
                    }
                    for (int sri = 0; sri < sliceSize; ++sri) {
                        for (int ci = 0; ci < columnFormatters.length; ++ci) {
                            if (ci == 0) {
                                // Writing our line separator at the beginning, rather than the end, seems like an
                                // unusual choice, but the code has behaved this way for some time. Maybe related to
                                // the multi-table writing functionality and `tableSeparator`?
                                writer.write(lineSeparator);
                            } else {
                                writer.write(separator);
                            }
                            columnFormatters[ci].write(writer, sri);
                        }
                    }
                    if (sharedContext != null) {
                        sharedContext.reset();
                    }
                    if (progress != null) {
                        progress.accept(rowsWritten += sliceSize, totalSize);
                    }
                }
            }
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(Stream.concat(Stream.of(sharedContext), Arrays.stream(columnFormatters)));
        }

        private ColumnFormatter<?> makeColumnFormatter(@NotNull final ColumnSource<?> source) {
            final Class<?> type = source.getType();
            if (type == char.class || type == Character.class) {
                Assert.eq(source.getChunkType(), "source.getChunkType()", ChunkType.Char, "ChunkType.Char");
                return new CharColumnFormatter(source);
            }
            if (type == byte.class || type == Byte.class) {
                Assert.eq(source.getChunkType(), "source.getChunkType()", ChunkType.Byte, "ChunkType.Byte");
                return new ByteColumnFormatter(source);
            }
            if (type == short.class || type == Short.class) {
                Assert.eq(source.getChunkType(), "source.getChunkType()", ChunkType.Short, "ChunkType.Short");
                return new ShortColumnFormatter(source);
            }
            if (type == int.class || type == Integer.class) {
                Assert.eq(source.getChunkType(), "source.getChunkType()", ChunkType.Int, "ChunkType.Int");
                return new IntColumnFormatter(source);
            }
            if (type == long.class || type == Long.class) {
                Assert.eq(source.getChunkType(), "source.getChunkType()", ChunkType.Long, "ChunkType.Long");
                return new LongColumnFormatter(source);
            }
            if (type == float.class || type == Float.class) {
                Assert.eq(source.getChunkType(), "source.getChunkType()", ChunkType.Float, "ChunkType.Float");
                return new FloatColumnFormatter(source);
            }
            if (type == double.class || type == Double.class) {
                Assert.eq(source.getChunkType(), "source.getChunkType()", ChunkType.Double, "ChunkType.Double");
                return new DoubleColumnFormatter(source);
            }
            Assert.eq(source.getChunkType(), "source.getChunkType()", ChunkType.Object, "ChunkType.Object");
            if (type == Instant.class) {
                return new InstantColumnFormatter(source);
            }
            if (type == ZonedDateTime.class) {
                return new ZonedDateTimeColumnFormatter(source);
            }
            return new ObjectColumnFormatter(source);
        }

        private abstract class ColumnFormatter<CHUNK_CLASS extends Chunk<? extends Any>> implements Context {

            private final ChunkSource<? extends Any> source;
            private final ChunkSource.GetContext getContext;

            CHUNK_CLASS values;

            private ColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                this.source = source;
                this.getContext = source.makeGetContext(CHUNK_CAPACITY, sharedContext);
            }

            @Override
            public void close() {
                getContext.close();
            }

            private void fill(@NotNull final RowSequence rows) {
                // noinspection unchecked
                values = (CHUNK_CLASS) source.getChunk(getContext, rows);
            }

            abstract void write(@NotNull Writer writer, int offset) throws IOException;

            void writeNull(@NotNull final Writer writer) throws IOException {
                writer.write(nullValue);
            }
        }

        private final class CharColumnFormatter extends ColumnFormatter<CharChunk<? extends Any>> {

            private CharColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final char value = values.get(offset);
                if (value == NULL_CHAR) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(String.valueOf(value), separator));
            }
        }

        private final class ByteColumnFormatter extends ColumnFormatter<ByteChunk<? extends Any>> {

            private ByteColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final byte value = values.get(offset);
                if (value == NULL_BYTE) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(Byte.toString(value), separator));
            }
        }

        private final class ShortColumnFormatter extends ColumnFormatter<ShortChunk<? extends Any>> {

            private ShortColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final short value = values.get(offset);
                if (value == NULL_SHORT) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(Short.toString(value), separator));
            }
        }

        private final class IntColumnFormatter extends ColumnFormatter<IntChunk<? extends Any>> {

            private IntColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final int value = values.get(offset);
                if (value == NULL_INT) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(Integer.toString(value), separator));
            }
        }

        private final class LongColumnFormatter extends ColumnFormatter<LongChunk<? extends Any>> {

            private LongColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final long value = values.get(offset);
                if (value == NULL_LONG) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(Long.toString(value), separator));
            }
        }

        private final class FloatColumnFormatter extends ColumnFormatter<FloatChunk<? extends Any>> {

            private FloatColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final float value = values.get(offset);
                if (value == NULL_FLOAT) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(Float.toString(value), separator));
            }
        }

        private final class DoubleColumnFormatter extends ColumnFormatter<DoubleChunk<? extends Any>> {

            private DoubleColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final double value = values.get(offset);
                if (value == NULL_DOUBLE) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(Double.toString(value), separator));
            }
        }

        private final class ObjectColumnFormatter extends ColumnFormatter<ObjectChunk<?, ? extends Any>> {

            private ObjectColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final Object value = values.get(offset);
                if (value == null) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(value.toString(), separator));
            }
        }

        private final class InstantColumnFormatter extends ColumnFormatter<ObjectChunk<Instant, ? extends Any>> {

            private InstantColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final Instant value = values.get(offset);
                if (value == null) {
                    writeNull(writer);
                    return;
                }
                final ZonedDateTime zdt = ZonedDateTime.ofInstant(value, timeZone);
                writer.write(separatorCsvEscape(zdt.toLocalDateTime().toString() + zdt.getOffset(), separator));
            }
        }

        private final class ZonedDateTimeColumnFormatter
                extends ColumnFormatter<ObjectChunk<ZonedDateTime, ? extends Any>> {

            private ZonedDateTimeColumnFormatter(@NotNull final ChunkSource<? extends Any> source) {
                super(source);
            }

            @Override
            void write(@NotNull final Writer writer, final int offset) throws IOException {
                final ZonedDateTime value = values.get(offset);
                if (value == null) {
                    writeNull(writer);
                    return;
                }
                writer.write(separatorCsvEscape(value.toLocalDateTime().toString() + value.getOffset(), separator));
            }
        }
    }

    public static CsvSpecs fromLegacyFormat(String format) {
        final Builder builder = builder();
        if (format == null) {
            return builder.build();
        } else if (format.length() == 1) {
            return builder.delimiter(format.charAt(0)).build();
        } else if ("TRIM".equals(format)) {
            return builder.trim(true).build();
        } else if ("DEFAULT".equals(format)) {
            return builder.ignoreSurroundingSpaces(false).build();
        } else if ("TDF".equals(format)) {
            return builder.delimiter('\t').build();
        }
        return null;
    }

    private static abstract class MySinkBase<TYPE, TARRAY> implements Sink<TARRAY> {
        protected final WritableColumnSource<TYPE> result;
        protected long resultSize;
        protected final WritableColumnSource<?> reinterpreted;
        protected final ChunkWrapInvoker<TARRAY, Chunk<? extends Values>> chunkWrapInvoker;

        public MySinkBase(WritableColumnSource<TYPE> result, Class<?> interpClass,
                ChunkWrapInvoker<TARRAY, Chunk<? extends Values>> chunkWrapInvoker) {
            this.result = result;
            this.resultSize = 0;
            if (interpClass != null) {
                reinterpreted = (WritableColumnSource<?>) result.reinterpret(interpClass);
            } else {
                reinterpreted = result;
            }
            this.chunkWrapInvoker = chunkWrapInvoker;
        }

        @Override
        public final void write(final TARRAY src, final boolean[] isNull, final long destBegin, final long destEnd,
                boolean appending_unused) {
            if (destBegin == destEnd) {
                return;
            }
            final int size = Math.toIntExact(destEnd - destBegin);
            nullFlagsToValues(src, isNull, size);
            reinterpreted.ensureCapacity(destEnd);
            resultSize = Math.max(resultSize, destEnd);
            try (final ChunkSink.FillFromContext context = reinterpreted.makeFillFromContext(size);
                    final RowSequence range = RowSequenceFactory.forRange(destBegin, destEnd - 1)) {
                Chunk<? extends Values> chunk = chunkWrapInvoker.apply(src, 0, size);
                reinterpreted.fillFromChunk(context, chunk, range);
            }
        }

        protected abstract void nullFlagsToValues(final TARRAY values, final boolean[] isNull, final int size);

        public WritableColumnSource<TYPE> result() {
            return result;
        }

        @Override
        public Object getUnderlying() {
            return result;
        }

        public long resultSize() {
            return resultSize;
        }

        protected interface ChunkWrapInvoker<TARRAY, TRESULT> {
            TRESULT apply(final TARRAY data, final int offset, final int capacity);
        }
    }

    private static abstract class MySourceAndSinkBase<TYPE, TARRAY> extends MySinkBase<TYPE, TARRAY>
            implements Source<TARRAY>, Sink<TARRAY> {
        private final ChunkWrapInvoker<TARRAY, WritableChunk<? super Values>> writableChunkWrapInvoker;

        public MySourceAndSinkBase(WritableColumnSource<TYPE> result, Class<?> interpClass,
                ChunkWrapInvoker<TARRAY, Chunk<? extends Values>> chunkWrapInvoker,
                ChunkWrapInvoker<TARRAY, WritableChunk<? super Values>> writeableChunkWrapInvoker) {
            super(result, interpClass, chunkWrapInvoker);
            this.writableChunkWrapInvoker = writeableChunkWrapInvoker;
        }

        @Override
        public final void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            if (srcBegin == srcEnd) {
                return;
            }
            final int size = Math.toIntExact(srcEnd - srcBegin);
            try (final ChunkSink.FillContext context = reinterpreted.makeFillContext(size);
                    final RowSequence range = RowSequenceFactory.forRange(srcBegin, srcEnd - 1)) {
                WritableChunk<? super Values> chunk = writableChunkWrapInvoker.apply(dest, 0, size);
                reinterpreted.fillChunk(context, chunk, range);
            }
            valuesToNullFlags(dest, isNull, size);
        }

        protected abstract void valuesToNullFlags(final TARRAY values, final boolean[] isNull, final int size);
    }

    private static final class MyCharSink extends MySinkBase<Character, char[]> {
        public MyCharSink(int columnIndex) {
            super(new CharacterArraySource(), null, CharChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final char[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = NULL_CHAR;
                }
            }
        }
    }

    private static final class MyBooleanAsByteSink extends MySinkBase<Boolean, byte[]> {
        public MyBooleanAsByteSink(int columnIndex) {
            super(new BooleanArraySource(), byte.class, ByteChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final byte[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = BooleanUtils.NULL_BOOLEAN_AS_BYTE;
                }
            }
        }
    }

    private static final class MyByteSink extends MySourceAndSinkBase<Byte, byte[]> {
        public MyByteSink(int columnIndex) {
            super(new ByteArraySource(), null, ByteChunk::chunkWrap, WritableByteChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final byte[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = NULL_BYTE;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final byte[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == NULL_BYTE;
            }
        }
    }

    private static final class MyShortSink extends MySourceAndSinkBase<Short, short[]> {
        public MyShortSink(int columnIndex) {
            super(new ShortArraySource(), null, ShortChunk::chunkWrap, WritableShortChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final short[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = NULL_SHORT;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final short[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == NULL_SHORT;
            }
        }
    }

    private static final class MyIntSink extends MySourceAndSinkBase<Integer, int[]> {
        public MyIntSink(int columnIndex) {
            super(new IntegerArraySource(), null, IntChunk::chunkWrap, WritableIntChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final int[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = NULL_INT;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final int[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == NULL_INT;
            }
        }
    }

    private static final class MyLongSink extends MySourceAndSinkBase<Long, long[]> {
        public MyLongSink(int columnIndex) {
            super(new LongArraySource(), null, LongChunk::chunkWrap, WritableLongChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = NULL_LONG;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == NULL_LONG;
            }
        }

    }

    private static final class MyFloatSink extends MySinkBase<Float, float[]> {
        public MyFloatSink(int columnIndex) {
            super(new FloatArraySource(), null, FloatChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final float[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = NULL_FLOAT;
                }
            }
        }
    }

    private static final class MyDoubleSink extends MySinkBase<Double, double[]> {
        public MyDoubleSink(int columnIndex) {
            super(new DoubleArraySource(), null, DoubleChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final double[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = NULL_DOUBLE;
                }
            }
        }
    }

    private static final class MyStringSink extends MySinkBase<String, String[]> {
        public MyStringSink(int columnIndex) {
            super(new ObjectArraySource<>(String.class), null, ObjectChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final String[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = null;
                }
            }
        }
    }

    private static final class MyInstantAsLongSink extends MySinkBase<Instant, long[]> {
        public MyInstantAsLongSink(int columnIndex) {
            super(new InstantArraySource(), long.class, LongChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = NULL_LONG;
                }
            }
        }
    }

    private static SinkFactory makeMySinkFactory() {
        return SinkFactory.of(
                MyByteSink::new, NULL_BYTE_BOXED,
                MyShortSink::new, NULL_SHORT_BOXED,
                MyIntSink::new, NULL_INT_BOXED,
                MyLongSink::new, NULL_LONG_BOXED,
                MyFloatSink::new, NULL_FLOAT_BOXED,
                MyDoubleSink::new, NULL_DOUBLE_BOXED,
                MyBooleanAsByteSink::new,
                MyCharSink::new, NULL_CHAR,
                MyStringSink::new, null,
                MyInstantAsLongSink::new, NULL_LONG,
                MyInstantAsLongSink::new, NULL_LONG);
    }
}
