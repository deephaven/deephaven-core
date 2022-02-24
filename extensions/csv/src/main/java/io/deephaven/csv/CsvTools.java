/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.csv;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.Procedure;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.csv.CsvSpecs.Builder;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.tokenization.Tokenizer.CustomTimeZoneParser;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.BooleanArraySource;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.engine.table.impl.sources.DateTimeArraySource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.engine.util.PathUtil;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.streams.BzipFileOutputStream;
import io.deephaven.time.DateTime;
import io.deephaven.time.TimeZone;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ScriptApi;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

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
     * @throws IOException if an I/O exception occurs
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
     * @throws IOException if the InputStream cannot be read
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
     * @throws IOException if an I/O exception occurs
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
     * @throws IOException if an I/O exception occurs
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
     * @throws IOException if an I/O exception occurs
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
        final String[] columnNames = result.columnNames();
        final Sink<?>[] sinks = result.columns();
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        long maxSize = 0;
        for (int ii = 0; ii < columnNames.length; ++ii) {
            final String columnName = columnNames[ii];
            final MySinkBase<?, ?> sink = (MySinkBase<?, ?>) sinks[ii];
            maxSize = Math.max(maxSize, sink.resultSize());
            columns.put(columnName, sink.result());
        }
        final TableDefinition tableDef = TableDefinition.inferFrom(columns);
        final TrackingRowSet rowSet = RowSetFactory.flat(maxSize).toTracking();
        return InMemoryTable.from(tableDef, rowSet, columns);
    }

    /**
     * Creates an in-memory table from {@code url} by importing CSV data according to the {@code specs}.
     *
     * @param url the url
     * @param specs the csv specs
     * @return the table
     * @throws CsvReaderException If some CSV reading error occurs.
     * @throws IOException if the URL cannot be opened.
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
     * @throws IOException if an I/O exception occurs
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
     * Convert an ordered collection of column names to use for a result table into a series of {@link MatchPair rename
     * pairs} to pass to {@link Table#renameColumns(MatchPair...)}.
     *
     * @param columnNames The column names
     * @return An array of {@link MatchPair rename columns}
     */
    public static MatchPair[] renamesForHeaderless(Collection<String> columnNames) {
        final MatchPair[] renames = new MatchPair[columnNames.size()];
        int ci = 0;
        for (String columnName : columnNames) {
            // CsvSpecs.headerless() column names are 1-based index
            renames[ci] = new MatchPair(columnName, String.format("Column%d", ci + 1));
            ++ci;
        }
        return renames;
    }

    /**
     * Convert an array of column names to use for a result table into a series of {@link MatchPair rename pairs} to
     * pass to {@link Table#renameColumns(MatchPair...)}.
     *
     * @param columnNames The column names
     * @return An array of {@link MatchPair rename columns}
     */
    public static MatchPair[] renamesForHeaderless(String... columnNames) {
        return renamesForHeaderless(Arrays.asList(columnNames));
    }

    /**
     * Equivalent to
     * {@code CsvTools.readCsv(filePath, CsvTools.builder().hasHeaderRow(false).build()).renameColumns(renamesForHeaderless(columnNames));}
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, Collection<String> columnNames) throws CsvReaderException {
        return CsvTools.readCsv(filePath, builder().hasHeaderRow(false).build())
                .renameColumns(renamesForHeaderless(columnNames));
    }

    /**
     * Equivalent to
     * {@code CsvTools.readCsv(filePath, CsvTools.builder().hasHeaderRow(false).build()).renameColumns(renamesForHeaderless(columnNames));}
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, String... columnNames) throws CsvReaderException {
        return CsvTools.readCsv(filePath, builder().hasHeaderRow(false).build())
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
     * @throws IOException if the InputStream cannot be read
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
     * @throws IOException if the InputStream cannot be read
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
        writeCsv(source, destPath, compressed, io.deephaven.time.TimeZone.TZ_DEFAULT, nullsAsEmpty, columns);
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
        writeCsv(source, destPath, false, io.deephaven.time.TimeZone.TZ_DEFAULT, nullsAsEmpty, columns);
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
        CsvTools.writeCsv(source, bufferedWriter, io.deephaven.time.TimeZone.TZ_DEFAULT, null, nullsAsEmpty,
                ',', columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed,
            io.deephaven.time.TimeZone timeZone,
            String... columns) throws IOException {
        CsvTools.writeCsv(source, destPath, compressed, timeZone, null, NULLS_AS_EMPTY_DEFAULT, ',', columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed,
            io.deephaven.time.TimeZone timeZone,
            boolean nullsAsEmpty, String... columns) throws IOException {
        CsvTools.writeCsv(source, destPath, compressed, timeZone, null, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed,
            io.deephaven.time.TimeZone timeZone,
            boolean nullsAsEmpty, char separator, String... columns) throws IOException {
        CsvTools.writeCsv(source, destPath, compressed, timeZone, null, nullsAsEmpty, separator, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed,
            io.deephaven.time.TimeZone timeZone,
            String tableSeparator, String... columns) throws IOException {
        writeCsv(sources, destPath, compressed, timeZone, tableSeparator, NULLS_AS_EMPTY_DEFAULT, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed,
            io.deephaven.time.TimeZone timeZone,
            String tableSeparator, boolean nullsAsEmpty, String... columns) throws IOException {
        writeCsv(sources, destPath, compressed, timeZone, tableSeparator, ',', nullsAsEmpty, columns);
    }

    /**
     * Writes a table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param fieldSeparator the delimiter for the CSV files
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed,
            io.deephaven.time.TimeZone timeZone,
            String tableSeparator, char fieldSeparator, boolean nullsAsEmpty, String... columns) throws IOException {
        BufferedWriter out =
                (compressed ? new BufferedWriter(new OutputStreamWriter(new BzipFileOutputStream(destPath + ".bz2")))
                        : new BufferedWriter(new FileWriter(destPath)));

        if (columns.length == 0) {
            List<String> columnNames = sources[0].getDefinition().getColumnNames();
            columns = columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
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
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, TimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, String... columns) throws IOException {
        writeCsv(source, destPath, compressed, timeZone, progress, NULLS_AS_EMPTY_DEFAULT, columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, TimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, String... columns)
            throws IOException {
        writeCsv(source, destPath, compressed, timeZone, progress, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, TimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, char separator, String... columns)
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
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, Writer out, TimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, String... columns)
            throws IOException {
        writeCsv(source, out, timeZone, progress, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out Writer used to write the CSV
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, Writer out, TimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, char separator, String... columns)
            throws IOException {

        if (columns == null || columns.length == 0) {
            List<String> columnNames = source.getDefinition().getColumnNames();
            columns = columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        }

        writeCsvHeader(out, separator, columns);
        writeCsvContents(source, out, timeZone, progress, nullsAsEmpty, separator, columns);

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
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            if (i > 0) {
                out.write(separator);
            }
            out.write(column);
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
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, TimeZone timeZone, String... colNames)
            throws IOException {
        writeCsvContents(source, out, timeZone, null, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, TimeZone timeZone, boolean nullsAsEmpty,
            String... colNames) throws IOException {
        writeCsvContents(source, out, timeZone, null, nullsAsEmpty, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, TimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, String... colNames) throws IOException {
        writeCsvContents(source, out, timeZone, progress, NULLS_AS_EMPTY_DEFAULT, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, TimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, String... colNames)
            throws IOException {
        writeCsvContents(source, out, timeZone, progress, nullsAsEmpty, ',', colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a Writer to which the header should be written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, Writer out, TimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, char separator, String... colNames)
            throws IOException {
        if (colNames.length == 0) {
            return;
        }
        final DataColumn[] cols = new DataColumn[colNames.length];
        for (int c = 0; c < colNames.length; ++c) {
            cols[c] = source.getColumn(colNames[c]);
        }
        final long size = cols[0].size();
        writeCsvContentsSeq(out, timeZone, cols, size, nullsAsEmpty, separator, progress);
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
     * Writes an array of Deephaven DataColumns out as a CSV file.
     *
     * @param out a Writer to which the header should be written
     * @param timeZone a TimeZone constant relative to which DateTime data should be adjusted
     * @param cols an array of Deephaven DataColumns to be written
     * @param size the size of the DataColumns
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @throws IOException if the target file cannot be written
     */
    private static void writeCsvContentsSeq(
            final Writer out,
            final TimeZone timeZone,
            final DataColumn[] cols,
            final long size,
            final boolean nullsAsEmpty,
            final char separator,
            @Nullable Procedure.Binary<Long, Long> progress) throws IOException {
        QueryPerformanceNugget nugget =
                QueryPerformanceRecorder.getInstance().getNugget("CsvTools.writeCsvContentsSeq()");
        try {
            String separatorStr = String.valueOf(separator);
            for (long i = 0; i < size; i++) {
                for (int j = 0; j < cols.length; j++) {
                    if (j > 0) {
                        out.write(separatorStr);
                    } else {
                        out.write("\n");
                    }
                    final Object o = cols[j].get(i);
                    if (o instanceof String) {
                        out.write("" + separatorCsvEscape((String) o, separatorStr));
                    } else if (o instanceof DateTime) {
                        out.write(separatorCsvEscape(((DateTime) o).toString(timeZone), separatorStr));
                    } else {
                        out.write(nullsAsEmpty
                                ? separatorCsvEscape(o == null ? "" : o.toString(), separatorStr)
                                : separatorCsvEscape(TableTools.nullToNullString(o), separatorStr));
                    }
                }
                if (progress != null) {
                    progress.call(i, size);
                }
            }
        } finally {
            nugget.done();
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
        protected final ArrayBackedColumnSource<TYPE> result;
        protected long resultSize;
        protected final WritableColumnSource<?> reinterpreted;
        protected final ChunkWrapInvoker<TARRAY, Chunk<? extends Values>> chunkWrapInvoker;

        public MySinkBase(ArrayBackedColumnSource<TYPE> result, Class<?> interpClass,
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

        public ArrayBackedColumnSource<TYPE> result() {
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

        public MySourceAndSinkBase(ArrayBackedColumnSource<TYPE> result, Class<?> interpClass,
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
        public MyCharSink() {
            super(new CharacterArraySource(), null, CharChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final char[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_CHAR;
                }
            }
        }
    }

    private static final class MyBooleanAsByteSink extends MySinkBase<Boolean, byte[]> {
        public MyBooleanAsByteSink() {
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
        public MyByteSink() {
            super(new ByteArraySource(), null, ByteChunk::chunkWrap, WritableByteChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final byte[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_BYTE;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final byte[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == QueryConstants.NULL_BYTE;
            }
        }
    }

    private static final class MyShortSink extends MySourceAndSinkBase<Short, short[]> {
        public MyShortSink() {
            super(new ShortArraySource(), null, ShortChunk::chunkWrap, WritableShortChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final short[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_SHORT;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final short[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == QueryConstants.NULL_SHORT;
            }
        }
    }

    private static final class MyIntSink extends MySourceAndSinkBase<Integer, int[]> {
        public MyIntSink() {
            super(new IntegerArraySource(), null, IntChunk::chunkWrap, WritableIntChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final int[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_INT;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final int[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == QueryConstants.NULL_INT;
            }
        }
    }

    private static final class MyLongSink extends MySourceAndSinkBase<Long, long[]> {
        public MyLongSink() {
            super(new LongArraySource(), null, LongChunk::chunkWrap, WritableLongChunk::writableChunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_LONG;
                }
            }
        }

        @Override
        protected void valuesToNullFlags(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                isNull[ii] = values[ii] == QueryConstants.NULL_LONG;
            }
        }

    }

    private static final class MyFloatSink extends MySinkBase<Float, float[]> {
        public MyFloatSink() {
            super(new FloatArraySource(), null, FloatChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final float[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_FLOAT;
                }
            }
        }
    }

    private static final class MyDoubleSink extends MySinkBase<Double, double[]> {
        public MyDoubleSink() {
            super(new DoubleArraySource(), null, DoubleChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final double[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_DOUBLE;
                }
            }
        }
    }

    private static final class MyStringSink extends MySinkBase<String, String[]> {
        public MyStringSink() {
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

    private static final class MyDateTimeAsLongSink extends MySinkBase<DateTime, long[]> {
        public MyDateTimeAsLongSink() {
            super(new DateTimeArraySource(), long.class, LongChunk::chunkWrap);
        }

        @Override
        protected void nullFlagsToValues(final long[] values, final boolean[] isNull, final int size) {
            for (int ii = 0; ii != size; ++ii) {
                if (isNull[ii]) {
                    values[ii] = QueryConstants.NULL_LONG;
                }
            }
        }
    }

    private static SinkFactory makeMySinkFactory() {
        return SinkFactory.of(
                MyByteSink::new, QueryConstants.NULL_BYTE_BOXED,
                MyShortSink::new, QueryConstants.NULL_SHORT_BOXED,
                MyIntSink::new, QueryConstants.NULL_INT_BOXED,
                MyLongSink::new, QueryConstants.NULL_LONG_BOXED,
                MyFloatSink::new, QueryConstants.NULL_FLOAT_BOXED,
                MyDoubleSink::new, QueryConstants.NULL_DOUBLE_BOXED,
                MyBooleanAsByteSink::new,
                MyCharSink::new, QueryConstants.NULL_CHAR,
                MyStringSink::new, null,
                MyDateTimeAsLongSink::new, QueryConstants.NULL_LONG,
                MyDateTimeAsLongSink::new, QueryConstants.NULL_LONG);
    }

}
