/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.csv;

import io.deephaven.base.Procedure;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.time.DateTime;
import io.deephaven.time.TimeZone;
import io.deephaven.engine.util.PathUtil;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.streams.BzipFileOutputStream;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Utilities for reading and writing CSV files to and from {@link Table}s
 */
public class CsvTools {

    // Public so it can be used from user scripts
    @SuppressWarnings("WeakerAccess")
    public final static int MAX_CSV_LINE_COUNT = 1000000;

    public final static boolean NULLS_AS_EMPTY_DEFAULT = true;

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
        return readCsv(path, CsvSpecs.csv());
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
        return readCsv(stream, CsvSpecs.csv());
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
        return readCsv(url, CsvSpecs.csv());
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
        return readCsv(path, CsvSpecs.csv());
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
     * @param stream the stream
     * @param specs the csv specs
     * @return the table
     * @throws CsvReaderException If some error occurs.
     */
    @ScriptApi
    public static Table readCsv(InputStream stream, CsvSpecs specs) throws CsvReaderException {
        return specs.parse(stream);
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
            return specs.parse(url.openStream());
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
            return specs.parse(PathUtil.open(path));
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
     * {@code CsvTools.readCsv(filePath, CsvSpecs.headerless()).renameColumns(renamesForHeaderless(columnNames));}
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, Collection<String> columnNames) throws CsvReaderException {
        return CsvTools.readCsv(filePath, CsvSpecs.headerless()).renameColumns(renamesForHeaderless(columnNames));
    }

    /**
     * Equivalent to
     * {@code CsvTools.readCsv(filePath, CsvSpecs.headerless()).renameColumns(renamesForHeaderless(columnNames));}
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, String... columnNames) throws CsvReaderException {
        return CsvTools.readCsv(filePath, CsvSpecs.headerless()).renameColumns(renamesForHeaderless(columnNames));
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
        final CsvSpecs specs = CsvSpecs.fromLegacyFormat(format);
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
        return CsvSpecs.builder().delimiter(separator).build().parse(is);
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
}
