/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.utils;

import io.deephaven.base.Procedure;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.tables.DataColumn;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.utils.csv.CsvSpecs;
import io.deephaven.engine.v2.InMemoryTable;
import io.deephaven.io.streams.BzipFileOutputStream;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Utilities for reading and writing CSV files to and from {@link Table}s
 */
public class CsvHelpers {
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
     * <p>
     * Equivalent to {@code readCsv(path, CsvSpecs.csv())}.
     *
     * @param path the path
     * @return the table
     * @throws IOException if an I/O exception occurs
     * @see #readCsv(String, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(String path) throws IOException {
        return readCsv(path, CsvSpecs.csv());
    }

    /**
     * Creates an in-memory table from {@code stream} by importing CSV data. The {@code stream} will be closed upon
     * return.
     *
     * <p>
     * Equivalent to {@code readCsv(stream, CsvSpecs.csv())}
     *
     * @param stream an InputStream providing access to the CSV data.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     * @see #readCsv(InputStream, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(InputStream stream) throws IOException {
        return readCsv(stream, CsvSpecs.csv());
    }

    /**
     * Creates an in-memory table from {@code url} by importing CSV data.
     *
     * <p>
     * Equivalent to {@code readCsv(url, CsvSpecs.csv())}.
     *
     * @param url the url
     * @return the table
     * @throws IOException if an I/O exception occurs
     * @see #readCsv(URL, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(URL url) throws IOException {
        return readCsv(url, CsvSpecs.csv());
    }

    /**
     * Creates an in-memory table from {@code path} by importing CSV data.
     *
     * <p>
     * Paths that end in ".tar.zip", ".tar.bz2", ".tar.gz", ".tar.7z", ".tar.zst", ".zip", ".bz2", ".gz", ".7z", ".zst",
     * or ".tar" will be decompressed.
     *
     * <p>
     * Equivalent to {@code readCsv(path, CsvSpecs.csv())}.
     *
     * @param path the file path
     * @return the table
     * @throws IOException if an I/O exception occurs
     * @see #readCsv(Path, CsvSpecs)
     */
    @ScriptApi
    public static Table readCsv(Path path) throws IOException {
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
    public static Table readCsv(String path, CsvSpecs specs) throws IOException {
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
     * @throws IOException if an I/O exception occurs
     */
    @ScriptApi
    public static Table readCsv(InputStream stream, CsvSpecs specs) throws IOException {
        return InMemoryTable.from(specs.parse(stream));
    }

    /**
     * Creates an in-memory table from {@code url} by importing CSV data according to the {@code specs}.
     *
     * @param url the url
     * @param specs the csv specs
     * @return the table
     * @throws IOException if an I/O exception occurs
     */
    @ScriptApi
    public static Table readCsv(URL url, CsvSpecs specs) throws IOException {
        return InMemoryTable.from(specs.parse(url.openStream()));
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
     * @throws IOException if an I/O exception occurs
     * @see PathUtil#open(Path)
     */
    @ScriptApi
    public static Table readCsv(Path path, CsvSpecs specs) throws IOException {
        return InMemoryTable.from(specs.parse(PathUtil.open(path)));
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, DBTimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, String... columns) throws IOException {
        writeCsv(source, destPath, compressed, timeZone, progress, NULLS_AS_EMPTY_DEFAULT, columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, DBTimeZone timeZone,
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
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, DBTimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, char separator, String... columns)
            throws IOException {
        final BufferedWriter out =
                (compressed ? new BufferedWriter(new OutputStreamWriter(new BzipFileOutputStream(destPath + ".bz2")))
                        : new BufferedWriter(new FileWriter(destPath)));
        writeCsv(source, out, timeZone, progress, nullsAsEmpty, separator, columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out BufferedWriter used to write the CSV
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, BufferedWriter out, DBTimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, String... columns)
            throws IOException {
        writeCsv(source, out, timeZone, progress, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out BufferedWriter used to write the CSV
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, BufferedWriter out, DBTimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, char separator, String... columns)
            throws IOException {

        if (columns == null || columns.length == 0) {
            List<String> columnNames = source.getDefinition().getColumnNames();
            columns = columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        }

        CsvHelpers.writeCsvHeader(out, separator, columns);
        CsvHelpers.writeCsvContents(source, out, timeZone, progress, nullsAsEmpty, separator, columns);

        out.close();
    }

    /**
     * Writes the column name header row to a CSV file.
     *
     * @param out the BufferedWriter to which the header should be written
     * @param columns a list of column names to be written
     * @throws IOException if the BufferedWriter cannot be written to
     */
    @ScriptApi
    public static void writeCsvHeader(BufferedWriter out, String... columns) throws IOException {
        writeCsvHeader(out, ',', columns);
    }

    /**
     * Writes the column name header row to a CSV file.
     *
     * @param out the BufferedWriter to which the header should be written
     * @param separator a char to use as the delimiter value when writing out the header
     * @param columns a list of column names to be written
     * @throws IOException if the BufferedWriter cannot be written to
     */
    @ScriptApi
    public static void writeCsvHeader(BufferedWriter out, char separator, String... columns) throws IOException {
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
            TableTools.writeCsv(source, destPath + filename + ".csv", nullsAsEmpty);
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
        Table part = table.getSubTable(table.getIndex().subindexByPos(startLine, startLine + MAX_CSV_LINE_COUNT));
        String partFilename = path + filename + "-" + startLine + ".csv";
        TableTools.writeCsv(part, partFilename, nullsAsEmpty);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a BufferedWriter to which the header should be written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, BufferedWriter out, DBTimeZone timeZone, String... colNames)
            throws IOException {
        writeCsvContents(source, out, timeZone, null, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a BufferedWriter to which the header should be written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, BufferedWriter out, DBTimeZone timeZone, boolean nullsAsEmpty,
            String... colNames) throws IOException {
        writeCsvContents(source, out, timeZone, null, nullsAsEmpty, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a BufferedWriter to which the header should be written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, BufferedWriter out, DBTimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, String... colNames) throws IOException {
        writeCsvContents(source, out, timeZone, progress, NULLS_AS_EMPTY_DEFAULT, colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a BufferedWriter to which the header should be written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, BufferedWriter out, DBTimeZone timeZone,
            @Nullable Procedure.Binary<Long, Long> progress, boolean nullsAsEmpty, String... colNames)
            throws IOException {
        writeCsvContents(source, out, timeZone, progress, nullsAsEmpty, ',', colNames);
    }

    /**
     * Writes a table out as a CSV file.
     *
     * @param source a Deephaven table object to be exported
     * @param out a BufferedWriter to which the header should be written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param colNames a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsvContents(Table source, BufferedWriter out, DBTimeZone timeZone,
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
     * @param out a BufferedWriter to which the header should be written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param cols an array of Deephaven DataColumns to be written
     * @param size the size of the DataColumns
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param progress a procedure that implements Procedure.Binary, and takes a progress Integer and a total size
     *        Integer to update progress
     * @throws IOException if the target file cannot be written
     */
    private static void writeCsvContentsSeq(
            final BufferedWriter out,
            final DBTimeZone timeZone,
            final DataColumn[] cols,
            final long size,
            final boolean nullsAsEmpty,
            final char separator,
            @Nullable Procedure.Binary<Long, Long> progress) throws IOException {
        QueryPerformanceNugget nugget =
                QueryPerformanceRecorder.getInstance().getNugget("TableTools.writeCsvContentsSeq()");
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
                    } else if (o instanceof DBDateTime) {
                        out.write(separatorCsvEscape(((DBDateTime) o).toString(timeZone), separatorStr));
                    } else {
                        out.write(nullsAsEmpty ? separatorCsvEscape(TableTools.nullToEmptyString(o), separatorStr)
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

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
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
    public static Table readCsv(InputStream is, final String format) throws IOException {
        final CsvSpecs specs = CsvSpecs.fromLegacyFormat(format);
        if (specs == null) {
            throw new IllegalArgumentException(String.format("Unable to map legacy format '%s' into CsvSpecs", format));
        }
        return readCsv(is, specs);
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
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
    public static Table readCsv(InputStream is, final char separator) throws IOException {
        return InMemoryTable.from(CsvSpecs.builder().delimiter(separator).build().parse(is));
    }

    private static boolean isStandardFile(URL url) {
        return "file".equals(url.getProtocol()) && url.getAuthority() == null && url.getQuery() == null
                && url.getRef() == null;
    }
}
