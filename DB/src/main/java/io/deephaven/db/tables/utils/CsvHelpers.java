/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.Procedure;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.csv.CsvSpecs;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.io.InputStreamFactory;
import io.deephaven.io.streams.BzipFileOutputStream;
import io.deephaven.io.streams.SevenZipInputStream;
import io.deephaven.io.streams.SevenZipInputStream.Behavior;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.tools.tar.TarInputStream;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

/**
 * Utilities for reading and writing CSV files to and from {@link Table}s
 */
public class CsvHelpers {
    // Public so it can be used from user scripts
    @SuppressWarnings("WeakerAccess")
    public final static int MAX_CSV_LINE_COUNT = 1000000;

    public final static boolean NULLS_AS_EMPTY_DEFAULT = true;

    /**
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     * Writes a DB table out as a CSV file.
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
     */
    @ScriptApi
    public static Table readCsv(InputStream is, final String format) throws IOException {
        return readCsv(is, CsvSpecs.fromLegacyFormat(format));
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param separator a char to use as the delimiter value when parsing the file.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    public static Table readCsv(InputStream is, final char separator) throws IOException {
        return InMemoryTable.from(CsvSpecs.builder().delimiter(separator).build().parse(is));
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    public static Table readCsv(InputStream is) throws IOException {
        return readCsv(is, CsvSpecs.csv());
    }

    /**
     * Opens a file, returning an input stream. Paths that end in ".tar.zip", ".tar.bz2", ".tar.gz", ".tar.7z",
     * ".tar.zst", ".zip", ".bz2", ".gz", ".7z", ".zst", or ".tar" will have appropriate decompression applied. The
     * returned stream may or may not be buffered.
     *
     * @param path the path
     * @return the input stream, potentially decompressed
     * @throws IOException if an I/O exception occurs
     * @see Files#newInputStream(Path, OpenOption...)
     */
    public static InputStream open(Path path) throws IOException {
        final String fileName = path.getFileName().toString();
        if (fileName.endsWith(".zip")) {
            final ZipInputStream in = new ZipInputStream(Files.newInputStream(path));
            in.getNextEntry();
            return fileName.endsWith(".tar.zip") ? untar(in) : in;
        }
        if (fileName.endsWith(".bz2")) {
            final BZip2CompressorInputStream in = new BZip2CompressorInputStream(Files.newInputStream(path));
            return fileName.endsWith(".tar.bz2") ? untar(in) : in;
        }
        if (fileName.endsWith(".gz")) {
            final GZIPInputStream in = new GZIPInputStream(Files.newInputStream(path));
            return fileName.endsWith(".tar.gz") ? untar(in) : in;
        }
        if (fileName.endsWith(".7z")) {
            final SevenZipInputStream in = new SevenZipInputStream(new InputStreamFactory() {
                @Override
                public InputStream createInputStream() throws IOException {
                    return Files.newInputStream(path);
                }

                @Override
                public String getDescription() {
                    return path.toString();
                }
            });
            in.getNextEntry(Behavior.SKIP_WHEN_NO_STREAM);
            return fileName.endsWith(".tar.7z") ? untar(in) : in;
        }
        if (fileName.endsWith(".zst")) {
            final ZstdCompressorInputStream in = new ZstdCompressorInputStream(Files.newInputStream(path));
            return fileName.endsWith(".tar.zst") ? untar(in) : in;
        }
        if (fileName.endsWith(".tar")) {
            return untar(Files.newInputStream(path));
        }
        return Files.newInputStream(path);
    }

    private static TarInputStream untar(InputStream in) throws IOException {
        final TarInputStream tarInputStream = new TarInputStream(in);
        tarInputStream.getNextEntry();
        return tarInputStream;
    }

    /**
     * Creates an in-memory table from {@code file} according to the {@code specs}.
     *
     * <p>
     * Paths that end in ".zip", ".bz2", ".gz", ".7z", or ".zst" will be decompressed.
     *
     * @param file the file
     * @param specs the csv specs
     * @return the table
     * @throws IOException if an I/O exception occurs
     * @see #open(Path)
     */
    @ScriptApi
    public static Table readCsv(String file, CsvSpecs specs) throws IOException {
        return readCsv(Paths.get(file), specs);
    }

    /**
     * Creates an in-memory table from {@code path} according to the {@code specs}.
     *
     * <p>
     * Paths that end in ".zip", ".bz2", ".gz", ".7z", or ".zst" will be decompressed.
     *
     * @param path the path
     * @param specs the csv specs
     * @return the table
     * @throws IOException if an I/O exception occurs
     * @see #open(Path)
     */
    @ScriptApi
    public static Table readCsv(Path path, CsvSpecs specs) throws IOException {
        return InMemoryTable.from(specs.parse(open(path)));
    }

    /**
     * Creates an in-memory table from {@code url} according to the {@code specs}.
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
     * Creates an in-memory table from {@code stream} according to the {@code specs}. The {@code stream} will be closed
     * upon return.
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
}
