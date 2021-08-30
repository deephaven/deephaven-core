/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.Procedure;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.io.streams.BzipFileOutputStream;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.progress.MinProcessStatus;
import io.deephaven.util.progress.ProgressLogger;
import io.deephaven.util.progress.StatusCallback;
import org.jetbrains.annotations.Nullable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVParser;

import java.io.*;
import java.util.*;

import static io.deephaven.db.tables.utils.NameValidator.legalizeColumnName;

/**
 * Utilities for reading and writing CSV files to and from {@link Table}s
 */
public class CsvHelpers {
    // Public so it can be used from user scripts
    @SuppressWarnings("WeakerAccess")
    public final static int MAX_CSV_LINE_COUNT = 1000000;

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
        writeCsv(source, destPath, compressed, timeZone, progress, false, columns);
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
        writeCsvPaginate(source, destPath, filename, false);
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
        writeToMultipleFiles(table, path, filename, startLine, false);
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
        writeCsvContents(source, out, timeZone, progress, false, colNames);
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
     * Return the provided {@link StatusCallback} if provided, otherwise create a new one and return it.
     *
     * @param progress use this if it is not null
     * @param withLog whether to create a StatusCallback that will annotate progress updates to the current log
     * @return a valid StatusCallback.
     */
    private static StatusCallback checkStatusCallback(StatusCallback progress, boolean withLog) {
        if (progress == null) {
            if (withLog) {
                return new ProgressLogger(new MinProcessStatus(), ProcessEnvironment.get().getLog());
            } else {
                return new MinProcessStatus();
            }
        }
        return progress;
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
        return readCsvInternal(is, format, false, checkStatusCallback(null, false));
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    static Table readHeaderlessCsv(InputStream is, final String format, StatusCallback progress,
            Collection<String> header) throws IOException {
        final StatusCallback lProgress = checkStatusCallback(progress, true);
        return readCsvInternal(is, format, false, lProgress, true, header);
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    public static Table readCsv(InputStream is, final String format, StatusCallback progress) throws IOException {
        final StatusCallback lProgress = checkStatusCallback(progress, true);
        return readCsvInternal(is, format, false, lProgress);
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
        return readCsvInternal(is, String.valueOf(separator), false, checkStatusCallback(null, false));
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param separator a char to use as the delimiter value when parsing the file.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    public static Table readCsv(InputStream is, final char separator, StatusCallback progress) throws IOException {
        final StatusCallback lProgress = checkStatusCallback(progress, true);
        return readCsvInternal(is, String.valueOf(separator), false, lProgress);
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param separator a char to use as the delimiter value when parsing the file.
     * @return a Deephaven QueryTable object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    public static QueryTable readCsv2(InputStream is, final char separator) throws IOException {
        return (QueryTable) readCsvInternal(is, String.valueOf(separator), true, checkStatusCallback(null, false));
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
        return readCsvInternal(is, null, false, checkStatusCallback(null, false));
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    public static Table readCsv(InputStream is, StatusCallback progress) throws IOException {
        final StatusCallback lProgress = checkStatusCallback(progress, true);
        return readCsvInternal(is, null, false, lProgress);
    }

    /**
     * Returns a memory table created by importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @return a Deephaven QueryTable object
     * @throws IOException if the InputStream cannot be read
     */
    // Public for backwards-compatibility for users calling readCsv2
    @SuppressWarnings("WeakerAccess")
    public static QueryTable readCsv2(InputStream is) throws IOException {
        return (QueryTable) readCsvInternal(is, null, true, checkStatusCallback(null, false));
    }

    /**
     * Does the work of creating a memory table by importing CSV data. The first row must be column names. Column data
     * types are inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @param v2 whether the process the import using the older QueryTable processing (v2 = true) or the newer
     *        InMemoryTable processing (v2 = false).
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     */
    private static Table readCsvInternal(InputStream is, String format, boolean v2, StatusCallback progress)
            throws IOException {
        return readCsvInternal(is, format, v2, progress, false, null);
    }


    /**
     * Does the work of creating a memory table by importing CSV data. The first row must be column names. Column data
     * types are inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @param v2 whether the process the import using the older QueryTable processing (v2 = true) or the newer
     *        InMemoryTable processing (v2 = false).
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @param noHeader True when the CSV does not have a header row.
     * @param header Column names to use as, or instead of, the header row for the CSV.
     * @return a Deephaven Table object
     * @throws IOException if the InputStream cannot be read
     */
    private static Table readCsvInternal(InputStream is, String format, boolean v2, StatusCallback progress,
            boolean noHeader, @Nullable Collection<String> header) throws IOException {
        final char separator;
        final InputStreamReader fileReader = new InputStreamReader(is);

        final StatusCallback lProgress = checkStatusCallback(progress, true);

        if (format == null) {
            format = "TRIM";
            separator = ',';
        } else {
            if (format.length() == 1) {
                separator = format.charAt(0);
                format = "TRIM";
            } else {
                separator = ',';
                format = format.toUpperCase();
            }
        }

        final CSVFormat parseFormat = CsvParserFormat.getCsvFormat(format, separator, (format.equals("TRIM")), noHeader,
                header == null ? null : new ArrayList<>(header));
        final CSVParser parser = new CSVParser(fileReader, parseFormat);

        lProgress.update(10, "Reading column names from CSV.");
        String[] columnNames = new String[0];
        if (!noHeader || !(header == null)) {
            columnNames = parser.getHeaderMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
            if (columnNames.length == 0) {
                throw new RuntimeException("No columns found in CSV.");
            }
        }

        lProgress.update(20, "Reading records from CSV.");
        long initialLineNumber = parser.getCurrentLineNumber();
        List<CSVRecord> csvData = parser.getRecords();
        final int colCount;
        if (csvData.size() == 0) {
            if (noHeader && header == null) {
                throw new RuntimeException("There was no header provided and there were no records found in the CSV.");
            }
            colCount = 0;
        } else {
            try {
                colCount = csvData.get(0).size();
            } catch (Exception e) {
                throw new RuntimeException("Failed to get number of columns from first record of CSV.", e);
            }
        }

        /*
         * Validate provided header: The parser will fail to read if there are more column headers than data columns,
         * but having less headers than data columns is perfectly valid, and there will be cases where columns are
         * variable, but there is a guaranteed left-subset, or a user is only interested in a left-subset of the data.
         */
        if (header != null && columnNames.length > colCount && csvData.size() > 0) {
            throw new RuntimeException("More column names provided in the header (" + columnNames.length
                    + ") than exist in the first record of the CSV (" + colCount + ").");
        }

        if (header == null && noHeader) {
            columnNames = new String[colCount];
            for (int i = 0; i < colCount; i++) {
                columnNames[i] = "Column" + (i + 1);
            }
        }

        Object[] columnData = new Object[columnNames.length];
        int numRows = csvData.size();

        for (int col = 0; col < columnNames.length; col++) {
            lProgress.update(20 + (col + 1) * 70 / columnNames.length,
                    "Parsing CSV column " + (col + 1) + " of " + columnNames.length + ".");
            columnData[col] = parseColumn(csvData, numRows, col, initialLineNumber);
        }

        HashSet<String> taken = new HashSet<>();
        for (int i = 0; i < columnNames.length; i++) {
            // The Apache parser does not allow duplicate column names, including blank/null, but it will allow one
            // blank/null.
            // Replace a single blank/null column name with the first unique value based on "Column1."
            if (columnNames[i] == null || columnNames[i].isEmpty()) {
                columnNames[i] = "Column" + (i + 1);
            }
            columnNames[i] = legalizeColumnName(columnNames[i], (s) -> s.replaceAll("[- ]", "_"), taken);
            taken.add(columnNames[i]);
        }

        if (v2) {
            Map<String, ColumnSource<?>> columnSources = new LinkedHashMap<>();
            for (int ii = 0; ii < columnNames.length; ii++) {
                lProgress.update(90 + (ii + 1) * 10 / columnNames.length,
                        "Mapping CSV column " + (ii + 1) + " of " + columnNames.length + " to table.");
                ColumnSource<?> arrayBackedSource =
                        ArrayBackedColumnSource.getMemoryColumnSourceUntyped(columnData[ii]);
                columnSources.put(columnNames[ii], arrayBackedSource);
            }
            lProgress.finish("");
            return new QueryTable(Index.FACTORY.getFlatIndex(numRows), columnSources);
        } else {
            lProgress.finish("");
            return new InMemoryTable(columnNames, columnData);
        }
    }

    /**
     * Returns a column of data, and inspects the data read from a CSV to determine what data type would best fit the
     * column.
     *
     * @param csvData a List of CSVRecords from the Apache Commons CSV parser used to read the CSV file
     * @param numRows how many rows to read from the List
     * @param col which column from each record should be read
     * @param initialLineNumber initial line number in the source file from which the data was read (i.e. 1 if there was
     *        a header, 0 if not)
     * @return an object representing an array of values from the column that was read
     */
    private static Object parseColumn(List<CSVRecord> csvData, int numRows, int col, long initialLineNumber)
            throws IOException {
        Boolean isInteger = null;
        Boolean isLong = null;
        Boolean isDouble = null;
        Boolean isBoolean = null;
        Boolean isDateTime = null;
        Boolean isLocalTime = null;

        long lineNumber = initialLineNumber;
        for (CSVRecord line : csvData) {
            if (col >= line.size()) {
                throw new IOException("Error parsing column " + (col + 1) + " on line " + (lineNumber + 1) +
                        " - line only has " + line.size() + " columns.");
            }

            final String value = line.get(col);

            if (isNull(value)) {
                continue;
            }

            if (isInteger == null || isInteger) {
                try {
                    Integer.parseInt(value);
                    isInteger = true;
                } catch (NumberFormatException e) {
                    isInteger = false;
                }
            }

            if (isLong == null || isLong) {
                try {
                    Long.parseLong(value);
                    isLong = true;
                } catch (NumberFormatException e) {
                    isLong = false;
                }
            }

            if (isDouble == null || isDouble) {
                try {
                    Double.parseDouble(value);
                    isDouble = true;
                } catch (NumberFormatException e) {
                    isDouble = false;
                }
            }

            if (isBoolean == null || isBoolean) {
                isBoolean = "true".equalsIgnoreCase(line.get(col)) || "false".equalsIgnoreCase(line.get(col));
            }

            if (isDateTime == null || isDateTime) {
                isDateTime = DBTimeUtils.convertDateTimeQuiet(value) != null;
            }

            if (isLocalTime == null || isLocalTime) {
                isLocalTime = DBTimeUtils.convertTimeQuiet(value) != io.deephaven.util.QueryConstants.NULL_LONG;
            }

            lineNumber++;
        }

        if (isInteger != null && isInteger) {
            int[] data = new int[numRows];

            for (int row = 0; row < numRows; row++) {
                String value = csvData.get(row).get(col);

                data[row] = isNull(value) ? io.deephaven.util.QueryConstants.NULL_INT : Integer.parseInt(value);
            }

            return data;
        } else if (isLong != null && isLong) {
            long[] data = new long[numRows];

            for (int row = 0; row < numRows; row++) {
                String value = csvData.get(row).get(col);

                data[row] = isNull(value) ? io.deephaven.util.QueryConstants.NULL_LONG : Long.parseLong(value);
            }

            return data;
        } else if (isDouble != null && isDouble) {
            double[] data = new double[numRows];

            for (int row = 0; row < numRows; row++) {
                String value = csvData.get(row).get(col);

                data[row] = isNull(value) ? io.deephaven.util.QueryConstants.NULL_DOUBLE : Double.parseDouble(value);
            }

            return data;
        } else if (isBoolean != null && isBoolean) {
            Boolean[] data = new Boolean[numRows];

            for (int row = 0; row < numRows; row++) {
                String value = csvData.get(row).get(col);

                data[row] = isNull(value) ? QueryConstants.NULL_BOOLEAN : Boolean.valueOf(value);
            }

            return data;
        } else if (isDateTime != null && isDateTime) {
            DBDateTime[] data = new DBDateTime[numRows];

            for (int row = 0; row < numRows; row++) {
                DBDateTime value = DBTimeUtils.convertDateTimeQuiet(csvData.get(row).get(col));

                data[row] = value;
            }

            return data;
        } else if (isLocalTime != null && isLocalTime) {
            long[] data = new long[numRows];

            for (int row = 0; row < numRows; row++) {
                data[row] = DBTimeUtils.convertTimeQuiet(csvData.get(row).get(col));
            }

            return data;
        } else {
            String[] data = new String[numRows];

            for (int row = 0; row < numRows; row++) {
                String value = csvData.get(row).get(col);

                data[row] = isNull(value) ? null : value;
            }

            return data;
        }
    }

    private static boolean isNull(String s) {
        return s.length() == 0 || s.equals(TableTools.NULL_STRING);
    }
}
