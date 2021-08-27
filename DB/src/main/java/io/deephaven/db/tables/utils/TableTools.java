/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.ClassUtil;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.io.CompressedFileUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.streams.BzipFileOutputStream;
import io.deephaven.io.util.NullOutputStream;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.util.caching.C14nUtil;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.replay.Replayer;
import io.deephaven.db.v2.replay.ReplayerInterface;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.*;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.progress.MinProcessStatus;
import io.deephaven.util.progress.StatusCallback;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.lang.reflect.Array;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Tools for working with tables. This includes methods to examine tables, combine them, convert them to and from CSV
 * files, and create and manipulate columns.
 */
@SuppressWarnings("unused")
public class TableTools {

    private static final Logger staticLog_ = ProcessEnvironment.getDefaultLog(TableTools.class);

    // Public so it can be used from user scripts
    @SuppressWarnings("WeakerAccess")
    public static final String NULL_STRING = "(null)";

    private static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }

    private static <T, K, U> Collector<T, ?, Map<K, U>> toLinkedMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends U> valueMapper) {
        return Collectors.toMap(keyMapper, valueMapper, throwingMerger(), LinkedHashMap::new);
    }

    @SuppressWarnings("unchecked")
    private static final Collector<ColumnHolder, ?, Map<String, ColumnSource<?>>> COLUMN_HOLDER_LINKEDMAP_COLLECTOR =
            toLinkedMap(ColumnHolder::getName, ColumnHolder::getColumnSource);

    /////////// Utilities To Display Tables /////////////////
    // region Show Utilities

    /**
     * Prints the first few rows of a table to standard output.
     *
     * @param source a Deephaven table object
     * @param columns varargs of column names to display
     */
    public static void show(Table source, String... columns) {
        show(source, 10, DBTimeZone.TZ_DEFAULT, System.out, columns);
    }

    /**
     * Prints the first few rows of a table to standard output, and also prints the details of the index and record
     * positions that provided the values.
     *
     * @param source a Deephaven table object
     * @param columns varargs of column names to display
     */
    public static void showWithIndex(Table source, String... columns) {
        showWithIndex(source, 10, DBTimeZone.TZ_DEFAULT, System.out, columns);
    }

    /**
     * Prints the first few rows of a table to standard output, with commas between values.
     *
     * @param source a Deephaven table object
     * @param columns varargs of column names to display
     */
    public static void showCommaDelimited(Table source, String... columns) {
        show(source, 10, DBTimeZone.TZ_DEFAULT, ",", System.out, false, columns);
    }

    /**
     * Prints the first few rows of a table to standard output.
     *
     * @param source a Deephaven table object
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param columns varargs of column names to display
     */
    public static void show(Table source, DBTimeZone timeZone, String... columns) {
        show(source, 10, timeZone, System.out, columns);
    }

    /**
     * Prints the first few rows of a table to standard output.
     *
     * @param source a Deephaven table object
     * @param maxRowCount the number of rows to return
     * @param columns varargs of column names to display
     */
    public static void show(Table source, long maxRowCount, String... columns) {
        show(source, maxRowCount, DBTimeZone.TZ_DEFAULT, System.out, columns);
    }

    /**
     * Prints the first few rows of a table to standard output, and also prints the details of the index and record
     * positions that provided the values.
     *
     * @param source a Deephaven table object
     * @param maxRowCount the number of rows to return
     * @param columns varargs of column names to display
     */
    public static void showWithIndex(Table source, long maxRowCount, String... columns) {
        showWithIndex(source, maxRowCount, DBTimeZone.TZ_DEFAULT, System.out, columns);
    }

    /**
     * Prints the first few rows of a table to standard output, with commas between values.
     *
     * @param source a Deephaven table object
     * @param maxRowCount the number of rows to return
     * @param columns varargs of column names to display
     */
    public static void showCommaDelimited(Table source, long maxRowCount, String... columns) {
        show(source, maxRowCount, DBTimeZone.TZ_DEFAULT, ",", System.out, false, columns);
    }

    /**
     * Prints the first few rows of a table to standard output.
     *
     * @param source a Deephaven table object
     * @param maxRowCount the number of rows to return
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param columns varargs of column names to display
     */
    public static void show(Table source, long maxRowCount, DBTimeZone timeZone, String... columns) {
        show(source, maxRowCount, timeZone, System.out, columns);
    }

    /**
     * Prints the first few rows of a table to standard output.
     *
     * @param source a Deephaven table object
     * @param maxRowCount the number of rows to return
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param out a PrintStream destination to which to print the data
     * @param columns varargs of column names to display
     */
    public static void show(Table source, long maxRowCount, DBTimeZone timeZone, PrintStream out, String... columns) {
        show(source, maxRowCount, timeZone, "|", out, false, columns);
    }

    /**
     * Prints the first few rows of a table to standard output, and also prints the details of the index and record
     * positions that provided the values.
     *
     * @param source a Deephaven table object
     * @param maxRowCount the number of rows to return
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param out a PrintStream destination to which to print the data
     * @param columns varargs of column names to display
     */
    public static void showWithIndex(Table source, long maxRowCount, DBTimeZone timeZone, PrintStream out,
            String... columns) {
        show(source, maxRowCount, timeZone, "|", out, true, columns);
    }

    /**
     * Prints the first few rows of a table to standard output, and also prints the details of the index and record
     * positions that provided the values.
     *
     * @param source a Deephaven table object
     * @param firstRow the firstRow to display
     * @param lastRow the lastRow (exclusive) to display
     * @param out a PrintStream destination to which to print the data
     * @param columns varargs of column names to display
     */
    public static void showWithIndex(Table source, long firstRow, long lastRow, PrintStream out, String... columns) {
        TableShowTools.showInternal(source, firstRow, lastRow, DBTimeZone.TZ_DEFAULT, "|", out, true, columns);
    }

    /**
     * Prints the first few rows of a table to standard output.
     *
     * @param source a Deephaven table object
     * @param maxRowCount the number of rows to return
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param delimiter a String value to use between printed values
     * @param out a PrintStream destination to which to print the data
     * @param showIndex a boolean indicating whether to also print index details
     * @param columns varargs of column names to display
     */
    public static void show(final Table source, final long maxRowCount, final DBTimeZone timeZone,
            final String delimiter, final PrintStream out, final boolean showIndex, String... columns) {
        TableShowTools.showInternal(source, 0, maxRowCount, timeZone, delimiter, out, showIndex, columns);
    }

    /**
     * Prints the first few rows of a table to standard output, and also prints the details of the index and record
     * positions that provided the values.
     *
     * @param source a Deephaven table object
     * @param firstRow the firstRow to display
     * @param lastRow the lastRow (exclusive) to display
     * @param columns varargs of column names to display
     */
    public static void showWithIndex(final Table source, final long firstRow, final long lastRow, String... columns) {
        TableShowTools.showInternal(source, firstRow, lastRow, DBTimeZone.TZ_DEFAULT, "|", System.out, true, columns);
    }

    /**
     * Returns the first few rows of a table as a pipe-delimited string.
     *
     * @param t a Deephaven table object
     * @param columns varargs of columns to include in the result
     * @return a String
     */
    public static String string(Table t, String... columns) {
        return string(t, 10, DBTimeZone.TZ_DEFAULT, columns);
    }

    /**
     * Returns the first few rows of a table as a pipe-delimited string.
     *
     * @param t a Deephaven table object
     * @param size the number of rows to return
     * @param columns varargs of columns to include in the result
     * @return a String
     */
    public static String string(Table t, int size, String... columns) {
        return string(t, size, DBTimeZone.TZ_DEFAULT, columns);
    }

    /**
     * Returns the first few rows of a table as a pipe-delimited string.
     *
     * @param t a Deephaven table object
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param columns varargs of columns to include in the result
     * @return a String
     */
    public static String string(Table t, DBTimeZone timeZone, String... columns) {
        return string(t, 10, timeZone, columns);
    }

    /**
     * Returns the first few rows of a table as a pipe-delimited string.
     *
     * @param t a Deephaven table object
     * @param size the number of rows to return
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param columns varargs of columns to include in the result
     * @return a String
     */
    public static String string(Table t, int size, DBTimeZone timeZone, String... columns) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        TableTools.show(t, size, timeZone, new PrintStream(os), columns);
        return os.toString();
    }

    /**
     * Returns a printout of a table formatted as HTML. Limit use to small tables to avoid running out of memory.
     *
     * @param source a Deephaven table object
     * @return a String of the table printout formatted as HTML
     */
    public static String html(Table source) {
        return HtmlTable.html(source);
    }
    // endregion

    // region Diff Utilities

    /**
     * Computes the difference of two tables for use in verification.
     *
     * @param actualResult first Deephaven table object to compare
     * @param expectedResult second Deephaven table object to compare
     * @param maxDiffLines stop comparing after this many differences are found
     * @return String report of the detected differences
     */
    public static String diff(Table actualResult, Table expectedResult, long maxDiffLines) {
        return diff(actualResult, expectedResult, maxDiffLines, EnumSet.noneOf(TableDiff.DiffItems.class));
    }

    /**
     * Computes the difference of two tables for use in verification.
     *
     * @param actualResult first Deephaven table object to compare
     * @param expectedResult second Deephaven table object to compare
     * @param maxDiffLines stop comparing after this many differences are found
     * @param itemsToSkip EnumSet of checks not to perform, such as checking column order, or exact match of double
     *        values
     * @return String report of the detected differences
     */
    public static String diff(Table actualResult, Table expectedResult, long maxDiffLines,
            EnumSet<TableDiff.DiffItems> itemsToSkip) {
        return TableDiff.diffInternal(actualResult, expectedResult, maxDiffLines, itemsToSkip).getFirst();
    }

    /**
     * Computes the difference of two tables for use in verification.
     *
     * @param actualResult first Deephaven table object to compare
     * @param expectedResult second Deephaven table object to compare
     * @param maxDiffLines stop comparing after this many differences are found
     * @param itemsToSkip EnumSet of checks not to perform, such as checking column order, or exact match of double
     *        values
     * @return a pair of String report of the detected differences, and the first different row (0 if there are no
     *         different data values)
     */
    public static Pair<String, Long> diffPair(Table actualResult, Table expectedResult, long maxDiffLines,
            EnumSet<TableDiff.DiffItems> itemsToSkip) {
        return TableDiff.diffInternal(actualResult, expectedResult, maxDiffLines, itemsToSkip);
    }
    // endregion

    static String nullToNullString(Object obj) {
        return obj == null ? NULL_STRING : obj.toString();
    }

    static String nullToEmptyString(Object obj) {
        return obj == null ? "" : obj.toString();
    }

    /////////// Utilities For CSV /////////////////
    // region CSV Utilities

    /**
     * Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @return a Deephaven DynamicTable object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    public static DynamicTable readCsv(InputStream is) throws IOException {
        return CsvHelpers.readCsv2(is);
    }

    /**
     * Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param is an InputStream providing access to the CSV data.
     * @param separator a char to use as the delimiter value when parsing the file.
     * @return a Deephaven DynamicTable object
     * @throws IOException if the InputStream cannot be read
     */
    @ScriptApi
    public static DynamicTable readCsv(InputStream is, final char separator) throws IOException {
        return CsvHelpers.readCsv2(is, separator);
    }

    /**
     * Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param filePath the fully-qualified path to a CSV file to be read.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readCsv(String filePath) throws IOException {
        return readCsv(new File(filePath));
    }

    /**
     * Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param filePath the fully-qualified path to a CSV file to be read.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readCsv(String filePath, String format) throws IOException {
        return readCsv(new File(filePath), format, new MinProcessStatus());
    }

    /**
     * Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param filePath the fully-qualified path to a CSV file to be read.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readCsv(String filePath, String format, StatusCallback progress) throws IOException {
        return readCsv(new File(filePath), format, progress);
    }

    /**
     * Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param file a file object providing access to the CSV file to be read.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readCsv(File file) throws IOException {
        return readCsv(file, null, new MinProcessStatus());
    }

    /**
     * Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param file a file object providing access to the CSV file to be read.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readCsv(File file, StatusCallback progress) throws IOException {
        return readCsv(file, null, progress);
    }

    /**
     * Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     * inferred from the data.
     *
     * @param file a file object providing access to the CSV file to be read.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readCsv(File file, String format, StatusCallback progress) throws IOException {
        Table table;
        try (final InputStream is = CompressedFileUtil.openPossiblyCompressedFile(file.getAbsolutePath())) {
            table = io.deephaven.db.tables.utils.CsvHelpers.readCsv(is, format, progress);
        }
        return table;
    }

    /**
     * Returns a memory table created from importing CSV data. Column data types are inferred from the data.
     *
     * @param filePath the fully-qualified path to a CSV file to be read.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath) throws IOException {
        return readHeaderlessCsv(new File(filePath), null, null, null);
    }

    /**
     * Returns a memory table created from importing CSV data. Column data types are inferred from the data.
     *
     * @param filePath the fully-qualified path to a CSV file to be read.
     * @param header Column names to use for the resultant table.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, Collection<String> header) throws IOException {
        return readHeaderlessCsv(new File(filePath), null, null, header);
    }

    /**
     * Returns a memory table created from importing CSV data. Column data types are inferred from the data.
     *
     * @param filePath the fully-qualified path to a CSV file to be read.
     * @param header Column names to use for the resultant table.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, String... header) throws IOException {
        return readHeaderlessCsv(new File(filePath), null, null, Arrays.asList(header));
    }

    /**
     * Returns a memory table created from importing CSV data. Column data types are inferred from the data.
     *
     * @param filePath the fully-qualified path to a CSV file to be read.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @param header Column names to use for the resultant table.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readHeaderlessCsv(String filePath, String format, StatusCallback progress,
            Collection<String> header) throws IOException {
        return readHeaderlessCsv(new File(filePath), format, progress, header);
    }

    /**
     * Returns a memory table created from importing CSV data. Column data types are inferred from the data.
     *
     * @param file a file object providing access to the CSV file to be read.
     * @param format an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
     *        use as a delimiter.
     * @param progress a StatusCallback object that can be used to log progress details or update a progress bar. If
     *        passed explicitly as null, a StatusCallback instance will be created to log progress to the current
     *        logger.
     * @param header Column names to use for the resultant table, or null if column names should be automatically
     *        generated.
     * @return a Deephaven Table object
     * @throws IOException if the file cannot be read
     */
    @ScriptApi
    public static Table readHeaderlessCsv(File file, String format, StatusCallback progress,
            @Nullable Collection<String> header) throws IOException {
        Table table;
        try (final InputStream is = CompressedFileUtil.openPossiblyCompressedFile(file.getAbsolutePath())) {
            table = io.deephaven.db.tables.utils.CsvHelpers.readHeaderlessCsv(is, format, progress, header);
        }
        return table;
    }

    /**
     * Writes a DB table out as a CSV.
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
        writeCsv(source, compressed, destPath, false, columns);
    }

    /**
     * Writes a DB table out as a CSV.
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
        writeCsv(source, destPath, compressed, DBTimeZone.TZ_DEFAULT, nullsAsEmpty, columns);
    }

    /**
     * Writes a DB table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, String... columns) throws IOException {
        writeCsv(source, destPath, false, columns);
    }

    /**
     * Writes a DB table out as a CSV.
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
        writeCsv(source, destPath, false, DBTimeZone.TZ_DEFAULT, nullsAsEmpty, columns);
    }

    /**
     * Writes a DB table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param out the stream to write to
     * @param columns a list of columns to include in the export
     * @throws IOException if there is a problem writing to the stream
     */
    @ScriptApi
    public static void writeCsv(Table source, PrintStream out, String... columns) throws IOException {
        writeCsv(source, out, false, columns);
    }

    /**
     * Writes a DB table out as a CSV.
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
        CsvHelpers.writeCsv(source, bufferedWriter, DBTimeZone.TZ_DEFAULT, null, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a DB table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, DBTimeZone timeZone,
            String... columns) throws IOException {
        CsvHelpers.writeCsv(source, destPath, compressed, timeZone, null, false, ',', columns);
    }

    /**
     * Writes a DB table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, DBTimeZone timeZone,
            boolean nullsAsEmpty, String... columns) throws IOException {
        CsvHelpers.writeCsv(source, destPath, compressed, timeZone, null, nullsAsEmpty, ',', columns);
    }

    /**
     * Writes a DB table out as a CSV.
     *
     * @param source a Deephaven table object to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to zip the file being written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param separator the delimiter for the CSV
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table source, String destPath, boolean compressed, DBTimeZone timeZone,
            boolean nullsAsEmpty, char separator, String... columns) throws IOException {
        CsvHelpers.writeCsv(source, destPath, compressed, timeZone, null, nullsAsEmpty, separator, columns);
    }

    /**
     * Writes a DB table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed, DBTimeZone timeZone,
            String tableSeparator, String... columns) throws IOException {
        writeCsv(sources, destPath, compressed, timeZone, tableSeparator, false, columns);
    }

    /**
     * Writes a DB table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed, DBTimeZone timeZone,
            String tableSeparator, boolean nullsAsEmpty, String... columns) throws IOException {
        writeCsv(sources, destPath, compressed, timeZone, tableSeparator, ',', nullsAsEmpty, columns);
    }

    /**
     * Writes a DB table out as a CSV.
     *
     * @param sources an array of Deephaven table objects to be exported
     * @param destPath path to the CSV file to be written
     * @param compressed whether to compress (bz2) the file being written
     * @param timeZone a DBTimeZone constant relative to which DBDateTime data should be adjusted
     * @param tableSeparator a String (normally a single character) to be used as the table delimiter
     * @param fieldSeparator the delimiter for the CSV files
     * @param nullsAsEmpty if nulls should be written as blank instead of '(null)'
     * @param columns a list of columns to include in the export
     * @throws IOException if the target file cannot be written
     */
    @ScriptApi
    public static void writeCsv(Table[] sources, String destPath, boolean compressed, DBTimeZone timeZone,
            String tableSeparator, char fieldSeparator, boolean nullsAsEmpty, String... columns) throws IOException {
        BufferedWriter out =
                (compressed ? new BufferedWriter(new OutputStreamWriter(new BzipFileOutputStream(destPath + ".bz2")))
                        : new BufferedWriter(new FileWriter(destPath)));

        if (columns.length == 0) {
            List<String> columnNames = sources[0].getDefinition().getColumnNames();
            columns = columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        }

        CsvHelpers.writeCsvHeader(out, fieldSeparator, columns);

        for (Table source : sources) {
            CsvHelpers.writeCsvContents(source, out, timeZone, null, nullsAsEmpty, fieldSeparator, columns);
            out.write(tableSeparator);
        }

        out.close();
    }
    // endregion

    /////////// Utilities for Creating Columns ///////////

    /**
     * Creates an in-memory column of the specified type for a collection of values.
     *
     * @param clazz the class to use for the new column
     * @param values a collection of values to populate the new column
     * @param <T> the type to use for the new column
     * @return a Deephaven ColumnSource object
     */
    public static <T> ColumnSource<T> colSource(Class<T> clazz, Collection<T> values) {
        ArrayBackedColumnSource<T> result = ArrayBackedColumnSource.getMemoryColumnSource(values.size(), clazz);
        int resultIndex = 0;
        for (T value : values) {
            result.set(resultIndex++, value);
        }
        return result;
    }

    /**
     * Creates an in-memory column of the specified type for a collection of values
     *
     * @param values a collection of values to populate the new column
     * @param <T> the type to use for the new column
     * @return a Deephaven ColumnSource object
     */
    @SuppressWarnings("unchecked")
    public static <T> ColumnSource<T> objColSource(T... values) {
        ArrayBackedColumnSource<T> result = (ArrayBackedColumnSource<T>) ArrayBackedColumnSource
                .getMemoryColumnSource(values.length, values.getClass().getComponentType());
        for (int i = 0; i < values.length; i++) {
            result.set(i, values[i]);
        }
        return result;
    }

    /**
     * Creates an in-memory column of type long for a collection of values.
     *
     * @param values a collection of values to populate the new column
     * @return a Deephaven ColumnSource object
     */
    public static ColumnSource<Long> colSource(long... values) {
        ArrayBackedColumnSource<Long> result =
                ArrayBackedColumnSource.getMemoryColumnSource(values.length, long.class);
        for (int i = 0; i < values.length; i++) {
            result.set(i, values[i]);
        }
        return result;
    }

    /**
     * Creates an in-memory column of type int for a collection of values.
     *
     * @param values a collection of values to populate the new column
     * @return a Deephaven ColumnSource object
     */
    public static ColumnSource<Integer> colSource(int... values) {
        ArrayBackedColumnSource<Integer> result =
                ArrayBackedColumnSource.getMemoryColumnSource(values.length, int.class);
        for (int i = 0; i < values.length; i++) {
            result.set(i, values[i]);
        }
        return result;
    }

    /**
     * Creates an in-memory column of type short for a collection of values.
     *
     * @param values a collection of values to populate the new column
     * @return a Deephaven ColumnSource object
     */
    public static ColumnSource<Short> colSource(short... values) {
        ArrayBackedColumnSource<Short> result =
                ArrayBackedColumnSource.getMemoryColumnSource(values.length, short.class);
        for (int i = 0; i < values.length; i++) {
            result.set(i, values[i]);
        }
        return result;
    }

    /**
     * Creates an in-memory column of type byte for a collection of values.
     *
     * @param values a collection of values to populate the new column
     * @return a Deephaven ColumnSource object
     */
    public static ColumnSource<Byte> colSource(byte... values) {
        ArrayBackedColumnSource<Byte> result =
                ArrayBackedColumnSource.getMemoryColumnSource(values.length, byte.class);
        for (int i = 0; i < values.length; i++) {
            result.set(i, values[i]);
        }
        return result;
    }

    /**
     * Creates an in-memory column of type char for a collection of values.
     *
     * @param values a collection of values to populate the new column
     * @return a Deephaven ColumnSource object
     */
    public static ColumnSource<Character> colSource(char... values) {
        ArrayBackedColumnSource<Character> result =
                ArrayBackedColumnSource.getMemoryColumnSource(values.length, char.class);
        for (int i = 0; i < values.length; i++) {
            result.set(i, values[i]);
        }
        return result;
    }

    /**
     * Creates an in-memory column of type double for a collection of values.
     *
     * @param values a collection of values to populate the new column
     * @return a Deephaven ColumnSource object
     */
    public static ColumnSource<Double> colSource(double... values) {
        ArrayBackedColumnSource<Double> result =
                ArrayBackedColumnSource.getMemoryColumnSource(values.length, double.class);
        for (int i = 0; i < values.length; i++) {
            result.set(i, values[i]);
        }
        return result;
    }

    /**
     * Creates an in-memory column of type float for a collection of values.
     *
     * @param values a collection of values to populate the new column
     * @return a Deephaven ColumnSource object
     */
    public static ColumnSource<Float> colSource(float... values) {
        ArrayBackedColumnSource<Float> result =
                ArrayBackedColumnSource.getMemoryColumnSource(values.length, float.class);
        for (int i = 0; i < values.length; i++) {
            result.set(i, values[i]);
        }
        return result;
    }

    /**
     * Returns a SmartKey for the specified row from a set of ColumnSources.
     *
     * @param groupByColumnSources a set of ColumnSources from which to retrieve the data
     * @param row the row number for which to retrieve data
     * @return a Deephaven SmartKey object
     */
    public static Object getKey(ColumnSource<?>[] groupByColumnSources, long row) {
        Object key;
        if (groupByColumnSources.length == 0) {
            return SmartKey.EMPTY;
        } else if (groupByColumnSources.length == 1) {
            key = C14nUtil.maybeCanonicalize(groupByColumnSources[0].get(row));
        } else {
            Object[] keyData = new Object[groupByColumnSources.length];
            for (int col = 0; col < groupByColumnSources.length; col++) {
                keyData[col] = groupByColumnSources[col].get(row);
            }
            key = C14nUtil.makeSmartKey(keyData);
        }
        return key;
    }

    /**
     * Returns a SmartKey for the row previous to the specified row from a set of ColumnSources.
     *
     * @param groupByColumnSources a set of ColumnSources from which to retrieve the data
     * @param row the row number for which to retrieve the previous row's data
     * @return a Deephaven SmartKey object
     */
    public static Object getPrevKey(ColumnSource<?>[] groupByColumnSources, long row) {
        Object key;
        if (groupByColumnSources.length == 0) {
            return SmartKey.EMPTY;
        } else if (groupByColumnSources.length == 1) {
            key = C14nUtil.maybeCanonicalize(groupByColumnSources[0].getPrev(row));
        } else {
            Object[] keyData = new Object[groupByColumnSources.length];
            for (int col = 0; col < groupByColumnSources.length; col++) {
                keyData[col] = groupByColumnSources[col].getPrev(row);
            }
            key = C14nUtil.makeSmartKey(keyData);
        }
        return key;
    }

    /**
     * Returns a ColumnHolder that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @param <T> the type of the column
     * @return a Deephaven ColumnHolder object
     */
    public static <T> ColumnHolder col(String name, T... data) {
        if (data.getClass().getComponentType() == Long.class) {
            return longCol(name, ArrayUtils.getUnboxedArray((Long[]) data));
        } else if (data.getClass().getComponentType() == Integer.class) {
            return intCol(name, ArrayUtils.getUnboxedArray((Integer[]) data));
        } else if (data.getClass().getComponentType() == Short.class) {
            return shortCol(name, ArrayUtils.getUnboxedArray((Short[]) data));
        } else if (data.getClass().getComponentType() == Byte.class) {
            return byteCol(name, ArrayUtils.getUnboxedArray((Byte[]) data));
        } else if (data.getClass().getComponentType() == Character.class) {
            return charCol(name, ArrayUtils.getUnboxedArray((Character[]) data));
        } else if (data.getClass().getComponentType() == Double.class) {
            return doubleCol(name, ArrayUtils.getUnboxedArray((Double[]) data));
        } else if (data.getClass().getComponentType() == Float.class) {
            return floatCol(name, ArrayUtils.getUnboxedArray((Float[]) data));
        }
        // noinspection unchecked
        return new ColumnHolder(name, data.getClass().getComponentType(),
                data.getClass().getComponentType().getComponentType(), false, data);
    }

    /**
     * Returns a ColumnHolder of type String that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder stringCol(String name, String... data) {
        // NB: IntelliJ says that we do not need to cast data, but javac warns about this statement otherwise
        // noinspection RedundantCast
        return new ColumnHolder(name, String.class, null, false, (Object[]) data);
    }

    /**
     * Returns a ColumnHolder of type DBDateTime that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder dateTimeCol(String name, DBDateTime... data) {
        // NB: IntelliJ says that we do not need to cast data, but javac warns about this statement otherwise
        // noinspection RedundantCast
        return new ColumnHolder(name, DBDateTime.class, null, false, (Object[]) data);
    }

    /**
     * Returns a ColumnHolder of type long that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder longCol(String name, long... data) {
        return new ColumnHolder(name, false, data);
    }

    /**
     * Returns a ColumnHolder of type int that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder intCol(String name, int... data) {
        return new ColumnHolder(name, false, data);
    }

    /**
     * Returns a ColumnHolder of type short that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder shortCol(String name, short... data) {
        return new ColumnHolder(name, false, data);
    }

    /**
     * Returns a ColumnHolder of type byte that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder byteCol(String name, byte... data) {
        return new ColumnHolder(name, false, data);
    }

    /**
     * Returns a ColumnHolder of type char that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder charCol(String name, char... data) {
        return new ColumnHolder(name, false, data);
    }

    /**
     * Returns a ColumnHolder of type double that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder doubleCol(String name, double... data) {
        return new ColumnHolder(name, false, data);
    }

    /**
     * Returns a ColumnHolder of type float that can be used when creating in-memory tables.
     *
     * @param name name of the column
     * @param data a list of values for the column
     * @return a Deephaven ColumnHolder object
     */
    public static ColumnHolder floatCol(String name, float... data) {
        return new ColumnHolder(name, false, data);
    }

    /////////// Utilities For Creating Tables /////////////////

    /**
     * Returns a new, empty Deephaven Table.
     *
     * @param size the number of rows to allocate space for
     * @return a Deephaven Table with no columns.
     */
    public static Table emptyTable(long size) {
        return new QueryTable(Index.FACTORY.getFlatIndex(size), Collections.emptyMap());
    }

    @SuppressWarnings("SameParameterValue")
    private static <MT extends Map<KT, VT>, KT, VT> MT newMapFromLists(Class<MT> mapClass, List<KT> keys,
            List<VT> values) {
        Require.eq(keys.size(), "keys.size()", values.size(), "values.size()");
        MT result;
        try {
            result = mapClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < keys.size(); ++i) {
            result.put(keys.get(i), values.get(i));
        }
        return result;
    }

    /**
     * Creates a new DynamicTable.
     *
     * @param size the number of rows to allocate
     * @param names a List of column names
     * @param columnSources a List of the ColumnSource(s)
     * @return a Deephaven DynamicTable
     */
    public static DynamicTable newTable(long size, List<String> names, List<ColumnSource<?>> columnSources) {
        // noinspection unchecked
        return new QueryTable(Index.FACTORY.getFlatIndex(size),
                newMapFromLists(LinkedHashMap.class, names, columnSources));
    }

    /**
     * Creates a new DynamicTable.
     *
     * @param size the number of rows to allocate
     * @param columns a Map of column names and ColumnSources
     * @return a Deephaven DynamicTable
     */
    public static DynamicTable newTable(long size, Map<String, ColumnSource<?>> columns) {
        return new QueryTable(Index.FACTORY.getFlatIndex(size), columns);
    }

    /**
     * Creates a new DynamicTable.
     *
     * @param definition the TableDefinition (column names and properties) to use for the new table
     * @return an empty Deephaven DynamicTable object
     */
    public static DynamicTable newTable(TableDefinition definition) {
        Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        for (ColumnDefinition<?> columnDefinition : definition.getColumnList()) {
            columns.put(columnDefinition.getName(), ArrayBackedColumnSource.getMemoryColumnSource(0,
                    columnDefinition.getDataType(), columnDefinition.getComponentType()));
        }
        return new QueryTable(definition, Index.FACTORY.getEmptyIndex(), columns);
    }

    /**
     * Creates a new DynamicTable.
     *
     * @param columnHolders a list of ColumnHolders from which to create the table
     * @return a Deephaven DynamicTable
     */
    public static DynamicTable newTable(ColumnHolder... columnHolders) {
        checkSizes(columnHolders);
        Index index = getIndex(columnHolders);
        Map<String, ColumnSource<?>> columns = Stream.of(columnHolders).collect(COLUMN_HOLDER_LINKEDMAP_COLLECTOR);
        return new QueryTable(index, columns);
    }

    public static DynamicTable newTable(TableDefinition definition, ColumnHolder... columnHolders) {
        checkSizes(columnHolders);
        Index index = getIndex(columnHolders);
        Map<String, ColumnSource<?>> columns = Stream.of(columnHolders).collect(COLUMN_HOLDER_LINKEDMAP_COLLECTOR);
        return new QueryTable(definition, index, columns);
    }

    private static void checkSizes(ColumnHolder[] columnHolders) {
        int[] sizes = Arrays.stream(columnHolders)
                .mapToInt(x -> x.data == null ? 0 : Array.getLength(x.data))
                .toArray();
        if (Arrays.stream(sizes).anyMatch(size -> size != sizes[0])) {
            throw new IllegalArgumentException(
                    "All columns must have the same number of rows, but sizes are: " + Arrays.toString(sizes));
        }
    }

    private static Index getIndex(ColumnHolder[] columnHolders) {
        return columnHolders.length == 0 ? Index.FACTORY.getEmptyIndex()
                : Index.FACTORY.getFlatIndex(Array.getLength(columnHolders[0].data));
    }

    // region Time tables

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param period time interval between new row additions.
     * @return time table
     */
    public static Table timeTable(String period) {
        return timeTable(period, (ReplayerInterface) null);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param period time interval between new row additions
     * @param replayer data replayer
     * @return time table
     */
    public static Table timeTable(String period, ReplayerInterface replayer) {
        final long periodValue = DBTimeUtils.expressionToNanos(period);
        return timeTable(periodValue, replayer);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param startTime start time for adding new rows
     * @param period time interval between new row additions
     * @return time table
     */
    public static Table timeTable(DBDateTime startTime, String period) {
        final long periodValue = DBTimeUtils.expressionToNanos(period);
        return timeTable(startTime, periodValue);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param startTime start time for adding new rows
     * @param period time interval between new row additions
     * @param replayer data replayer
     * @return time table
     */
    public static Table timeTable(DBDateTime startTime, String period, ReplayerInterface replayer) {
        final long periodValue = DBTimeUtils.expressionToNanos(period);
        return timeTable(startTime, periodValue, replayer);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param startTime start time for adding new rows
     * @param period time interval between new row additions
     * @return time table
     */
    public static Table timeTable(String startTime, String period) {
        return timeTable(DBTimeUtils.convertDateTime(startTime), period);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param startTime start time for adding new rows
     * @param period time interval between new row additions
     * @param replayer data replayer
     * @return time table
     */
    public static Table timeTable(String startTime, String period, ReplayerInterface replayer) {
        return timeTable(DBTimeUtils.convertDateTime(startTime), period, replayer);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param periodNanos time interval between new row additions in nanoseconds.
     * @return time table
     */
    public static Table timeTable(long periodNanos) {
        return timeTable(periodNanos, null);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param periodNanos time interval between new row additions in nanoseconds.
     * @param replayer data replayer
     * @return time table
     */
    public static Table timeTable(long periodNanos, ReplayerInterface replayer) {
        final TimeTable timeTable = new TimeTable(Replayer.getTimeProvider(replayer), null, periodNanos);
        LiveTableMonitor.DEFAULT.addTable(timeTable);
        return timeTable;
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param startTime start time for adding new rows
     * @param periodNanos time interval between new row additions in nanoseconds.
     * @return time table
     */
    public static Table timeTable(DBDateTime startTime, long periodNanos) {
        final TimeTable timeTable = new TimeTable(Replayer.getTimeProvider(null), startTime, periodNanos);
        LiveTableMonitor.DEFAULT.addTable(timeTable);
        return timeTable;
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param startTime start time for adding new rows
     * @param periodNanos time interval between new row additions in nanoseconds.
     * @param replayer data replayer
     * @return time table
     */
    public static Table timeTable(DBDateTime startTime, long periodNanos, ReplayerInterface replayer) {
        final TimeTable timeTable = new TimeTable(Replayer.getTimeProvider(replayer), startTime, periodNanos);
        LiveTableMonitor.DEFAULT.addTable(timeTable);
        return timeTable;
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param startTime start time for adding new rows
     * @param periodNanos time interval between new row additions in nanoseconds.
     * @return time table
     */
    public static Table timeTable(String startTime, long periodNanos) {
        return timeTable(DBTimeUtils.convertDateTime(startTime), periodNanos);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param startTime start time for adding new rows
     * @param periodNanos time interval between new row additions in nanoseconds.
     * @param replayer data replayer
     * @return time table
     */
    public static Table timeTable(String startTime, long periodNanos, ReplayerInterface replayer) {
        return timeTable(DBTimeUtils.convertDateTime(startTime), periodNanos, replayer);
    }

    /**
     * Creates a table that adds a new row on a regular interval.
     *
     * @param timeProvider the time provider
     * @param startTime start time for adding new rows
     * @param periodNanos time interval between new row additions in nanoseconds.
     * @return time table
     */
    public static Table timeTable(TimeProvider timeProvider, DBDateTime startTime, long periodNanos) {
        final TimeTable timeTable = new TimeTable(timeProvider, startTime, periodNanos);
        LiveTableMonitor.DEFAULT.addTable(timeTable);
        return timeTable;
    }

    // endregion time tables

    /////////// Utilities For Merging Tables /////////////////

    /**
     * Concatenates multiple Deephaven Tables into a single Table.
     *
     * <p>
     * The resultant table will have rows from the same table together, in the order they are specified as inputs.
     * </p>
     *
     * <p>
     * When ticking tables grow, they may run out of the 'pre-allocated' space for newly added rows. When more key-
     * space is needed, tables in higher key-space are shifted to yet higher key-space to make room for new rows. Shifts
     * are handled efficiently, but some downstream operations generate a linear O(n) amount of work per shifted row.
     * When possible, one should favor ordering the constituent tables first by static/non-ticking sources followed by
     * tables that are expected to grow at slower rates, and finally by tables that grow without bound.
     * </p>
     *
     * @param theList a List of Tables to be concatenated
     * @return a Deephaven table object
     */
    public static Table merge(List<Table> theList) {
        return merge(theList.toArray(Table.ZERO_LENGTH_TABLE_ARRAY));
    }

    /**
     * Concatenates multiple Deephaven Tables into a single Table.
     *
     * <p>
     * The resultant table will have rows from the same table together, in the order they are specified as inputs.
     * </p>
     *
     * <p>
     * When ticking tables grow, they may run out of the 'pre-allocated' space for newly added rows. When more key-
     * space is needed, tables in higher key-space are shifted to yet higher key-space to make room for new rows. Shifts
     * are handled efficiently, but some downstream operations generate a linear O(n) amount of work per shifted row.
     * When possible, one should favor ordering the constituent tables first by static/non-ticking sources followed by
     * tables that are expected to grow at slower rates, and finally by tables that grow without bound.
     * </p>
     *
     * @param tables a Collection of Tables to be concatenated
     * @return a Deephaven table object
     */
    public static Table merge(Collection<Table> tables) {
        return merge(tables.toArray(Table.ZERO_LENGTH_TABLE_ARRAY));
    }

    /**
     * Concatenates multiple Deephaven Tables into a single Table.
     *
     * <p>
     * The resultant table will have rows from the same table together, in the order they are specified as inputs.
     * </p>
     *
     * <p>
     * When ticking tables grow, they may run out of the 'pre-allocated' space for newly added rows. When more key-
     * space is needed, tables in higher key-space are shifted to yet higher key-space to make room for new rows. Shifts
     * are handled efficiently, but some downstream operations generate a linear O(n) amount of work per shifted row.
     * When possible, one should favor ordering the constituent tables first by static/non-ticking sources followed by
     * tables that are expected to grow at slower rates, and finally by tables that grow without bound.
     * </p>
     *
     * @param tables a list of Tables to be concatenated
     * @return a Deephaven table object
     */
    public static Table merge(Table... tables) {
        return QueryPerformanceRecorder.withNugget("merge", () -> {
            // TODO (deephaven/deephaven-core/issues/257): When we have a new Table proxy implementation, we should
            // reintroduce remote merge for proxies.
            // If all of the tables are proxies, then we should ship this request over rather than trying to do it
            // locally.
            // Table proxyMerge = io.deephaven.db.tables.utils.TableTools.mergeByProxy(tables);
            // if (proxyMerge != null) {
            // return proxyMerge;
            // }

            final List<Table> tableList = TableToolsMergeHelper.getTablesToMerge(Arrays.stream(tables), tables.length);
            if (tableList == null || tableList.isEmpty()) {
                throw new IllegalArgumentException("no tables provided to merge");
            }

            return TableToolsMergeHelper.mergeInternal(tableList.get(0).getDefinition(), tableList, null);
        });
    }

    /**
     * Concatenates multiple sorted Deephaven Tables into a single Table sorted by the specified key column.
     * <p>
     * The input tables must each individually be sorted by keyColumn, otherwise results are undefined.
     *
     * @param tables sorted Tables to be concatenated
     * @param keyColumn the column to use when sorting the concatenated results
     * @return a Deephaven table object
     */
    public static Table mergeSorted(@SuppressWarnings("SameParameterValue") String keyColumn, Table... tables) {
        return mergeSorted(keyColumn, Arrays.asList(tables));
    }

    /**
     * Concatenates multiple sorted Deephaven Tables into a single Table sorted by the specified key column.
     * <p>
     * The input tables must each individually be sorted by keyColumn, otherwise results are undefined.
     *
     * @param tables a Collection of sorted Tables to be concatenated
     * @param keyColumn the column to use when sorting the concatenated results
     * @return a Deephaven table object
     */
    public static Table mergeSorted(String keyColumn, Collection<Table> tables) {
        return MergeSortedHelper.mergeSortedHelper(keyColumn, tables);
    }

    /////////// Other Utilities /////////////////

    /**
     * Produce a new table with all the columns of this table, in the same order, but with {@code double} and
     * {@code float} columns rounded to {@code long}s.
     *
     * @return The new {@code Table}, with all {@code double} and {@code float} columns rounded to {@code long}s.
     */
    @ScriptApi
    public static Table roundDecimalColumns(Table table) {
        Set<String> columnsToRound = new HashSet<>(table.getColumns().length);
        for (ColumnDefinition<?> columnDefinition : table.getDefinition().getColumns()) {
            Class<?> type = columnDefinition.getDataType();
            if (type.equals(double.class) || type.equals(float.class)) {
                columnsToRound.add(columnDefinition.getName());
            }
        }
        return roundDecimalColumns(table, columnsToRound.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    /**
     * Produce a new table with all the columns of this table, in the same order, but with all {@code double} and
     * {@code float} columns rounded to {@code long}s, except for the specified {@code columnsNotToRound}.
     *
     * @param columnsNotToRound The names of the {@code double} and {@code float} columns <i>not</i> to round to
     *        {@code long}s
     * @return The new {@code Table}, with columns modified as explained above
     */
    @ScriptApi
    public static Table roundDecimalColumnsExcept(Table table, String... columnsNotToRound) {
        Set<String> columnsNotToRoundSet = new HashSet<>(columnsNotToRound.length * 2);
        Collections.addAll(columnsNotToRoundSet, columnsNotToRound);

        Set<String> columnsToRound = new HashSet<>(table.getColumns().length);
        for (ColumnDefinition<?> columnDefinition : table.getDefinition().getColumns()) {
            Class<?> type = columnDefinition.getDataType();
            String colName = columnDefinition.getName();
            if ((type.equals(double.class) || type.equals(float.class)) && !columnsNotToRoundSet.contains(colName)) {
                columnsToRound.add(colName);
            }
        }
        return roundDecimalColumns(table, columnsToRound.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    /**
     * Produce a new table with all the columns of this table, in the same order, but with {@code double} and
     * {@code float} columns rounded to {@code long}s.
     *
     * @param columns The names of the {@code double} and {@code float} columns to round.
     * @return The new {@code Table}, with the specified columns rounded to {@code long}s.
     * @throws java.lang.IllegalArgumentException If {@code columns} is null, or if one of the specified {@code columns}
     *         is neither a {@code double} column nor a {@code float} column.
     */
    @ScriptApi
    public static Table roundDecimalColumns(Table table, String... columns) {
        if (columns == null) {
            throw new IllegalArgumentException("columns cannot be null");
        }
        List<String> updateDescriptions = new LinkedList<>();
        for (String colName : columns) {
            Class<?> colType = table.getColumn(colName).getType();
            if (!(colType.equals(double.class) || colType.equals(float.class)))
                throw new IllegalArgumentException("Column \"" + colName + "\" is not a decimal column!");
            updateDescriptions.add(colName + "=round(" + colName + ')');
        }
        return table.updateView(updateDescriptions.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    /**
     * <p>
     * Compute the SHA256 hash of the input table.
     * </p>
     * <p>
     * The hash is computed using every value in each row, using toString for unrecognized objects. The hash also
     * includes the input table definition column names and types.
     * </p>
     *
     * @param source The table to fingerprint
     * @return The SHA256 hash of the table data and {@link TableDefinition}
     * @throws IOException If an error occurs during the hashing.
     */
    public static byte[] computeFingerprint(Table source) throws IOException {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "Runtime does not suport SHA-256 hashing required for resultsTable fingerprints.", e);
        }

        final DataOutputStream osw = new DataOutputStream(new DigestOutputStream(new NullOutputStream(), md));

        for (final ColumnSource<?> col : source.getColumnSourceMap().values()) {
            processColumnForFingerprint(source.getIndex(), col, osw);
        }

        // Now add in the Table definition
        final TableDefinition def = source.getDefinition();
        for (final ColumnDefinition<?> cd : def.getColumnList()) {
            osw.writeChars(cd.getName());
            osw.writeChars(cd.getDataType().getName());
        }

        return md.digest();
    }

    /**
     * <p>
     * Compute the SHA256 hash of the input table and return it in base64 string format.
     * </p>
     *
     * @param source The table to fingerprint
     * @return The SHA256 hash of the table data and {@link TableDefinition}
     * @throws IOException If an error occurs during the hashing.
     */
    public static String base64Fingerprint(Table source) throws IOException {
        return Base64.getEncoder().encodeToString(computeFingerprint(source));
    }

    private static void processColumnForFingerprint(OrderedKeys ok, ColumnSource<?> col, DataOutputStream outputStream)
            throws IOException {
        if (col.getType() == DBDateTime.class) {
            col = ReinterpretUtilities.dateTimeToLongSource(col);
        }

        final int chunkSize = 1 << 16;

        final ChunkType chunkType = col.getChunkType();
        switch (chunkType) {
            case Char:
                try (final ColumnSource.GetContext getContext = col.makeGetContext(chunkSize);
                        final OrderedKeys.Iterator okit = ok.getOrderedKeysIterator()) {
                    while (okit.hasMore()) {
                        final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                        final CharChunk<? extends Values> valuesChunk = col.getChunk(getContext, chunkOk).asCharChunk();
                        for (int ii = 0; ii < valuesChunk.size(); ++ii) {
                            outputStream.writeChar(valuesChunk.get(ii));
                        }
                    }
                }
                break;
            case Byte:
                try (final ColumnSource.GetContext getContext = col.makeGetContext(chunkSize);
                        final OrderedKeys.Iterator okit = ok.getOrderedKeysIterator()) {
                    while (okit.hasMore()) {
                        final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                        final ByteChunk<? extends Values> valuesChunk = col.getChunk(getContext, chunkOk).asByteChunk();
                        for (int ii = 0; ii < valuesChunk.size(); ++ii) {
                            outputStream.writeByte(valuesChunk.get(ii));
                        }
                    }
                }
                break;
            case Short:
                try (final ColumnSource.GetContext getContext = col.makeGetContext(chunkSize);
                        final OrderedKeys.Iterator okit = ok.getOrderedKeysIterator()) {
                    while (okit.hasMore()) {
                        final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                        final ShortChunk<? extends Values> valuesChunk =
                                col.getChunk(getContext, chunkOk).asShortChunk();
                        for (int ii = 0; ii < valuesChunk.size(); ++ii) {
                            outputStream.writeShort(valuesChunk.get(ii));
                        }
                    }
                }
                break;
            case Int:
                try (final ColumnSource.GetContext getContext = col.makeGetContext(chunkSize);
                        final OrderedKeys.Iterator okit = ok.getOrderedKeysIterator()) {
                    while (okit.hasMore()) {
                        final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                        final IntChunk<? extends Values> valuesChunk = col.getChunk(getContext, chunkOk).asIntChunk();
                        for (int ii = 0; ii < valuesChunk.size(); ++ii) {
                            outputStream.writeInt(valuesChunk.get(ii));
                        }
                    }
                }
                break;
            case Long:
                try (final ColumnSource.GetContext getContext = col.makeGetContext(chunkSize);
                        final OrderedKeys.Iterator okit = ok.getOrderedKeysIterator()) {
                    while (okit.hasMore()) {
                        final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                        final LongChunk<? extends Values> valuesChunk = col.getChunk(getContext, chunkOk).asLongChunk();
                        for (int ii = 0; ii < valuesChunk.size(); ++ii) {
                            outputStream.writeLong(valuesChunk.get(ii));
                        }
                    }
                }
                break;
            case Float:
                try (final ColumnSource.GetContext getContext = col.makeGetContext(chunkSize);
                        final OrderedKeys.Iterator okit = ok.getOrderedKeysIterator()) {
                    while (okit.hasMore()) {
                        final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                        final FloatChunk<? extends Values> valuesChunk =
                                col.getChunk(getContext, chunkOk).asFloatChunk();
                        for (int ii = 0; ii < valuesChunk.size(); ++ii) {
                            outputStream.writeFloat(valuesChunk.get(ii));
                        }
                    }
                }
                break;
            case Double:
                try (final ColumnSource.GetContext getContext = col.makeGetContext(chunkSize);
                        final OrderedKeys.Iterator okit = ok.getOrderedKeysIterator()) {
                    while (okit.hasMore()) {
                        final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                        final DoubleChunk<? extends Values> valuesChunk =
                                col.getChunk(getContext, chunkOk).asDoubleChunk();
                        for (int ii = 0; ii < valuesChunk.size(); ++ii) {
                            outputStream.writeDouble(valuesChunk.get(ii));
                        }
                    }
                }
                break;
            case Object:
                try (final ColumnSource.GetContext getContext = col.makeGetContext(chunkSize);
                        final OrderedKeys.Iterator okit = ok.getOrderedKeysIterator()) {
                    while (okit.hasMore()) {
                        final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);
                        final ObjectChunk<?, ? extends Values> valuesChunk =
                                col.getChunk(getContext, chunkOk).asObjectChunk();
                        for (int ii = 0; ii < valuesChunk.size(); ++ii) {
                            outputStream.writeChars(Objects.toString(valuesChunk.get(ii).toString()));
                        }
                    }
                }
                break;
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    public static String nullTypeAsString(final Class<?> dataType) {
        if (dataType == int.class) {
            return "NULL_INT";
        }
        if (dataType == long.class) {
            return "NULL_LONG";
        }
        if (dataType == char.class) {
            return "NULL_CHAR";
        }
        if (dataType == double.class) {
            return "NULL_DOUBLE";
        }
        if (dataType == float.class) {
            return "NULL_FLOAT";
        }
        if (dataType == short.class) {
            return "NULL_SHORT";
        }
        if (dataType == byte.class) {
            return "NULL_BYTE";
        }
        return "(" + dataType.getName() + ")" + " null";
    }

    public static Class<?> typeFromName(final String dataTypeStr) {
        final Class<?> dataType;
        try {
            dataType = ClassUtil.lookupClass(dataTypeStr);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Type " + dataTypeStr + " not known", e);
        }
        return dataType;
    }
}
