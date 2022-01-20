package io.deephaven.csv.reading;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.densestorage.DenseStorageWriter;
import io.deephaven.csv.parsers.Parser;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.parsers.Parsers;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.tokenization.Tokenizer;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.Renderer;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;

import java.io.InputStream;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A class for reading CSV data. Typical usage is:
 * <ol>
 * <li>Construct a CsvReader.</li>
 * <li>Customize the CsvReader by calling the various setXXX methods.</li>
 * <li>Arrange for the input text to be in a {@link Reader}.</li>
 * <li>Prepare a {@link SinkFactory} which can in turn provide Sink&lt;T&gt; objects for the output data.</li>
 * <li>Call the {@link #read} method.</li>
 * </ol>
 * Furthermore the setXXX methods can be used in a builder pattern. Example:
 * 
 * <pre>
 * final CsvReader csvr = new CsvReader()
 *   .setQuoteChar('#')
 *   .setAsync(false)
 *   .setParserFor("Timestamp", Parsers.DATETIME);
 * final Reader r = ...;
 * final SinkFactory f = ...;
 * final CsvReader.Result res = csvr.read(r, f);
 * </pre>
 */
public final class CsvReader {
    /**
     * Whether to trim leading and trailing blanks from non-quoted values.
     */
    private boolean ignoreSurroundingSpaces = false;
    /**
     * Whether to trim leading and trailing blanks from inside quoted values.
     */
    private boolean trim = false;
    /**
     * Whether the incoming data has column headers.
     */
    private boolean hasHeaders = true;
    /**
     * The quote character (used when you want field or line delimiters to be interpreted as literal text. For example:
     * 
     * <pre>
     * 123,"hello, there",456,
     * </pre>
     * 
     * Would be read as the three fields:
     * <ul>
     * <li>123</li>
     * <li>hello, there</li>
     * <li>456</li>
     * </ul>
     */
    private byte quoteChar = '"';
    /**
     * The field delimiter (the character that separates one column from the next.
     */
    private byte fieldDelimiter = ',';
    /**
     * Whether to run concurrently. In particular, the operation of reading the raw file, breaking it into columns, and
     * storing that column text in memory can run in parallel with parsing the data for a given column, and all the
     * column data parsers can themselves run in parallel.
     */
    private boolean concurrent = true;
    /**
     * The user-defined set of parsers that participate in type inference. Defaults to Parsers.DEFAULT
     */
    private List<Parser<?>> parsers = new ArrayList<>(Parsers.DEFAULT);
    /**
     * Client-specified headers that can be used to override the existing headers in the input (if hasHeaders is true),
     * or to provide absent headers (if hasHeaders is false).
     */
    private List<String> clientSpecifiedHeaders = new ArrayList<>();
    /**
     * Override a specific column header by number. This is applied *after* clientSpecifiedHeaders. Column numbers start
     * with 1.
     */
    private final Map<Integer, String> columnHeaderOverrides = new HashMap<>();
    /**
     * Used to force a specific parser for a specific column, specified by column name.
     */
    private final Map<String, Parser<?>> parsersByColumnName = new HashMap<>();
    /**
     * Used to force a specific parser for a specific column, specified by column number. Column numbers start with 1.
     */
    private final Map<Integer, Parser<?>> parsersByColumnNumber = new HashMap<>();
    /**
     * The default string that means "null value" in the input. It is used if not overridden on a per-column basis. It
     * defaults to the empty string.
     */
    private String nullValueLiteral = "";
    /**
     * Used to force a specific parser for a specific column, specified by column name.
     */
    private final Map<String, String> nullValueLiteralByColumnName = new HashMap<>();
    /**
     * Used to force a specific parser for a specific column, specified by column number. Column numbers start with 1.
     */
    private final Map<Integer, String> nullValueLiteralByColumnNumber = new HashMap<>();
    /**
     * The parser to be used when a column is entirely null (unless a specific parser has been forced by setting an
     * entry in the parsers collection.
     */
    private Parser<?> nullParser;
    /**
     * An optional low-level parser that understands custom time zones.
     */
    private Tokenizer.CustomTimeZoneParser customTimeZoneParser;
    /**
     * An optional validator for column headers.
     */
    private Predicate<String> headerValidator = s -> true;
    /**
     * An optional legalizer for column headers.
     */
    private Function<String[], String[]> headerLegalizer = Function.identity();

    /**
     * Read the data.
     * 
     * @param stream The input data, encoded in UTF-8.
     * @param sinkFactory A factory that can provide Sink&lt;T&gt; of all appropriate types for the output data. Once
     *        the CsvReader determines what the column type is, t will use the SinkFactory to create an appropriate
     *        Sink&lt;T&gt; for the type. Note that the CsvReader might guess wrong, so it might create a Sink,
     *        partially populate it, and then abandon it. The final set of fully-populated Sinks will be returned in in
     *        the CsvReader.Result.
     * @return A CsvReader.Result containing the column names, the number of columns, and the final set of
     *         fully-populated Sinks.
     */
    public Result read(final InputStream stream, final SinkFactory sinkFactory) throws CsvReaderException {
        final CellGrabber grabber = new CellGrabber(stream, quoteChar, fieldDelimiter, ignoreSurroundingSpaces, trim);
        // For an "out" parameter
        final MutableObject<byte[][]> firstDataRowHolder = new MutableObject<>();
        final String[] headersTemp = determineHeadersToUse(grabber, firstDataRowHolder);
        final byte[][] firstDataRow = firstDataRowHolder.getValue();
        final int numInputCols = headersTemp.length;

        // If the final column has a blank header, we assume the whole column is blank (we confirm this assumption
        // in ParseInputToDenseStorage, as we're reading the file.
        final String[] headersTemp2;
        if (numInputCols != 0 && headersTemp[numInputCols - 1].isEmpty()) {
            headersTemp2 = Arrays.copyOf(headersTemp, numInputCols - 1);
        } else {
            headersTemp2 = headersTemp;
        }
        final int numOutputCols = headersTemp2.length;
        final String[] headersToUse = canonicalizeHeaders(headersTemp2);

        // Create a DenseStorageWriter and two readers for each column.
        final DenseStorageWriter[] dsws = new DenseStorageWriter[numInputCols];
        final DenseStorageReader[] dsr0s = new DenseStorageReader[numInputCols];
        final DenseStorageReader[] dsr1s = new DenseStorageReader[numInputCols];
        // The arrays are sized to "numInputCols" but only populated up to "numOutputCols".
        // The code in ParseInputToDenseStorge knows that a null DenseStorageWriter means that the column
        // is all-empty and (once the data is confirmed to be empty) just drop the data.
        for (int ii = 0; ii < numOutputCols; ++ii) {
            final DenseStorageWriter dsw = new DenseStorageWriter();
            dsws[ii] = dsw;
            dsr0s[ii] = dsw.newReader();
            dsr1s[ii] = dsw.newReader();
        }

        // Select an Excecutor based on whether the user wants the code to run asynchronously
        // or not.
        final ExecutorService exec =
                concurrent ? Executors.newFixedThreadPool(numOutputCols + 1) : Executors.newSingleThreadExecutor();

        final Future<Long> numRowsFuture = exec.submit(
                () -> ParseInputToDenseStorage.doit(firstDataRow, nullValueLiteral, grabber, dsws));


        final ArrayList<Future<Sink<?>>> sinkFutures = new ArrayList<>();

        for (int ii = 0; ii < numOutputCols; ++ii) {
            final List<Parser<?>> parsersToUse = calcParsersToUse(headersToUse[ii], ii + 1);
            final String nullValueLiteralToUse = calcNullValueLiteralToUse(headersToUse[ii], ii + 1);

            final int iiCopy = ii;
            final Future<Sink<?>> fcb = exec.submit(
                    () -> ParseDenseStorageToColumn.doit(dsr0s[iiCopy], dsr1s[iiCopy],
                            parsersToUse, nullParser, customTimeZoneParser,
                            nullValueLiteralToUse, sinkFactory));
            sinkFutures.add(fcb);
        }

        final long numRows;
        final Sink<?>[] sinks = new Sink<?>[numOutputCols];
        try {
            numRows = numRowsFuture.get();
            for (int ii = 0; ii < numOutputCols; ++ii) {
                sinks[ii] = sinkFutures.get(ii).get();
            }
        } catch (Exception inner) {
            throw new CsvReaderException("Caught exception", inner);
        }

        return new Result(numRows, headersToUse, sinks);
    }

    /**
     * Determine which list of parsers to use for type inference. Returns {@link #parsers} unless the user has set an
     * override on a column name or column number basis.
     */
    private List<Parser<?>> calcParsersToUse(final String columnName, final int oneBasedColumnNumber) {
        Parser<?> specifiedParser = parsersByColumnName.get(columnName);
        if (specifiedParser != null) {
            return List.of(specifiedParser);
        }
        specifiedParser = parsersByColumnNumber.get(oneBasedColumnNumber);
        if (specifiedParser != null) {
            return List.of(specifiedParser);
        }
        return parsers;
    }

    /**
     * Determine which null value literal to use. Returns {@link #nullValueLiteral} unless the user has set an override
     * on a column name or column number basis.
     */
    private String calcNullValueLiteralToUse(final String columnName, final int oneBasedColumnNumber) {
        String result = nullValueLiteralByColumnName.get(columnName);
        if (result != null) {
            return result;
        }
        result = nullValueLiteralByColumnNumber.get(oneBasedColumnNumber);
        if (result != null) {
            return result;
        }
        return nullValueLiteral;
    }

    /**
     * Determine which headers to use. The result comes from either the first row of the file or the user-specified
     * overrides.
     */
    private String[] determineHeadersToUse(final CellGrabber grabber, final MutableObject<byte[][]> firstDataRowHolder)
            throws CsvReaderException {
        String[] headersToUse = null;
        if (hasHeaders) {
            final byte[][] firstRow = tryReadOneRow(grabber);
            if (firstRow == null) {
                throw new CsvReaderException("Can't proceed because hasHeaders is set but input file is empty");
            }
            headersToUse = Arrays.stream(firstRow).map(String::new).toArray(String[]::new);
        }

        // Whether or not the input had headers, maybe override with client-specified headers.
        if (clientSpecifiedHeaders.size() != 0) {
            headersToUse = clientSpecifiedHeaders.toArray(new String[0]);
        }

        // If we still have nothing, try generate synthetic column headers (works only if the file is non-empty,
        // because we need to infer the column count).
        final byte[][] firstDataRow;
        if (headersToUse == null) {
            firstDataRow = tryReadOneRow(grabber);
            if (firstDataRow == null) {
                throw new CsvReaderException(
                        "Can't proceed because input file is empty and client has not specified headers");
            }
            headersToUse = new String[firstDataRow.length];
            for (int ii = 0; ii < headersToUse.length; ++ii) {
                headersToUse[ii] = "Column" + (ii + 1);
            }
        } else {
            firstDataRow = null;
        }

        // Apply column specific overrides.
        for (Map.Entry<Integer, String> entry : columnHeaderOverrides.entrySet()) {
            headersToUse[entry.getKey() - 1] = entry.getValue();
        }

        firstDataRowHolder.setValue(firstDataRow);
        return headersToUse;
    }

    private String[] canonicalizeHeaders(final String[] headers) throws CsvReaderException {
        final String[] legalized = headerLegalizer.apply(headers);
        final Set<String> unique = new HashSet<>();
        final List<String> repeats = new ArrayList<>();
        final List<String> invalidNames = new ArrayList<>();
        for (String header : legalized) {
            if (!unique.add(header)) {
                repeats.add(header);
            } else if (!headerValidator.test(header)) {
                // Using an "else if" because we only want to run each unique name once through the validator.
                invalidNames.add(header);
            }
        }

        if (repeats.isEmpty() && invalidNames.isEmpty()) {
            return legalized;
        }

        final StringBuilder sb = new StringBuilder("Some column headers are invalid.");
        if (!repeats.isEmpty()) {
            sb.append(" Repeated headers: ");
            sb.append(Renderer.renderList(repeats));
        }
        if (!invalidNames.isEmpty()) {
            sb.append(" Invalid headers: ");
            sb.append(Renderer.renderList(invalidNames));
        }
        throw new CsvReaderException(sb.toString());
    }

    /**
     * Try to read one row from the input. Returns false if the input ends before one row has been read.
     * 
     * @return The first row as a byte[][] or null if the input was exhausted.
     */
    private static byte[][] tryReadOneRow(final CellGrabber grabber) throws CsvReaderException {
        final List<byte[]> headers = new ArrayList<>();

        // Grab the header
        final ByteSlice slice = new ByteSlice();
        final MutableBoolean lastInRow = new MutableBoolean();
        do {
            if (!grabber.grabNext(slice, lastInRow)) {
                return null;
            }
            final byte[] item = new byte[slice.size()];
            slice.copyTo(item, 0);
            headers.add(item);
        } while (!lastInRow.booleanValue());
        return headers.toArray(new byte[0][]);
    }

    /**
     * Sets whether to trim leading and trailing blanks from non-quoted values. This really only matters for columns
     * that are inferred to be of type String. Numeric columns ignore surrounding whitespace regardless of this setting.
     */
    public CsvReader setIgnoreSurroundingSpaces(final boolean value) {
        ignoreSurroundingSpaces = value;
        return this;
    }

    /**
     * Sets whether to trim leading and trailing blanks from inside quoted values. This really only matters for columns
     * that are inferred to be of type String. Numeric columns ignore surrounding whitespace regardless of this setting.
     */
    public CsvReader setTrim(final boolean value) {
        trim = value;
        return this;
    }

    /**
     * Sets whether the first row of the input is column headers.
     */
    public CsvReader setHasHeaders(final boolean value) {
        hasHeaders = value;
        return this;
    }

    /**
     * Sets the field delimiter. Typically the comma or tab character.
     */
    public CsvReader setFieldDelimiter(final char value) {
        if (value > 0x7f) {
            throw new IllegalArgumentException("Field delimiter needs to be a 7-bit ASCII character");
        }
        fieldDelimiter = (byte) value;
        return this;
    }

    /**
     * Sets the quote character. Used by the input when it needs to escape special characters like field or line
     * delimiters. A doubled quote character represents itself. Examples (assuming the quote character is set to '"'):
     * <ul>
     * <li>"Hello, there": the string Hello, there</li>
     * <li>"Hello""there": the string Hello"there</li>
     * <li>"""": the string "</li>
     * </ul>
     */
    public CsvReader setquoteChar(final char value) {
        if (value > 0x7f) {
            throw new IllegalArgumentException("Quote character needs to be a 7-bit ASCII character");
        }
        quoteChar = (byte) value;
        return this;
    }

    /**
     * Whether the reader should run the file tokenizer and column parsing jobs concurrently, using multiple threads.
     * This typically yields better performance.
     */
    public CsvReader setConcurrent(final boolean value) {
        this.concurrent = value;
        return this;
    }

    /**
     * Set the list of parsers participating in type inference.
     */
    public CsvReader setParsers(final Collection<Parser<?>> parsers) {
        this.parsers = new ArrayList<>(parsers);
        return this;
    }

    /**
     * Add parsers to the existing list of parsers participating in type inference.
     */
    public CsvReader addParsers(Parser<?>... parsers) {
        this.parsers.addAll(List.of(parsers));
        return this;
    }

    /**
     * Overrides (if hasHeaders is true) or provides (if hasHeaders is false) the column headers.
     */
    public CsvReader setHeaders(final Collection<String> headers) {
        clientSpecifiedHeaders = new ArrayList<>(headers);
        return this;
    }

    /**
     * Overrides (if hasHeaders is true) or provides (if hasHeaders is false) the column headers.
     */
    public CsvReader setHeaders(final String... headers) {
        clientSpecifiedHeaders = List.of(headers);
        return this;
    }

    /**
     * Overrides a specific column header by index. Columns are numbered starting with 1.
     */
    public CsvReader setHeader(final int columnNumber, final String header) {
        columnHeaderOverrides.put(columnNumber, header);
        return this;
    }

    /**
     * Specify a parser for a given column name, rather than using inference to pick a type.
     */
    public CsvReader setParserFor(final String name, final Parser<?> parser) {
        this.parsersByColumnName.put(name, parser);
        return this;
    }

    /**
     * Specify a parser for a given column number, rather than using inference to pick a type. The column numbers are
     * 1-based.
     */
    public CsvReader setParserFor(final int columnNumber, final Parser<?> parser) {
        this.parsersByColumnNumber.put(columnNumber, parser);
        return this;
    }

    /**
     * Specify the default null value literal to be used if not overridden for a column.
     */
    public CsvReader setNullValueLiteral(final String nullValueLiteral) {
        this.nullValueLiteral = nullValueLiteral;
        return this;
    }

    /**
     * Specify the null value literal for a given column name.
     */
    public CsvReader setNullValueLiteralFor(final String name, final String nullValueLiteral) {
        this.nullValueLiteralByColumnName.put(name, nullValueLiteral);
        return this;
    }

    /**
     * Specify a parser for a given column number, rather than using inference to pick a type. The column numbers are
     * 1-based.
     */
    public CsvReader setNullValueLiteralFor(final int columnNumber, final String nullValueLiteral) {
        this.nullValueLiteralByColumnNumber.put(columnNumber, nullValueLiteral);
        return this;
    }

    /**
     * Specify the parser to be used for columns that contain all nulls. (Unless that column has a parser specified by
     * {@link #setParserFor}.
     */
    public CsvReader setNullParser(final Parser<?> nullParser) {
        this.nullParser = nullParser;
        return this;
    }

    /**
     * Specify a plugin to be used to parse custom time zones. This permits the caller to support custom time zones such
     * as the " NY" that appears in "2020-05-05 12:34:56 NY". The first digit (here, space) must be something other than
     * "Z".
     */
    public CsvReader setCustomTimeZoneParser(final Tokenizer.CustomTimeZoneParser customTimeZoneParser) {
        this.customTimeZoneParser = customTimeZoneParser;
        return this;
    }

    public CsvReader setHeaderValidator(final Predicate<String> headerValidator) {
        this.headerValidator = headerValidator;
        return this;
    }

    public CsvReader setHeaderLegalizer(final Function<String[], String[]> headerLegalizer) {
        this.headerLegalizer = headerLegalizer;
        return this;
    }

    /**
     * Result of {@link #read}.
     */
    public static final class Result {
        private final long numRows;
        private final String[] columnNames;
        private final Sink<?>[] columns;

        public Result(final long numRows, final String[] columnNames, final Sink<?>[] columns) {
            this.numRows = numRows;
            this.columnNames = columnNames;
            this.columns = columns;
        }

        /**
         * Number of rows in the input.
         */
        public long numRows() {
            return numRows;
        }

        /**
         * The column names.
         */
        public String[] columnNames() {
            return columnNames;
        }

        /**
         * Data for each column. Each Sink was constructed by some method in the SinkFactory that the caller passed to
         * {@link #read}.
         */
        public Sink<?>[] columns() {
            return columns;
        }

        /**
         * The number of columns.
         */
        public int numCols() {
            return columns.length;
        }
    }
}
