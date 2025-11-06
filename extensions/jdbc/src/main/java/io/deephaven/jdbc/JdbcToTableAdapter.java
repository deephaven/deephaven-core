//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jdbc;

import com.google.common.base.CaseFormat;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ChunkedBackingStoreExposedWritableSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;

/**
 * The JdbcToTableAdapter class provides a simple interface to convert a Java Database Connectivity (JDBC)
 * {@link ResultSet} to a Deephaven {@link Table}.
 *
 * <p>
 * To use, first create a result set using your provided JDBC driver of choice:
 * </p>
 *
 * <pre>
 * Connection connection = DriverManager.getConnection("jdbc:sqlite:/path/to/db.sqlite");
 * Statement statement = connection.createStatement();
 * ResultSet resultSet = statement.executeQuery("SELECT * FROM Invoice");
 * </pre>
 * <p>
 * Then convert the {@code ResultSet} to a {@code Table}:
 *
 * <pre>
 * Table resultTable = JdbcToTableAdapter.readJdbc(resultSet);
 * </pre>
 * <p/>
 * <p>
 * There are several options than can be set to change the behavior of the ingestion. Provide the customized options
 * object to {@link JdbcToTableAdapter#readJdbc(ResultSet, ReadJdbcOptions, String...)} like this:
 *
 * <pre>
 * JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
 * Table resultTable = JdbcToTableAdapter.readJdbc(resultSet, options);
 * </pre>
 * <p>
 * There are many supported mappings from JDBC type to Deephaven type. The default can be overridden by specifying the
 * desired result type in the options. For example, convert BigDecimal to double on 'MyCol' via
 * {@code options.columnTargetType("MyCol", double.class)}.
 */
public class JdbcToTableAdapter {

    /**
     * String formatting styles for use when standardizing externally supplied column names. Casing of the enum members
     * indicates the resultant format. None means no change to the casing of the source string.
     */
    public enum CasingStyle {
        UpperCamel, lowerCamel, UPPERCASE, lowercase, None
    }

    /**
     * Options applicable when reading JDBC data into a Deephaven in-memory table. Designed to constructed in a "fluent"
     * manner, with defaults applied if not specified by the user.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static class ReadJdbcOptions {
        private CasingStyle casingStyle = null;
        private String replacement = "_";
        private int maxRows = -1;
        private boolean strict = true;
        private TimeZone sourceTimeZone = TimeZone.getTimeZone(DateTimeUtils.timeZone());
        private String arrayDelimiter = ",";
        private final Map<String, Class<?>> targetTypeMap = new HashMap<>();

        private ReadJdbcOptions() {}

        /**
         * An option that will convert source JDBC column names to an alternate style using the CasingStyle class. The
         * default is to pass through column names as-is with a minimum of normalization. The source columns must
         * consistently match the expected source format for the to function appropriately. See {@link CasingStyle} for
         * more details.
         *
         * @param casingStyle if not null, CasingStyle to apply to column names - None or null = no change to casing
         * @param replacement character, or empty String, to use for replacements of space or hyphen in source column
         *        names
         * @return customized options object
         */
        public ReadJdbcOptions columnNameFormat(CasingStyle casingStyle, @NotNull String replacement) {
            this.casingStyle = casingStyle;
            this.replacement = replacement;
            return this;
        }

        /**
         * Maximum number of rows to read, defaults to no limit. A number less than zero means no limit. This is useful
         * to read just a sample of a given query into memory, although depending on the JDBC driver used, it may be
         * more efficient to apply a "LIMIT" operation in the query itself.
         *
         * @param maxRows maximum number of rows to read
         * @return customized options object
         */
        public ReadJdbcOptions maxRows(final int maxRows) {
            this.maxRows = maxRows;
            return this;
        }

        /**
         * Whether to apply strict mode when mapping from JDBC to Deephaven; for example throwing an exception if an
         * out-of-range value is encountered instead of truncating. Defaults to true.
         *
         * @param strict use strict mode
         * @return customized options object
         */
        public ReadJdbcOptions strict(final boolean strict) {
            this.strict = strict;
            return this;
        }

        /**
         * Specify the timezone to use when interpreting date-time/timestamp JDBC values. Defaults to the server
         * time-zone, if discoverable. Otherwise, defaults to the local time zone.
         *
         * @param sourceTimeZone the source time zone
         * @return customized options object
         */
        public ReadJdbcOptions sourceTimeZone(@NotNull final TimeZone sourceTimeZone) {
            this.sourceTimeZone = sourceTimeZone;
            return this;
        }

        /**
         * Specify the delimiter to expect when mapping JDBC String columns to Deephaven arrays. Defaults to ",".
         *
         * @param arrayDelimiter the delimiter
         * @return customized options object
         */
        public ReadJdbcOptions arrayDelimiter(@NotNull final String arrayDelimiter) {
            this.arrayDelimiter = arrayDelimiter;
            return this;
        }

        /**
         * Specify the target type for the given column. For columns with multiple possible type mappings, this permits
         * the user to specify which Deephaven type should be used. Any columns for which a type is not specified will
         * receive the default type mapping.
         *
         * @param columnName the column name
         * @param targetType the desired Deephaven column type
         * @return customized options object
         */
        public ReadJdbcOptions columnTargetType(@NotNull final String columnName, @NotNull final Class<?> targetType) {
            targetTypeMap.put(columnName, targetType);
            return this;
        }
    }

    /**
     * Returns a new options object that the user can use to customize a readJdbc operation.
     *
     * @return a new ReadJdbcOptions object
     */
    public static ReadJdbcOptions readJdbcOptions() {
        return new ReadJdbcOptions();
    }

    /**
     * A factory to produce a {@link RowSink} that will consume rows from a {@link ResultSet} as driven by
     * {@link #readJdbc(ResultSet, ReadJdbcOptions, RowSinkFactory)} and produce a result of type {@code RESULT_TYPE}.
     *
     * @param <RESULT_TYPE> The result type produced by {@link RowSink row sinks}
     *        {@link #make(ResultSet, int, ReadJdbcOptions) made} by this factory
     */
    public interface RowSinkFactory<RESULT_TYPE> {
        /**
         * Build and return a {@link RowSink} that will return a result of type {@code RESULT_TYPE}.
         *
         * @param resultSet The {@link ResultSet} that will be consumed
         * @param numRows The number of rows expected to be consumed, 0 if the number is unknown
         * @param options {@link ReadJdbcOptions} that should apply to the returned sink and its result
         * @return The {@link RowSink} to be used to consume {@code resultSet}
         * @throws SQLException If the RowSinkFactory encountered a {@link SQLException} while interacting with the
         *         {@link ResultSet}
         */
        RowSink<RESULT_TYPE> make(
                @NotNull ResultSet resultSet,
                int numRows,
                @NotNull ReadJdbcOptions options)
                throws SQLException;
    }

    /**
     * A sink that {@link #consumeRow() consumes rows} and produces a {@link #result() result} from the data thus
     * consumed.
     *
     * @param <RESULT_TYPE> The result type produced by {@link #result()}
     */
    public interface RowSink<RESULT_TYPE> extends SafeCloseable {

        /**
         * Consume a single row from the {@link ResultSet} provided when constructing this RowSink.
         *
         * @throws SQLException If the RowSink encountered a {@link SQLException} while interacting with the
         *         {@link ResultSet}
         */
        void consumeRow() throws SQLException;

        /**
         * Perform any necessary final work and return the result for this RowSink.
         *
         * @return The result
         */
        RESULT_TYPE result();
    }

    /**
     * Construct a {@link RowSink} for {@code resultSet} and {@code options} using {@code rowSinkFactory},
     * {@link RowSink#consumeRow() consume} all rows from {@code resultSet} (limited by {@code options.maxRows}), and
     * return the {@link RowSink#result() result}, ensuring that the sink is {@link RowSink#close() closed} before
     * return.
     *
     * @param resultSet The {@link ResultSet} that will be consumed
     * @param options {@link ReadJdbcOptions} that should apply to the returned sink and its result
     * @param rowSinkFactory The {@link RowSinkFactory} to be used to consume {@code resultSet}
     * @return The {@link RowSink#result() result}
     * @throws SQLException If a {@link SQLException} was encountered while interacting with the {@link ResultSet}
     * @param <RESULT_TYPE> The result type produced by the sink's {@link RowSink#result()}
     */
    public static <RESULT_TYPE> RESULT_TYPE readJdbc(
            @NotNull final ResultSet resultSet,
            final ReadJdbcOptions options,
            @NotNull final RowSinkFactory<RESULT_TYPE> rowSinkFactory) throws SQLException {
        // Note: JDBC result set cardinality is limited to Integer.MAX_VALUE
        final int maxRows = options.maxRows;
        final int numRows = maxRows < 0
                ? getExpectedSize(resultSet)
                : Math.min(maxRows, getExpectedSize(resultSet));

        try (final RowSink<RESULT_TYPE> rowSink = rowSinkFactory.make(resultSet, numRows, options)) {
            int numRowsConsumed = 0;
            while (resultSet.next() && (maxRows == -1 || numRowsConsumed < maxRows)) {
                rowSink.consumeRow();
                ++numRowsConsumed;
            }
            return rowSink.result();
        }
    }

    /**
     * {@link RowSink} implementation that will return a static, coalesced {@link Table}.
     */
    private static final class TableRowSink implements RowSink<Table> {

        private final ResultSet resultSet;

        private final Map<String, ? extends ColumnSource<?>> columnSources;
        private final SourceFiller[] sourceFillers;
        final JdbcTypeMapper.Context typeMapperContext;

        boolean errorEncountered;
        int numRowsConsumed;

        private TableRowSink(
                @NotNull final ResultSet resultSet,
                final int numRows,
                @NotNull final ReadJdbcOptions options,
                @Nullable final Function<String, String> resultSetColumnNameToTableColumnName,
                @NotNull String... resultSetColumnNames) throws SQLException {
            this.resultSet = resultSet;

            final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            if (resultSetColumnNames.length == 0) {
                resultSetColumnNames = new String[resultSetMetaData.getColumnCount()];
                for (int ii = 0; ii < resultSetColumnNames.length; ++ii) {
                    resultSetColumnNames[ii] = resultSetMetaData.getColumnName(ii + 1);
                }
            }

            final int numColumns = resultSetColumnNames.length;
            final String[] columnNames;
            if (resultSetColumnNameToTableColumnName == null) {
                columnNames = fixColumnNames(options, resultSetColumnNames);
            } else {
                columnNames = Arrays.stream(resultSetColumnNames)
                        .map(resultSetColumnNameToTableColumnName)
                        .toArray(String[]::new);
            }

            final Map<String, WritableColumnSource<?>> columnSources = new LinkedHashMap<>(numColumns);
            final SourceFiller[] sourceFillers = new SourceFiller[numColumns];
            try {
                for (int ci = 0; ci < numColumns; ++ci) {
                    final int columnIndex = resultSet.findColumn(resultSetColumnNames[ci]);
                    final String columnName = columnNames[ci];
                    final Class<?> destinationType = options.targetTypeMap.get(columnName);

                    final JdbcTypeMapper.DataTypeMapping<?> typeMapping =
                            JdbcTypeMapper.getColumnTypeMapping(resultSet, columnIndex, destinationType);

                    final Class<?> deephavenType = typeMapping.getDeephavenType();
                    final Class<?> componentType = deephavenType.getComponentType();
                    final WritableColumnSource<?> columnSource = numRows == 0
                            ? ArrayBackedColumnSource.getMemoryColumnSource(0, deephavenType, componentType)
                            : InMemoryColumnSource.getImmutableMemoryColumnSource(numRows, deephavenType,
                                    componentType);
                    if (numRows > 0) {
                        columnSource.ensureCapacity(numRows, false);
                    }
                    columnSources.put(columnName, columnSource);

                    if (ChunkedBackingStoreExposedWritableSource.exposesChunkedBackingStore(columnSource)) {
                        // noinspection resource
                        sourceFillers[ci] = new BackingStoreSourceFiller(columnIndex, typeMapping, columnSource);
                    } else {
                        // noinspection resource
                        sourceFillers[ci] = new ChunkFlushingSourceFiller(columnIndex, typeMapping, columnSource);
                    }
                }
            } catch (final Throwable t) {
                SafeCloseable.closeAll(sourceFillers);
                throw t;
            }

            this.columnSources = columnSources;
            this.sourceFillers = sourceFillers;
            typeMapperContext = JdbcTypeMapper.Context.of(
                    options.sourceTimeZone, options.arrayDelimiter, options.strict);
        }

        @Override
        public void consumeRow() throws SQLException {
            if (errorEncountered) {
                throw new IllegalStateException("Previously encountered an error in append");
            }
            try {
                for (SourceFiller filler : sourceFillers) {
                    filler.readRow(resultSet, typeMapperContext, numRowsConsumed);
                }
            } catch (final Throwable t) {
                errorEncountered = true;
                throw t;
            }
            ++numRowsConsumed;
        }

        @Override
        public Table result() {
            if (errorEncountered) {
                throw new IllegalStateException(
                        "Unexpected call to result(), an error was encountered in consumeRow()!");
            }
            // noinspection resource
            return new QueryTable(RowSetFactory.flat(numRowsConsumed).toTracking(), columnSources);
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(sourceFillers);
        }
    }

    /**
     * Returns a table that was populated from the provided result set.
     *
     * @param rs result set to read, its cursor should be before the first row to import
     * @param origColumnNames columns to include or all if none provided
     * @return a deephaven static table
     * @throws SQLException if reading from the result set fails
     */
    public static Table readJdbc(final ResultSet rs, final String... origColumnNames) throws SQLException {
        return readJdbc(rs, readJdbcOptions(), origColumnNames);
    }

    /**
     * Returns a table that was populated from the provided result set.
     *
     * @param resultSet result set to read, its cursor should be before the first row to import
     * @param options options to change the way readJdbc behaves
     * @param origColumnNames columns to include or all if none provided
     * @return a deephaven static table
     * @throws SQLException if reading from the result set fails
     */
    public static Table readJdbc(final ResultSet resultSet, final ReadJdbcOptions options, String... origColumnNames)
            throws SQLException {
        return readJdbc(resultSet, options,
                (rs, nr, o) -> new TableRowSink(rs, nr, o, null, origColumnNames));
    }

    private interface SourceFiller extends SafeCloseable {
        void readRow(ResultSet rs, JdbcTypeMapper.Context context, long destRowKey) throws SQLException;
    }

    private static class BackingStoreSourceFiller implements SourceFiller {
        final int columnIndex;
        final JdbcTypeMapper.DataTypeMapping<?> typeMapping;
        final WritableColumnSource<?> columnSource;
        final ResettableWritableChunk<Values> destChunk;

        int destChunkOffset = 0;

        BackingStoreSourceFiller(
                int columnIndex, JdbcTypeMapper.DataTypeMapping<?> typeMapping, WritableColumnSource<?> columnSource) {
            this.columnIndex = columnIndex;
            this.typeMapping = typeMapping;
            this.columnSource = columnSource;
            destChunk = columnSource.getChunkType().makeResettableWritableChunk();
        }

        @Override
        public void readRow(ResultSet rs, JdbcTypeMapper.Context context, long destRowKey) throws SQLException {
            if (destChunkOffset >= destChunk.capacity()) {
                columnSource.ensureCapacity(destRowKey + 1, false);
                ChunkedBackingStoreExposedWritableSource ws = (ChunkedBackingStoreExposedWritableSource) columnSource;
                final long firstRowOffset = ws.resetWritableChunkToBackingStore(destChunk, destRowKey);
                destChunkOffset = LongSizedDataStructure.intSize("JdbcToTableAdapter", destRowKey - firstRowOffset);
            }

            typeMapping.bindToChunk(destChunk, destChunkOffset++, rs, columnIndex, context);
        }

        @Override
        public void close() {
            destChunk.close();
        }
    }

    private static class ChunkFlushingSourceFiller implements SourceFiller {
        final int columnIndex;
        final JdbcTypeMapper.DataTypeMapping<?> typeMapping;
        final WritableColumnSource<?> columnSource;
        final WritableChunk<Values> destChunk;
        final ChunkSink.FillFromContext fillFromContext;

        long destRowOffset = 0;
        int destChunkOffset = 0;

        ChunkFlushingSourceFiller(
                int columnIndex, JdbcTypeMapper.DataTypeMapping<?> typeMapping, WritableColumnSource<?> columnSource) {
            this.columnIndex = columnIndex;
            this.typeMapping = typeMapping;
            this.columnSource = columnSource;
            final int chunkSize = ArrayBackedColumnSource.BLOCK_SIZE;
            destChunk = columnSource.getChunkType().makeWritableChunk(chunkSize);
            fillFromContext = columnSource.makeFillFromContext(chunkSize);
        }

        @Override
        public void readRow(ResultSet rs, JdbcTypeMapper.Context context, long destRowKey) throws SQLException {
            if (destChunkOffset >= destChunk.capacity()) {
                flush();
            }

            typeMapping.bindToChunk(destChunk, destChunkOffset++, rs, columnIndex, context);
        }

        public void flush() {
            columnSource.ensureCapacity(destRowOffset + destChunkOffset, false);
            try (final RowSequence rows =
                    RowSequenceFactory.forRange(destRowOffset, destRowOffset + destChunkOffset - 1)) {
                columnSource.fillFromChunk(fillFromContext, destChunk, rows);
            }
            destRowOffset += destChunkOffset;
            destChunkOffset = 0;
        }

        @Override
        public void close() {
            flush();
            destChunk.close();
            fillFromContext.close();
        }
    }

    private static final CaseFormat fromFormat = CaseFormat.LOWER_HYPHEN;
    private static final Map<CasingStyle, CaseFormat> caseFormats;

    static {
        Map<CasingStyle, CaseFormat> initFormats = new HashMap<>();
        initFormats.put(CasingStyle.lowerCamel, CaseFormat.LOWER_CAMEL);
        initFormats.put(CasingStyle.UpperCamel, CaseFormat.UPPER_CAMEL);
        initFormats.put(CasingStyle.lowercase, CaseFormat.LOWER_UNDERSCORE);
        initFormats.put(CasingStyle.UPPERCASE, CaseFormat.UPPER_UNDERSCORE);
        initFormats.put(CasingStyle.None, null);

        caseFormats = Collections.unmodifiableMap(initFormats);
    }

    /**
     * Ensures that columns names are valid for use in Deephaven and applies optional casing rules.
     *
     * @param originalColumnName Column name to be checked for validity and uniqueness
     * @param usedNames List of names already used in the table
     * @param casing Optional {@link CasingStyle} to use when processing source names, if null or
     *        {@link CasingStyle#None} the source name's casing is not modified
     * @param replacement A String to use as a replacement for invalid characters in the source name
     * @return Legalized, uniquified, column name, with specified casing applied
     */
    private static String fixColumnName(final String originalColumnName,
            @NotNull final Set<String> usedNames,
            final CasingStyle casing,
            @NotNull final String replacement) {
        if (casing == null || caseFormats.get(casing) == null) {
            return NameValidator.legalizeColumnName(originalColumnName, (s) -> s.replaceAll("[- ]", replacement),
                    usedNames);
        }

        // Run through the legalization and casing process twice, in case legalization returns a name that doesn't
        // conform with casing.
        // During casing adjustment, we'll allow hyphen, space, backslash, forward slash, and period as word separators.
        // noinspection unchecked
        final String intermediateLegalName =
                NameValidator.legalizeColumnName(
                        originalColumnName.replaceAll("[- .\\\\/]", "_"),
                        (s) -> s,
                        Collections.EMPTY_SET)
                        .replaceAll("_", "-").toLowerCase();

        // There should be no reason for the casing options to return a String with hyphen or space in them, but, in
        // case we add other CasingStyles later, we'll check.
        return NameValidator.legalizeColumnName(
                fromFormat.to(
                        caseFormats.get(casing),
                        intermediateLegalName),
                (s) -> s.replaceAll("[- _]", replacement),
                usedNames);
    }

    private static String[] fixColumnNames(ReadJdbcOptions options, String[] origColumnNames) {
        final Set<String> usedNames = new HashSet<>();
        final String[] columnNames = new String[origColumnNames.length];
        for (int ii = 0; ii < origColumnNames.length; ++ii) {
            columnNames[ii] = fixColumnName(origColumnNames[ii], usedNames, options.casingStyle, options.replacement);
            usedNames.add(columnNames[ii]);
        }
        return columnNames;
    }

    /**
     * Gets the expected size of the {@link ResultSet}, or 0 if we can not figure it out. This method may move the
     * cursor, but will restore it before returning if so.
     *
     * @param resultSet The result to determine the size of
     * @return The expected size (or 0 if unknown)
     */
    private static int getExpectedSize(@NotNull final ResultSet resultSet) {
        // It would be swell to get the size of our ResultSet, but only if it is scrollable
        final int type;
        try {
            type = resultSet.getType();
        } catch (SQLException e) {
            throw new UncheckedDeephavenException("Can not determine ResultSet type!", e);
        }

        if (type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_SCROLL_SENSITIVE) {
            try {
                final int firstRow = resultSet.getRow();
                if (!resultSet.isBeforeFirst() && firstRow == 0 || resultSet.isAfterLast()) {
                    // this result set appears to be empty
                    return 0;
                }

                final int lastRow;
                try {
                    resultSet.last();
                    lastRow = resultSet.getRow();
                } finally {
                    resultSet.absolute(firstRow);
                }
                return lastRow - firstRow;
            } catch (SQLException ignored) {
            }
        }
        return 0;
    }
}
