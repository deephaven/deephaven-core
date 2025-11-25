//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jdbc;

import io.deephaven.UncheckedDeephavenException;
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
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

import static io.deephaven.jdbc.JdbcReadInstructions.NO_ROW_LIMIT;

/**
 * The JdbcTools class provides a simple interface to convert a Java Database Connectivity (JDBC) {@link ResultSet} to a
 * desired result, typically a Deephaven {@link Table} or {@link StreamPublisher}.
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
 * Table resultTable = JdbcTools.readTable(resultSet);
 * </pre>
 * <p/>
 * <p>
 * There are several options that can be set to change the behavior of the ingestion. Provide the customized
 * instructions object to {@link JdbcTools#readTable(ResultSet, JdbcReadInstructions, String...)} like this:
 *
 * <pre>
 * JdbcReadInstructions instructions = JdbcReadInstructions.builder()
 *         .maxRows(1000)
 *         .strict(false)
 *         .build();
 * Table resultTable = JdbcToTableAdapter.readJdbc(resultSet, instructions);
 * </pre>
 * <p>
 * There are many supported mappings from JDBC type to Deephaven type. The default can be overridden by specifying the
 * desired result type in the instructions. For example, convert BigDecimal to double on 'MyCol' via
 * {@code instructions = JdbcReadInstructions.builder().putTargetTypeMap("MyCol", double.class).build()}.
 */
public class JdbcTools {

    /**
     * A factory to produce a {@link RowSink} that will consume rows from a {@link ResultSet} as driven by
     * {@link #readJdbc(ResultSet, JdbcReadInstructions, RowSinkFactory)} and produce a result of type
     * {@code RESULT_TYPE}.
     *
     * @param <RESULT_TYPE> The result type produced by {@link RowSink row sinks}
     *        {@link #make(ResultSet, int, JdbcReadInstructions) made} by this factory
     */
    public interface RowSinkFactory<RESULT_TYPE> {
        /**
         * Build and return a {@link RowSink} that will return a result of type {@code RESULT_TYPE}.
         *
         * @param resultSet The {@link ResultSet} that will be consumed
         * @param numRows The number of rows expected to be consumed, 0 if the number is unknown
         * @param instructions {@link JdbcReadInstructions} that should apply to the returned sink and its result
         * @return The {@link RowSink} to be used to consume {@code resultSet}
         * @throws SQLException If the RowSinkFactory encountered a {@link SQLException} while interacting with the
         *         {@link ResultSet}
         */
        RowSink<RESULT_TYPE> make(
                @NotNull ResultSet resultSet,
                int numRows,
                @NotNull JdbcReadInstructions instructions)
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
     * Construct a {@link RowSink} for {@code resultSet} and {@code instructions} using {@code rowSinkFactory},
     * {@link RowSink#consumeRow() consume} all rows from {@code resultSet} after its current cursor (limited by
     * {@code instructions.maxRows()}), and return the {@link RowSink#result() result}, ensuring that the sink is
     * {@link RowSink#close() closed} before return.
     *
     * @param resultSet The {@link ResultSet} that will be consumed, with its cursor before the first row to read
     * @param instructions {@link JdbcReadInstructions} that should apply to the returned sink and its result
     * @param rowSinkFactory The {@link RowSinkFactory} to be used to consume {@code resultSet}
     * @return The {@link RowSink#result() result}
     * @throws SQLException If a {@link SQLException} was encountered while interacting with {@code resultSet}
     * @param <RESULT_TYPE> The result type produced by the sink's {@link RowSink#result()}
     */
    public static <RESULT_TYPE> RESULT_TYPE readJdbc(
            @NotNull final ResultSet resultSet,
            final JdbcReadInstructions instructions,
            @NotNull final RowSinkFactory<RESULT_TYPE> rowSinkFactory) throws SQLException {
        // Note: JDBC result set cardinality is limited to Integer.MAX_VALUE
        final int maxRows = instructions.maxRows();
        final int numRows = maxRows < 0
                ? getExpectedSize(resultSet)
                : Math.min(maxRows, getExpectedSize(resultSet));

        try (final RowSink<RESULT_TYPE> rowSink = rowSinkFactory.make(resultSet, numRows, instructions)) {
            int numRowsConsumed = 0;
            while (resultSet.next() && (maxRows == NO_ROW_LIMIT || numRowsConsumed < maxRows)) {
                rowSink.consumeRow();
                ++numRowsConsumed;
            }
            return rowSink.result();
        }
    }

    /**
     * Gets the expected size of the {@link ResultSet}, or 0 if it cannot be determined. This method may move the
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

    /**
     * Produce a {@link Table} populated from the rows of {@code resultSet}. The returned table will never be
     * {@link Table#isRefreshing() refreshing}.
     *
     * @param resultSet {@link ResultSet} to read, with its cursor before the first row to include
     * @param inputColumnsToInclude Columns from {@code resultSet} to include; an empty input means "include all
     *        columns"
     * @return The resulting {@link Table}
     * @throws SQLException If a {@link SQLException} was encountered while interacting with {@code resultSet}
     */
    public static Table readTable(final ResultSet resultSet, final String... inputColumnsToInclude)
            throws SQLException {
        return readTable(resultSet, JdbcReadInstructions.builder().build(), inputColumnsToInclude);
    }

    /**
     * Produce a {@link Table} populated from the rows of {@code resultSet}. The returned table will never be
     * {@link Table#isRefreshing() refreshing}.
     *
     * @param resultSet {@link ResultSet} to read, with its cursor before the first row to include
     * @param instructions {@link JdbcReadInstructions} that should apply
     * @param inputColumnsToInclude Columns from {@code resultSet} to include; an empty input means "include all
     *        columns"
     * @return The resulting {@link Table}
     * @throws SQLException If a {@link SQLException} was encountered while interacting with {@code resultSet}
     */
    public static Table readTable(
            final ResultSet resultSet,
            final JdbcReadInstructions instructions,
            String... inputColumnsToInclude) throws SQLException {
        return readJdbc(resultSet, instructions,
                (rs, nr, ri) -> new TableRowSink(rs, nr, ri, null, inputColumnsToInclude));
    }

    // region Name Mapping

    /**
     * Ensure we have result set column names, either from the supplied array or by querying the result set metadata.
     *
     * @param resultSet The result set to examine if {@code resultSetColumnNames} is null or empty
     * @param inputResultSetColumnNames The supplied result set column names
     * @return The result set column names to use
     * @throws SQLException If we encounter an error querying the result set metadata
     */
    private static @NotNull String @NotNull [] ensureResultSetColumnNames(
            @NotNull final ResultSet resultSet,
            @NotNull final String[] inputResultSetColumnNames) throws SQLException {
        if (inputResultSetColumnNames != null && inputResultSetColumnNames.length > 0) {
            return inputResultSetColumnNames;
        }
        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        final String[] resultSetColumnNames = new String[resultSetMetaData.getColumnCount()];
        for (int ii = 0; ii < resultSetColumnNames.length; ++ii) {
            resultSetColumnNames[ii] = resultSetMetaData.getColumnName(ii + 1);
        }
        return resultSetColumnNames;
    }

    /**
     * Ensure we have a result set column name to output column name mapping function, either from the supplied function
     * or by creating a default one.
     *
     * @param instructions The {@link JdbcReadInstructions} to use when creating a default mapping function
     * @param resultSetColumnNameToOutputColumnName The supplied mapping function
     * @return The mapping function to use
     */
    private static @NotNull Function<String, String> ensureResultSetColumnNameToOutputColumnName(
            @NotNull final JdbcReadInstructions instructions,
            @Nullable final Function<String, String> resultSetColumnNameToOutputColumnName) {
        return resultSetColumnNameToOutputColumnName != null
                ? resultSetColumnNameToOutputColumnName
                : new StandardColumnNameMappingFunction(instructions);
    }

    // endregion Name Mapping

    private interface SourceFiller extends SafeCloseable {
        void readRow(ResultSet rs, JdbcTypeMapper.Context context, long destRowKey) throws SQLException;
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
                @NotNull final JdbcReadInstructions instructions,
                @Nullable Function<String, String> resultSetColumnNameToTableColumnName,
                @NotNull String... resultSetColumnNames) throws SQLException {
            this.resultSet = resultSet;

            resultSetColumnNames = ensureResultSetColumnNames(resultSet, resultSetColumnNames);
            resultSetColumnNameToTableColumnName =
                    ensureResultSetColumnNameToOutputColumnName(instructions, resultSetColumnNameToTableColumnName);

            final int numColumns = resultSetColumnNames.length;
            final String[] outputColumnNames = Arrays.stream(resultSetColumnNames)
                    .map(resultSetColumnNameToTableColumnName)
                    .toArray(String[]::new);

            final Map<String, WritableColumnSource<?>> columnSources = new LinkedHashMap<>(numColumns);
            final SourceFiller[] sourceFillers = new SourceFiller[numColumns];
            try {
                for (int ci = 0; ci < numColumns; ++ci) {
                    final int columnIndex = resultSet.findColumn(resultSetColumnNames[ci]);
                    final String outputColumnName = outputColumnNames[ci];
                    final Class<?> destinationType = instructions.columnTargetTypes().get(outputColumnName);

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
                    columnSources.put(outputColumnName, columnSource);

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
                    instructions.sourceTimeZone(),
                    instructions.arrayDelimiter(),
                    instructions.strict());
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
//
//    /**
//     * A {@link StreamPublisher} that is also a {@link RowSinkFactory} implementation.
//     * {@link StreamPublisherRowSinkFactory#make(ResultSet, int, JdbcReadInstructions) created} {@link RowSink row
//     * sinks} will publish data via the publisher/factory.
//     */
//    private static final class StreamPublisherRowSinkFactory implements StreamPublisher, RowSinkFactory<Void> {
//
//
//        private final SourceFiller[] sourceFillers;
//        // TODO: We have a chicken-and-egg problem regarding chunk types, and the type mappers for Instant need to fill
//        // long chunks...
//        final JdbcTypeMapper.Context typeMapperContext;
//
//        // TODO: We probably need to record at most one error and deliver it...
//        boolean errorEncountered;
//
//        private StreamPublisherRowSinkFactory(
//                @NotNull final ResultSet resultSet,
//                @NotNull final JdbcReadInstructions instructions,
//                @Nullable Function<String, String> resultSetColumnNameToPublisherColumnName,
//                @NotNull String... resultSetColumnNames) throws SQLException {
//
//            resultSetColumnNames = ensureResultSetColumnNames(resultSet, resultSetColumnNames);
//            resultSetColumnNameToPublisherColumnName =
//                    ensureResultSetColumnNameToOutputColumnName(instructions, resultSetColumnNameToPublisherColumnName);
//
//            final int numColumns = resultSetColumnNames.length;
//            final String[] outputColumnNames;
//            if (resultSetColumnNameToPublisherColumnName == null) {
//                outputColumnNames = fixColumnNames(instructions, resultSetColumnNames);
//            } else {
//                outputColumnNames = Arrays.stream(resultSetColumnNames)
//                        .map(resultSetColumnNameToPublisherColumnName)
//                        .toArray(String[]::new);
//            }
//
//            final Map<String, WritableColumnSource<?>> columnSources = new LinkedHashMap<>(numColumns);
//            final SourceFiller[] sourceFillers = new SourceFiller[numColumns];
//            try {
//                for (int ci = 0; ci < numColumns; ++ci) {
//                    final int columnIndex = resultSet.findColumn(resultSetColumnNames[ci]);
//                    final String columnName = outputColumnNames[ci];
//                    final Class<?> destinationType = instructions.columnTargetTypes().get(columnName);
//
//                    final JdbcTypeMapper.DataTypeMapping<?> typeMapping =
//                            JdbcTypeMapper.getColumnTypeMapping(resultSet, columnIndex, destinationType);
//
//                    final Class<?> deephavenType = typeMapping.getDeephavenType();
//                    final Class<?> componentType = deephavenType.getComponentType();
//                    final WritableColumnSource<?> columnSource = numRows == 0
//                            ? ArrayBackedColumnSource.getMemoryColumnSource(0, deephavenType, componentType)
//                            : InMemoryColumnSource.getImmutableMemoryColumnSource(numRows, deephavenType,
//                                    componentType);
//                    if (numRows > 0) {
//                        columnSource.ensureCapacity(numRows, false);
//                    }
//                    columnSources.put(columnName, columnSource);
//
//                    if (ChunkedBackingStoreExposedWritableSource.exposesChunkedBackingStore(columnSource)) {
//                        // noinspection resource
//                        sourceFillers[ci] = new BackingStoreSourceFiller(columnIndex, typeMapping, columnSource);
//                    } else {
//                        // noinspection resource
//                        sourceFillers[ci] = new ChunkFlushingSourceFiller(columnIndex, typeMapping, columnSource);
//                    }
//                }
//            } catch (final Throwable t) {
//                SafeCloseable.closeAll(sourceFillers);
//                throw t;
//            }
//
//            this.columnSources = columnSources;
//            this.sourceFillers = sourceFillers;
//            typeMapperContext = JdbcTypeMapper.Context.of(
//                    options.sourceTimeZone, options.arrayDelimiter, options.strict);
//        }
//
//        @Override
//        public RowSink<Void> make(
//                @NotNull final ResultSet resultSet,
//                final int numRows,
//                @NotNull final JdbcReadInstructions instructions)
//                throws SQLException {
//            return new RowSink<>() {
//
//                int numRowsConsumed;
//
//                @Override
//                public void consumeRow() throws SQLException {
//                    if (errorEncountered) {
//                        throw new IllegalStateException("Previously encountered an error in append");
//                    }
//                    try {
//                        for (SourceFiller filler : sourceFillers) {
//                            filler.readRow(resultSet, typeMapperContext, numRowsConsumed);
//                        }
//                    } catch (final Throwable t) {
//                        errorEncountered = true;
//                        throw t;
//                    }
//                    ++numRowsConsumed;
//                }
//
//                @Override
//                public Void result() {
//                    return null;
//                }
//
//                @Override
//                public void close() {
//                    SafeCloseable.closeAll(sourceFillers);
//                }
//            };
//        }
//
//        @Override
//        public void register(@NotNull final StreamConsumer consumer) {
//
//        }
//
//        @Override
//        public void flush() {
//
//        }
//
//        @Override
//        public void shutdown() {
//
//        }
//    }
}
