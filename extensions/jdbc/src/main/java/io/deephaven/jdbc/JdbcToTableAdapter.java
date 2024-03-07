//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

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
 * 
 * Then convert the {@code ResultSet} to a {@code Table}:
 * 
 * <pre>
 * Table resultTable = JdbcToTableAdapter.readJdbc(resultSet);
 * </pre>
 * <p/>
 *
 * There are several options than can be set to change the behavior of the ingestion. Provide the customized options
 * object to {@link JdbcToTableAdapter#readJdbc(ResultSet, ReadJdbcOptions, String...)} like this:
 *
 * <pre>
 * JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
 * Table resultTable = JdbcToTableAdapter.readJdbc(resultSet, options);
 * </pre>
 *
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
     * @param rs result set to read, its cursor should be before the first row to import
     * @param options options to change the way readJdbc behaves
     * @param origColumnNames columns to include or all if none provided
     * @return a deephaven static table
     * @throws SQLException if reading from the result set fails
     */
    public static Table readJdbc(final ResultSet rs, final ReadJdbcOptions options, String... origColumnNames)
            throws SQLException {
        final ResultSetMetaData md = rs.getMetaData();

        if (origColumnNames.length == 0) {
            origColumnNames = new String[md.getColumnCount()];
            for (int ii = 0; ii < origColumnNames.length; ++ii) {
                origColumnNames[ii] = md.getColumnName(ii + 1);
            }
        }

        // Note: JDBC result set cardinality is limited to Integer.MAX_VALUE
        final int numRows = options.maxRows < 0 ? getExpectedSize(rs) : Math.min(options.maxRows, getExpectedSize(rs));
        final int numColumns = origColumnNames.length;
        final String[] columnNames = fixColumnNames(options, origColumnNames);
        final SourceFiller[] sourceFillers = new SourceFiller[numColumns];

        final HashMap<String, ColumnSource<?>> columnMap = new LinkedHashMap<>();
        long numRowsRead = 0;
        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            for (int ii = 0; ii < numColumns; ++ii) {
                final int columnIndex = rs.findColumn(origColumnNames[ii]);
                final String columnName = columnNames[ii];
                final Class<?> destType = options.targetTypeMap.get(columnName);

                final JdbcTypeMapper.DataTypeMapping<?> typeMapping =
                        JdbcTypeMapper.getColumnTypeMapping(rs, columnIndex, destType);

                final Class<?> deephavenType = typeMapping.getDeephavenType();
                final Class<?> componentType = deephavenType.getComponentType();
                final WritableColumnSource<?> cs = numRows == 0
                        ? ArrayBackedColumnSource.getMemoryColumnSource(0, deephavenType, componentType)
                        : InMemoryColumnSource.getImmutableMemoryColumnSource(numRows, deephavenType, componentType);

                if (numRows > 0) {
                    cs.ensureCapacity(numRows, false);
                }

                if (ChunkedBackingStoreExposedWritableSource.exposesChunkedBackingStore(cs)) {
                    sourceFillers[ii] = toClose.add(new BackingStoreSourceFiller(columnIndex, typeMapping, cs));
                } else {
                    sourceFillers[ii] = toClose.add(new ChunkFlushingSourceFiller(columnIndex, typeMapping, cs));
                }

                columnMap.put(columnName, cs);
            }

            final JdbcTypeMapper.Context context = JdbcTypeMapper.Context.of(
                    options.sourceTimeZone, options.arrayDelimiter, options.strict);

            while (rs.next() && (options.maxRows == -1 || numRowsRead < options.maxRows)) {
                for (SourceFiller filler : sourceFillers) {
                    filler.readRow(rs, context, numRowsRead);
                }
                ++numRowsRead;
            }
        }

        return new QueryTable(RowSetFactory.flat(numRowsRead).toTracking(), columnMap);
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

            // noinspection unchecked
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
     * Ensures that columns names are valid for use in Deephaven and applies optional casing rules
     *
     * @param originalColumnName Column name to be checked for validity and uniqueness
     * @param usedNames List of names already used in the table
     * @param casing Optional CasingStyle to use when processing source names, if null or None the source name's casing
     *        is not modified
     * @param replacement A String to use as a replacement for invalid characters in the source name
     * @return legalized, uniqueified, column name, with specified Guava casing applied
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
        // conform with casing
        // During casing adjustment, we'll allow hyphen, space, backslash, forward slash, and period as word separators
        // noinspection unchecked
        final String intermediateLegalName =
                NameValidator.legalizeColumnName(
                        originalColumnName.replaceAll("[- .\\\\/]", "_"),
                        (s) -> s,
                        Collections.EMPTY_SET)
                        .replaceAll("[_]", "-").toLowerCase();

        // There should be no reason for the casing options to return a String with hyphen or space in them, but, in
        // case we add other CasingStyles later, we'll check
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
     * Gets the expected size of the ResultSet, or 0 if we can not figure it out.
     *
     * @param rs the result to determine the size of
     * @return the expected size (or 0 if unknown)
     */
    private static int getExpectedSize(ResultSet rs) {
        // it would be swell to get the size of our ResultSet, but only if it is scrollable
        final int type;
        try {
            type = rs.getType();
        } catch (SQLException e) {
            throw new UncheckedDeephavenException("Can not determine ResultSet type!", e);
        }

        if (type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_SCROLL_SENSITIVE) {
            try {
                final int firstRow = rs.getRow();
                if (!rs.isBeforeFirst() && firstRow == 0 || rs.isAfterLast()) {
                    // this result set appears to be empty
                    return 0;
                }

                rs.last();
                final int lastRow = rs.getRow();
                rs.absolute(firstRow);
                return lastRow - firstRow;
            } catch (SQLException ignored) {
            }
        }
        return 0;
    }
}
