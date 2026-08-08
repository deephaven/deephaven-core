//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jdbc;

import io.deephaven.engine.table.Table;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static io.deephaven.jdbc.JdbcReadInstructions.NO_ROW_LIMIT;

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
 * There are several options that can be set to change the behavior of the ingestion. Provide the customized options *
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
 * 
 * @deprecated Use {@link JdbcTools} instead
 */
@Deprecated(forRemoval = true)
public class JdbcToTableAdapter {

    /**
     * String formatting styles for use when standardizing externally supplied column names. Casing of the enum members
     * indicates the resultant format. None means no change to the casing of the source string.
     *
     * @deprecated Use {@link io.deephaven.jdbc.CasingStyle} instead. This enum is maintained for backward compatibility
     *             only and will be removed in a future release.
     */
    @Deprecated(forRemoval = true)
    public enum CasingStyle {
        /**
         * UpperCamelCase (e.g., "MyColumnName")
         */
        UpperCamel,

        /**
         * lowerCamelCase (e.g., "myColumnName")
         */
        lowerCamel,

        /**
         * UPPERCASE (e.g., "MY_COLUMN_NAME")
         */
        UPPERCASE,

        /**
         * lowercase (e.g., "my_column_name")
         */
        lowercase,

        /**
         * No change to casing
         */
        None
    }

    /**
     * Options applicable when reading JDBC data into a Deephaven in-memory table. Designed to constructed in a "fluent"
     * manner, with defaults applied if not specified by the user.
     *
     * @deprecated Use {@link JdbcReadInstructions} and its builder instead. This class is maintained for backward
     *             compatibility only and will be removed in a future release.
     */
    @Deprecated(forRemoval = true)
    @SuppressWarnings("UnusedReturnValue")
    public static class ReadJdbcOptions {
        private CasingStyle casingStyle = CasingStyle.None;
        private String replacement = "_";
        private int maxRows = NO_ROW_LIMIT;
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

        /**
         * @return the casing style for column names
         */
        public CasingStyle getColumnNameCasingStyle() {
            return casingStyle;
        }

        /**
         * @return the replacement string for invalid characters in column names
         */
        public String getColumnNameInvalidCharacterReplacement() {
            return replacement;
        }

        /**
         * @return the maximum number of rows to read, or {@link JdbcReadInstructions#NO_ROW_LIMIT} for no limit
         */
        public int getMaxRows() {
            return maxRows;
        }

        /**
         * @return whether strict mode is enabled
         */
        public boolean isStrict() {
            return strict;
        }

        /**
         * @return the source time zone for date/time columns
         */
        public TimeZone getSourceTimeZone() {
            return sourceTimeZone;
        }

        /**
         * @return the delimiter for array columns
         */
        public String getArrayDelimiter() {
            return arrayDelimiter;
        }

        /**
         * @return the target type map for columns
         */
        public Map<String, Class<?>> getColumnTargetTypes() {
            return new HashMap<>(targetTypeMap);
        }

        /**
         * Convert this ReadJdbcOptions to a JdbcReadInstructions instance.
         * 
         * @return an immutable JdbcReadInstructions with the same settings
         * @deprecated Use JdbcReadInstructions and its builder directly.
         */
        @Deprecated
        public JdbcReadInstructions toInstructions() {
            return JdbcReadInstructions.builder()
                    .columnNameMapping(
                            convertCasingStyle(getColumnNameCasingStyle()),
                            getColumnNameInvalidCharacterReplacement())
                    .maxRows(getMaxRows())
                    .strict(isStrict())
                    .sourceTimeZone(getSourceTimeZone().toZoneId())
                    .arrayDelimiter(getArrayDelimiter())
                    .putAllColumnTargetTypes(getColumnTargetTypes())
                    .build();
        }

        private static io.deephaven.jdbc.CasingStyle convertCasingStyle(CasingStyle style) {
            if (style == null) {
                return null;
            }
            switch (style) {
                case UpperCamel:
                    return io.deephaven.jdbc.CasingStyle.UpperCamel;
                case lowerCamel:
                    return io.deephaven.jdbc.CasingStyle.lowerCamel;
                case UPPERCASE:
                    return io.deephaven.jdbc.CasingStyle.UPPERCASE;
                case lowercase:
                    return io.deephaven.jdbc.CasingStyle.lowercase;
                case None:
                    return io.deephaven.jdbc.CasingStyle.None;
                default:
                    throw new IllegalArgumentException("Unknown CasingStyle: " + style);
            }
        }
    }

    /**
     * Returns a new options object that the user can use to customize a readJdbc operation.
     *
     * @return a new ReadJdbcOptions object
     * @deprecated Use {@link JdbcReadInstructions#builder()} instead. This method is maintained for backward
     *             compatibility only and will be removed in a future release.
     */
    @Deprecated(forRemoval = true)
    public static ReadJdbcOptions readJdbcOptions() {
        return new ReadJdbcOptions();
    }

    /**
     * Returns a table that was populated from the provided result set.
     *
     * @param resultSet result set to read, its cursor should be before the first row to import
     * @param origColumnNames columns to include or all if none provided
     * @return a deephaven static table
     * @throws SQLException if reading from the result set fails
     * @deprecated Use {@link JdbcTools#readTable(ResultSet, String...)} instead. This method is maintained for backward
     *             compatibility only and will be removed in a future release.
     */
    public static Table readJdbc(final ResultSet resultSet, final String... origColumnNames) throws SQLException {
        return readJdbc(resultSet, readJdbcOptions(), origColumnNames);
    }

    /**
     * Returns a table that was populated from the provided result set.
     *
     * @param resultSet result set to read, its cursor should be before the first row to import
     * @param options options to change the way readJdbc behaves
     * @param origColumnNames columns to include or all if none provided
     * @return a deephaven static table
     * @throws SQLException if reading from the result set fails
     * @deprecated Use {@link JdbcTools#readTable(ResultSet, JdbcReadInstructions, String...)} instead. This method is
     *             maintained for backward compatibility only and will be removed in a future release.
     */
    @Deprecated(forRemoval = true)
    public static Table readJdbc(
            final ResultSet resultSet,
            final ReadJdbcOptions options,
            final String... origColumnNames)
            throws SQLException {
        return JdbcTools.readTable(resultSet, options.toInstructions(), origColumnNames);
    }
}
