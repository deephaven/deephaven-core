//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jdbc;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.time.DateTimeUtils;
import org.immutables.value.Value;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.ZoneId;
import java.util.Map;

/**
 * Immutable instructions for reading JDBC data.
 */
@Immutable
@BuildableStyle
public abstract class JdbcReadInstructions {

    /**
     * Constant indicating no limit on the number of rows to read.
     */
    public static final int NO_ROW_LIMIT = -1;

    /**
     * The casing style to use for column names.
     *
     * @return the casing style, defaults to {@link CasingStyle#None}
     */
    @Default
    public CasingStyle columnNameCasingStyle() {
        return CasingStyle.None;
    }

    /**
     * The replacement string for invalid characters in column names.
     *
     * @return the replacement string, defaults to "_"
     */
    @Default
    public String columnNameInvalidCharacterReplacement() {
        return "_";
    }

    /**
     * The maximum number of rows to read, or {@link #NO_ROW_LIMIT} for no limit.
     *
     * @return the maximum number of rows, defaults to {@link #NO_ROW_LIMIT} (no limit)
     */
    @Default
    public int maxRows() {
        return NO_ROW_LIMIT;
    }

    /**
     * Whether strict mode is enabled. When strict mode is enabled, the adapter will throw exceptions for data type
     * conversions that would lose precision or otherwise be problematic (for example, a JDBC BIGINT value that exceeds
     * the range of a Java long). When strict mode is disabled, such conversions will be performed with potential data
     * loss or truncation.
     *
     * @return true if strict mode is enabled, defaults to true
     */
    @Default
    public boolean strict() {
        return true;
    }

    /**
     * The source time zone for date/time columns.
     *
     * @return the source time zone, defaults to the server time zone
     */
    @Default
    public ZoneId sourceTimeZone() {
        return DateTimeUtils.timeZone();
    }

    /**
     * The delimiter for array columns.
     *
     * @return the array delimiter, defaults to ","
     */
    @Default
    public String arrayDelimiter() {
        return ",";
    }

    /**
     * A map from column name to the desired Deephaven type for that column. For columns with multiple possible type
     * mappings, this permits the user to specify which Deephaven type should be used. Any columns not present in this
     * map will receive the default type mapping.
     *
     * @return the column target type map, defaults to an empty map
     */
    @Value
    public abstract Map<String, Class<?>> columnTargetTypes();

    /**
     * Returns a new builder for JdbcReadInstructions.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return ImmutableJdbcReadInstructions.builder();
    }

    /**
     * Builder for {@link JdbcReadInstructions}.
     */
    public interface Builder {
        /**
         * Sets the casing style for column names.
         *
         * @param style the casing style, defaults to {@link CasingStyle#None}
         * @return this builder
         * @see JdbcReadInstructions#columnNameCasingStyle()
         */
        Builder columnNameCasingStyle(CasingStyle style);

        /**
         * Sets the replacement string for invalid characters in column names.
         *
         * @param replacement the replacement string, defaults to "_"
         * @return this builder
         * @see JdbcReadInstructions#columnNameInvalidCharacterReplacement()
         */
        Builder columnNameInvalidCharacterReplacement(String replacement);

        /**
         * Sets the maximum number of rows to read.
         *
         * @param maxRows the maximum number of rows, or {@link #NO_ROW_LIMIT} for no limit, defaults to
         *        {@link #NO_ROW_LIMIT}
         * @return this builder
         * @see JdbcReadInstructions#maxRows()
         */
        Builder maxRows(int maxRows);

        /**
         * Sets whether strict mode is enabled.
         *
         * @param strict true for strict mode, defaults to true
         * @return this builder
         * @see JdbcReadInstructions#strict()
         */
        Builder strict(boolean strict);

        /**
         * Sets the source time zone for date/time columns.
         *
         * @param sourceTimeZone the time zone, defaults to the server time zone
         * @return this builder
         * @see JdbcReadInstructions#sourceTimeZone()
         */
        Builder sourceTimeZone(ZoneId sourceTimeZone);

        /**
         * Sets the delimiter for array columns.
         *
         * @param arrayDelimiter the delimiter, defaults to ","
         * @return this builder
         * @see JdbcReadInstructions#arrayDelimiter()
         */
        Builder arrayDelimiter(String arrayDelimiter);

        /**
         * Adds a single column target type mapping. This can be called multiple times to build up the map.
         *
         * @param columnName the column name
         * @param targetType the desired Deephaven type for this column
         * @return this builder
         * @see JdbcReadInstructions#columnTargetTypes()
         */
        Builder putColumnTargetTypes(String columnName, Class<?> targetType);

        /**
         * Adds multiple column target type mappings. This can be called multiple times to build up the map.
         *
         * @param columnTargetTypes the target type mappings to add
         * @return this builder
         * @see JdbcReadInstructions#columnTargetTypes()
         */
        Builder putAllColumnTargetTypes(Map<String, ? extends Class<?>> columnTargetTypes);

        /**
         * Builds the immutable JdbcReadInstructions instance.
         *
         * @return the built instance
         */
        JdbcReadInstructions build();
    }
}
