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
import java.util.function.Function;
import java.util.function.Supplier;

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
     * A supplier for the column name mapping function that converts JDBC result set column names to output column
     * names. The supplier is invoked once per read operation to obtain a possibly-fresh mapping function instance, thus
     * allowing the mapping function to keep state.
     *
     * @return the column name mapping supplier, defaults to creating a new {@link StandardColumnNameMappingFunction}
     *         with default naming parameters
     */
    @Default
    public Supplier<Function<String, String>> columnNameMappingSupplier() {
        return () -> new StandardColumnNameMappingFunction(StandardColumnNameMappingFunction.NamingParams.defaults());
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
         * Sets the column name mapping supplier. The supplier should provide a fresh function instance for each
         * invocation if the function may maintain internal state.
         *
         * @param columnNameMappingSupplier the supplier for column name mapping functions
         * @return this builder
         * @see JdbcReadInstructions#columnNameMappingSupplier()
         */
        Builder columnNameMappingSupplier(Supplier<Function<String, String>> columnNameMappingSupplier);

        /**
         * Convenience method to configure column name mapping using {@link StandardColumnNameMappingFunction} with the
         * specified casing style and invalid character replacement.
         *
         * @param casingStyle the casing style to use for column names
         * @param invalidCharacterReplacement the replacement string for invalid characters in column names
         * @return this builder
         */
        default Builder columnNameMapping(CasingStyle casingStyle, String invalidCharacterReplacement) {
            return columnNameMapping(StandardColumnNameMappingFunction.NamingParams.builder()
                    .casingStyle(casingStyle)
                    .invalidCharacterReplacement(invalidCharacterReplacement)
                    .build());
        }

        /**
         * Convenience method to configure column name mapping using {@link StandardColumnNameMappingFunction} with the
         * specified naming parameters.
         *
         * @param namingParams the naming parameters to use
         * @return this builder
         */
        default Builder columnNameMapping(StandardColumnNameMappingFunction.NamingParams namingParams) {
            return columnNameMappingSupplier(() -> new StandardColumnNameMappingFunction(namingParams));
        }

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
