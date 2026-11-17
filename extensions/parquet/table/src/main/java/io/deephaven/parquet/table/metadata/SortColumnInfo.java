//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.jetbrains.annotations.NotNull;

/**
 * A column the file is sorted by, as recorded in key-value metadata for Deephaven-written Parquet files. Like the rest
 * of that metadata, it names the column by its parquet column name, which a reader translates to the Deephaven column
 * reading it. Any parquet column name may be recorded, whether or not it is a valid Deephaven column name.
 */
@Immutable
@SimpleStyle
@JsonSerialize(as = ImmutableSortColumnInfo.class)
@JsonDeserialize(as = ImmutableSortColumnInfo.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class SortColumnInfo {

    public enum SortDirection {
        Ascending, Descending
    }

    /**
     * @return The parquet column name
     */
    @Parameter
    public abstract String columnName();

    @Parameter
    public abstract SortDirection sortDirection();

    /**
     * @param columnName The parquet column name
     * @param sortDirection The direction the column is sorted in
     */
    public static SortColumnInfo of(@NotNull final String columnName, @NotNull final SortDirection sortDirection) {
        return ImmutableSortColumnInfo.of(columnName, sortDirection);
    }
}
