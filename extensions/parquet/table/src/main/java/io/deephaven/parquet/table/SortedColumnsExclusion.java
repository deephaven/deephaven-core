//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Sortedness, declared by the data being read, that a read should ignore. Deephaven takes the sort order of the data
 * from the Deephaven metadata in a parquet file's footer, or, for Iceberg, from the table's sort order; it does not
 * verify that the data is actually sorted. That sortedness lets filters binary-search the sorted column instead of
 * scanning it, and lets a sort by that column return the table as it is -- and both then give wrong results if the data
 * is not sorted the way Deephaven sorts.
 * <p>
 * Exclusions combine: a column is excluded if any exclusion applies to it. The sort is a prefix -- each column is
 * sorted within runs of the columns before it -- so excluding a column also excludes every sort column after it.
 * <p>
 * Where Deephaven wrote the data, its order is Deephaven's. Other writers can differ in the ways described for each
 * exclusion.
 *
 * @see ParquetInstructions.Builder#addSortedColumnsExclusions(SortedColumnsExclusion...)
 */
public enum SortedColumnsExclusion {
    /**
     * Ignore sortedness declared for {@code String} columns.
     * <p>
     * Deephaven orders strings by UTF-16 code unit ({@link String#compareTo(String)}); the parquet format, Iceberg and
     * most other writers order them by code point (equivalently, by their UTF-8 bytes). The two orders differ only
     * where a character above U+FFFF (most emoji, supplementary CJK) is compared with one in U+E000 through U+FFFF (for
     * example U+FE0F, the emoji presentation selector, and the fullwidth forms), so data sorted by another writer can
     * be out of Deephaven's order where both occur.
     */
    STRING,

    /**
     * Ignore sortedness declared for {@code float} and {@code double} columns.
     * <p>
     * Deephaven reads a stored {@code -Float.MAX_VALUE} or {@code -Double.MAX_VALUE} as null, and orders null before
     * negative infinity. Data sorted by another writer that contains both negative infinity and such a value is
     * therefore out of Deephaven's order.
     */
    FLOATING_POINT,

    /**
     * Ignore all declared sortedness, for example when the metadata does not describe the data.
     */
    ALL_COLUMNS;

    private boolean appliesTo(@Nullable final ColumnDefinition<?> columnDefinition) {
        if (this == ALL_COLUMNS || columnDefinition == null) {
            // Without a definition we cannot tell whether the column is of an excluded type
            return true;
        }
        final Class<?> dataType = columnDefinition.getDataType();
        switch (this) {
            case STRING:
                return dataType == String.class;
            case FLOATING_POINT:
                return dataType == float.class || dataType == double.class;
            default:
                throw new IllegalStateException("Unexpected exclusion " + this);
        }
    }

    /**
     * Returns the prefix of {@code sortColumns} that none of {@code exclusions} applies to.
     *
     * @param exclusions The exclusions to apply
     * @param sortColumns The sort columns, in Deephaven column names
     * @param definition The definition of the table being read, if known; when it is not known, any exclusion applies
     *        to every column
     * @return The sort columns that remain
     */
    @InternalUseOnly
    @NotNull
    public static List<SortColumn> apply(
            @NotNull final Set<SortedColumnsExclusion> exclusions,
            @NotNull final List<SortColumn> sortColumns,
            @Nullable final TableDefinition definition) {
        if (exclusions.isEmpty() || sortColumns.isEmpty()) {
            return sortColumns;
        }
        final List<SortColumn> remaining = new ArrayList<>(sortColumns.size());
        for (final SortColumn sortColumn : sortColumns) {
            final ColumnDefinition<?> columnDefinition =
                    definition == null ? null : definition.getColumn(sortColumn.column().name());
            if (exclusions.stream().anyMatch(exclusion -> exclusion.appliesTo(columnDefinition))) {
                break;
            }
            remaining.add(sortColumn);
        }
        return Collections.unmodifiableList(remaining);
    }
}
