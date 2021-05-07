package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for {@link GroupingProvider}s that operate on metadata derived from a {@link ColumnLocation} for a given
 * {@link io.deephaven.db.v2.utils.Index} key range.
 */
public interface KeyRangeGroupingProvider<DATA_TYPE> extends GroupingProvider<DATA_TYPE> {

    /**
     * Add a column location for consideration when constructing groupings.
     * @param columnLocation   The column location to add
     * @param firstKey         The first key in the range for this column location
     * @param lastKey          The last key in the range for this column location (inclusive)
     */
    void addSource(@NotNull ColumnLocation columnLocation, long firstKey, long lastKey);
}
