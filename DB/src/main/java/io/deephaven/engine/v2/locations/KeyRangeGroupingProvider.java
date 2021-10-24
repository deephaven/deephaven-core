package io.deephaven.engine.v2.locations;

import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for {@link GroupingProvider}s that operate on metadata derived from a {@link ColumnLocation} for a given
 * {@link TrackingMutableRowSet} key range.
 */
public interface KeyRangeGroupingProvider<DATA_TYPE> extends GroupingProvider<DATA_TYPE> {

    /**
     * Add a column location for consideration when constructing groupings.
     * 
     * @param columnLocation The column location to add
     * @param addedIndexInTable The location's rowSet in the table
     */
    void addSource(@NotNull ColumnLocation columnLocation, @NotNull RowSet locationIndexInTable);
}
