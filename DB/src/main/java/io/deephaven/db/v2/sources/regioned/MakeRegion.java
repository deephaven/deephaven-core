package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface MakeRegion<ATTR extends Attributes.Values, REGION_TYPE extends ColumnRegion<ATTR>> {

    /**
     * Get a new or re-usable column region appropriate for this source.
     *
     * @param columnDefinition The {@link ColumnDefinition}
     * @param columnLocation   The {@link ColumnLocation}
     * @param regionIndex      The index of the region to add.
     *
     * @return A new or re-usable column region appropriate for this source and the supplied parameters.  A null value
     *         may be returned, which should be interpreted to mean use a special null column, which has size() 0
     *         and is full of the appropriate "null" value for the column's type.
     */
    @Nullable
    REGION_TYPE makeRegion(@NotNull ColumnDefinition<?> columnDefinition,
                           @NotNull ColumnLocation columnLocation,
                           int regionIndex);
}
