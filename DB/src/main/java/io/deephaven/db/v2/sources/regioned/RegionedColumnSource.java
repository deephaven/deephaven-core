/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.DeferredGroupingColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSource;
import io.deephaven.db.v2.sources.SizedColumnSource;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

/**
 * <p>Regioned column source interface.
 *
 * <p>"V2" {@link io.deephaven.db.v2.SourceTable}s can be thought of a tree of partitions with
 * {@link io.deephaven.db.v2.locations.TableLocation}s at the leaf nodes. When building the
 * {@link io.deephaven.db.v2.utils.Index} for such a {@link io.deephaven.db.tables.Table}, we statically
 * partition the available element address space from [0, {@value Long#MAX_VALUE} <i>(2^63-1)</i>].
 *
 * <p>We constrain the size at these leaf nodes in order to support a partitioning of the element address space into
 * region index and sub-region element index. In order to make the calculations as inexpensive as possible, this is
 * done by assigning X bits of each index key (element address) to the region index, and the remaining Y = 63 - X to the
 * sub-region element index.
 *
 * <p>This type of address space allocation allows very cheap O(1) element access. Denser alternatives tend to
 * introduce more complication and/or O(log n) lookups.
 *
 * <p>Currently, X is 23 and Y is 40, allowing tables to consist of more than 8 million locations with more than
 * 1 trillion elements each.
 */
@VisibleForTesting // This could be package-private, but for mock-based unit testing purposes it must be public
public interface RegionedColumnSource<DATA_TYPE> extends DeferredGroupingColumnSource<DATA_TYPE>,
        ImmutableColumnSource<DATA_TYPE>, SizedColumnSource<DATA_TYPE> {

    /**
     * Address bits allocated to the region index.
     **/
    int REGION_INDEX_ADDRESS_BITS = 23;

    /**
     * Address bits allocated to the sub-region element index.
     */
    int SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS = Long.SIZE - 1 /* sign bit */ - REGION_INDEX_ADDRESS_BITS;

    /**
     * The maximum number of regions that may be addressed.
     */
    int MAXIMUM_REGION_COUNT = 1 << REGION_INDEX_ADDRESS_BITS;

    /**
     * The size used for *all* regions.
     */
    long REGION_CAPACITY_IN_ELEMENTS = 1L << SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS;

    /**
     * The mask for converting from an element index to a region sub-index.
     */
    long ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK = REGION_CAPACITY_IN_ELEMENTS - 1;

    /**
     * Add a region to this regioned column source.
     * Elements in this region are ordered after elements in other regions added previously.
     *
     * @param columnDefinition The column definition for this column source (potentially varies by region)
     * @param columnLocation   The column location for the region being added
     * @return The index assigned to the added region
     */
    int addRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                  @NotNull final ColumnLocation<?> columnLocation);
}
