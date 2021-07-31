/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.DeferredGroupingColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSource;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

/**
 * <p>Regioned column source interface.
 *
 * <p>{@link io.deephaven.db.v2.SourceTable source tables} can be thought of a tree of partitions with
 * {@link io.deephaven.db.v2.locations.TableLocation table locations} at the leaf nodes. When building the
 * {@link io.deephaven.db.v2.utils.Index Index} for such a {@link io.deephaven.db.tables.Table table}, we statically
 * partition the available element address space from [0, {@value Long#MAX_VALUE} <i>(2^63-1)</i>].
 *
 * <p>We constrain the size at these leaf nodes in order to support a partitioning of the element address space into
 * region index and sub-region element index. In order to make the calculations as inexpensive as possible, this is
 * done by assigning {@link #REGION_INDEX_ADDRESS_BITS some bits} of each index key (element address) to the region
 * index, and the {@link #SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS remaining bits} to the sub-region element index.
 *
 * <p>This type of address space allocation allows very cheap O(1) element access. Denser alternatives tend to
 * introduce more complication and/or O(log n) lookups.
 *
 * <p>Currently, region indices use {@value REGION_INDEX_ADDRESS_BITS} and region offsets use
 * {@value SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS},  allowing tables to consist of {@value MAXIMUM_REGION_COUNT}
 * locations with {@value REGION_CAPACITY_IN_ELEMENTS} each.
 */
@VisibleForTesting // This could be package-private, but for mock-based unit testing purposes it must be public
public interface RegionedColumnSource<DATA_TYPE>
        extends DeferredGroupingColumnSource<DATA_TYPE>, ImmutableColumnSource<DATA_TYPE> {

    /**
     * Address bits allocated to the region index.
     **/
    int REGION_INDEX_ADDRESS_BITS = 20;

    /**
     * Address bits allocated to the sub-region element index.
     * <p>Note that we do not use the sign bit, as negative index keys are not permitted (or used to signify the
     * {@link io.deephaven.db.v2.utils.ReadOnlyIndex#NULL_KEY null key}).
     */
    int SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS = Long.SIZE - 1 - REGION_INDEX_ADDRESS_BITS;

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
     * Get the first element index for a region index.
     *
     * @return The first element index for a region index
     */
    static long getFirstElementIndex(final int regionIndex) {
        return (long) regionIndex << SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS;
    }

    /**
     * Get the last element index for a region index.
     *
     * @return The last element index for a region index
     */
    static long getLastElementIndex(final int regionIndex) {
        return (long) regionIndex << SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS | ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK;
    }

    /**
     * Get the element index implied by a region index and a region offset.
     *
     * @return The element index for a particular region offset of a region index
     */
    static long getElementIndex(final int regionIndex, final long regionOffset) {
        return (long) regionIndex << SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS | regionOffset;
    }

    /**
     * <p>Add a region to this regioned column source.
     *
     * <p>Elements in this region are ordered after elements in other regions added previously.
     *
     * @param columnDefinition The column definition for this column source (potentially varies by region)
     * @param columnLocation   The column location for the region being added
     * @return The index assigned to the added region
     */
    int addRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                  @NotNull final ColumnLocation columnLocation);
}
