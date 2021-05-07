package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.sources.chunk.page.PageStore;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.sources.regioned.RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK;

public interface RegionedPageStore<ATTR extends Attributes.Any, INNER_ATTR extends ATTR, REGION_TYPE extends Page<INNER_ATTR>>
        extends PageStore<ATTR, INNER_ATTR, REGION_TYPE>, LongSizedDataStructure {

    long REGION_MASK = ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK;
    long REGION_MASK_NUM_BITS = Helper.getNumBitsOfMask();

    /**
     * @return The total number of rows across all regions.
     *
     * @implNote We intentionally do not derive from Page and implement Page.length() because our index is not
     * contiguous.
     */

    default long size() {
        long size = 0;
        int regionCount = getRegionCount();

        for (int i = 0; i < regionCount; i++) {
            REGION_TYPE region = getRegion(i);
            size += region.length();
        }

        return size;
    }

    /**
     * Get the region index.
     * @return The region index for an element index.
     */
    static int getRegionIndex(long elementIndex) {
        return (int) (elementIndex >> REGION_MASK_NUM_BITS);
    }

    /**
     * Get the first element index.
     * @return the first element index for a region index.
     */
    static long getFirstElementIndex(int regionIndex) {
        return (long) regionIndex << REGION_MASK_NUM_BITS;
    }

    /**
     * Get the last element index.
     * @return the last element index for a region index.
     */
    static long getLastElementIndex(int regionIndex) {
        return (long) regionIndex << REGION_MASK_NUM_BITS | REGION_MASK;
    }

    /**
     * Get the element index.
     * @return the element index for a particular region offset of a region index.
     */
    static long getElementIndex(int regionIndex, long regionOffset) {
        return (long) regionIndex << REGION_MASK_NUM_BITS | regionOffset;
    }

    /**
     * Get the number of regions.
     * @return The number of regions that have been added
     */
    int getRegionCount();

    /**
     * Map from a region index to its corresponding region.
     *
     * @param regionIndex The region index
     * @return The region for the supplied region index
     */
    REGION_TYPE getRegion(final int regionIndex);

    /**
     * Perform region lookup for an element index.
     *
     * @param elementIndex The element index to get the region for
     * @return The appropriate region
     */
    @FinalDefault
    default REGION_TYPE lookupRegion(final long elementIndex) {
        return getRegion(getRegionIndex(elementIndex));
    }

    @Override @NotNull
    @FinalDefault
    default REGION_TYPE getPageContaining(FillContext fillContext, long row) {
        return lookupRegion(row);
    }

    @FinalDefault
    default long mask() {
        // This PageStore is a concatenation of regions which cover the entire positive address space of {@OrderKeys}.
        return Long.MAX_VALUE;
    }

    @Override
    default FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ColumnRegionFillContext();
    }

    class Helper {
        static long getNumBitsOfMask() {
            long numBits = MathUtil.ceilLog2(REGION_MASK);
            Require.eq(REGION_MASK + 1, "MAX_REGION_SIZE", 1L << numBits, "1 << RIGHT_SHIFT");
            return numBits;
        }
    }
}
