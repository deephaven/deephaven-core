package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.sources.chunk.page.PageStore;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public interface RegionedPageStore<ATTR extends Any, INNER_ATTR extends ATTR, REGION_TYPE extends Page<INNER_ATTR>>
        extends PageStore<ATTR, INNER_ATTR, REGION_TYPE>, LongSizedDataStructure {

    long mask();

    /**
     * @return The mask that should be applied to {@link io.deephaven.db.v2.utils.OrderedKeys} indices when
     * calculating their address within a page
     */
    long regionMask();

    /**
     * @return The number of bits masked by {@link #regionMask()}
     */
    int regionMaskNumBits();

    /**
     * @return The total number of rows across all regions.
     * @implNote We intentionally do not derive from Page and implement Page.length() because our index is not
     * contiguous.
     */
    default long size() {
        long size = 0;
        final int regionCount = getRegionCount();

        for (int ri = 0; ri < regionCount; ri++) {
            REGION_TYPE region = getRegion(ri);
            // TODO-RWC: This is inaccurate. What do we need it for?
            size += region.length();
        }

        return size;
    }

    /**
     * Get the region index.
     *
     * @return The region index for an element index.
     */
    @FinalDefault
    default int getRegionIndex(final long elementIndex) {
        return (int) ((mask() & elementIndex) >> regionMaskNumBits());
    }

    /**
     * Get the number of regions.
     *
     * @return The number of regions that have been added
     */
    int getRegionCount();

    /**
     * Map from a region index to its corresponding region.
     *
     * @param regionIndex The region index
     * @return The region for the supplied region index
     */
    REGION_TYPE getRegion(int regionIndex);

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

    @Override
    @NotNull
    @FinalDefault
    default REGION_TYPE getPageContaining(final FillContext fillContext, final long row) {
        return lookupRegion(row);
    }

    @Override
    default FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new RegionContextHolder();
    }

    /**
     * A regioned page store for nested use when the full set of regions and their sizes are known.
     */
    abstract class StaticNested<ATTR extends Any, INNER_ATTR extends ATTR, REGION_TYPE extends Page<INNER_ATTR>>
            implements RegionedPageStore<ATTR, INNER_ATTR, REGION_TYPE> {

        private final long mask;
        private final REGION_TYPE[] regions;
        private final long regionMask;
        private final int regionMaskNumBits;

        /**
         * @param mask    The {@link Page#mask} for this page
         * @param regions Array of all regions in this page store. Array becomes property of the page store.
         */
        public StaticNested(final long mask, @NotNull final REGION_TYPE[] regions) {
            this.mask = mask;
            Require.elementsNeqNull(regions, "regions");
            Require.gtZero(regions.length, "regions.length");
            this.regions = regions;
            final int regionNumBits = MathUtil.ceilLog2(regions.length);
            regionMask = mask >>> regionNumBits;
            regionMaskNumBits = MathUtil.ceilLog2(regionMask);
            final long maxRegionSize = Arrays.stream(regions).mapToLong(Page::length).max().orElseThrow(IllegalStateException::new);
            final long maxRegionSizeNumBits = MathUtil.ceilLog2(maxRegionSize);
            if (maxRegionSizeNumBits > regionMaskNumBits) {
                throw new IllegalArgumentException("Maximum region size " + maxRegionSize
                        + " is too large to access with page mask " + Long.toHexString(mask)
                        + " and region count " + regions.length);
            }
        }

        @Override
        public final long mask() {
            return mask;
        }

        @Override
        public final long regionMask() {
            return regionMask;
        }

        @Override
        public final int regionMaskNumBits() {
            return regionMaskNumBits;
        }

        @Override
        public final int getRegionCount() {
            return regions.length;
        }

        @Override
        public final REGION_TYPE getRegion(final int regionIndex) {
            return regions[regionIndex];
        }
    }
}
