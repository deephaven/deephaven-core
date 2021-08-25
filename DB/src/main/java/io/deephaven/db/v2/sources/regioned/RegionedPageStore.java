package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.sources.chunk.page.PageStore;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

public interface RegionedPageStore<ATTR extends Any, INNER_ATTR extends ATTR, REGION_TYPE extends Page<INNER_ATTR>>
        extends PageStore<ATTR, INNER_ATTR, REGION_TYPE> {

    /**
     * @return The parameters object that describes this regioned page store
     */
    Parameters parameters();

    /**
     * @inheritDoc Note that this represent this regioned page store's mask as a {@link Page}.
     */
    @Override
    @FinalDefault
    default long mask() {
        return parameters().pageMask;
    }

    /**
     * @return The mask that should be applied to {@link io.deephaven.db.v2.utils.OrderedKeys} indices when calculating
     *         their address within a region
     */
    @FinalDefault
    default long regionMask() {
        return parameters().regionMask;
    }

    /**
     * @return The number of bits masked by {@link #regionMask()}
     */
    @FinalDefault
    default int regionMaskNumBits() {
        return parameters().regionMaskNumBits;
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
     * Class to calculate and encapsulate the parameters of a RegionedPageStore.
     */
    final class Parameters {

        public final long pageMask;
        public final int maximumRegionCount;
        public final long maximumRegionSize;
        public final long regionMask;
        public final int regionMaskNumBits;

        public Parameters(final long pageMask, final int maximumRegionCount, final long maximumRegionSize) {
            this.pageMask = validateMask(pageMask, "page");
            this.maximumRegionCount = Require.geqZero(maximumRegionCount, "maximum region count");
            this.maximumRegionSize = Require.geqZero(maximumRegionSize, "maximum region size");

            final int regionNumBits = maximumRegionCount == 0 ? 0 : MathUtil.ceilLog2(maximumRegionCount);
            regionMask = validateMask(pageMask >>> regionNumBits, "region");
            regionMaskNumBits = MathUtil.ceilLog2(regionMask);
            final long maxRegionSizeNumBits = maximumRegionSize == 0 ? 0 : MathUtil.ceilLog2(maximumRegionSize);
            if (maxRegionSizeNumBits > regionMaskNumBits) {
                throw new IllegalArgumentException(String.format(
                        "Maximum region size %,d is too large to access with page mask %#016X and maximum region count %,d",
                        maximumRegionSize, pageMask, maximumRegionCount));
            }
        }

        private static long validateMask(final long mask, final String name) {
            if (mask < 0 || (Long.SIZE - Long.numberOfLeadingZeros(mask)) != Long.bitCount(mask)) {
                throw new IllegalArgumentException(String.format("Invalid %s mask %#016X", name, mask));
            }
            return mask;
        }
    }

    /**
     * A regioned page store for use when the full set of regions and their sizes are known.
     */
    abstract class Static<ATTR extends Any, INNER_ATTR extends ATTR, REGION_TYPE extends Page<INNER_ATTR>>
            implements RegionedPageStore<ATTR, INNER_ATTR, REGION_TYPE> {

        private final Parameters parameters;
        private final REGION_TYPE[] regions;

        /**
         * @param parameters Mask and shift parameters
         * @param regions Array of all regions in this page store. Array becomes property of the page store.
         */
        public Static(@NotNull final Parameters parameters,
                @NotNull final REGION_TYPE[] regions) {
            this.parameters = parameters;
            this.regions = Require.elementsNeqNull(regions, "regions");
            Require.leq(regions.length, "regions.length", parameters.maximumRegionCount,
                    "parameters.maximumRegionCount");
            for (final REGION_TYPE region : regions) {
                Require.eq(region.mask(), "region.mask()", parameters.regionMask, "parameters.regionMask");
            }
        }

        @Override
        public final Parameters parameters() {
            return parameters;
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
