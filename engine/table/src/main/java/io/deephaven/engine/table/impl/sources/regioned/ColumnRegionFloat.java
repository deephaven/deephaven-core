/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive floats.
 */
public interface ColumnRegionFloat<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single float from this region.
     *
     * @param elementIndex Element row key in the table's address space
     * @return The float value at the specified element row key
     */
    float getFloat(long elementIndex);

    /**
     * Get a single float from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element row key in the table's address space
     * @return The float value at the specified element row key
     */
    default float getFloat(@NotNull final FillContext context, final long elementIndex) {
        return getFloat(elementIndex);
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Float;
    }

    static <ATTR extends Any> ColumnRegionFloat<ATTR> createNull(final long pageMask) {
        //noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<ATTR>(pageMask);
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionFloat<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionFloat DEFAULT_INSTANCE = new ColumnRegionFloat.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        private Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public float getFloat(final long elementIndex) {
            return QueryConstants.NULL_FLOAT;
        }
    }

    final class Constant<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionFloat<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final float value;

        public Constant(final long pageMask, final float value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public float getFloat(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableFloatChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }
    }

    final class StaticPageStore<ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionFloat<ATTR>>
            implements ColumnRegionFloat<ATTR> {

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionFloat<ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public float getFloat(final long elementIndex) {
            return lookupRegion(elementIndex).getFloat(elementIndex);
        }

        @Override
        public float getFloat(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getFloat(context, elementIndex);
        }
    }
}
