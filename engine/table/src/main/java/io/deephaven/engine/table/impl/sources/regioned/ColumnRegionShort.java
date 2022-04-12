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
 * Column region interface for regions that support fetching primitive shorts.
 */
public interface ColumnRegionShort<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single short from this region.
     *
     * @param elementIndex Element row key in the table's address space
     * @return The short value at the specified element row key
     */
    short getShort(long elementIndex);

    /**
     * Get a single short from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element row key in the table's address space
     * @return The short value at the specified element row key
     */
    default short getShort(@NotNull final FillContext context, final long elementIndex) {
        return getShort(elementIndex);
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Short;
    }

    static <ATTR extends Any> ColumnRegionShort<ATTR> createNull(final long pageMask) {
        //noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<ATTR>(pageMask);
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionShort<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionShort DEFAULT_INSTANCE = new ColumnRegionShort.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        private Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public short getShort(final long elementIndex) {
            return QueryConstants.NULL_SHORT;
        }
    }

    final class Constant<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionShort<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final short value;

        public Constant(final long pageMask, final short value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public short getShort(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableShortChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }
    }

    final class StaticPageStore<ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionShort<ATTR>>
            implements ColumnRegionShort<ATTR> {

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionShort<ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public short getShort(final long elementIndex) {
            return lookupRegion(elementIndex).getShort(elementIndex);
        }

        @Override
        public short getShort(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getShort(context, elementIndex);
        }
    }
}
