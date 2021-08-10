/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive longs.
 */
public interface ColumnRegionLong<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single long from this region.
     *
     * @param elementIndex Element (long) index in the table's address space
     * @return The long value at the specified element (long) index
     */
    long getLong(long elementIndex);

    /**
     * Get a single long from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (long) index in the table's address space
     * @return The long value at the specified element (long) index
     */
    default long getLong(@NotNull final FillContext context, final long elementIndex) {
        return getLong(elementIndex);
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Long;
    }

    static <ATTR extends Any> ColumnRegionLong<ATTR> createNull(final long pageMask) {
        //noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<ATTR>(pageMask);
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionLong<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionLong DEFAULT_INSTANCE = new ColumnRegionLong.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        private Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public long getLong(final long elementIndex) {
            return QueryConstants.NULL_LONG;
        }
    }

    final class Constant<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionLong<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final long value;

        public Constant(final long pageMask, final long value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public long getLong(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableLongChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }
    }

    final class StaticPageStore<ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionLong<ATTR>>
            implements ColumnRegionLong<ATTR> {

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionLong<ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public long getLong(final long elementIndex) {
            return lookupRegion(elementIndex).getLong(elementIndex);
        }

        @Override
        public long getLong(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getLong(context, elementIndex);
        }
    }
}
