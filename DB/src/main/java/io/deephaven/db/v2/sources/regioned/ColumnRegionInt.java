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
 * Column region interface for regions that support fetching primitive ints.
 */
public interface ColumnRegionInt<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single int from this region.
     *
     * @param elementIndex Element (int) index in the table's address space
     * @return The int value at the specified element (int) index
     */
    int getInt(long elementIndex);

    /**
     * Get a single int from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (int) index in the table's address space
     * @return The int value at the specified element (int) index
     */
    default int getInt(@NotNull final FillContext context, final long elementIndex) {
        return getInt(elementIndex);
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Int;
    }

    static <ATTR extends Any> ColumnRegionInt<ATTR> createNull(final long pageMask) {
        //noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<ATTR>(pageMask);
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionInt<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionInt DEFAULT_INSTANCE = new ColumnRegionInt.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        private Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public int getInt(final long elementIndex) {
            return QueryConstants.NULL_INT;
        }
    }

    final class Constant<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionInt<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final int value;

        public Constant(final long pageMask, final int value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public int getInt(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableIntChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }
    }

    final class StaticPageStore<ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionInt<ATTR>>
            implements ColumnRegionInt<ATTR> {

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionInt<ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public int getInt(final long elementIndex) {
            return lookupRegion(elementIndex).getInt(elementIndex);
        }

        @Override
        public int getInt(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getInt(context, elementIndex);
        }
    }
}
