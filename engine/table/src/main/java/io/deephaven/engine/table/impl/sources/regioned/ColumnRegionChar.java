package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive chars.
 */
public interface ColumnRegionChar<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single char from this region.
     *
     * @param elementIndex Element row key in the table's address space
     * @return The char value at the specified element row key
     */
    char getChar(long elementIndex);

    /**
     * Get a single char from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element row key in the table's address space
     * @return The char value at the specified element row key
     */
    default char getChar(@NotNull final FillContext context, final long elementIndex) {
        return getChar(elementIndex);
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Char;
    }

    static <ATTR extends Any> ColumnRegionChar<ATTR> createNull(final long pageMask) {
        //noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<ATTR>(pageMask);
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionChar<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionChar DEFAULT_INSTANCE = new ColumnRegionChar.Null(RegionedColumnSourceBase.PARAMETERS.regionMask);

        private Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public char getChar(final long elementIndex) {
            return QueryConstants.NULL_CHAR;
        }
    }

    final class Constant<ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionChar<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final char value;

        public Constant(final long pageMask, final char value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public char getChar(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableCharChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }
    }

    final class StaticPageStore<ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionChar<ATTR>>
            implements ColumnRegionChar<ATTR> {

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionChar<ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public char getChar(final long elementIndex) {
            return lookupRegion(elementIndex).getChar(elementIndex);
        }

        @Override
        public char getChar(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getChar(context, elementIndex);
        }
    }
}
