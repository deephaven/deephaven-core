package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching objects.
 */
public interface ColumnRegionObject<DATA_TYPE, ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single object from this region.
     *
     * @param elementIndex Element (object) index in the table's address space
     * @return The object value at the specified element (object) index
     */
    DATA_TYPE getObject(long elementIndex);

    /**
     * Get a single object from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (object) index in the table's address space
     * @return The object value at the specified element (object) index
     */
    default DATA_TYPE getObject(@NotNull final FillContext context, final long elementIndex) {
        return getObject(elementIndex);
    }

    default ColumnRegionObject<DATA_TYPE, ATTR> skipCache() {
        return this;
    }

    @Override
    @FinalDefault
    default ChunkType getChunkType() {
        return ChunkType.Object;
    }

    static <T, ATTR extends Any> ColumnRegionObject<T, ATTR> createNull(final long pageMask) {
        //noinspection unchecked
        return pageMask == Null.DEFAULT_INSTANCE.mask() ? Null.DEFAULT_INSTANCE : new Null<T, ATTR>(pageMask);
    }

    final class Null<DATA_TYPE, ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionObject<DATA_TYPE, ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionObject DEFAULT_INSTANCE = new ColumnRegionObject.Null(RegionedColumnSourceBase.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK);

        private Null(final long pageMask) {
            super(pageMask);
        }

        @Override
        public DATA_TYPE getObject(final long elementIndex) {
            return null;
        }
    }

    final class Constant<DATA_TYPE, ATTR extends Any>
            extends GenericColumnRegionBase<ATTR>
            implements ColumnRegionObject<DATA_TYPE, ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final DATA_TYPE value;

        public Constant(final long pageMask, final DATA_TYPE value) {
            super(pageMask);
            this.value = value;
        }

        @Override
        public DATA_TYPE getObject(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableObjectChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }
    }

    final class StaticPageStore<DATA_TYPE, ATTR extends Any>
            extends RegionedPageStore.Static<ATTR, ATTR, ColumnRegionObject<DATA_TYPE, ATTR>>
            implements ColumnRegionObject<DATA_TYPE, ATTR> {

        public StaticPageStore(@NotNull final Parameters parameters, @NotNull final ColumnRegionObject<DATA_TYPE, ATTR>[] regions) {
            super(parameters, regions);
        }

        @Override
        public DATA_TYPE getObject(final long elementIndex) {
            return lookupRegion(elementIndex).getObject(elementIndex);
        }

        @Override
        public DATA_TYPE getObject(@NotNull final FillContext context, final long elementIndex) {
            return lookupRegion(elementIndex).getObject(context, elementIndex);
        }
    }
}
