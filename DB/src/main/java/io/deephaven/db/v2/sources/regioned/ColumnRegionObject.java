package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
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
    Class<?> getNativeType();

    static <T, ATTR extends Any> ColumnRegionObject.Null<T, ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    class Null<DATA_TYPE, ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionObject<DATA_TYPE, ATTR> {

        @SuppressWarnings("rawtypes")
        private static final ColumnRegionObject.Null INSTANCE = new ColumnRegionObject.Null();

        Null() {
        }

        @Override
        public DATA_TYPE getObject(final long elementIndex) {
            return null;
        }

        /**
         * @return null
         * @implNote This is never called, so we just return null so this class can remain a singleton.
         */
        @Override
        public Class<DATA_TYPE> getNativeType() {
            return null;
        }
    }

    final class Constant<DATA_TYPE, ATTR extends Any> implements ColumnRegionObject<DATA_TYPE, ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final DATA_TYPE value;

        public Constant(final DATA_TYPE value) {
            this.value = value;
        }

        @Override
        public Class<?> getNativeType() {
            return value.getClass();
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
}
