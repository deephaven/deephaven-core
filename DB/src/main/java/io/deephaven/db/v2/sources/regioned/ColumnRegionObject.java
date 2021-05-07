package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching objects.
 */
public interface ColumnRegionObject<T, ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single object from this region.
     *
     * @param elementIndex Element (object) index in the table's address space
     * @return The object value at the specified element (object) index
     */
    T getObject(long elementIndex);

    /**
     * Get a single object from this region.
     *
     * @param context      A {@link ColumnRegionFillContext} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (object) index in the table's address space
     * @return The object value at the specified element (object) index
     */
    default T getObject(@NotNull FillContext context, long elementIndex) {
        return getObject(elementIndex);
    }

    default ColumnRegionObject<T, ATTR> skipCache() {
        return this;
    }

    @Override
    Class<?> getNativeType();

    static <T, ATTR extends Attributes.Any> ColumnRegionObject.Null<T, ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    class Null<T, ATTR extends Attributes.Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionObject<T, ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionObject.Null INSTANCE = new ColumnRegionObject.Null();

        Null() {}

        @Override
        public T getObject(long elementIndex) {
            return null;
        }

        /**
         * @return null
         * @implNote This is never called, so we just return null so this class can remain a singleton.
         */
        @Override
        public Class<T> getNativeType() {
            return null;
        }
    }
}
