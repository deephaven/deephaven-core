/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive floats.
 */
public interface ColumnRegionFloat<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single float from this region.
     *
     * @param elementIndex Element (float) index in the table's address space
     * @return The float value at the specified element (float) index
     */
    float getFloat(long elementIndex);

    /**
     * Get a single float from this region.
     *
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (float) index in the table's address space
     * @return The float value at the specified element (float) index
     */
    default float getFloat(@NotNull final FillContext context, final long elementIndex) {
        return getFloat(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return float.class;
    }

    static <ATTR extends Any> ColumnRegionFloat.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionFloat<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionFloat.Null INSTANCE = new ColumnRegionFloat.Null();

        private Null() {
        }

        @Override
        public float getFloat(final long elementIndex) {
            return QueryConstants.NULL_FLOAT;
        }
    }

    final class Constant<ATTR extends Any> implements ColumnRegionFloat<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final float value;

        public Constant(final float value) {
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
}
