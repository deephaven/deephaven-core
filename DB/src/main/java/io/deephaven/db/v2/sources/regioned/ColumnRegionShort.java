/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive shorts.
 */
public interface ColumnRegionShort<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single short from this region.
     *
     * @param elementIndex Element (short) index in the table's address space
     * @return The short value at the specified element (short) index
     */
    short getShort(long elementIndex);

    /**
     * Get a single short from this region.
     *
     * @param context      A {@link ColumnRegionFillContext} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (short) index in the table's address space
     * @return The short value at the specified element (short) index
     */
    default short getShort(@NotNull final FillContext context, final long elementIndex) {
        return getShort(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return short.class;
    }

    static <ATTR extends Any> ColumnRegionShort.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionShort<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionShort.Null INSTANCE = new ColumnRegionShort.Null();

        private Null() {
        }

        @Override
        public short getShort(final long elementIndex) {
            return QueryConstants.NULL_SHORT;
        }
    }

    final class Constant<ATTR extends Any> implements ColumnRegionShort<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final short value;

        public Constant(final short value) {
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
}
