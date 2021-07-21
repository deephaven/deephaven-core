/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive doubles.
 */
public interface ColumnRegionDouble<ATTR extends Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single double from this region.
     *
     * @param elementIndex Element (double) index in the table's address space
     * @return The double value at the specified element (double) index
     */
    double getDouble(long elementIndex);

    /**
     * Get a single double from this region.
     *
     * @param context      A {@link ColumnRegionFillContext} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (double) index in the table's address space
     * @return The double value at the specified element (double) index
     */
    default double getDouble(@NotNull final FillContext context, final long elementIndex) {
        return getDouble(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return double.class;
    }

    static <ATTR extends Any> ColumnRegionDouble.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionDouble<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionDouble.Null INSTANCE = new ColumnRegionDouble.Null();

        private Null() {
        }

        @Override
        public double getDouble(final long elementIndex) {
            return QueryConstants.NULL_DOUBLE;
        }
    }

    final class Constant<ATTR extends Any> implements ColumnRegionDouble<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final double value;

        public Constant(final double value) {
            this.value = value;
        }

        @Override
        public double getDouble(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableDoubleChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }
    }
}
