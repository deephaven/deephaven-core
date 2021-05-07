/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive doubles.
 */
public interface ColumnRegionDouble<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {

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
    default double getDouble(@NotNull FillContext context, long elementIndex) {
        return getDouble(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return double.class;
    }

    static <ATTR extends Attributes.Any> ColumnRegionDouble.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Attributes.Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionDouble<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionDouble.Null INSTANCE = new ColumnRegionDouble.Null();

        private Null() {}

        @Override
        public double getDouble(long elementIndex) {
            return QueryConstants.NULL_DOUBLE;
        }
    }
}
