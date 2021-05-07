/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive floats.
 */
public interface ColumnRegionFloat<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {

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
     * @param context      A {@link ColumnRegionFillContext} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (float) index in the table's address space
     * @return The float value at the specified element (float) index
     */
    default float getFloat(@NotNull FillContext context, long elementIndex) {
        return getFloat(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return float.class;
    }

    static <ATTR extends Attributes.Any> ColumnRegionFloat.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Attributes.Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionFloat<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionFloat.Null INSTANCE = new ColumnRegionFloat.Null();

        private Null() {}

        @Override
        public float getFloat(long elementIndex) {
            return QueryConstants.NULL_FLOAT;
        }
    }
}
