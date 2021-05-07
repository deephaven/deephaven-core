/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive shorts.
 */
public interface ColumnRegionShort<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {

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
    default short getShort(@NotNull FillContext context, long elementIndex) {
        return getShort(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return short.class;
    }

    static <ATTR extends Attributes.Any> ColumnRegionShort.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Attributes.Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionShort<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionShort.Null INSTANCE = new ColumnRegionShort.Null();

        private Null() {}

        @Override
        public short getShort(long elementIndex) {
            return QueryConstants.NULL_SHORT;
        }
    }
}
