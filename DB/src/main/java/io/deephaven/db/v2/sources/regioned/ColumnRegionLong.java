/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive longs.
 */
public interface ColumnRegionLong<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single long from this region.
     *
     * @param elementIndex Element (long) index in the table's address space
     * @return The long value at the specified element (long) index
     */
    long getLong(long elementIndex);

    /**
     * Get a single long from this region.
     *
     * @param context      A {@link ColumnRegionFillContext} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (long) index in the table's address space
     * @return The long value at the specified element (long) index
     */
    default long getLong(@NotNull FillContext context, long elementIndex) {
        return getLong(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return long.class;
    }

    static <ATTR extends Attributes.Any> ColumnRegionLong.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Attributes.Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionLong<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionLong.Null INSTANCE = new ColumnRegionLong.Null();

        private Null() {}

        @Override
        public long getLong(long elementIndex) {
            return QueryConstants.NULL_LONG;
        }
    }
}
