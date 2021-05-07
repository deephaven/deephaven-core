/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive ints.
 */
public interface ColumnRegionInt<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single int from this region.
     *
     * @param elementIndex Element (int) index in the table's address space
     * @return The int value at the specified element (int) index
     */
    int getInt(long elementIndex);

    /**
     * Get a single int from this region.
     *
     * @param context      A {@link ColumnRegionFillContext} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (int) index in the table's address space
     * @return The int value at the specified element (int) index
     */
    default int getInt(@NotNull FillContext context, long elementIndex) {
        return getInt(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return int.class;
    }

    static <ATTR extends Attributes.Any> ColumnRegionInt.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Attributes.Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionInt<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionInt.Null INSTANCE = new ColumnRegionInt.Null();

        private Null() {}

        @Override
        public int getInt(long elementIndex) {
            return QueryConstants.NULL_INT;
        }
    }
}
