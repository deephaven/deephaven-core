package io.deephaven.db.v2.sources.regioned;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive chars.
 */
public interface ColumnRegionChar<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single char from this region.
     *
     * @param elementIndex Element (char) index in the table's address space
     * @return The char value at the specified element (char) index
     */
    char getChar(long elementIndex);

    /**
     * Get a single char from this region.
     *
     * @param context      A {@link ColumnRegionFillContext} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (char) index in the table's address space
     * @return The char value at the specified element (char) index
     */
    default char getChar(@NotNull FillContext context, long elementIndex) {
        return getChar(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return char.class;
    }

    static <ATTR extends Attributes.Any> ColumnRegionChar.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Attributes.Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionChar<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionChar.Null INSTANCE = new ColumnRegionChar.Null();

        private Null() {}

        @Override
        public char getChar(long elementIndex) {
            return QueryConstants.NULL_CHAR;
        }
    }
}
