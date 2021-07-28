package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching primitive chars.
 */
public interface ColumnRegionChar<ATTR extends Any> extends ColumnRegion<ATTR> {

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
     * @param context      A {@link RegionContextHolder} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (char) index in the table's address space
     * @return The char value at the specified element (char) index
     */
    default char getChar(@NotNull final FillContext context, final long elementIndex) {
        return getChar(elementIndex);
    }

    @Override
    default Class<?> getNativeType() {
        return char.class;
    }

    static <ATTR extends Any> ColumnRegionChar.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionChar<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionChar.Null INSTANCE = new ColumnRegionChar.Null();

        private Null() {
        }

        @Override
        public char getChar(final long elementIndex) {
            return QueryConstants.NULL_CHAR;
        }
    }

    final class Constant<ATTR extends Any> implements ColumnRegionChar<ATTR>, WithDefaultsForRepeatingValues<ATTR> {

        private final char value;

        public Constant(final char value) {
            this.value = value;
        }

        @Override
        public char getChar(final long elementIndex) {
            return value;
        }

        @Override
        public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, final int length) {
            final int offset = destination.size();
            destination.asWritableCharChunk().fillWithValue(offset, length, value);
            destination.setSize(offset + length);
        }
    }
}
