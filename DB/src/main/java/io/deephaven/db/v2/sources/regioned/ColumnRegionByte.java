package io.deephaven.db.v2.sources.regioned;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Column region interface for regions that support fetching primitive bytes.
 */
public interface ColumnRegionByte<ATTR extends Attributes.Any> extends ColumnRegion<ATTR> {

    /**
     * Get a single byte from this region.
     *
     * @param elementIndex Element (byte) index in the table's address space
     * @return The byte value at the specified element (byte) index
     */
    byte getByte(long elementIndex);

    /**
     * Get a single byte from this region.
     *
     * @param context      A {@link ColumnRegionFillContext} to enable resource caching where suitable, with current
     *                     region index pointing to this region
     * @param elementIndex Element (byte) index in the table's address space
     * @return The byte value at the specified element (byte) index
     */
    default byte getByte(@NotNull FillContext context, long elementIndex) {
        return getByte(elementIndex);
    }

    /**
     * Get a range of bytes from this region. Implementations are not required to verify that the range specified is
     * meaningful.
     *
     * @param firstElementIndex First element (byte) index in the table's address space
     * @param destination       Array to store results
     * @param destinationOffset Offset into {@code destination} to begin storing at
     * @param length            Number of bytes to get
     * @return {@code destination}, to enable method chaining
     */
    byte[] getBytes(long firstElementIndex,
                    @NotNull byte[] destination,
                    int destinationOffset,
                    int length
    );

    @Override
    default Class<?> getNativeType() {
        return byte.class;
    }

    static <ATTR extends Attributes.Any> ColumnRegionByte.Null<ATTR> createNull() {
        //noinspection unchecked
        return Null.INSTANCE;
    }

    final class Null<ATTR extends Attributes.Any> extends ColumnRegion.Null<ATTR> implements ColumnRegionByte<ATTR> {
        @SuppressWarnings("rawtypes")
        private static final ColumnRegionByte.Null INSTANCE = new ColumnRegionByte.Null();

        private Null() {}

        @Override
        public byte getByte(long elementIndex) {
            return QueryConstants.NULL_BYTE;
        }

        @Override
        public byte[] getBytes(long firstElementIndex, @NotNull byte[] destination, int destinationOffset, int length) {
            Arrays.fill(destination, destinationOffset, destinationOffset + length, QueryConstants.NULL_BYTE);
            return destination;
        }
    }
}
