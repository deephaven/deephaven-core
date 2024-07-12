//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.codec;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * <p>
 * Codec superinterface for Object translation from byte arrays for serialization and deserialization.
 * <p>
 * Implementations must follow several rules to enable correct usage:
 * <ol>
 * <li>They must be stateless or designed for concurrent use (e.g. by using only ThreadLocal state), as they will
 * generally be cached and re-used.</li>
 * <li>They must not modify their inputs in any way, retain references to their inputs, or return results that retain
 * references to their inputs.</li>
 * </ol>
 */
public interface ObjectDecoder<TYPE> {
    /**
     * The value which represents variable width columns.
     */
    int VARIABLE_WIDTH_SENTINEL = Integer.MIN_VALUE;

    /**
     * Decode an object from an array of bytes.
     *
     * @param input The input byte array containing bytes to decode
     * @param offset The offset into the byte array to start decoding from
     * @param length The number of bytes to decode, starting at the offset
     * @return The output object, possibly null
     */
    @Nullable
    TYPE decode(byte @NotNull [] input, int offset, int length);

    /**
     * Decode an object from a ByteBuffer. The position of the input buffer may or may not be modified by this method.
     *
     * @param buffer The input ByteBuffer containing bytes to decode
     * @return The output object, possibly null
     */
    @Nullable
    default TYPE decode(@NotNull final ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return decode(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            // Make a copy of the buffer's contents
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return decode(bytes, 0, bytes.length);
        }
    }

    /**
     * What width byte array does this ObjectCodec expect to encode and decode?
     *
     * @return VARIABLE_WIDTH_SENTINEL if the codec must encode and decode variable width columns, otherwise the fixed
     *         size of byte array that must be decoded and encoded.
     */
    int expectedObjectWidth();

    /**
     * Verify that this codec is capable of supporting a column that has an actual width of {@code actualWidth}.
     *
     * @param actualWidth the actual width of the instantiated column
     * @throws IllegalArgumentException if {@code actualWidth} is not compatible with this codec
     */
    default void checkWidth(int actualWidth) throws IllegalArgumentException {
        final int expectedWidth = expectedObjectWidth();
        if (expectedWidth != actualWidth) {
            throw new IllegalArgumentException(
                    "Expected width `" + expectedWidth + "` does not match actual width `" + actualWidth + "`");
        }
    }
}
