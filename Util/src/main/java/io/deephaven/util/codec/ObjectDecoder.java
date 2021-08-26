package io.deephaven.util.codec;

import io.deephaven.base.verify.Assert;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
     * @param length The length of the byte array to decode from, starting at the offset
     * @return The output object, possibly null
     */
    @Nullable
    TYPE decode(@NotNull byte[] input, int offset, int length);

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
        Assert.eq(expectedWidth, "expectedWidth", actualWidth, "actualWidth");
    }
}
