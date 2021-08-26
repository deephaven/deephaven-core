package io.deephaven.util;

import io.deephaven.base.MathUtil;
import io.deephaven.base.string.EncodingInfo;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Utility methods for working with ByteBuffers.
 */
public class BufferUtil {

    /**
     * <p>
     * Allocate a new ByteBuffer larger than the supplied buffer. This is generally used when
     * dynamically resizing an output buffer.
     * <p>
     * The resulting buffer's size will be a power-of-two at least double the supplied buffer's size
     * and at least the specified minimum size, unless constrained by {@link Integer#MAX_VALUE}.
     * <p>
     * The resulting buffer will be direct if and only if the supplied buffer was direct.
     * <p>
     * The contents of the supplied buffer are copied into the result buffer as in
     * {@link ByteBuffer#put(ByteBuffer)}. <br>
     * See also io.deephaven.tablelogger.AbstractBinaryStoreWriter#ensureSpace
     *
     * @param buffer The buffer to grow (and copy from)
     * @param minimumSize The minimum size for the result buffer
     * @return The new buffer, including contents of buffer
     */
    public static ByteBuffer reallocateBuffer(@NotNull final ByteBuffer buffer,
        final int minimumSize) {
        final int newCapacity = (int) Math.min(Integer.MAX_VALUE,
            1L << MathUtil.ceilLog2(Math.max(minimumSize, (long) buffer.capacity() << 1)));
        final ByteBuffer newBuffer = buffer.isDirect() ? ByteBuffer.allocateDirect(newCapacity)
            : ByteBuffer.allocate(newCapacity);
        newBuffer.put(buffer);
        return newBuffer;
    }

    /**
     * Return a buffer with at least requiredSize remaining capacity. The provided buffer will be
     * copied to the new buffer, and might be the buffer returned. The new buffer's limit will be
     * unchanged if the buffer is not reallocated, and equal to the new capacity if it is
     * reallocated. The new buffer will be allocated with allocateDirect() if the original buffer is
     * direct, else with allocate().
     *
     * @param dataBuffer a byte buffer in write mode
     * @param requiredSize additional capacity needed
     * @return a byte buffer in write mode with at least requiredSize remaining capacity
     */
    public static ByteBuffer ensureSpace(ByteBuffer dataBuffer, int requiredSize) {
        final int remainingCapacity = dataBuffer.capacity() - dataBuffer.position();
        if (remainingCapacity < requiredSize) {
            final int newCapacity = Math.max(
                (dataBuffer.capacity() > 1024 * 1024) ? dataBuffer.capacity() + 1024 * 1024
                    : dataBuffer.capacity() * 2,
                dataBuffer.capacity() + (requiredSize - remainingCapacity));
            final ByteBuffer newBuffer =
                dataBuffer.isDirect() ? ByteBuffer.allocateDirect(newCapacity)
                    : ByteBuffer.allocate(newCapacity);
            dataBuffer.flip();
            newBuffer.put(dataBuffer);
            dataBuffer = newBuffer;
        }
        Require.geq(dataBuffer.capacity() - dataBuffer.position(),
            "dataBuffer.capacity() - dataBuffer.position()", requiredSize, "requiredSize");
        return dataBuffer;
    }

    /**
     * Encode the given string as UTF_8 and write the size and data to the given buffer. The buffer
     * might be reallocated using {@link BufferUtil#ensureSpace} if necessary.
     *
     * @param dataBuffer Write to this ByteBuffer.
     * @param value Encode this string.
     * @return The modified input ByteBuffer.
     */
    static public ByteBuffer writeUtf8(ByteBuffer dataBuffer, final String value) {
        final byte[] data = value.getBytes(EncodingInfo.UTF_8.getCharset());

        dataBuffer = ensureSpace(dataBuffer, data.length + Integer.BYTES);
        dataBuffer.putInt(data.length);
        dataBuffer.put(data);

        return dataBuffer;
    }
}
