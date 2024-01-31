/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base.util;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.ReadableByteChannel;

public class Helpers {
    public static void readBytes(ReadableByteChannel f, byte[] buffer) throws IOException {
        readExact(f, ByteBuffer.wrap(buffer));
    }

    public static BytesInput readBytes(ReadableByteChannel f, int expected) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(expected);
        readExact(f, buffer);
        buffer.flip();
        return BytesInput.from(buffer);
    }

    public static void readExact(ReadableByteChannel f, ByteBuffer buffer) throws IOException {
        final int expected = buffer.remaining();
        while (buffer.hasRemaining()) {
            if (f.read(buffer) == -1) {
                throw new EOFException(String.format("Reached end-of-file before completing, expected=%d, remaining=%d",
                        expected, buffer.remaining()));
            }
        }
    }

    static int readUnsignedVarInt(ByteBuffer in) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.get()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
        }
        return value | (b << i);
    }

    static int readIntLittleEndianPaddedOnBitWidth(ByteBuffer in, int bitWidth)
            throws IOException {

        int bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
        switch (bytesWidth) {
            case 0:
                return 0;
            case 1:
                return in.get();
            case 2:
                return in.getShort();
            case 3:
                return readIntLittleEndianOnThreeBytes(in);
            case 4:
                return in.getInt();
            default:
                throw new IOException(
                        String.format("Encountered bitWidth (%d) that requires more than 4 bytes", bitWidth));
        }
    }

    private static int readIntLittleEndianOnThreeBytes(ByteBuffer in) throws EOFException {
        int ch1 = in.get();
        int ch2 = in.get();
        int ch3 = in.get();
        if ((ch1 | ch2 | ch3) < 0) {
            throw new EOFException();
        }
        return ((ch3 << 16) + (ch2 << 8) + (ch1));
    }

    @NotNull
    public static IntBuffer ensureCapacity(IntBuffer nullOffset) {
        if (!nullOffset.hasRemaining()) {
            IntBuffer newOffset = IntBuffer.allocate(nullOffset.capacity() * 2);
            nullOffset.flip();
            newOffset.put(nullOffset);
            nullOffset = newOffset;
        }
        return nullOffset;
    }
}
