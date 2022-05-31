package io.deephaven.parquet.base.util;

import org.apache.parquet.bytes.BytesUtils;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.SeekableByteChannel;

public class Helpers {
    public static void readFully(SeekableByteChannel f, byte[] buffer) throws IOException {
        int read = f.read(ByteBuffer.wrap(buffer));
        if (read != buffer.length) {
            throw new IOException("Expected for bytes, only read " + read + " while it expected " + buffer.length);
        }
    }

    public static void readFully(SeekableByteChannel f, ByteBuffer buffer) throws IOException {
        int expected = buffer.remaining();
        int read = f.read(buffer);
        if (read != expected) {
            throw new IOException("Expected for bytes, only read " + read + " while it expected " + expected);
        }
    }

    public static ByteBuffer readFully(SeekableByteChannel f, int expected) throws IOException {
        ByteBuffer buffer = allocate(expected);
        int read = f.read(buffer);
        if (read != expected) {
            throw new IOException("Expected for bytes, only read " + read + " while it expected " + expected);
        }
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer allocate(int capacity) {
        return ByteBuffer.allocate(capacity);
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
