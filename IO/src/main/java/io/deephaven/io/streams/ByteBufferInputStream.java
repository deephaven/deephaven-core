/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import io.deephaven.base.string.cache.CharSequenceAdapterBuilder;
import io.deephaven.base.string.cache.StringCache;
import org.jetbrains.annotations.NotNull;

/**
 * This is an InputStream implementation which reads from a java.nio.ByteBuffer. If a read operation
 * crosses the end of the buffer, the BufferUnderflowException is converted to an EOFException.
 *
 * The stream contains no state other than that in in the buffer itself, so the buffer can be
 * exchanged at will with the setBuffer() method.
 */
public class ByteBufferInputStream extends java.io.InputStream implements DataInput {

    /** the buffer from which we read */
    protected ByteBuffer buf;

    private char[] utfChars;

    /**
     * The DataOutput interface always writes bytes in big-endian order, while ByteBuffer allows the
     * order to be big- or little-endian. Set this flag true to assume that the buffer is
     * bid-endian, or false to check the buffer's order at each write.
     */
    // protected static final boolean ASSUME_BIG_ENDIAN = true;

    /**
     * Construct a new stream which reads from a byte buffer/
     */
    public ByteBufferInputStream(ByteBuffer buf) {
        this.buf = buf;
        this.utfChars = new char[0];
    }

    /**
     * Set the buffer to be used for future read operations.
     */
    public void setBuffer(ByteBuffer buf) {
        this.buf = buf;
    }

    // -----------------------------------------------------------------------------------
    // java.io.InputStream implementation
    // -----------------------------------------------------------------------------------

    @Override
    public int read() throws IOException {
        try {
            return (int) buf.get() & 0xFF;
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public int read(byte b[]) throws IOException {
        int n = Math.min(buf.remaining(), b.length);
        if (n == 0 && b.length > 0) {
            return -1;
        }
        buf.get(b, 0, n);
        return n;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int n = Math.min(buf.remaining(), len);
        if (n == 0 && len > 0) {
            return -1;
        }
        buf.get(b, off, n);
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n < 0) {
            return 0;
        }
        n = Math.min(buf.remaining(), n);
        buf.position(buf.position() + (int) n);
        return n;
    }

    @Override
    public int available() throws IOException {
        return buf.remaining();
    }

    @Override
    public void close() throws IOException {
        // empty
    }

    @Override
    public synchronized void mark(int readlimit) {
        buf.mark();
    }

    @Override
    public synchronized void reset() throws IOException {
        buf.reset();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    // -----------------------------------------------------------------------------------
    // java.io.InputStream implementation
    // -----------------------------------------------------------------------------------

    @Override
    public void readFully(byte b[]) throws IOException {
        try {
            buf.get(b, 0, b.length);
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public void readFully(byte b[], int off, int len) throws IOException {
        try {
            buf.get(b, off, len);
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return (int) skip(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        try {
            return buf.get() != 0;
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return buf.get();
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public int readUnsignedByte() throws IOException {
        try {
            return (int) buf.get() & 0xff;
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public short readShort() throws IOException {
        try {
            // if ( ASSUME_BIG_ENDIAN || buf.order() == ByteOrder.BIG_ENDIAN ) {
            return buf.getShort();
            // }
            // else {
            // return (short) ((buf.get() << 8) | buf.get());
            // }
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public int readUnsignedShort() throws IOException {
        try {
            // if ( ASSUME_BIG_ENDIAN || buf.order() == ByteOrder.BIG_ENDIAN ) {
            return (int) buf.getShort() & 0xffff;
            // }
            // else {
            // return (short) (((buf.get() << 8) | buf.get()) & 0xffff);
            // }
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public char readChar() throws IOException {
        try {
            // if ( ASSUME_BIG_ENDIAN || buf.order() == ByteOrder.BIG_ENDIAN ) {
            return buf.getChar();
            // }
            // else {
            // return (char) (buf.get() << 8 | buf.get());
            // }
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            // if ( ASSUME_BIG_ENDIAN || buf.order() == ByteOrder.BIG_ENDIAN ) {
            return buf.getInt();
            // }
            // else {
            // return ((buf.get() << 24) | (buf.get() << 16) | buf.get() << 8 | buf.get());
            // }
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            // if ( ASSUME_BIG_ENDIAN || buf.order() == ByteOrder.BIG_ENDIAN ) {
            return buf.getLong();
            // }
            // else {
            // int hi = ((buf.get() << 24) | (buf.get() << 16) | buf.get() << 8 | buf.get());
            // int lo = ((buf.get() << 24) | (buf.get() << 16) | buf.get() << 8 | buf.get());
            // return ((long) hi << 32) | ((long) lo);
            // }
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public float readFloat() throws IOException {
        try {
            // if ( ASSUME_BIG_ENDIAN || buf.order() == ByteOrder.BIG_ENDIAN ) {
            return buf.getFloat();
            // }
            // else {
            // int bits = ((buf.get() << 24) | (buf.get() << 16) | buf.get() << 8 | buf.get());
            // return Float.intBitsToFloat(bits);
            // }
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public double readDouble() throws IOException {
        try {
            // if ( ASSUME_BIG_ENDIAN || buf.order() == ByteOrder.BIG_ENDIAN ) {
            return buf.getDouble();
            // }
            // else {
            // int hi = ((buf.get() << 24) | (buf.get() << 16) | buf.get() << 8 | buf.get());
            // int lo = ((buf.get() << 24) | (buf.get() << 16) | buf.get() << 8 | buf.get());
            // long bits = ((long) hi << 32) | ((long) lo);
            // return Double.longBitsToDouble(bits);
            // }
        } catch (BufferUnderflowException x) {
            throw new EOFException();
        }
    }

    @Override
    public String readLine() throws IOException {
        int begin = buf.position(), end;
        try {
            while (true) {
                int b = buf.get() & 0xff;
                if (b == '\n') {
                    end = buf.position() - 1;
                    break;
                } else if (b == '\r') {
                    b = buf.get() & 0xff;
                    if (b == '\n') {
                        end = buf.position() - 2;
                        break;
                    }
                    buf.position(buf.position() - 1);
                    end = buf.position() - 1;
                    break;
                }
            }
        } catch (BufferUnderflowException x) {
            end = buf.position();
            // ignore
        }
        char[] chars = new char[end - begin];
        for (int i = begin; i < end; ++i) {
            chars[i - begin] = (char) ((int) buf.get(i) & 0xff);
        }
        return new String(chars);
    }

    @Override
    public String readUTF() throws IOException {
        int length = 0;
        int total = readUnsignedShort();

        final char[] chars = new char[total];

        while (total > 0) {
            final int b1 = buf.get();
            if ((b1 & 0x80) == 0) {
                chars[length++] = (char) (b1 & 0xff);
                total--;
            } else if ((b1 & 0xe0) == 0xc0) {
                final int b2 = buf.get();
                if ((b2 & 0xc0) != 0x80) {
                    throw new UTFDataFormatException("malformed second byte " + b2);
                }
                chars[length++] = (char) (((b1 & 0x1F) << 6) | (b2 & 0x3F));
                total -= 2;
            } else if ((b1 & 0xf0) == 0xe0) {
                final int b2 = buf.get();
                final int b3 = buf.get();
                if ((b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80) {
                    throw new UTFDataFormatException(
                        "malformed second byte " + b2 + " or third byte " + b3);
                }
                chars[length++] = (char) (((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F));
                total -= 3;
            } else {
                throw new UTFDataFormatException("malformed first byte " + b1);
            }
        }

        return new String(chars, 0, length);
    }

    public String readUTF(@NotNull final CharSequenceAdapterBuilder output,
        @NotNull final StringCache<String> cache) throws IOException {
        readUTF(output);
        return cache.getCachedString(output);
    }

    public void readUTF(@NotNull final CharSequenceAdapterBuilder output) throws IOException {
        int total = readUnsignedShort();

        output.clear().reserveCapacity(total);

        while (total > 0) {
            final int b1 = buf.get();
            if ((b1 & 0x80) == 0) {
                output.append((char) (b1 & 0xff));
                total--;
            } else if ((b1 & 0xe0) == 0xc0) {
                final int b2 = buf.get();
                if ((b2 & 0xc0) != 0x80) {
                    throw new UTFDataFormatException("malformed second byte " + b2);
                }
                output.append((char) (((b1 & 0x1F) << 6) | (b2 & 0x3F)));
                total -= 2;
            } else if ((b1 & 0xf0) == 0xe0) {
                final int b2 = buf.get();
                final int b3 = buf.get();
                if ((b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80) {
                    throw new UTFDataFormatException(
                        "malformed second byte " + b2 + " or third byte " + b3);
                }
                output.append((char) (((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F)));
                total -= 3;
            } else {
                throw new UTFDataFormatException("malformed first byte " + b1);
            }
        }
    }

    public void read(ByteBuffer dest, int length) {
        final int sourceLimit = buf.limit();
        buf.limit(buf.position() + length); // Constrain buf.remaining() to length
        dest.put(buf);
        buf.limit(sourceLimit);
    }
}
