//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.streams;

import io.deephaven.base.text.Convert;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.DataOutput;
import java.io.UTFDataFormatException;
import java.util.Objects;

// TODO: this implementation is not suited for application which need to fill the underlying
// TODO: buffers to their full capacity, because it calculates (or, for CSV appenders, estimates)
// TODO: the number of bytes needed by each write operation and will not begin the operation
// TODO: in the current buffer unless the entire operation can be completed there. The
// TODO: alternative would be to begin the operation anyway and react to the
// TODO: BufferOverflowExceptions by calling the sink and then continuing, which would be both
// TODO: very expensive and extremely complex.

/**
 * This is an OutputStream implementation which places the output into a java.nio.ByteBuffer. The constructor accepts an
 * initial buffer and an instance of ByteBufferSink. When an output operation would cause the buffer to overflow, it is
 * handed to the sink's acceptBuffer() method, which is expected to dispose of the existing contents and return a buffer
 * in which writing can continue.
 *
 * Note that the stream contains no state other than the buffer itself, so the buffer and/or the sink can be switched at
 * any time by calling setBuffer() or setSink().
 */
public class ByteBufferOutputStream extends java.io.OutputStream implements DataOutput {
    private ByteBuffer buf;
    private ByteBufferSink sink;

    /**
     * Returns a new ByteBufferOutputStream with the specified initial buffer and sink.
     * 
     * @param b The initial buffer
     * @param sink The sink that should be used to accept full buffers
     */
    public ByteBufferOutputStream(ByteBuffer b, ByteBufferSink sink) {
        this.buf = b;
        this.sink = Objects.requireNonNull(sink);
    }

    /**
     * Install a new buffer for all future writes.
     */
    public void setBuffer(ByteBuffer b) {
        this.buf = b;
    }

    /**
     * Install a new sink for all future writes.
     */
    public void setSink(ByteBufferSink sink) {
        this.sink = Objects.requireNonNull(sink);
    }

    // -----------------------------------------------------------------------------------
    // java.io.OutputStream implementation
    // -----------------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        ByteBuffer b = buf;
        buf = null;
        sink.close(b);
    }

    @Override
    public void flush() throws IOException {
        if (buf.position() != 0) {
            updateBuffer(0);
        }
    }

    @Override
    public void write(int b) throws IOException {
        ensureRemaining(Byte.BYTES);
        buf.put((byte) b);
    }

    @Override
    public void write(byte ba[]) throws IOException {
        write(ba, 0, ba.length);
    }

    @Override
    public void write(byte ba[], int off, int len) throws IOException {
        int remaining;
        while ((remaining = buf.remaining()) < len) {
            buf.put(ba, off, remaining);
            updateBuffer(buf.capacity());
            len -= remaining;
            off += remaining;
        }

        buf.put(ba, off, len);
    }

    public void write(ByteBuffer b) throws IOException {
        int len = b.remaining();
        int remaining;
        while ((remaining = buf.remaining()) < len) {
            for (int i = 0; i < remaining; ++i) {
                buf.put(b.get());
            }
            updateBuffer(buf.capacity());
            len -= remaining;
        }

        buf.put(b);
    }

    // @Override
    // public boolean isOpen() {
    // return sink != null;
    // }

    // -----------------------------------------------------------------------------------
    // java.io.DataOutput implementation
    // -----------------------------------------------------------------------------------

    @Override
    public void writeBoolean(boolean v) throws IOException {
        ensureRemaining(Byte.BYTES);
        buf.put((byte) (v ? 1 : 0));
    }

    @Override
    public void writeByte(int v) throws IOException {
        ensureRemaining(Byte.BYTES);
        buf.put((byte) v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        ensureRemaining(Short.BYTES);
        buf.putShort((short) v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        ensureRemaining(Character.BYTES);
        buf.putChar((char) v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        ensureRemaining(Integer.BYTES);
        buf.putInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        ensureRemaining(Long.BYTES);
        buf.putLong(v);
    }

    @Override
    public void writeFloat(float f) throws IOException {
        ensureRemaining(Float.BYTES);
        buf.putFloat(f);
    }

    @Override
    public void writeDouble(double d) throws IOException {
        ensureRemaining(Double.BYTES);
        buf.putDouble(d);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        appendBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        appendChars(s);
    }

    @Override
    public void writeUTF(String str) throws IOException {
        writeUTF((CharSequence) str);
    }

    public void writeUTF(CharSequence str) throws IOException {
        int len = str.length();
        int total = 0;
        for (int i = 0; i < len; ++i) {
            int c = str.charAt(i);
            if (c <= 0x7f) {
                total++;
                if (c == 0) {
                    total++;
                }
            } else if (c <= 0x7ff) {
                total += 2;
            } else {
                total += 3;
            }
        }
        if (total >= 65536) {
            throw new UTFDataFormatException();
        }

        int position = 0;
        int remaining;

        ensureRemaining(Short.BYTES);
        buf.putShort((short) total);

        // underestimate the number remaining
        while ((remaining = buf.remaining() / 3) < len) {
            for (int i = 0; i < remaining; ++i) {
                putUtf8(str.charAt(position++));
            }
            updateBuffer(Math.max(buf.capacity(), 3));
            len -= remaining;
        }

        for (int i = 0; i < len; ++i) {
            putUtf8(str.charAt(position++));
        }
    }

    private void putUtf8(int c) {
        if (c <= 0x7f) {
            if (c == 0) {
                buf.put((byte) 0xC0);
                buf.put((byte) 0x80);
            } else {
                buf.put((byte) c);
            }
        } else if (c <= 0x7ff) {
            buf.put((byte) (0xc0 | (0x1f & (c >> 6))));
            buf.put((byte) (0x80 | (0x3f & c)));
        } else {
            buf.put((byte) (0xe0 | (0x0f & (c >> 12))));
            buf.put((byte) (0x80 | (0x3f & (c >> 6))));
            buf.put((byte) (0x80 | (0x3f & c)));
        }
    }

    // -----------------------------------------------------------------------------------
    // csv appenders
    // -----------------------------------------------------------------------------------

    public ByteBufferOutputStream appendByteBuffer(ByteBuffer bb) throws IOException {
        final int origPos = bb.position();
        final int origLimit = bb.limit();
        int remaining;
        while ((remaining = buf.remaining()) < bb.remaining()) {
            buf.put((ByteBuffer) bb.limit(bb.position() + remaining));
            bb.limit(origLimit);
            updateBuffer(buf.capacity());
        }
        buf.put(bb);
        bb.position(origPos);
        return this;
    }

    public ByteBufferOutputStream appendByte(byte n) throws IOException {
        ensureRemaining(Byte.BYTES);
        buf.put(n);
        return this;
    }

    public ByteBufferOutputStream appendShort(short n) throws IOException {
        ensureRemaining(Convert.MAX_SHORT_BYTES);
        Convert.appendShort(n, buf);
        return this;
    }

    public ByteBufferOutputStream appendInt(int n) throws IOException {
        ensureRemaining(Convert.MAX_INT_BYTES);
        Convert.appendInt(n, buf);
        return this;
    }

    public ByteBufferOutputStream appendLong(long n) throws IOException {
        ensureRemaining(Convert.MAX_LONG_BYTES);
        Convert.appendLong(n, buf);
        return this;
    }

    public ByteBufferOutputStream appendDouble(double p) throws IOException {
        ensureRemaining(Convert.MAX_DOUBLE_BYTES);
        Convert.appendDouble(p, buf);
        return this;
    }

    @SuppressWarnings("WeakerAccess")
    public ByteBufferOutputStream appendChars(CharSequence s) throws IOException {
        return appendChars(s, 0, s.length());
    }

    @SuppressWarnings("WeakerAccess")
    public ByteBufferOutputStream appendChars(CharSequence s, @SuppressWarnings("SameParameterValue") int position,
            int len) throws IOException {
        int remaining;
        while ((remaining = buf.remaining() / 2) < len) {
            for (int i = 0; i < remaining; ++i) {
                buf.putChar(s.charAt(position++));
            }
            updateBuffer(Math.max(buf.capacity(), 2));
            len -= remaining;
        }

        for (int i = 0; i < len; ++i) {
            buf.putChar(s.charAt(position++));
        }

        return this;
    }

    public ByteBufferOutputStream appendBytes(CharSequence s) throws IOException {
        return appendBytes(s, 0, s.length());
    }

    public ByteBufferOutputStream appendBytes(CharSequence s, int position, int len) throws IOException {
        int remaining;
        while ((remaining = buf.remaining()) < len) {
            for (int i = 0; i < remaining; ++i) {
                buf.put((byte) s.charAt(position++));
            }
            updateBuffer(buf.capacity());
            len -= remaining;
        }

        for (int i = 0; i < len; ++i) {
            buf.put((byte) s.charAt(position++));
        }

        return this;
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    private void updateBuffer(int need) throws IOException {
        buf = Objects.requireNonNull(sink.acceptBuffer(buf, need));
    }

    private void ensureRemaining(int need) throws IOException {
        if (buf.remaining() < need) {
            updateBuffer(need);
        }
    }

}
