/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import io.deephaven.base.ArrayUtil;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * A pair of stream implementations which write to and read from sequences of byte buffers. They are
 * guaranteed to be "compatible", that is, a stream of buffers written by the Output stream is
 * guaranteed to be readable by the Input stream - given that the client correctly implements the
 * source and sink interfaces.
 */
public class ByteBufferStreams {

    // -------------------------------------------------------------------------------------------------------
    // Sink interface - handles complete buffers from the Output stream
    // -------------------------------------------------------------------------------------------------------

    public interface Sink {
        /**
         * Dispose of the contents of the buffer b, probably by writing them to a channel, and
         * return a new buffer in which writing can continue. The returned buffer must have at least
         * need bytes of space remaining. The return value may be the same buffer, as long as it's
         * remaining() value has been increased to be >= need.
         * 
         * @param b the buffer whose contents need to be disposed of.
         * @return the buffer in which further output should be written.
         */
        ByteBuffer acceptBuffer(ByteBuffer b, int need) throws IOException;

        /**
         * Dispose of the contents of the final buffer in an output sequence, probably by writing
         * them to a channel. Note that the argument buffer may be empty. Then do whatever it takes
         * to release the resources of the sink, probably by closing a channel.
         */
        void close(ByteBuffer b) throws IOException;
    }

    // -------------------------------------------------------------------------------------------------------
    // Output stream - writes to a sequence of byye buffers
    // -------------------------------------------------------------------------------------------------------

    public static class Output extends java.io.OutputStream implements DataOutput/*
                                                                                  * ,
                                                                                  * WritableByteChannel
                                                                                  */ {
        protected volatile ByteBuffer buf;
        protected Sink sink;

        /**
         * Returns a new Output stream with the specified initial buffer and sink.
         * 
         * @param b
         * @param sink
         */
        public Output(ByteBuffer b, Sink sink) {
            this.buf = b;
            this.sink = sink;
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
        public void setSink(Sink sink) {
            this.sink = sink;
        }

        // -----------------------------------------------------------------------------------
        // java.io.OutputStream implementation
        // -----------------------------------------------------------------------------------

        public void close() throws IOException {
            ByteBuffer b = buf;
            buf = null;
            sink.close(b);
        }

        public void flush() throws IOException {
            if (buf.position() != 0) {
                buf = sink.acceptBuffer(buf, 0);
            }
        }

        public void write(int b) throws IOException {
            if (buf.remaining() < 1) {
                buf = sink.acceptBuffer(buf, 1);
            }
            buf.put((byte) b);
        }

        public void write(byte ba[]) throws IOException {
            write(ba, 0, ba.length);
        }

        public void write(byte ba[], int off, int len) throws IOException {
            int remaining;
            while ((remaining = buf.remaining()) < len) {
                buf.put(ba, off, remaining);
                buf = sink.acceptBuffer(buf, buf.capacity());
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
                buf = sink.acceptBuffer(buf, buf.capacity());
                len -= remaining;
            }
            buf.put(b);
        }

        // -----------------------------------------------------------------------------------
        // java.io.DataOutput implementation
        // -----------------------------------------------------------------------------------

        public void writeBoolean(boolean v) throws IOException {
            if (buf.remaining() < 1) {
                buf = sink.acceptBuffer(buf, 1);
            }
            buf.put((byte) (v ? 1 : 0));
        }

        public void writeByte(int v) throws IOException {
            if (buf.remaining() < 1) {
                buf = sink.acceptBuffer(buf, 1);
            }
            buf.put((byte) v);
        }

        public void writeShort(int v) throws IOException {
            if (buf.remaining() < 2) {
                buf = sink.acceptBuffer(buf, 2);
            }
            buf.putShort((short) v);
        }

        public void writeChar(int v) throws IOException {
            if (buf.remaining() < 2) {
                buf = sink.acceptBuffer(buf, 2);
            }
            buf.putChar((char) v);
        }

        public void writeInt(int v) throws IOException {
            if (buf.remaining() < 4) {
                buf = sink.acceptBuffer(buf, 4);
            }
            buf.putInt(v);
        }

        public void writeLong(long v) throws IOException {
            if (buf.remaining() < 8) {
                buf = sink.acceptBuffer(buf, 8);
            }
            buf.putLong(v);
        }

        public void writeFloat(float f) throws IOException {
            if (buf.remaining() < 8) {
                buf = sink.acceptBuffer(buf, 8);
            }
            buf.putFloat(f);
        }

        public void writeDouble(double d) throws IOException {
            if (buf.remaining() < 8) {
                buf = sink.acceptBuffer(buf, 8);
            }
            buf.putDouble(d);
        }

        public void writeBytes(String s) throws IOException {
            appendBytes(s);
        }

        public void writeChars(String s) throws IOException {
            appendChars(s);
        }

        public void writeUTF(String str) throws IOException {
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

            if (buf.remaining() < 2) {
                buf = sink.acceptBuffer(buf, 2);
            }

            buf.putShort((short) total);

            // underestimate the number remaining
            while ((remaining = buf.remaining() / 3) < len) {
                for (int i = 0; i < remaining; ++i) {
                    int c = str.charAt(position++);
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
                buf = sink.acceptBuffer(buf, Math.max(buf.capacity(), 3));
                len -= remaining;
            }

            for (int i = 0; i < len; ++i) {
                int c = str.charAt(position++);
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
        }

        public Output appendChars(CharSequence s) throws IOException {
            return appendChars(s, 0, s.length());
        }

        public Output appendChars(CharSequence s, int position, int len) throws IOException {
            int remaining;
            while ((remaining = buf.remaining() / 2) < len) {
                for (int i = 0; i < remaining; ++i) {
                    buf.putChar(s.charAt(position++));
                }
                buf = sink.acceptBuffer(buf, Math.max(buf.capacity(), 2));
                len -= remaining;
            }

            for (int i = 0; i < len; ++i) {
                buf.putChar(s.charAt(position++));
            }

            return this;
        }

        public Output appendBytes(CharSequence s) throws IOException {
            return appendBytes(s, 0, s.length());
        }

        public Output appendBytes(CharSequence s, int position, int len) throws IOException {
            int remaining;
            while ((remaining = buf.remaining()) < len) {
                for (int i = 0; i < remaining; ++i) {
                    buf.put((byte) s.charAt(position++));
                }
                buf = sink.acceptBuffer(buf, buf.capacity());
                len -= remaining;
            }

            for (int i = 0; i < len; ++i) {
                buf.put((byte) s.charAt(position++));
            }

            return this;
        }
    }

    // -------------------------------------------------------------------------------------------------------
    // Source interface - provides additional buffers to the input stream
    // -------------------------------------------------------------------------------------------------------

    public interface Source {
        ByteBuffer nextBuffer(ByteBuffer lastBuffer);
    }

    // -------------------------------------------------------------------------------------------------------
    // Input stream - reads from a sequence of byte buffers
    // -------------------------------------------------------------------------------------------------------

    public static class Input extends java.io.InputStream implements DataInput {

        /** the buffer from which we read */
        protected ByteBuffer buf;

        /** optional source for the next buffer, when the current one is exhausted */
        private Source source;

        // buffer for constructing strings in readLine and readUtf
        private char[] stringBuffer;
        private int stringBufferCount;
        private static final char[] EMPTY_STRING_BUFFER = new char[0];

        /**
         * Construct a new stream which reads from a byte buffer
         */
        public Input(ByteBuffer buf) {
            this.buf = buf;
            this.source = null;
            this.stringBuffer = EMPTY_STRING_BUFFER;
            this.stringBufferCount = 0;
        }

        /**
         * Construct a new stream which reads from a byte buffer, and has a source for more buffers
         */
        public Input(ByteBuffer buf, Source source) {
            this.buf = buf;
            this.source = source;
            this.stringBuffer = EMPTY_STRING_BUFFER;
            this.stringBufferCount = 0;
        }

        /**
         * Set the buffer to be used for future read operations.
         */
        public void setBuffer(ByteBuffer buf) {
            this.buf = buf;
        }

        /**
         * Make sure we have n bytes in the current buffer, try to get another one if we don't.
         */
        private void need(int n) throws BufferUnderflowException, EOFException {
            if (buf == null) {
                throw new EOFException();
            }
            if (buf.remaining() < n) {
                if (buf.remaining() != 0) {
                    throw new IllegalStateException(
                        "Partial primitive, input was not written by ByteBufferOutputStream?");
                }
                if (next() == null) {
                    throw new BufferUnderflowException();
                }
            }
        }

        /**
         * Get the next buffer from the source, if we have one. Skip empty buffers. The return value
         * is either null (signaling the end of the buffer sequence) or a buffer with remaining() >
         * 0.
         */
        private ByteBuffer next() {
            if (source == null) {
                return null;
            }
            while ((buf = source.nextBuffer(buf)) != null && buf.remaining() == 0) {
                // empty
            }
            return buf;
        }

        /**
         * Return true if we are at EOF. If the return value is true, then there is at least one
         * byte immediately available in buf.
         */
        private boolean eof() {
            return buf == null || (buf.remaining() == 0 && next() == null);
        }

        /**
         * peek at the next byte
         */
        private int peek() {
            if (eof()) {
                return -1;
            }
            return (int) buf.get(buf.position()) & 0xFF;
        }

        // -----------------------------------------------------------------------------------
        // java.io.InputStream implementation
        // -----------------------------------------------------------------------------------

        public int read() throws IOException {
            if (eof()) {
                return -1;
            }
            return (int) buf.get() & 0xFF;
        }

        public int read(byte b[]) throws IOException {
            return read(b, 0, b.length);
        }

        public int read(byte b[], int off, int len) throws IOException {
            if (len <= 0) {
                return 0;
            }
            if (eof()) {
                return -1;
            }
            int n = Math.min(buf.remaining(), len);
            if (n == 0 && len > 0) {
                return -1;
            }
            buf.get(b, off, n);
            return n;
        }

        public long skip(long n) throws IOException {
            if (n < 0 || eof()) {
                return 0;
            }
            n = Math.min(buf.remaining(), n);
            buf.position(buf.position() + (int) n);
            return n;
        }

        public int available() throws IOException {
            if (eof()) {
                return 0;
            }
            return buf.remaining();
        }

        public void close() throws IOException {
            // empty
        }

        public synchronized void mark(int readlimit) {
            // empty
        }

        public synchronized void reset() throws IOException {
            throw new IOException("mark/reset is not supported");
        }

        public boolean markSupported() {
            return false;
        }

        // -----------------------------------------------------------------------------------
        // java.io.InputStream implementation
        // -----------------------------------------------------------------------------------

        public void readFully(byte b[]) throws IOException {
            readFully(b, 0, b.length);
        }

        public void readFully(byte b[], int off, int len) throws IOException {
            try {
                int nread = 0;
                while ((nread += read(b, off + nread, len - nread)) < len) {
                    // empty
                }
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public int skipBytes(int n) throws IOException {
            return (int) skip(n);
        }

        public boolean readBoolean() throws IOException {
            try {
                need(1);
                return buf.get() != 0;
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public byte readByte() throws IOException {
            try {
                need(1);
                return buf.get();
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public int readUnsignedByte() throws IOException {
            try {
                need(1);
                return (int) buf.get() & 0xff;
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public short readShort() throws IOException {
            try {
                need(2);
                return buf.getShort();
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public int readUnsignedShort() throws IOException {
            try {
                need(2);
                return (int) buf.getShort() & 0xffff;
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public char readChar() throws IOException {
            try {
                need(2);
                return buf.getChar();
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public int readInt() throws IOException {
            try {
                need(4);
                return buf.getInt();
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public long readLong() throws IOException {
            try {
                need(8);
                return buf.getLong();
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public float readFloat() throws IOException {
            try {
                need(4);
                return buf.getFloat();
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public double readDouble() throws IOException {
            try {
                need(8);
                return buf.getDouble();
            } catch (BufferUnderflowException x) {
                throw new EOFException();
            }
        }

        public String readLine() throws IOException {
            stringBufferCount = 0;
            int b = read();
            if (b == -1) {
                return null;
            }
            do {
                if (b == '\n') {
                    break;
                } else if (b == '\r') {
                    b = peek();
                    if (b == '\n') {
                        b = read();
                    }
                    break;
                } else {
                    stringBuffer = ArrayUtil.put(stringBuffer, stringBufferCount++, (char) b);
                }
            } while ((b = read()) != -1);
            return new String(stringBuffer, 0, stringBufferCount);
        }

        public String readUTF() throws IOException {
            int length = 0;
            int total = readUnsignedShort();
            if (total > this.stringBuffer.length) {
                stringBuffer = new char[Math.max(total, this.stringBuffer.length * 2)];
            }
            while (total > 0) {
                int b1 = read();
                if (b1 == -1) {
                    throw new UTFDataFormatException();
                }
                if ((b1 & 0x80) == 0) {
                    stringBuffer[length++] = (char) (b1 & 0xff);
                    total--;
                } else if ((b1 & 0xe0) == 0xc0) {
                    int b2 = read();
                    if (b2 == -1 || (b2 & 0xc0) != 0x80) {
                        throw new UTFDataFormatException();
                    }
                    stringBuffer[length++] = (char) (((b1 & 0x1F) << 6) | (b2 & 0x3F));
                    total -= 2;
                } else if ((b1 & 0xf0) == 0xe0) {
                    int b2 = read();
                    int b3 = read();
                    if (b2 == -1 || b3 == -1 || (b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80) {
                        throw new UTFDataFormatException();
                    }
                    stringBuffer[length++] =
                        (char) (((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F));
                    total -= 3;
                }
            }
            return new String(stringBuffer, 0, length);
        }
    }
}
