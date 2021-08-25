/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import junit.framework.TestCase;

import java.io.*;
import java.nio.ByteBuffer;

public class OutputInputStreamTest extends TestCase {


    private static class SmallByteSink implements ByteBufferSink {
        private final ByteBuffer flushBuffer;

        private SmallByteSink(ByteBuffer flushBuffer) {
            this.flushBuffer = flushBuffer;
            if (flushBuffer.limit() != 0) {
                fail("limit must initially be 0");
            }
        }

        @Override
        public ByteBuffer acceptBuffer(ByteBuffer b, int need) throws IOException {
            writeBuffer(b);

            if (b.remaining() < need) {
                if (need > 100) {
                    fail("Can't get sink of more than 100");
                }
                return ByteBuffer.allocate(100);
            }

            return b;
        }

        private void writeBuffer(ByteBuffer b) {
            b.flip();
            flushBuffer.mark();
            flushBuffer.position(flushBuffer.limit());
            flushBuffer.limit(flushBuffer.position() + b.limit());
            flushBuffer.put(b);
            flushBuffer.reset();
            b.clear();
        }

        @Override
        public void close(ByteBuffer b) throws IOException {
            writeBuffer(b);
        }
    }

    private OutputStream getOutputStream(int i, ByteBuffer buffer) {
        switch (i) {
            case 0:
                return new ByteBufferOutputStream(ByteBuffer.allocate(1), new SmallByteSink(buffer));

            default:
                fail("No such output stream");
                return null;
        }
    }

    private int getNumOutputStreams() {
        return 1;
    }

    private InputStream getInputStream(int i, ByteBuffer buffer) {
        switch (i) {
            case 0:
                return new ByteBufferInputStream(buffer);

            default:
                fail("No such input stream");
                return null;
        }
    }

    private int getNumInputStreams() {
        return 1;
    }

    private int getNumDataInputs() {
        return 1;
    }

    private ByteBufferOutputStream getByteBufferOutputStream(int i, ByteBuffer buffer) {
        switch (i) {
            case 0:
                return new ByteBufferOutputStream(ByteBuffer.allocate(1), new SmallByteSink(buffer));

            default:
                fail("No such output stream");
                return null;
        }
    }

    private int getNumByteBufferOutputStreams() {
        return 1;
    }

    private ByteBufferInputStream getByteBufferInputStream(int i, ByteBuffer buffer) {
        switch (i) {
            case 0:
                return new ByteBufferInputStream(buffer);

            default:
                fail("No such input stream");
                return null;
        }
    }

    private int getNumByteBufferInputStreams() {
        return 1;
    }

    public void testAll() {
        ByteBuffer buffer = ByteBuffer.allocate(102400);

        int outs = getNumOutputStreams();
        int ins = getNumInputStreams();

        for (int in = 0; in < ins; ++in) {
            for (int out = 0; out < outs; ++out) {
                buffer.clear();
                buffer.limit(0);
                testWriteReadByte(getOutputStream(out, buffer), getInputStream(in, buffer));

                buffer.clear();
                buffer.limit(0);
                testWriteReadByteInterleave(getOutputStream(out, buffer), getInputStream(in, buffer));

                buffer.clear();
                buffer.limit(0);
                testWriteBytes(getOutputStream(out, buffer), getInputStream(in, buffer));
            }
        }

        outs = getNumByteBufferOutputStreams();
        ins = getNumByteBufferInputStreams();

        for (int in = 0; in < ins; ++in) {
            for (int out = 0; out < outs; ++out) {
                buffer.clear();
                buffer.limit(0);
                testWriteByteBuffer(getByteBufferOutputStream(out, buffer), getByteBufferInputStream(in, buffer));

                buffer.clear();
                buffer.limit(0);
                testAppendBytesCharSequence(getByteBufferOutputStream(out, buffer),
                        getByteBufferInputStream(in, buffer));

                buffer.clear();
                buffer.limit(0);
                testAppendCharsCharSequence(getByteBufferOutputStream(out, buffer),
                        getByteBufferInputStream(in, buffer));

                buffer.clear();
                buffer.limit(0);
                testWriteUTF(getByteBufferOutputStream(out, buffer), getByteBufferInputStream(in, buffer));
            }
        }
    }

    public void testWriteReadByte(OutputStream os, InputStream is) {
        for (int i = 0; i < 256; ++i) {
            try {
                os.write(i);
            } catch (IOException e) {
                fail(e.getMessage());
            }
        }
        try {
            os.flush();
            os.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }


        for (int i = 0; i < 256; ++i) {
            try {
                assertEquals(i, is.read());
            } catch (IOException e) {
                fail(e.getMessage());
            }
        }

        // can't test -1
        // try {
        // assertEquals(-1, is.read());
        // is.close();
        // } catch (IOException e) {
        // fail(e.getMessage());
        // }
    }

    public void testWriteReadByteInterleave(OutputStream os, InputStream is) {
        for (int i = 0; i < 256; ++i) {
            try {
                os.write(i);
                os.flush();
                assertEquals(i, is.read());
            } catch (IOException e) {
                fail(e.getMessage());
            }
        }

        try {
            os.close();
            is.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public void testWriteBytes(OutputStream os, InputStream is) {
        byte[] bytes = new byte[1025];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte) (i * i);
        }

        try {
            os.write(bytes, 1, 1024);
            os.flush();
            for (int i = 1; i < bytes.length; ++i) {
                assertEquals(bytes[i], (byte) is.read());
            }
            os.close();
            is.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public void testWriteByteBuffer(ByteBufferOutputStream os, InputStream is) {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
        for (int i = 0; i < 1024; ++i) {
            byteBuffer.put((byte) (i * i));
        }

        try {
            byteBuffer.flip();
            os.write(byteBuffer);
            os.flush();
            byteBuffer.flip();

            for (int i = 0; i < 1024; ++i) {
                assertEquals(byteBuffer.get(), (byte) is.read());
            }
            os.close();
            is.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public void testAppendBytesCharSequence(ByteBufferOutputStream os, InputStream is) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; ++i) {
            char c = (char) ((int) 'a' + (i * i) % 26);
            sb.append(c);
        }

        try {
            os.appendBytes(sb.toString());
            os.flush();
            for (int i = 0; i < 1024; ++i) {
                assertEquals((byte) sb.charAt(i), (byte) is.read());
            }
            os.close();
            is.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public void testAppendCharsCharSequence(ByteBufferOutputStream os, InputStream is) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; ++i) {
            char c = (char) ((int) 'a' + (i * i) % 26);
            sb.append(c);
        }

        try {
            os.appendChars(sb.toString());
            os.flush();
            for (int i = 0; i < 1024; ++i) {
                assertEquals(sb.charAt(i), (char) ((is.read() << 8) | is.read()));
            }
            os.close();
            is.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public void testWriteUTF(ByteBufferOutputStream os, DataInput is) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; ++i) {
            char c = (char) ((int) 'a' + (i * i) % 26);
            sb.append(c);
        }

        try {
            os.writeUTF(sb.toString());
            os.flush();

            String utf = is.readUTF();

            assertEquals(sb.toString(), utf);

            os.close();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }
}

