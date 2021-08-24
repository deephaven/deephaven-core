/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import io.deephaven.base.ArrayUtil;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class TestByteBufferStreams extends TestCase {

    // buffer sink and source for the test scripts
    private ByteBuffer[] buffers = new ByteBuffer[4];
    private int bufferPtr = 0;

    private static final int BUFSZ = 32;

    private final ByteBufferStreams.Sink SINK = new ByteBufferStreams.Sink() {
        @Override
        public ByteBuffer acceptBuffer(ByteBuffer b, int need) throws IOException {
            b.flip();
            buffers = ArrayUtil.put(buffers, bufferPtr++, b, ByteBuffer.class);
            return ByteBuffer.allocate(BUFSZ);
        }

        @Override
        public void close(ByteBuffer b) throws IOException {
            b.flip();
            buffers = ArrayUtil.put(buffers, bufferPtr++, b, ByteBuffer.class);
        }
    };

    private final ByteBufferStreams.Source SOURCE = new ByteBufferStreams.Source() {
        @Override
        public ByteBuffer nextBuffer(ByteBuffer lastBuffer) {
            ByteBuffer b = null;
            if (bufferPtr < buffers.length && (b = buffers[bufferPtr++]) != null) {
                buffers[bufferPtr - 1] = null;
            }
            return b;
        }
    };

    public void setUp() throws Exception {
        super.setUp();
    }

    private static class FLUSH {
    }
    private static class BYTES {
        String s;

        BYTES(String s) {
            this.s = s;
        }
    }
    private static class CHARS {
        String s;

        CHARS(String s) {
            this.s = s;
        }
    }

    public void script(Object... os) throws Exception {
        ByteBufferStreams.Output out =
            new ByteBufferStreams.Output(ByteBuffer.allocate(BUFSZ), SINK);
        ByteBufferStreams.Input in = new ByteBufferStreams.Input(null, SOURCE);
        Arrays.fill(buffers, null);
        bufferPtr = 0;

        for (Object o : os) {
            if (o.getClass() == Boolean.class) {
                out.writeBoolean((boolean) (Boolean) o);
            } else if (o.getClass() == Byte.class) {
                out.writeByte((byte) (Byte) o);
            } else if (o.getClass() == Short.class) {
                out.writeShort((short) (Short) o);
            } else if (o.getClass() == Character.class) {
                out.writeChar((char) (Character) o);
            } else if (o.getClass() == Integer.class) {
                out.writeInt((int) (Integer) o);
            } else if (o.getClass() == Long.class) {
                out.writeLong((long) (Long) o);
            } else if (o.getClass() == float.class) {
                out.writeFloat((float) (Float) o);
            } else if (o.getClass() == Double.class) {
                out.writeDouble((double) (Double) o);
            } else if (o.getClass() == String.class) {
                out.writeUTF((String) o);
            } else if (o.getClass() == BYTES.class) {
                out.writeBytes(((BYTES) o).s);
            } else if (o.getClass() == CHARS.class) {
                out.writeChars(((CHARS) o).s);
            } else if (o.getClass() == FLUSH.class) {
                out.flush();
            } else {
                fail("script can't handle object of class " + o.getClass() + ": " + o);
            }
        }

        out.close();
        in.setBuffer(buffers[0]);
        bufferPtr = 1;

        for (Object o : os) {
            if (o.getClass() == Boolean.class) {
                boolean v = in.readBoolean();
                assertEquals((boolean) (Boolean) o, v);
            } else if (o.getClass() == Byte.class) {
                int v = in.read();
                assertEquals((byte) (Byte) o, (byte) v);
            } else if (o.getClass() == Short.class) {
                short v = in.readShort();
                assertEquals((short) (Short) o, v);
            } else if (o.getClass() == Character.class) {
                final char v = in.readChar();
                assertEquals((char) (Character) o, v);
            } else if (o.getClass() == Integer.class) {
                final int v = in.readInt();
                assertEquals((int) (Integer) o, v);
            } else if (o.getClass() == Long.class) {
                final long v = in.readLong();
                assertEquals((long) (Long) o, v);
            } else if (o.getClass() == Float.class) {
                final float v = in.readFloat();
                assertEquals((float) (Float) o, v);
            } else if (o.getClass() == Double.class) {
                final double v = in.readDouble();
                assertEquals((double) (Double) o, v);
            } else if (o.getClass() == String.class) {
                String s = in.readUTF();
                assertEquals((String) o, s);
            } else if (o.getClass() == BYTES.class) {
                for (int i = 0; i < ((BYTES) o).s.length(); ++i) {
                    int v = in.read();
                    assertEquals((byte) ((BYTES) o).s.charAt(i), (byte) v);
                }
            } else if (o.getClass() == CHARS.class) {
                for (int i = 0; i < ((CHARS) o).s.length(); ++i) {
                    char v = in.readChar();
                    assertEquals(((CHARS) o).s.charAt(i), v);
                }
            } else if (o.getClass() == FLUSH.class) {
                // do nothing
            } else {
                fail("script can't handle object of class " + o.getClass() + ": " + o);
            }
        }
        assertEquals(-1, in.read());
    }

    public void testIt() throws Exception {
        script(true);
        script(false);
        script((byte) 123);
        script((byte) 255);
        script((short) 12345);
        script((short) 65535);
        script((int) 314159);
        script((int) Integer.MAX_VALUE);
        script((int) 4294967295L);
        script((long) 12345678901L);
        script((long) Long.MAX_VALUE);
        script((short) 3.14159);
        script((double) 3.14159);
        script((int) 314159, new FLUSH(), (int) 4711);
        script("123456789012345678901234567890", (int) 314159, "abcdefghijklmnopqrstuvwxyzabcd");
        script(new BYTES("123456789012345678901234567890"), (int) 314159,
            new BYTES("abcdefghijklmnopqrstuvwxyzabcd"));
        script(new CHARS("123456789012345"), (int) 314159,
            new CHARS("abcdefghijklmnopqrstuvwxyzabcd"));
    }
}
