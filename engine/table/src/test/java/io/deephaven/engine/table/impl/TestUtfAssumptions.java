//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class TestUtfAssumptions {

    @Test
    public void testUtfAssumptions1() throws IOException {
        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final ObjectOutputStream out;
        out = new ObjectOutputStream(outBytes);
        out.writeUTF("\0");
        out.flush();

        final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(outBytes.toByteArray()));
        assertEquals(4, in.available());
        final String bad = in.readUTF();
        assertEquals(1, bad.length());
        assertEquals('\0', bad.charAt(0));
        assertEquals(0, in.available());
    }

    static final byte MAGIC_NUMBER = (byte) 0b10001111;

    @Test
    public void testUtfAssumptions2() throws IOException {
        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final ObjectOutputStream out;
        out = new ObjectOutputStream(outBytes);
        out.writeShort(2);
        out.writeByte(MAGIC_NUMBER);
        out.writeByte(1);
        out.flush();

        final ObjectInputStream in1 = new ObjectInputStream(new ByteArrayInputStream(outBytes.toByteArray()));
        assertEquals(4, in1.available());
        assertEquals(2, in1.readUnsignedShort());
        assertEquals(MAGIC_NUMBER, in1.readByte());
        assertEquals(1, in1.readByte());
        assertEquals(0, in1.available());

        final ObjectInputStream in2 = new ObjectInputStream(new ByteArrayInputStream(outBytes.toByteArray()));
        assertEquals(4, in2.available());
        try {
            in2.readUTF();
            fail("Excpected UTF data format exception!");
        } catch (UTFDataFormatException e) {
            // Expected
        }
    }
}
