//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.file;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.*;

/**
 * Test case for {@link FileHandle}.
 */
public class TestFileHandle {

    private static final byte[] DATA = new byte[] {(byte) -1, (byte) 1, Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0};

    private File file;
    private FileHandle FHUT;

    @Before
    public void setup() throws IOException {
        file = File.createTempFile("TestFileHandle-", ".dat");
        FHUT = FileHandle.open(file.toPath(), () -> () -> {
        }, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
    }

    @After
    public void tearDown() throws IOException {
        assertTrue(FHUT.isOpen());
        FHUT.close();
        assertFalse(FHUT.isOpen());
        tryToDelete(file);
    }

    @Test
    public void testFileHandle() throws IOException {
        assertEquals(0, FHUT.size());
        assertEquals(0, FHUT.position());

        final ByteBuffer readBuffer = ByteBuffer.allocate(DATA.length);
        final ByteBuffer writeBuffer = ByteBuffer.wrap(DATA);

        FHUT.write(writeBuffer, 10);
        assertEquals(10 + DATA.length, FHUT.size());
        assertEquals(0, FHUT.position());

        assertEquals(DATA.length, FHUT.read(readBuffer, 10));
        assertEquals(10 + DATA.length, FHUT.size());
        assertEquals(0, FHUT.position());
        assertEquals(DATA.length, readBuffer.position());
        for (int bi = 0; bi < DATA.length; ++bi) {
            assertEquals(DATA[bi], readBuffer.get(bi));
        }

        readBuffer.clear();
        writeBuffer.clear();

        FHUT.position(5);
        FHUT.write(writeBuffer);
        assertEquals(10 + DATA.length, FHUT.size());
        assertEquals(5 + DATA.length, FHUT.position());

        FHUT.position(5);
        assertEquals(DATA.length, FHUT.read(readBuffer));
        assertEquals(10 + DATA.length, FHUT.size());
        assertEquals(5 + DATA.length, FHUT.position());
        assertEquals(DATA.length, readBuffer.position());
        for (int bi = 0; bi < DATA.length; ++bi) {
            assertEquals(DATA[bi], readBuffer.get(bi));
        }

        FHUT.truncate(5);
        assertEquals(5, FHUT.size());
        assertEquals(5, FHUT.position());

        FHUT.force();
    }

    /**
     * Utility for file deletion in unit tests. <b>Note:</b> Each attempt after the first failure is preceded by an
     * invocation of the garbage collector.
     *
     * @param file The file to delete
     * @param maxRetries The number of retries
     */
    private static void tryToDelete(final File file, final int maxRetries) {
        boolean deleted;
        for (int attempts = 0; !(deleted = file.delete()) && attempts < maxRetries; ++attempts) {
            System.gc();
        }
        assertTrue(deleted);
    }

    private static final int DEFAULT_DELETE_RETRIES = 10;

    /**
     * Invokes the two-argument version of tryToDelete with a default number of retries.
     *
     * @param file The file to delete
     */
    public static void tryToDelete(final File file) {
        tryToDelete(file, DEFAULT_DELETE_RETRIES);
    }
}
