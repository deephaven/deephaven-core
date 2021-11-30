/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.file;

import io.deephaven.configuration.Configuration;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Test case for {@link FileHandle}.
 */
public class TestFileHandle {

    private static final byte[] DATA = new byte[] {(byte) -1, (byte) 1, Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0};

    private File file;
    private FileHandle FHUT;

    @Before
    public void setup() throws IOException {
        file = File.createTempFile("TestFileHandle-", ".dat", new File(Configuration.getInstance().getWorkspacePath()));
        FHUT = new FileHandle(FileChannel.open(file.toPath(),
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE),
                () -> {
                });
    }

    @After
    public void tearDown() throws IOException {
        TestCase.assertTrue(FHUT.isOpen());
        FHUT.close();
        TestCase.assertFalse(FHUT.isOpen());
        tryToDelete(file);
    }

    @Test
    public void testFileHandle() throws IOException {
        TestCase.assertEquals(0, FHUT.size());
        TestCase.assertEquals(0, FHUT.position());

        final ByteBuffer readBuffer = ByteBuffer.allocate(DATA.length);
        final ByteBuffer writeBuffer = ByteBuffer.wrap(DATA);

        FHUT.write(writeBuffer, 10);
        TestCase.assertEquals(10 + DATA.length, FHUT.size());
        TestCase.assertEquals(0, FHUT.position());

        TestCase.assertEquals(DATA.length, FHUT.read(readBuffer, 10));
        TestCase.assertEquals(10 + DATA.length, FHUT.size());
        TestCase.assertEquals(0, FHUT.position());
        TestCase.assertEquals(DATA.length, readBuffer.position());
        for (int bi = 0; bi < DATA.length; ++bi) {
            TestCase.assertEquals(DATA[bi], readBuffer.get(bi));
        }

        readBuffer.clear();
        writeBuffer.clear();

        FHUT.position(5);
        FHUT.write(writeBuffer);
        TestCase.assertEquals(10 + DATA.length, FHUT.size());
        TestCase.assertEquals(5 + DATA.length, FHUT.position());

        FHUT.position(5);
        TestCase.assertEquals(DATA.length, FHUT.read(readBuffer));
        TestCase.assertEquals(10 + DATA.length, FHUT.size());
        TestCase.assertEquals(5 + DATA.length, FHUT.position());
        TestCase.assertEquals(DATA.length, readBuffer.position());
        for (int bi = 0; bi < DATA.length; ++bi) {
            TestCase.assertEquals(DATA[bi], readBuffer.get(bi));
        }

        FHUT.truncate(5);
        TestCase.assertEquals(5, FHUT.size());
        TestCase.assertEquals(5, FHUT.position());

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
        TestCase.assertTrue(deleted);
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
