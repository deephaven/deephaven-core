//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.streams;

import io.deephaven.io.InputStreamFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestSevenZipInputStream {
    /**
     * {@code lines.txt.7z} holds a single LZMA-compressed entry, {@code lines.txt}, made with
     * {@code 7z a -t7z -m0=LZMA lines.txt.7z lines.txt} from the text that {@link #expectedLines()} returns.
     */
    private static final String RESOURCE = "lines.txt.7z";

    @Test
    public void testReadLzmaEntry() throws IOException {
        try (final SevenZipInputStream in = new SevenZipInputStream(resourceFactory(RESOURCE))) {
            final SevenZipInputStream.Entry entry = in.getNextEntry(SevenZipInputStream.Behavior.SKIP_WHEN_NO_STREAM);
            assertNotNull(entry);
            assertEquals("lines.txt", entry.getName());
            assertFalse(entry.isDirectory());

            final byte[] expected = expectedLines().getBytes(StandardCharsets.UTF_8);
            assertArrayEquals(expected, readFully(in));

            assertNull(in.getNextEntry(SevenZipInputStream.Behavior.SKIP_WHEN_NO_STREAM));
        }
    }

    @Test
    public void testSingleByteReads() throws IOException {
        try (final SevenZipInputStream in = new SevenZipInputStream(resourceFactory(RESOURCE))) {
            assertNotNull(in.getNextEntry(SevenZipInputStream.Behavior.SKIP_WHEN_NO_STREAM));

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            int value;
            while ((value = in.read()) != -1) {
                out.write(value);
            }
            assertArrayEquals(expectedLines().getBytes(StandardCharsets.UTF_8), out.toByteArray());
        }
    }

    private static String expectedLines() {
        final StringBuilder sb = new StringBuilder();
        for (long ii = 0; ii < 2000; ii++) {
            sb.append(ii).append(',').append(ii * ii).append(',').append((ii * 2654435761L) % 4294967296L)
                    .append('\n');
        }
        return sb.toString();
    }

    private static byte[] readFully(final InputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final byte[] buffer = new byte[1000];
        int read;
        while ((read = in.read(buffer, 0, buffer.length)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static InputStreamFactory resourceFactory(final String name) {
        return new InputStreamFactory() {
            @Override
            public InputStream createInputStream() throws IOException {
                final InputStream in = TestSevenZipInputStream.class.getResourceAsStream(name);
                if (in == null) {
                    throw new IOException("Missing test resource " + name);
                }
                return in;
            }

            @Override
            public String getDescription() {
                return name;
            }
        };
    }
}
