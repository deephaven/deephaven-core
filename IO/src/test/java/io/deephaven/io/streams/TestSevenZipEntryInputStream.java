//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.streams;

import io.deephaven.io.CompressedFileUtil;
import io.deephaven.io.InputStreamFactory;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestSevenZipEntryInputStream {
    /**
     * {@code lines.txt.7z} holds a single LZMA-compressed entry, {@code lines.txt}, made with
     * {@code 7z a -t7z -m0=LZMA lines.txt.7z lines.txt} from the text that {@link #expectedLines()} returns.
     */
    private static final String RESOURCE = "lines.txt.7z";

    @Test
    public void testReadFirstEntry() throws IOException, URISyntaxException {
        try (final SevenZipEntryInputStream in = openResource()) {
            assertEquals("lines.txt", in.getEntryName());
            assertArrayEquals(expectedLines().getBytes(StandardCharsets.UTF_8), readFully(in));
        }
    }

    @Test
    public void testSingleByteReads() throws IOException, URISyntaxException {
        try (final SevenZipEntryInputStream in = openResource()) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            int value;
            while ((value = in.read()) != -1) {
                out.write(value);
            }
            assertArrayEquals(expectedLines().getBytes(StandardCharsets.UTF_8), out.toByteArray());
        }
    }

    @Test
    public void testOpenPossiblyCompressedFileName() throws IOException, URISyntaxException {
        try (final InputStream in = CompressedFileUtil.openPossiblyCompressedFile(resourcePath().toString())) {
            assertArrayEquals(expectedLines().getBytes(StandardCharsets.UTF_8), readFully(in));
        }
    }

    @Test
    public void testOpenPossiblyCompressedStreamFactory() throws IOException {
        try (final InputStream in = CompressedFileUtil.openPossiblyCompressedFile(resourceFactory())) {
            assertArrayEquals(expectedLines().getBytes(StandardCharsets.UTF_8), readFully(in));
        }
    }

    private static SevenZipEntryInputStream openResource() throws IOException, URISyntaxException {
        final Path path = resourcePath();
        return SevenZipEntryInputStream.openFirstEntry(SevenZFile.builder().setPath(path).get(), path.toString());
    }

    private static Path resourcePath() throws URISyntaxException {
        final URL resource = TestSevenZipEntryInputStream.class.getResource(RESOURCE);
        assertNotNull(RESOURCE, resource);
        return Paths.get(resource.toURI());
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

    private static InputStreamFactory resourceFactory() {
        return new InputStreamFactory() {
            @Override
            public InputStream createInputStream() throws IOException {
                final InputStream in = TestSevenZipEntryInputStream.class.getResourceAsStream(RESOURCE);
                if (in == null) {
                    throw new IOException("Missing test resource " + RESOURCE);
                }
                return in;
            }

            @Override
            public String getDescription() {
                return RESOURCE;
            }
        };
    }
}
