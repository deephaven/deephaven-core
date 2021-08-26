package io.deephaven.configuration;

import static org.junit.Assert.assertArrayEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import junit.framework.TestCase;
import org.apache.commons.compress.utils.IOUtils;

public class PropertyInputStreamLoaderTraditionalTest extends TestCase {

    private static final PropertyInputStreamLoaderTraditional loader = new PropertyInputStreamLoaderTraditional();

    private static InputStream open(String name) {
        return loader.openConfiguration(name);
    }

    public void testPriorityIs100() {
        assertEquals(100, loader.getPriority());
    }

    public void testContentFromResource() throws IOException {
        final byte[] bytes;
        try (final InputStream in = open("hello-world.prop")) {
            bytes = IOUtils.toByteArray(in);
        }
        assertArrayEquals("hello=world\n".getBytes(), bytes);
    }

    public void testContentFromFile() throws IOException, URISyntaxException {
        // ensure that the resource hello-world.prop is fully scoped out as a filesystem path
        String path = Paths
                .get(PropertyInputStreamLoaderTraditionalTest.class.getResource("/hello-world.prop").toURI())
                .toString();
        final byte[] bytes;
        try (final InputStream in = open(path)) {
            bytes = IOUtils.toByteArray(in);
        }
        assertArrayEquals("hello=world\n".getBytes(), bytes);
    }

    public void testMissingOpenException() {
        try {
            open("missing.prop");
            fail("Expecting ConfigurationException");
        } catch (ConfigurationException e) {
            assertTrue(e.getCause() instanceof FileNotFoundException);
            assertEquals("missing.prop (No such file or directory)", e.getCause().getMessage());
        }
    }


    public void testInvalidFilePath() {
        try {
            // the only "invalid" file path is one which includes a Nul char
            // see File.isInvalid()
            open("invalid-file.prop\0");
            fail("Expecting ConfigurationException");
        } catch (ConfigurationException e) {
            assertTrue(e.getCause() instanceof FileNotFoundException);
            assertEquals("Invalid file path", e.getCause().getMessage());
        }
    }
}
