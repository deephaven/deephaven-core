package io.deephaven.engine.util.file;

import io.deephaven.configuration.Configuration;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Test case for {@link TestTrackedSeekableByteChannel}.
 */
public class TestTrackedSeekableByteChannel {

    private File file;
    private TrackedSeekableByteChannel channel;
    private FileHandle handle;

    @Before
    public void setup() throws IOException {
        file = File.createTempFile("TestTrackedSeekableByteChannel-", ".dat", new File(Configuration.getInstance().getWorkspacePath()));
        channel = new TrackedSeekableByteChannel(
                f -> handle = new FileHandle(FileChannel.open(f.toPath(),
                        StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE),
                        () -> {
                        }),
                file);
    }

    @After
    public void tearDown() throws IOException {
        TestCase.assertFalse(channel.isOpen());
        TestFileHandle.tryToDelete(file);
    }

    @Test
    public void testChannel() throws IOException {
        handle.close();
        TestCase.assertEquals(0, channel.size());
        handle.close();
        TestCase.assertEquals(0, channel.position());

        handle.close();
        channel.write(ByteBuffer.wrap("Hello".getBytes()));
        handle.close();
        TestCase.assertEquals(5, channel.size());
        handle.close();
        TestCase.assertEquals(5, channel.position());

        handle.close();
        channel.position(1);
        ByteBuffer readBuffer = ByteBuffer.wrap(new byte[4]);
        handle.close();
        channel.read(readBuffer);
        TestCase.assertEquals("ello", new String(readBuffer.array()));

        handle.close();
        TestCase.assertEquals(5, channel.size());
        handle.close();
        TestCase.assertEquals(5, channel.position());

        handle.close();
        channel.truncate(1);
        handle.close();
        TestCase.assertEquals(1, channel.size());
        handle.close();
        TestCase.assertEquals(1, channel.position());

        handle.close();
        channel.position(0);
        readBuffer = ByteBuffer.wrap(new byte[1]);
        handle.close();
        channel.read(readBuffer);
        TestCase.assertEquals("H", new String(readBuffer.array()));

        handle.close();
        channel.position(0);
        handle.close();
        channel.write(ByteBuffer.wrap("Hello".getBytes()));
        handle.close();
        TestCase.assertEquals(5, channel.size());
        handle.close();
        TestCase.assertEquals(5, channel.position());
        handle.close();
        channel.write(ByteBuffer.wrap("World".getBytes()));
        handle.close();
        TestCase.assertEquals(10, channel.size());
        handle.close();
        TestCase.assertEquals(10, channel.position());

        handle.close();
        channel.position(5);
        readBuffer = ByteBuffer.wrap(new byte[5]);
        handle.close();
        channel.read(readBuffer);
        TestCase.assertEquals("World", new String(readBuffer.array()));

        channel.close();

        try {
            channel.position();
            TestCase.fail("Expected exception");
        } catch (ClosedChannelException expected) {
        }
    }
}
