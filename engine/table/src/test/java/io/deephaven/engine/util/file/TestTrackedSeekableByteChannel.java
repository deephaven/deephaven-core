//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.file;

import junit.framework.TestCase;
import org.assertj.core.api.Assumptions;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Test case for {@link TestTrackedSeekableByteChannel}.
 */
public class TestTrackedSeekableByteChannel {

    private File file;
    private TrackedSeekableByteChannel channel;
    private FileHandle handle;

    @Before
    public void setup() throws IOException {
        file = File.createTempFile("TestTrackedSeekableByteChannel-", ".dat");
        channel = new TrackedSeekableByteChannel(this::makeAndSet, file);
    }

    private FileHandle makeAndSet(@NotNull final File file) throws IOException {
        handle = FileHandle.open(file.toPath(), () -> () -> {
        }, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        return handle;
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

    @Test
    public void fileHandleSafetyCheck() throws IOException {
        try {
            // We can only use this test if we have some confidence that the underlying filesystem implementation:
            //
            // 1. Provides non-null file keys
            // 2. Does not re-use file keys during a delete / re-create cycle
            //
            // At least in one GH Action CI case, this was not the case (so we need to gracefully bail out if the
            // filesystem can't support this test case)
            Assumptions.assumeThat(filesystemUsesNewKeys()).isTrue();
            fileHandleSafetyCheckImpl();
        } finally {
            channel.close();
        }
    }

    private void fileHandleSafetyCheckImpl() throws IOException {
        handle.close();
        // We are making the assumption that this delete / recreate will result in a _new_ file key
        Files.delete(file.toPath());
        Files.createFile(file.toPath());
        try {
            channel.size();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("The file key has changed during a refresh");
        }
        try {
            channel.read(ByteBuffer.allocate(1));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("The file key has changed during a refresh");
        }
        try {
            channel.write(ByteBuffer.allocate(1));
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("The file key has changed during a refresh");
        }
        try {
            channel.truncate(0);
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e).hasMessageContaining("The file key has changed during a refresh");
        }
    }

    private static boolean filesystemUsesNewKeys() throws IOException {
        final File file1 = File.createTempFile("fileHandleSafetyCheck-", ".txt");
        final Path path = file1.toPath();
        final Object key1;
        try {
            key1 = Files.readAttributes(path, BasicFileAttributes.class).fileKey();
            if (key1 == null) {
                return false;
            }
        } finally {
            Files.delete(path);
        }
        Files.createFile(path);
        final Object key2;
        try {
            key2 = Files.readAttributes(path, BasicFileAttributes.class).fileKey();
        } finally {
            Files.delete(path);
        }
        return !key1.equals(key2);
    }
}
