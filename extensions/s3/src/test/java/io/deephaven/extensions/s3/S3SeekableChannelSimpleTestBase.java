//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.extensions.s3.testlib.S3SeekableChannelTestSetup;
import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.CompletableOutputStream;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import junit.framework.TestCase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

abstract class S3SeekableChannelSimpleTestBase extends S3SeekableChannelTestSetup {

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        doSetUp();
    }

    @AfterEach
    void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
        doTearDown();
    }

    @Test
    void readSimpleFiles()
            throws IOException, URISyntaxException, ExecutionException, InterruptedException, TimeoutException {
        uploadDirectory(Path.of(S3SeekableChannelSimpleTestBase.class.getResource("readSimpleFiles").toURI()), null);
        {
            final URI uri = uri("empty.txt");
            final ByteBuffer buffer = ByteBuffer.allocate(1);
            try (
                    final SeekableChannelsProvider providerImpl = providerImpl();
                    final SeekableChannelsProvider provider = CachedChannelProvider.create(providerImpl, 32);
                    final SeekableChannelContext context = provider.makeContext();
                    final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
                assertThat(readChannel.read(buffer)).isEqualTo(-1);
            }
        }
        {
            final URI uri = uri("hello/world.txt");
            try (
                    final SeekableChannelsProvider providerImpl = providerImpl();
                    final SeekableChannelsProvider provider = CachedChannelProvider.create(providerImpl, 32);
                    final SeekableChannelContext context = provider.makeContext();
                    final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
                final ByteBuffer bytes = readAll(readChannel, 32);
                assertThat(bytes).isEqualTo(ByteBuffer.wrap("Hello, world!".getBytes(StandardCharsets.UTF_8)));
            }
        }
    }

    @Test
    void read32MiB() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        final int numBytes = 33554432;
        putObject("32MiB.bin", AsyncRequestBody.fromInputStream(new InputStream() {
            @Override
            public int read() {
                return 42;
            }
        }, (long) numBytes, executor));
        final URI uri = uri("32MiB.bin");
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        try (
                final SeekableChannelsProvider providerImpl = providerImpl();
                final SeekableChannelsProvider provider = CachedChannelProvider.create(providerImpl, 32);
                final SeekableChannelContext context = provider.makeContext();
                final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
            for (long p = 0; p < numBytes; ++p) {
                assertThat(readChannel.read(buffer)).isEqualTo(1);
                assertThat(buffer.get(0)).isEqualTo((byte) 42);
                buffer.clear();
            }
            assertThat(readChannel.read(buffer)).isEqualTo(-1);
        }
    }

    @Test
    void readWriteTest() throws IOException {
        final URI uri = uri("writeReadTest.txt");
        final String content = "Hello, world!";
        final byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        try (
                final SeekableChannelsProvider providerImpl = providerImpl();
                final SeekableChannelsProvider provider = CachedChannelProvider.create(providerImpl, 32);
                final CompletableOutputStream outputStream = provider.getOutputStream(uri, 0)) {
            final int numBytes = 36 * 1024 * 1024; // 36 Mib -> Three 10-MiB parts + One 6-MiB part
            final int numIters = numBytes / contentBytes.length;
            for (int i = 0; i < numIters; ++i) {
                outputStream.write(contentBytes);
            }
            outputStream.flush();
            outputStream.flush();
            outputStream.write(contentBytes);
            outputStream.flush();
            outputStream.flush();
            outputStream.done();
            outputStream.flush();
            try {
                outputStream.write(contentBytes);
                TestCase.fail("Failure expected on writing since the stream is marked as done.");
            } catch (IOException expected) {
            }

            // Push data to S3, but don't close the stream
            outputStream.complete();
            try (
                    final SeekableChannelContext context = provider.makeContext();
                    final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
                final ByteBuffer buffer = ByteBuffer.allocate(contentBytes.length);
                // We wrote total of numIters + 1 times
                for (int i = 0; i < numIters + 1; ++i) {
                    fillBuffer(readChannel, buffer);
                    assertThat(buffer).isEqualTo(ByteBuffer.wrap(contentBytes));
                    buffer.clear();
                }
                // We should have read all the data from the channel
                assertThat(readChannel.read(buffer)).isEqualTo(-1);
            }

            // Try rollback, should not delete the file
            outputStream.rollback();
            try (
                    final SeekableChannelContext context = provider.makeContext();
                    final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
                final ByteBuffer buffer = ByteBuffer.allocate(contentBytes.length);
                readChannel.read(buffer);
                buffer.flip();
                assertThat(buffer).isEqualTo(ByteBuffer.wrap(contentBytes));
            }
        }
    }
}
