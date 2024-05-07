//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.extensions.s3.testlib.S3Helper;
import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

abstract class S3SeekableChannelCommonTests extends S3SeekableChannelTestBase {

    @Test
    void readSimpleFiles()
            throws IOException, URISyntaxException, ExecutionException, InterruptedException, TimeoutException {
        uploadDirectory("readSimpleFiles");
        {
            final URI uri = uri("empty.txt");
            final ByteBuffer buffer = ByteBuffer.allocate(1);
            try (
                    final SeekableChannelsProvider providerImpl = providerImpl(uri);
                    final SeekableChannelsProvider provider = new CachedChannelProvider(providerImpl, 32);
                    final SeekableChannelContext context = provider.makeContext();
                    final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
                assertThat(readChannel.read(buffer)).isEqualTo(-1);
            }
        }
        {
            final URI uri = uri("hello/world.txt");
            try (
                    final SeekableChannelsProvider providerImpl = providerImpl(uri);
                    final SeekableChannelsProvider provider = new CachedChannelProvider(providerImpl, 32);
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
                final SeekableChannelsProvider providerImpl = providerImpl(uri);
                final SeekableChannelsProvider provider = new CachedChannelProvider(providerImpl, 32);
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

    private void uploadDirectory(String resourceDir)
            throws URISyntaxException, ExecutionException, InterruptedException, TimeoutException {
        S3Helper.uploadDirectory(
                asyncClient,
                Path.of(io.deephaven.extensions.s3.S3SeekableChannelTestBase.class.getResource(resourceDir).toURI()),
                bucket,
                null,
                Duration.ofSeconds(5));
    }

    private SeekableChannelsProvider providerImpl(URI uri) {
        final SeekableChannelsProviderPlugin plugin = new S3SeekableChannelProviderPlugin();
        final S3Instructions instructions = s3Instructions(S3Instructions.builder()).build();
        return plugin.createProvider(uri, instructions);
    }

    private static ByteBuffer readAll(ReadableByteChannel channel, int maxBytes) throws IOException {
        final ByteBuffer dst = ByteBuffer.allocate(maxBytes);
        while (dst.remaining() > 0 && channel.read(dst) != -1) {
            // continue
        }
        if (dst.remaining() == 0) {
            if (channel.read(ByteBuffer.allocate(1)) != -1) {
                throw new RuntimeException(String.format("channel has more than %d bytes", maxBytes));
            }
        }
        dst.flip();
        return dst;
    }
}
