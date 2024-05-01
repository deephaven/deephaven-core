//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;


import io.deephaven.extensions.s3.testlib.S3Helper;
import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class S3SeekableChannelTestBase {

    public abstract S3AsyncClient s3AsyncClient();

    public abstract S3Instructions.Builder s3Instructions(S3Instructions.Builder builder);

    private ExecutorService executor;
    private S3AsyncClient asyncClient;
    private String bucket;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        executor = Executors.newCachedThreadPool();
        bucket = UUID.randomUUID().toString();
        asyncClient = s3AsyncClient();
        asyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
        S3Helper.deleteAllKeys(asyncClient, bucket);
        asyncClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()).get(5, TimeUnit.SECONDS);
        asyncClient.close();
        executor.shutdownNow();
    }

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
                Path.of(S3SeekableChannelTestBase.class.getResource(resourceDir).toURI()),
                bucket,
                null,
                Duration.ofSeconds(5));
    }

    private URI uri(String key) {
        return URI.create(String.format("s3://%s/%s", bucket, key));
    }

    private void putObject(String key, AsyncRequestBody body)
            throws ExecutionException, InterruptedException, TimeoutException {
        asyncClient.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), body).get(5,
                TimeUnit.SECONDS);
    }

    private SeekableChannelsProvider providerImpl(URI uri) {
        final S3SeekableChannelProviderPlugin plugin = new S3SeekableChannelProviderPlugin();
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
