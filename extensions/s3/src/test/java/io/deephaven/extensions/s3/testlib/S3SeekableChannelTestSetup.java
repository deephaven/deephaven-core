//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3.testlib;

import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.extensions.s3.S3SeekableChannelProviderPlugin;
import io.deephaven.util.channel.SeekableChannelsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class S3SeekableChannelTestSetup {

    protected ExecutorService executor;
    protected S3AsyncClient asyncClient;
    protected String bucket;

    protected abstract S3AsyncClient s3AsyncClient();

    protected abstract S3Instructions.Builder s3Instructions(S3Instructions.Builder builder);

    protected final void doSetUp() throws ExecutionException, InterruptedException, TimeoutException {
        executor = Executors.newCachedThreadPool();
        bucket = UUID.randomUUID().toString();
        asyncClient = s3AsyncClient();
        asyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).get(5, TimeUnit.SECONDS);
    }

    protected final void doTearDown() throws ExecutionException, InterruptedException, TimeoutException {
        S3Helper.deleteAllKeys(asyncClient, bucket);
        asyncClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()).get(5, TimeUnit.SECONDS);
        asyncClient.close();
        executor.shutdownNow();
    }

    protected void uploadDirectory(final Path directory, final String prefix)
            throws ExecutionException, InterruptedException, TimeoutException {
        S3Helper.uploadDirectory(asyncClient, directory, bucket, prefix, Duration.ofSeconds(5));
    }

    protected final URI uri(String key) {
        return URI.create(String.format("s3://%s/%s", bucket, key));
    }

    protected final void putObject(String key, AsyncRequestBody body)
            throws ExecutionException, InterruptedException, TimeoutException {
        asyncClient.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), body).get(5,
                TimeUnit.SECONDS);
    }

    protected final SeekableChannelsProvider providerImpl(URI uri) {
        final S3SeekableChannelProviderPlugin plugin = new S3SeekableChannelProviderPlugin();
        final S3Instructions instructions = s3Instructions(S3Instructions.builder()).build();
        return plugin.createProvider(uri, instructions);
    }

    protected static ByteBuffer readAll(ReadableByteChannel channel, int maxBytes) throws IOException {
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
