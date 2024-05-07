//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.extensions.s3.testlib.S3Helper;
import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class S3SeekableChannelTestBase {

    public abstract S3AsyncClient s3AsyncClient();

    public abstract S3Instructions.Builder s3Instructions(S3Instructions.Builder builder);

    ExecutorService executor;
    protected S3AsyncClient asyncClient;
    protected String bucket;

    private final Collection<String> keys = new ArrayList<>();

    @BeforeEach
    protected void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        executor = Executors.newCachedThreadPool();
        bucket = UUID.randomUUID().toString();
        asyncClient = s3AsyncClient();
        asyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    protected void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
        S3Helper.deleteAllKeys(asyncClient, bucket);
        asyncClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()).get(5, TimeUnit.SECONDS);
        asyncClient.close();
        executor.shutdownNow();
    }

    protected URI uri(String key) {
        return URI.create(String.format("s3://%s/%s", bucket, key));
    }

    protected void putObject(String key, AsyncRequestBody body)
            throws ExecutionException, InterruptedException, TimeoutException {
        asyncClient.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), body).get(5,
                TimeUnit.SECONDS);
    }
}
