/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.time.Duration;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from AWS S3 instances.
 */
final class S3SeekableChannelProvider implements SeekableChannelsProvider {

    private final S3AsyncClient s3AsyncClient;
    private final int fragmentSize;
    private final int maxCacheSize;
    private final int readAheadCount;
    private final Duration readTimeout;

    S3SeekableChannelProvider(final S3Instructions s3Instructions) {
        final SdkAsyncHttpClient asyncHttpClient = AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(s3Instructions.maxConcurrentRequests())
                .connectionTimeout(s3Instructions.connectionTimeout())
                .build();
        this.s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(s3Instructions.awsRegionName()))
                .httpClient(asyncHttpClient)
                .build();
        this.fragmentSize = s3Instructions.fragmentSize();
        this.maxCacheSize = s3Instructions.maxCacheSize();
        this.readAheadCount = s3Instructions.readAheadCount();
        this.readTimeout = s3Instructions.readTimeout();
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelsProvider.ChannelContext context,
            @NotNull final URI uri) {
        return new S3SeekableByteChannel(context, uri, s3AsyncClient, fragmentSize, readAheadCount, readTimeout);
    }

    @Override
    public ChannelContext makeContext() {
        return new S3SeekableByteChannel.S3ChannelContext(maxCacheSize);
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append) {
        throw new UnsupportedOperationException("Don't support writing to S3 yet");
    }

    public void close() {
        s3AsyncClient.close();
    }
}
