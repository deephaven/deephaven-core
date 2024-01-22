/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.parquet.base.util.SeekableChannelContext;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from AWS S3 instances.
 */
final class S3SeekableChannelProvider implements SeekableChannelsProvider {

    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    S3SeekableChannelProvider(final S3Instructions s3Instructions) {
        final SdkAsyncHttpClient asyncHttpClient = AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(s3Instructions.maxConcurrentRequests())
                .connectionTimeout(s3Instructions.connectionTimeout())
                .build();
        this.s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(s3Instructions.awsRegionName()))
                .httpClient(asyncHttpClient)
                .build();
        this.s3Instructions = s3Instructions;
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) {
        // context is unused here, will be set before reading from the channel
        return new S3SeekableByteChannel(uri, s3AsyncClient, s3Instructions);
    }

    @Override
    public SeekableChannelContext makeContext() {
        return new S3SeekableByteChannel.S3ChannelContext(s3Instructions.maxCacheSize());
    }

    @Override
    public boolean isCompatibleWith(@NotNull final SeekableChannelContext channelContext) {
        // A null context implies no caching or read ahead
        return channelContext == SeekableChannelContext.NULL
                || channelContext instanceof S3SeekableByteChannel.S3ChannelContext;
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append) {
        throw new UnsupportedOperationException("Writing to S3 is currently unsupported");
    }

    public void close() {
        s3AsyncClient.close();
    }
}
