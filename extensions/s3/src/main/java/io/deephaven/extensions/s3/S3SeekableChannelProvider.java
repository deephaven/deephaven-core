/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderBase;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Uri;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from AWS S3 instances.
 */
final class S3SeekableChannelProvider extends SeekableChannelsProviderBase {

    /**
     * We always allocate buffers of maximum allowed size for re-usability across reads with different fragment sizes.
     * There can be a performance penalty though if the fragment size is much smaller than the maximum size.
     */
    private static final BufferPool BUFFER_POOL = new BufferPool(S3Instructions.MAX_FRAGMENT_SIZE);

    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    S3SeekableChannelProvider(@NotNull final S3Instructions s3Instructions) {
        // TODO(deephaven-core#5062): Add support for async client recovery and auto-close
        // TODO(deephaven-core#5063): Add support for caching clients for re-use
        final S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .httpClient(AwsCrtAsyncHttpClient.builder()
                        .maxConcurrency(s3Instructions.maxConcurrentRequests())
                        .connectionTimeout(s3Instructions.connectionTimeout())
                        .build())
                .overrideConfiguration(b -> b
                        // .addMetricPublisher(LoggingMetricPublisher.create(Level.INFO, Format.PRETTY))
                        // .retryPolicy(RetryPolicy.builder(RetryMode.ADAPTIVE).fastFailRateLimiting(true).build())
                        .retryPolicy(RetryMode.STANDARD)
                        .apiCallAttemptTimeout(s3Instructions.readTimeout().dividedBy(3))
                        .apiCallTimeout(s3Instructions.readTimeout()))
                .region(Region.of(s3Instructions.regionName()))
                .credentialsProvider(s3Instructions.awsV2CredentialsProvider());
        s3Instructions.endpointOverride().ifPresent(builder::endpointOverride);
        this.s3AsyncClient = builder.build();
        this.s3Instructions = s3Instructions;
    }

    @Override
    protected boolean readChannelIsBuffered() {
        // io.deephaven.extensions.s3.S3SeekableByteChannel is buffered based on context / options
        return true;
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) {
        final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
        // context is unused here, will be set before reading from the channel
        return new S3SeekableByteChannel(s3Uri);
    }

    @Override
    public SeekableChannelContext makeContext() {
        return new S3ChannelContext(s3AsyncClient, s3Instructions, BUFFER_POOL);
    }

    @Override
    public SeekableChannelContext makeSingleUseContext() {
        return new S3ChannelContext(s3AsyncClient, s3Instructions.withReadAheadCount(0), BUFFER_POOL);
    }

    @Override
    public boolean isCompatibleWith(@NotNull final SeekableChannelContext channelContext) {
        return channelContext instanceof S3ChannelContext;
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append) {
        throw new UnsupportedOperationException("Writing to S3 is currently unsupported");
    }

    @Override
    public void close() {
        s3AsyncClient.close();
    }
}
