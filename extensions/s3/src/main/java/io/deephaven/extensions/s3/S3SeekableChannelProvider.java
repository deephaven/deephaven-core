//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.util.channel.Channels;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static io.deephaven.extensions.s3.S3ChannelContext.handleS3Exception;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from an S3-compatible API.
 */
final class S3SeekableChannelProvider implements SeekableChannelsProvider {

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
        this.s3AsyncClient = buildClient(s3Instructions);
        this.s3Instructions = s3Instructions;
    }

    private static S3AsyncClient buildClient(@NotNull S3Instructions s3Instructions) {
        final S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .httpClient(AwsCrtAsyncHttpClient.builder()
                        .maxConcurrency(s3Instructions.maxConcurrentRequests())
                        .connectionTimeout(s3Instructions.connectionTimeout())
                        .build())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        // If we find that the STANDARD retry policy does not work well in all situations, we might
                        // try experimenting with ADAPTIVE retry policy, potentially with fast fail.
                        // .retryPolicy(RetryPolicy.builder(RetryMode.ADAPTIVE).fastFailRateLimiting(true).build())
                        .retryPolicy(RetryMode.STANDARD)
                        .apiCallAttemptTimeout(s3Instructions.readTimeout().dividedBy(3))
                        .apiCallTimeout(s3Instructions.readTimeout())
                        // Adding a metrics publisher may be useful for debugging, but it's very verbose.
                        // .addMetricPublisher(LoggingMetricPublisher.create(Level.INFO, Format.PRETTY))
                        .build())
                .region(Region.of(s3Instructions.regionName()))
                .credentialsProvider(s3Instructions.awsV2CredentialsProvider());
        s3Instructions.endpointOverride().ifPresent(builder::endpointOverride);
        return builder.build();
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) {
        final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
        // context is unused here, will be set before reading from the channel
        return new S3SeekableByteChannel(s3Uri);
    }

    @Override
    public InputStream getInputStream(SeekableByteChannel channel) {
        // S3SeekableByteChannel is internally buffered, no need to re-buffer
        return Channels.newInputStreamNoClose(channel);
    }

    @Override
    public SeekableChannelContext makeContext() {
        return new S3ChannelContext(s3AsyncClient, s3Instructions, BUFFER_POOL);
    }

    @Override
    public SeekableChannelContext makeSingleUseContext() {
        return new S3ChannelContext(s3AsyncClient, s3Instructions.singleUse(), BUFFER_POOL);
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
    public void applyToChildURIs(@NotNull final URI directoryURI, @NotNull final Consumer<URI> processor)
            throws IOException {
        applyToChildURIsHelper(directoryURI, processor, false);
    }

    @Override
    public void applyToChildURIsRecursively(@NotNull final URI directoryURI, @NotNull final Consumer<URI> processor)
            throws IOException {
        applyToChildURIsHelper(directoryURI, processor, true);
    }

    private void applyToChildURIsHelper(@NotNull final URI directoryURI, @NotNull final Consumer<URI> processor,
            final boolean isRecursive) throws IOException {
        final S3Uri s3DirectoryURI = s3AsyncClient.utilities().parseUri(directoryURI);
        final String bucketName = s3DirectoryURI.bucket().orElseThrow();
        final String directoryKey = s3DirectoryURI.key().orElseThrow();
        final ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(s3DirectoryURI.key().orElseThrow());
        if (!isRecursive) {
            // Add a delimiter to the request if we don't want to fetch all files recursively
            requestBuilder.delimiter("/");
        }
        String continuationToken = null;
        final long readTimeoutNanos = s3Instructions.readTimeout().toNanos();
        ListObjectsV2Response response;
        do {
            final ListObjectsV2Request request = requestBuilder.continuationToken(continuationToken).build();
            try {
                response = s3AsyncClient.listObjectsV2(request).get(readTimeoutNanos, TimeUnit.NANOSECONDS);
            } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
                throw handleS3Exception(e, String.format("fetching list of files in directory %s", directoryURI),
                        s3Instructions);
            }
            response.contents().stream()
                    .filter(s3Object -> !s3Object.key().equals(directoryKey)) // Skip the directory itself
                    .map(s3Object -> URI.create("s3://" + bucketName + "/" + s3Object.key()))
                    .forEach(processor);

            // If the response is truncated, fetch the next page using the continuation token from the response
            // Usually the response is truncated when there are more than 1000 objects in the bucket
            // TODO Test this
            continuationToken = response.nextContinuationToken();
        } while (response.isTruncated());
    }

    @Override
    public void close() {
        s3AsyncClient.close();
    }
}
