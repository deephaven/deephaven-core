//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
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
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.deephaven.base.FileUtils.REPEATED_URI_SEPARATOR;
import static io.deephaven.base.FileUtils.REPEATED_URI_SEPARATOR_PATTERN;
import static io.deephaven.base.FileUtils.URI_SEPARATOR;
import static io.deephaven.extensions.s3.S3ChannelContext.handleS3Exception;
import static io.deephaven.extensions.s3.S3SeekableChannelProviderPlugin.S3_URI_SCHEME;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from an S3-compatible API.
 */
final class S3SeekableChannelProvider implements SeekableChannelsProvider {

    private static final int MAX_KEYS_PER_BATCH = 1000;

    private static final Logger log = LoggerFactory.getLogger(S3SeekableChannelProvider.class);

    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    /**
     * A shared cache for S3 requests. This cache is shared across all S3 channels created by this provider.
     */
    private final S3RequestCache sharedCache;

    S3SeekableChannelProvider(@NotNull final S3Instructions s3Instructions) {
        // TODO(deephaven-core#5062): Add support for async client recovery and auto-close
        // TODO(deephaven-core#5063): Add support for caching clients for re-use
        this.s3AsyncClient = buildClient(s3Instructions);
        this.sharedCache = new S3RequestCache();
        this.s3Instructions = s3Instructions;
    }

    private static S3AsyncClient buildClient(@NotNull final S3Instructions s3Instructions) {
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
        if (log.isDebugEnabled()) {
            log.debug().append("Building client with instructions: ").append(s3Instructions).endl();
        }
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
        return new S3ChannelContext(s3AsyncClient, s3Instructions, sharedCache);
    }

    @Override
    public SeekableChannelContext makeSingleUseContext() {
        return new S3ChannelContext(s3AsyncClient, s3Instructions.singleUse(), sharedCache);
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
    public Stream<URI> list(@NotNull final URI directory) {
        if (log.isDebugEnabled()) {
            log.debug().append("Fetching child URIs for directory: ").append(directory.toString()).endl();
        }
        return createStream(directory, false);
    }

    @Override
    public Stream<URI> walk(@NotNull final URI directory) {
        if (log.isDebugEnabled()) {
            log.debug().append("Performing recursive traversal from directory: ").append(directory.toString()).endl();
        }
        return createStream(directory, true);
    }

    private Stream<URI> createStream(@NotNull final URI directory, final boolean isRecursive) {
        // The following iterator fetches URIs from S3 in batches and creates a stream
        final Iterator<URI> iterator = new Iterator<>() {
            private final String bucketName;
            private final String directoryKey;

            private Iterator<URI> currentBatchIt;
            private String continuationToken;

            {
                final S3Uri s3DirectoryURI = s3AsyncClient.utilities().parseUri(directory);
                bucketName = s3DirectoryURI.bucket().orElseThrow();
                directoryKey = s3DirectoryURI.key().orElseThrow();
            }

            @Override
            public boolean hasNext() {
                if (currentBatchIt != null) {
                    if (currentBatchIt.hasNext()) {
                        return true;
                    }
                    // End of current batch
                    if (continuationToken == null) {
                        // End of the directory
                        return false;
                    }
                }
                try {
                    fetchNextBatch();
                } catch (final IOException e) {
                    throw new UncheckedDeephavenException("Failed to fetch next batch of URIs from S3", e);
                }
                Assert.neqNull(currentBatchIt, "currentBatch");
                return currentBatchIt.hasNext();
            }

            @Override
            public URI next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more URIs available in the directory");
                }
                return currentBatchIt.next();
            }

            private void fetchNextBatch() throws IOException {
                final ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(directoryKey)
                        .maxKeys(MAX_KEYS_PER_BATCH);
                if (!isRecursive) {
                    // Add a delimiter to the request if we don't want to fetch all files recursively
                    requestBuilder.delimiter("/");
                }
                final long readTimeoutNanos = s3Instructions.readTimeout().toNanos();
                final ListObjectsV2Request request = requestBuilder.continuationToken(continuationToken).build();
                final ListObjectsV2Response response;
                try {
                    response = s3AsyncClient.listObjectsV2(request).get(readTimeoutNanos, TimeUnit.NANOSECONDS);
                } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
                    throw handleS3Exception(e, String.format("fetching list of files in directory %s", directory),
                            s3Instructions);
                }
                currentBatchIt = response.contents().stream()
                        .filter(s3Object -> !s3Object.key().equals(directoryKey))
                        .map(s3Object -> {
                            String path = "/" + s3Object.key();
                            if (path.contains(REPEATED_URI_SEPARATOR)) {
                                path = REPEATED_URI_SEPARATOR_PATTERN.matcher(path).replaceAll(URI_SEPARATOR);
                            }
                            try {
                                return new URI(S3_URI_SCHEME, directory.getUserInfo(), directory.getHost(),
                                        directory.getPort(), path, null, null);
                            } catch (final URISyntaxException e) {
                                throw new UncheckedDeephavenException("Failed to create URI for S3 object with key: "
                                        + s3Object.key() + " and bucket " + bucketName + " inside directory "
                                        + directory, e);
                            }
                        }).iterator();
                // The following token is null when the last batch is fetched.
                continuationToken = response.nextContinuationToken();
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.NONNULL), false);
    }

    @Override
    public void close() {
        s3AsyncClient.close();
        sharedCache.clear();
    }
}
