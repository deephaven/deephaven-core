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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.deephaven.base.FileUtils.DUPLICATE_SLASH_PATTERN;
import static io.deephaven.extensions.s3.S3ChannelContext.handleS3Exception;
import static io.deephaven.extensions.s3.S3SeekableChannelProviderPlugin.S3_URI_SCHEME;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from an S3-compatible API.
 */
final class S3SeekableChannelProvider implements SeekableChannelsProvider {

    private static final int MAX_KEYS_PER_BATCH = 1000;

    private static final Logger log = LoggerFactory.getLogger(S3SeekableChannelProvider.class);

    /**
     * We always allocate buffers of maximum allowed size for re-usability across reads with different fragment sizes.
     * There can be a performance penalty though if the fragment size is much smaller than the maximum size.
     */
    private static final BufferPool BUFFER_POOL = new BufferPool(S3Instructions.MAX_FRAGMENT_SIZE);

    /**
     * Cache of shared {@link S3AsyncClient} objects and request caches, keyed by the S3 instructions.
     */
    private static final Map<S3Instructions, SharedClientData> SHARED_CLIENT_CACHE = new ConcurrentHashMap<>();

    private static class SharedClientData { // TODO better name
        private final S3AsyncClient client;
        private final S3RequestCache requestCache;

        SharedClientData(final S3AsyncClient client, final S3RequestCache requestCache) {
            this.client = client;
            this.requestCache = requestCache;
        }
    }

    // Local references of the shared client and request cache
    private final S3AsyncClient sharedAsyncClient;
    private final S3RequestCache sharedCache;

    private final S3Instructions instructions;

    S3SeekableChannelProvider(@NotNull final S3Instructions instructions) {
        final SharedClientData clientData = SHARED_CLIENT_CACHE.compute(instructions, (key, sharedClientData) -> {
            if (sharedClientData == null) {
                // No existing client, create a new one
                final S3AsyncClient newClient = buildClient(instructions);
                final S3RequestCache newCache = new ModuloBasedRequestCache(instructions.maxCacheSize());
                return new SharedClientData(newClient, newCache);
            } else {
                // Existing client, reconnect if necessary
                if (isConnectionOpen(sharedClientData.client)) {
                    return sharedClientData;
                }
                final S3AsyncClient newClient = buildClient(instructions);
                return new SharedClientData(newClient, sharedClientData.requestCache);
            }
        });
        this.instructions = instructions;
        this.sharedAsyncClient = clientData.client;
        this.sharedCache = clientData.requestCache;
    }

    /**
     * Check if the connection is open by making a light-weight request and catching any exceptions.
     */
    private static boolean isConnectionOpen(@NotNull final S3AsyncClient client) {
        try {
            client.listBuckets().get();
        } catch (final InterruptedException | ExecutionException | RuntimeException e) {
            return false;
        }
        return true;
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
        final S3Uri s3Uri = sharedAsyncClient.utilities().parseUri(uri);
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
        return new S3ChannelContext(sharedAsyncClient, instructions, BUFFER_POOL, sharedCache);
    }

    @Override
    public SeekableChannelContext makeSingleUseContext() {
        return new S3ChannelContext(sharedAsyncClient, instructions.singleUse(), BUFFER_POOL, sharedCache);
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
                final S3Uri s3DirectoryURI = sharedAsyncClient.utilities().parseUri(directory);
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
                final long readTimeoutNanos = instructions.readTimeout().toNanos();
                final ListObjectsV2Request request = requestBuilder.continuationToken(continuationToken).build();
                final ListObjectsV2Response response;
                try {
                    response = sharedAsyncClient.listObjectsV2(request).get(readTimeoutNanos, TimeUnit.NANOSECONDS);
                } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
                    throw handleS3Exception(e, String.format("fetching list of files in directory %s", directory),
                            instructions);
                }
                currentBatchIt = response.contents().stream()
                        .filter(s3Object -> !s3Object.key().equals(directoryKey))
                        .map(s3Object -> {
                            String path = "/" + s3Object.key();
                            if (path.contains("//")) {
                                path = DUPLICATE_SLASH_PATTERN.matcher(path).replaceAll("/");
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
        // Do nothing since we are using a shared client and cache
    }
}
