//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.channel.Channels;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
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

    /**
     * We always allocate buffers of maximum allowed size for re-usability across reads with different fragment sizes.
     * There can be a performance penalty though if the fragment size is much smaller than the maximum size.
     */
    private static final BufferPool BUFFER_POOL = new BufferPool(S3Instructions.MAX_FRAGMENT_SIZE);

    private static final int MAX_KEYS_PER_BATCH = 1000;

    private static final Logger log = LoggerFactory.getLogger(S3SeekableChannelProvider.class);

    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    private SoftReference<KeyedObjectHashMap<URI, FileSizeInfo>> uriToFileSizeSoftRef;

    private static final class FileSizeInfo {
        private final URI uri;
        private final long size;

        FileSizeInfo(@NotNull final URI uri, final long size) {
            this.uri = Require.neqNull(uri, "uri");
            this.size = size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(uri, size);
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final FileSizeInfo that = (FileSizeInfo) other;
            return size == that.size && uri.equals(that.uri);
        }
    }

    private static final KeyedObjectKey<URI, FileSizeInfo> URI_MATCH_KEY = new KeyedObjectKey.Basic<>() {
        @Override
        public URI getKey(@NotNull final FileSizeInfo value) {
            return value.uri;
        }
    };

    S3SeekableChannelProvider(@NotNull final S3Instructions s3Instructions) {
        this.s3AsyncClient = S3AsyncClientFactory.getAsyncClient(s3Instructions);
        this.s3Instructions = s3Instructions;
        this.uriToFileSizeSoftRef = new SoftReference<>(new KeyedObjectHashMap<>(URI_MATCH_KEY));
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) {
        final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
        // context is unused here, will be set before reading from the channel
        final KeyedObjectHashMap<URI, FileSizeInfo> uriToFileSize = uriToFileSizeSoftRef.get();
        if (uriToFileSize != null && uriToFileSize.containsKey(uri)) {
            return new S3SeekableByteChannel(s3Uri, uriToFileSize.get(uri).size);
        }
        return new S3SeekableByteChannel(s3Uri);
    }

    @Override
    public InputStream getInputStream(final SeekableByteChannel channel) {
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
                            final URI uri;
                            try {
                                uri = new URI(S3_URI_SCHEME, directory.getUserInfo(), directory.getHost(),
                                        directory.getPort(), path, null, null);
                            } catch (final URISyntaxException e) {
                                throw new UncheckedDeephavenException("Failed to create URI for S3 object with key: "
                                        + s3Object.key() + " and bucket " + bucketName + " inside directory "
                                        + directory, e);
                            }
                            updateFileSizeCache(uri, s3Object.size());
                            return uri;
                        }).iterator();
                // The following token is null when the last batch is fetched.
                continuationToken = response.nextContinuationToken();
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.NONNULL), false);
    }

    /**
     * Update the file size cache with the given URI and size.
     */
    private void updateFileSizeCache(@NotNull final URI uri, final long size) {
        KeyedObjectHashMap<URI, FileSizeInfo> uriToFileSize = uriToFileSizeSoftRef.get();
        if (uriToFileSize != null) {
            uriToFileSize.compute(uri, (key, existingInfo) -> {
                if (existingInfo == null) {
                    return new FileSizeInfo(uri, size);
                } else if (existingInfo.size != size) {
                    throw new IllegalStateException("Existing size " + existingInfo.size + " does not match "
                            + " the new size " + size + " for key " + key);
                }
                return existingInfo;
            });
        } else {
            uriToFileSize = new KeyedObjectHashMap<>(URI_MATCH_KEY);
            uriToFileSize.put(uri, new FileSizeInfo(uri, size));
            uriToFileSizeSoftRef = new SoftReference<>(uriToFileSize);
        }
    }

    @Override
    public void close() {
        s3AsyncClient.close();
    }
}
