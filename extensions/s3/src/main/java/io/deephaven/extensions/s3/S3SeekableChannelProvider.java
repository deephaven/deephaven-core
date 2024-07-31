//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import edu.colorado.cires.cmg.s3out.AwsS3ClientMultipartUpload;
import edu.colorado.cires.cmg.s3out.ContentTypeResolver;
import edu.colorado.cires.cmg.s3out.MultipartUploadRequest;
import edu.colorado.cires.cmg.s3out.NoContentTypeResolver;
import edu.colorado.cires.cmg.s3out.S3ClientMultipartUpload;
import edu.colorado.cires.cmg.s3out.S3OutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.channel.Channels;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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
    private static final int UNKNOWN_SIZE = -1;
    private static final ContentTypeResolver NO_CONTENT_TYPE_RESOLVER = new NoContentTypeResolver();

    private static final Logger log = LoggerFactory.getLogger(S3SeekableChannelProvider.class);

    private static final int UPLOAD_PART_SIZE_MB =
            Configuration.getInstance().getIntegerWithDefault("S3.uploadPartSizeMiB", 5);
    private static final int UPLOAD_QUEUE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("S3.uploadQueueSize", 1);

    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    // Initialized lazily when needed
    private S3ClientMultipartUpload s3MultipartUploader;
    private S3Client s3Client;

    /**
     * A shared cache for S3 requests. This cache is shared across all S3 channels created by this provider.
     */
    private final S3RequestCache sharedCache;

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<S3SeekableChannelProvider, SoftReference> FILE_SIZE_CACHE_REF_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(S3SeekableChannelProvider.class, SoftReference.class,
                    "fileSizeCacheRef");

    private volatile SoftReference<Map<URI, FileSizeInfo>> fileSizeCacheRef;

    S3SeekableChannelProvider(@NotNull final S3Instructions s3Instructions) {
        this.s3AsyncClient = S3ClientFactory.getAsyncClient(s3Instructions);
        this.s3Instructions = s3Instructions;
        this.sharedCache = new S3RequestCache(s3Instructions.fragmentSize());
        this.fileSizeCacheRef = new SoftReference<>(new KeyedObjectHashMap<>(FileSizeInfo.URI_MATCH_KEY));
    }

    @Override
    public boolean exists(@NotNull final URI uri) {
        if (getCachedSize(uri) != UNKNOWN_SIZE) {
            return true;
        }
        final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
        try {
            fetchFileSize(s3Uri);
        } catch (final NoSuchKeyException e) {
            return false;
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("Error fetching file size for URI " + uri, e);
        }
        return true;
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) {
        final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
        // context is unused here, will be set before reading from the channel
        final long cachedSize = getCachedSize(uri);
        if (cachedSize != UNKNOWN_SIZE) {
            return new S3SeekableByteChannel(s3Uri, cachedSize);
        }
        return new S3SeekableByteChannel(s3Uri);
    }

    @Override
    public InputStream getInputStream(final SeekableByteChannel channel, final int sizeHint) {
        // S3SeekableByteChannel is internally buffered, no need to re-buffer
        return Channels.newInputStreamNoClose(channel);
    }

    @Override
    public SeekableChannelContext makeContext() {
        return new S3ChannelContext(this, s3AsyncClient, s3Instructions, sharedCache);
    }

    @Override
    public SeekableChannelContext makeSingleUseContext() {
        return new S3ChannelContext(this, s3AsyncClient, s3Instructions.singleUse(), sharedCache);
    }

    @Override
    public boolean isCompatibleWith(@NotNull final SeekableChannelContext channelContext) {
        return channelContext instanceof S3ChannelContext;
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final URI uri, final boolean append) {
        throw new UnsupportedOperationException("Creating seekable write channels for S3 is currently unsupported, " +
                "use getOutputStream instead");
    }

    @Override
    public OutputStream getOutputStream(@NotNull final URI uri, final boolean append, final int bufferSizeHint)
            throws IOException {
        if (append) {
            throw new UnsupportedOperationException("Appending to S3 is currently unsupported");
        }
        // return getStreamingWriteOutputStream(uri, bufferSizeHint);
        return getLocalWriteAndPushOutputStream(uri, bufferSizeHint);
    }

    private OutputStream getStreamingWriteOutputStream(@NotNull final URI uri, final int bufferSizeHint) {
        // TODO Use bufferSizeHint as part size
        if (s3Client == null) {
            s3Client = S3ClientFactory.getClient(s3Instructions);
            s3MultipartUploader = AwsS3ClientMultipartUpload.builder()
                    .s3(s3Client)
                    .contentTypeResolver(NO_CONTENT_TYPE_RESOLVER)
                    .build();
        }
        final S3Uri s3Uri = s3Client.utilities().parseUri(uri);
        return S3OutputStream.builder()
                .s3(s3MultipartUploader)
                .uploadRequest(MultipartUploadRequest.builder()
                        .bucket(s3Uri.bucket().orElseThrow())
                        .key(s3Uri.key().orElseThrow())
                        .build())
                .partSizeMib(UPLOAD_PART_SIZE_MB) // TODO Can tweak this for performance
                .uploadQueueSize(UPLOAD_QUEUE_SIZE)
                .autoComplete(true) // Do better handling of errors
                .build();
    }

    private OutputStream getLocalWriteAndPushOutputStream(@NotNull final URI uri, final int bufferSizeHint)
            throws IOException {
        return new BufferedOutputStream(java.nio.channels.Channels.newOutputStream(
                new S3WritableByteChannel(uri)), bufferSizeHint);
    }

    private final class S3WritableByteChannel implements WritableByteChannel {
        private final URI uri;
        private final SeekableByteChannel channel;
        private final Path localTempFile;

        private boolean isOpen;

        private S3WritableByteChannel(@NotNull final URI uri) throws IOException {
            this.uri = uri;
            this.localTempFile = Files.createTempFile("s3-write", ".tmp");
            this.channel = Files.newByteChannel(localTempFile, StandardOpenOption.WRITE);
            this.isOpen = true;
        }

        @Override
        public int write(final ByteBuffer src) throws IOException {
            return channel.write(src);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public void close() throws IOException {
            if (!isOpen) {
                throw new IOException("Channel already closed");
            }
            channel.close();
            uploadLocalTempFile();
            Files.deleteIfExists(localTempFile);
            isOpen = false;
        }

        private void uploadLocalTempFile() {
            final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
            try (final S3TransferManager manager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
                final CompletableFuture<?> uploadCompletableFuture = manager.uploadFile(
                        UploadFileRequest.builder()
                                .putObjectRequest(PutObjectRequest.builder()
                                        .bucket(s3Uri.bucket().orElseThrow())
                                        .key(s3Uri.key().orElseThrow())
                                        .build())
                                .source(localTempFile)
                                .build())
                        .completionFuture();
                final long writeNanos = Duration.ofSeconds(60).toNanos();
                try {
                    uploadCompletableFuture.get(writeNanos, TimeUnit.NANOSECONDS);
                } catch (final InterruptedException | ExecutionException | TimeoutException e) {
                    throw new UncheckedDeephavenException("Failed to upload file to S3 uri " + uri, e);
                }
            }
        }
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
     * Get a strong reference to the file size cache, creating it if necessary.
     */
    private Map<URI, FileSizeInfo> getFileSizeCache() {
        SoftReference<Map<URI, FileSizeInfo>> cacheRef;
        Map<URI, FileSizeInfo> cache;
        while ((cache = (cacheRef = fileSizeCacheRef).get()) == null) {
            if (FILE_SIZE_CACHE_REF_UPDATER.compareAndSet(this, cacheRef,
                    new SoftReference<>(cache = new KeyedObjectHashMap<>(FileSizeInfo.URI_MATCH_KEY)))) {
                return cache;
            }
        }
        return cache;
    }

    /**
     * Fetch the size of the file at the given S3 URI.
     *
     * @throws NoSuchKeyException if the file does not exist
     * @throws IOException if there is an error fetching the file size
     */
    long fetchFileSize(@NotNull final S3Uri s3Uri) throws IOException {
        final long cachedSize = getCachedSize(s3Uri.uri());
        if (cachedSize != UNKNOWN_SIZE) {
            return cachedSize;
        }
        // Fetch the size of the file using a blocking HEAD request, and store it in the cache for future use
        if (log.isDebugEnabled()) {
            log.debug().append("Head: ").append(s3Uri.toString()).endl();
        }
        final HeadObjectResponse headObjectResponse;
        try {
            headObjectResponse = s3AsyncClient
                    .headObject(HeadObjectRequest.builder()
                            .bucket(s3Uri.bucket().orElseThrow())
                            .key(s3Uri.key().orElseThrow())
                            .build())
                    .get(s3Instructions.readTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
            throw handleS3Exception(e, String.format("fetching HEAD for file %s", s3Uri), s3Instructions);
        }
        final long fileSize = headObjectResponse.contentLength();
        updateFileSizeCache(s3Uri.uri(), fileSize);
        return fileSize;
    }

    /**
     * Get the cached size for the given URI, or {@value UNKNOWN_SIZE} if the size is not cached.
     */
    private long getCachedSize(final URI uri) {
        final Map<URI, FileSizeInfo> fileSizeCache = fileSizeCacheRef.get();
        if (fileSizeCache != null) {
            final FileSizeInfo sizeInfo = fileSizeCache.get(uri);
            if (sizeInfo != null) {
                return sizeInfo.size;
            }
        }
        return UNKNOWN_SIZE;
    }

    /**
     * Cache the file size for the given URI.
     */
    private void updateFileSizeCache(@NotNull final URI uri, final long size) {
        if (size < 0) {
            throw new IllegalArgumentException("Invalid file size: " + size + " for URI " + uri);
        }
        final Map<URI, FileSizeInfo> fileSizeCache = getFileSizeCache();
        fileSizeCache.compute(uri, (key, existingInfo) -> {
            if (existingInfo == null) {
                return new FileSizeInfo(uri, size);
            } else if (existingInfo.size != size) {
                throw new IllegalStateException("Existing size " + existingInfo.size + " does not match "
                        + " the new size " + size + " for key " + key);
            }
            return existingInfo;
        });
    }

    private static final class FileSizeInfo {
        private final URI uri;
        private final long size;

        FileSizeInfo(@NotNull final URI uri, final long size) {
            this.uri = Require.neqNull(uri, "uri");
            this.size = size;
        }

        private static final KeyedObjectKey<URI, FileSizeInfo> URI_MATCH_KEY = new KeyedObjectKey.Basic<>() {
            @Override
            public URI getKey(@NotNull final FileSizeInfo value) {
                return value.uri;
            }
        };
    }

    @Override
    public void close() {
        s3AsyncClient.close();
        sharedCache.clear();
    }
}
