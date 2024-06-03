//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.BaseSeekableChannelContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Context object used to store read-ahead buffers for efficiently reading from S3. A single context object can only be
 * associated with a single URI at a time.
 */
final class S3ChannelContext extends BaseSeekableChannelContext implements SeekableChannelContext {
    private static final Logger log = LoggerFactory.getLogger(S3ChannelContext.class);
    static final long UNINITIALIZED_SIZE = -1;
    private static final long UNINITIALIZED_NUM_FRAGMENTS = -1;

    final S3SeekableChannelProvider provider;
    final S3AsyncClient client;
    final S3Instructions instructions;

    /**
     * The URI associated with this context. A single context object can only be associated with a single URI at a time.
     * But it can be re-associated with a different URI after {@link #reset() resetting}.
     */
    S3Uri uri;

    /**
     * Used to cache recently fetched fragments from the {@link #uri} for faster lookup. Note that this cache can be
     * shared across multiple contexts and be accessed concurrently.
     */
    final S3RequestCache sharedCache;

    /**
     * Used to cache recently fetched fragments as well as the ownership token for the request. This cache is local to
     * the context and is used to keep the requests alive as long as the context is alive.
     */
    private final S3Request.AcquiredRequest[] localCache;

    /**
     * The size of the object in bytes, stored in context to avoid fetching multiple times
     */
    long size;

    /**
     * The number of fragments in the object, ceil(size / fragmentSize)
     */
    private long numFragments;

    S3ChannelContext(
            @NotNull final S3SeekableChannelProvider provider,
            @NotNull final S3AsyncClient client,
            @NotNull final S3Instructions instructions,
            @NotNull final S3RequestCache sharedCache) {
        this.provider = Objects.requireNonNull(provider);
        this.client = Objects.requireNonNull(client);
        this.instructions = Objects.requireNonNull(instructions);
        this.localCache = new S3Request.AcquiredRequest[instructions.maxCacheSize()];
        this.sharedCache = sharedCache;
        if (sharedCache.getFragmentSize() != instructions.fragmentSize()) {
            throw new IllegalArgumentException("Fragment size mismatch between shared cache and instructions, "
                    + sharedCache.getFragmentSize() + " != " + instructions.fragmentSize());
        }
        uri = null;
        size = UNINITIALIZED_SIZE;
        numFragments = UNINITIALIZED_NUM_FRAGMENTS;
        if (log.isDebugEnabled()) {
            log.debug().append("Creating context: ").append(ctxStr()).endl();
        }
    }

    void setURI(@NotNull final S3Uri uri) {
        if (!uri.equals(this.uri)) {
            reset();
        }
        this.uri = uri;
    }

    void verifyOrSetSize(final long size) {
        if (this.size == UNINITIALIZED_SIZE) {
            setSize(size);
        } else if (this.size != size) {
            throw new IllegalStateException(
                    String.format("Inconsistent size. expected=%d, actual=%d, ctx=%s", size, this.size, ctxStr()));
        }
    }

    long size() throws IOException {
        ensureSize();
        return size;
    }

    int fill(final long position, final ByteBuffer dest) throws IOException {
        final int destRemaining = dest.remaining();
        if (destRemaining == 0) {
            return 0;
        }
        ensureSize();
        // Send async read requests for current fragment as well as read ahead fragments
        final long firstFragmentIx = fragmentIndex(position);
        final long readAhead;
        {
            final long lastFragmentIx = fragmentIndex(position + destRemaining - 1);
            final int impliedReadAhead = (int) (lastFragmentIx - firstFragmentIx);
            final int desiredReadAhead = instructions.readAheadCount();
            final long totalRemainingFragments = numFragments - firstFragmentIx - 1;
            final int maxReadAhead = instructions.maxCacheSize() - 1;
            readAhead = Math.min(
                    Math.max(impliedReadAhead, desiredReadAhead),
                    (int) Math.min(maxReadAhead, totalRemainingFragments));
        }
        final S3Request firstRequest = getOrCreateRequest(firstFragmentIx);
        for (int i = 0; i < readAhead; ++i) {
            getOrCreateRequest(firstFragmentIx + i + 1);
        }
        // blocking
        int filled = firstRequest.fill(position, dest);
        for (int i = 0; dest.hasRemaining(); ++i) {
            // Since we have already created requests for read ahead fragments, we can retrieve them from the local
            // cache
            final S3Request request = getRequestFromLocalCache(firstFragmentIx + i + 1);
            if (request == null || !request.isDone()) {
                break;
            }
            // non-blocking since we know isDone
            filled += request.fill(position + filled, dest);
        }
        return filled;
    }

    private void reset() {
        releaseOutstanding();
        // Reset the internal state
        uri = null;
        size = UNINITIALIZED_SIZE;
        numFragments = UNINITIALIZED_NUM_FRAGMENTS;
    }

    /**
     * Close the context, releasing all outstanding requests and resources associated with it.
     */
    @Override
    public void close() {
        super.close();
        if (log.isDebugEnabled()) {
            log.debug().append("Closing context: ").append(ctxStr()).endl();
        }
        releaseOutstanding();
    }

    /**
     * Release all outstanding requests associated with this context. Eventually, the request will be canceled when the
     * objects are garbage collected.
     */
    private void releaseOutstanding() {
        Arrays.fill(localCache, null);
    }

    // --------------------------------------------------------------------------------------------------

    @Nullable
    private S3Request getRequestFromLocalCache(final long fragmentIndex) {
        return getRequestFromLocalCache(fragmentIndex, cacheIndex(fragmentIndex));
    }

    @Nullable
    private S3Request getRequestFromLocalCache(final long fragmentIndex, final int cacheIdx) {
        if (localCache[cacheIdx] != null && localCache[cacheIdx].request.isFragment(fragmentIndex)) {
            return localCache[cacheIdx].request;
        }
        return null;
    }

    @NotNull
    private S3Request getOrCreateRequest(final long fragmentIndex) {
        final int cacheIdx = cacheIndex(fragmentIndex);
        final S3Request locallyCached = getRequestFromLocalCache(fragmentIndex, cacheIdx);
        if (locallyCached != null) {
            return locallyCached;
        }
        final S3Request.AcquiredRequest sharedCacheRequest = sharedCache.getOrCreateRequest(uri, fragmentIndex, this);
        // Cache the request and the ownership token locally
        localCache[cacheIdx] = sharedCacheRequest;
        // Send the request, if not sent already. The following method is idempotent, so we always call it.
        sharedCacheRequest.request.sendRequest();
        return sharedCacheRequest.request;
    }

    private int cacheIndex(final long fragmentIndex) {
        return (int) (fragmentIndex % instructions.maxCacheSize());
    }

    private long fragmentIndex(final long pos) {
        return pos / instructions.fragmentSize();
    }

    private String ctxStr() {
        if (uri != null) {
            return String.format("ctx=%d %s/%s", System.identityHashCode(S3ChannelContext.this),
                    uri.bucket().orElseThrow(), uri.key().orElseThrow());
        } else {
            return String.format("ctx=%d", System.identityHashCode(S3ChannelContext.this));
        }
    }

    static IOException handleS3Exception(final Exception e, final String operationDescription,
            final S3Instructions instructions) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new IOException(String.format("Thread interrupted while %s", operationDescription), e);
        }
        if (e instanceof ExecutionException) {
            return new IOException(String.format("Execution exception occurred while %s", operationDescription), e);
        }
        if (e instanceof TimeoutException) {
            return new IOException(String.format(
                    "Operation timeout while %s after waiting for duration %s", operationDescription,
                    instructions.readTimeout()), e);
        }
        if (e instanceof CancellationException) {
            return new IOException(String.format("Cancelled an operation while %s", operationDescription), e);
        }
        return new IOException(String.format("Exception caught while %s", operationDescription), e);
    }

    private void ensureSize() throws IOException {
        if (size != UNINITIALIZED_SIZE) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug().append("Head: ").append(ctxStr()).endl();
        }
        // Fetch the size of the file on the first read using a blocking HEAD request, and store it in the context
        // for future use
        final HeadObjectResponse headObjectResponse;
        try {
            headObjectResponse = client
                    .headObject(HeadObjectRequest.builder()
                            .bucket(uri.bucket().orElseThrow())
                            .key(uri.key().orElseThrow())
                            .build())
                    .get(instructions.readTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
            throw handleS3Exception(e, String.format("fetching HEAD for file %s, %s", uri, ctxStr()), instructions);
        }
        final long fileSize = headObjectResponse.contentLength();
        setSize(fileSize);
        provider.updateFileSizeCache(uri.uri(), fileSize);
    }

    private void setSize(final long size) {
        this.size = size;
        // ceil(size / fragmentSize)
        this.numFragments = (size + instructions.fragmentSize() - 1) / instructions.fragmentSize();
    }
}
