//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.BaseSeekableChannelContext;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Context object used to store read-ahead buffers for efficiently reading from S3. A single context object can only be
 * associated with a single URI at a time.
 */
final class S3ChannelContext extends BaseSeekableChannelContext implements SeekableChannelContext {
    private static final Logger log = LoggerFactory.getLogger(S3ChannelContext.class);
    static final long UNINITIALIZED_SIZE = -1;
    private static final long UNINITIALIZED_NUM_FRAGMENTS = -1;

    private final S3SeekableChannelProvider provider;
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
            readAhead = Math.min(Math.max(impliedReadAhead, desiredReadAhead), totalRemainingFragments);
        }
        int filled;
        {
            // Hold a reference to the first request to ensure it is not evicted from the cache
            final S3Request.Acquired acquiredRequest = getOrCreateRequest(firstFragmentIx);
            for (int i = 0; i < readAhead; ++i) {
                // Do not hold references to the read-ahead requests
                getOrCreateRequest(firstFragmentIx + i + 1);
            }
            // blocking
            filled = acquiredRequest.fill(position, dest);
        }
        for (int i = 0; dest.hasRemaining(); ++i) {
            final S3Request.Acquired readAheadRequest = sharedCache.getRequest(uri, firstFragmentIx + i + 1);
            if (readAheadRequest == null || !readAheadRequest.isDone()) {
                break;
            }
            // non-blocking since we know isDone
            filled += readAheadRequest.fill(position + filled, dest);
        }
        return filled;
    }

    private void reset() {
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
    }

    // --------------------------------------------------------------------------------------------------

    @NotNull
    private S3Request.Acquired getOrCreateRequest(final long fragmentIndex) {
        final S3Request.Acquired cachedRequest = sharedCache.getOrCreateRequest(uri, fragmentIndex, this);
        // Send the request, if not sent already. The following method is idempotent, so we always call it.
        cachedRequest.send();
        return cachedRequest;
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
            if (e.getCause() instanceof NoSuchKeyException) {
                throw (NoSuchKeyException) e.getCause();
            }
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
        setSize(provider.fetchFileSize(uri));
    }

    private void setSize(final long size) {
        this.size = size;
        // ceil(size / fragmentSize)
        this.numFragments = (size + instructions.fragmentSize() - 1) / instructions.fragmentSize();
    }
}
