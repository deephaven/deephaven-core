/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.base.verify.Assert;

import java.util.Objects;

import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * {@link SeekableByteChannel} class used to fetch objects from AWS S3 buckets using an async client with the ability to
 * read ahead and cache fragments of the object.
 */
final class S3SeekableByteChannel implements SeekableByteChannel, CachedChannelProvider.ContextHolder {

    private static final long CLOSED_SENTINEL = -1;

    private static final long UNINITIALIZED_SIZE = -1;

    private static final long UNINITIALIZED_FRAGMENT_INDEX = -1;

    /**
     * Context object used to store read-ahead buffers for efficiently reading from S3.
     */
    static final class S3ChannelContext implements SeekableChannelContext {

        /**
         * Used to store information related to a single fragment
         */
        private static final class FragmentState {

            /**
             * The index of the fragment in the object.
             */
            private long fragmentIndex = UNINITIALIZED_FRAGMENT_INDEX;

            /**
             * The future that will be completed with the fragment's bytes.
             */
            private Future<ByteBuffer> future;

            /**
             * The {@link SafeCloseable} that will be used to release outstanding resources post-cancellation.
             */
            private SafeCloseable bufferRelease;

            private boolean matches(final int fragmentIndex) {
                return this.fragmentIndex == fragmentIndex;
            }

            private void cancelAndRelease() {
                try (final SafeCloseable ignored = () -> SafeCloseable.closeAll(
                        future == null ? null : () -> future.cancel(true), bufferRelease)) {
                    fragmentIndex = UNINITIALIZED_FRAGMENT_INDEX;
                    future = null;
                    bufferRelease = null;
                }
            }

            private void set(
                    final long fragmentIndex,
                    @NotNull final Future<ByteBuffer> future,
                    @NotNull final SafeCloseable bufferRelease) {
                this.fragmentIndex = fragmentIndex;
                this.future = future;
                this.bufferRelease = bufferRelease;
            }
        }

        /**
         * Used to cache recently fetched fragments for faster lookup
         */
        private final FragmentState[] bufferCache;

        /**
         * The size of the object in bytes, stored in context to avoid fetching multiple times
         */
        private long size;

        S3ChannelContext(final int maxCacheSize) {
            bufferCache = new FragmentState[maxCacheSize];
            size = UNINITIALIZED_SIZE;
        }

        private int getIndex(final int fragmentIndex) {
            // TODO(deephaven-core#5061): Experiment with LRU caching
            return fragmentIndex % bufferCache.length;
        }

        private FragmentState getFragmentState(final int fragmentIndex) {
            final int cacheIdx = getIndex(fragmentIndex);
            FragmentState cachedEntry = bufferCache[cacheIdx];
            if (cachedEntry == null) {
                bufferCache[cacheIdx] = cachedEntry = new FragmentState();
            }
            return cachedEntry;
        }

        /**
         * Will return the {@link CompletableFuture} corresponding to provided fragment index if present in the cache,
         * else will return {@code null}
         */
        @Nullable
        private Future<ByteBuffer> getCachedFuture(final int fragmentIndex) {
            final FragmentState cachedFragment = bufferCache[getIndex(fragmentIndex)];
            if (cachedFragment != null && cachedFragment.matches(fragmentIndex)) {
                return cachedFragment.future;
            }
            return null;
        }

        private long getSize() {
            return size;
        }

        private void setSize(final long size) {
            this.size = size;
        }

        @Override
        public void close() {
            // Cancel all outstanding requests
            for (final FragmentState fragmentState : bufferCache) {
                if (fragmentState != null) {
                    fragmentState.cancelAndRelease();
                }
            }
        }
    }

    private final S3AsyncClient s3AsyncClient;
    private final String bucket;
    private final String key;
    private final S3Instructions s3Instructions;
    private final BufferPool bufferPool;

    /**
     * The size of the object in bytes, fetched at the time of first read
     */
    private long size;
    private int numFragmentsInObject;

    /**
     * The {@link SeekableChannelContext} object used to cache read-ahead buffers for efficiently reading from S3. This
     * is set before the read and cleared when closing the channel.
     */
    private SeekableChannelContext channelContext;

    private long position;

    S3SeekableByteChannel(@NotNull final URI uri, @NotNull final S3AsyncClient s3AsyncClient,
            @NotNull final S3Instructions s3Instructions, @NotNull final BufferPool bufferPool) {
        final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
        this.bucket = s3Uri.bucket().orElse(null);
        this.key = s3Uri.key().orElse(null);
        this.s3AsyncClient = s3AsyncClient;
        this.s3Instructions = s3Instructions;
        this.bufferPool = bufferPool;
        this.size = UNINITIALIZED_SIZE;
        this.position = 0;
    }

    /**
     * @param channelContext The {@link SeekableChannelContext} object used to cache read-ahead buffers for efficiently
     *        reading from S3. An appropriate channel context should be set before the read and should be cleared after
     *        the read is complete. A {@code null} parameter value is equivalent to clearing the context. This parameter
     *        will be {@link SeekableChannelContext#NULL} if no caching and read ahead is desired.
     */
    @Override
    public void setContext(@Nullable final SeekableChannelContext channelContext) {
        this.channelContext = channelContext;
    }

    @Override
    public int read(@NotNull final ByteBuffer destination) throws IOException {
        Assert.neqNull(channelContext, "channelContext");
        if (!destination.hasRemaining()) {
            return 0;
        }
        final long localPosition = position;
        checkClosed(localPosition);

        final int numBytesCopied;
        final S3ChannelContext s3ChannelContext = getS3ChannelContextFrom(channelContext);
        try (final SafeCloseable ignored = s3ChannelContext == channelContext ? null : s3ChannelContext) {
            // Fetch the file size if this is the first read
            populateSize(s3ChannelContext);
            if (localPosition >= size) {
                // We are finished reading
                return -1;
            }

            // Send async read requests for current fragment as well as read ahead fragments
            final int currFragmentIndex = fragmentIndexForByteNumber(localPosition);
            final int numReadAheadFragments = channelContext != s3ChannelContext
                    ? 0 // We have a local S3ChannelContext, we don't want to do any read-ahead caching
                    : Math.min(s3Instructions.readAheadCount(), numFragmentsInObject - currFragmentIndex - 1);
            for (int idx = currFragmentIndex; idx <= currFragmentIndex + numReadAheadFragments; idx++) {
                sendAsyncRequest(idx, s3ChannelContext);
            }

            // Wait till the current fragment is fetched
            final Future<ByteBuffer> currFragmentFuture = s3ChannelContext.getCachedFuture(currFragmentIndex);
            final ByteBuffer currentFragment;
            try {
                currentFragment = currFragmentFuture.get(s3Instructions.readTimeout().toNanos(), TimeUnit.NANOSECONDS);
            } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
                throw handleS3Exception(e,
                        String.format("fetching fragment %d for file %s in S3 bucket %s", currFragmentIndex, key,
                                bucket));
            }

            // Copy the bytes from fragment from the offset up to the min of remaining fragment and destination bytes.
            // Therefore, the number of bytes read by this method can be less than the number of bytes remaining in the
            // destination buffer.
            final int fragmentOffset = (int) (localPosition - (currFragmentIndex * s3Instructions.fragmentSize()));
            currentFragment.position(fragmentOffset);
            numBytesCopied = Math.min(currentFragment.remaining(), destination.remaining());
            final int originalBufferLimit = currentFragment.limit();
            currentFragment.limit(currentFragment.position() + numBytesCopied);
            destination.put(currentFragment);
            // Need to reset buffer limit, so we can read from the same buffer again in future
            currentFragment.limit(originalBufferLimit);
        }
        position = localPosition + numBytesCopied;
        return numBytesCopied;
    }

    /**
     * If the provided {@link SeekableChannelContext} is {@link SeekableChannelContext#NULL}, this method creates and
     * returns a new {@link S3ChannelContext} with a cache size of 1 to support a single read with no read ahead, and
     * the caller is responsible to close the context after the read is complete. Else returns the provided
     * {@link SeekableChannelContext} cast to {@link S3ChannelContext}.
     */
    private static S3ChannelContext getS3ChannelContextFrom(@NotNull final SeekableChannelContext channelContext) {
        final S3ChannelContext s3ChannelContext;
        if (channelContext == SeekableChannelContext.NULL) {
            s3ChannelContext = new S3ChannelContext(1);
        } else {
            Assert.instanceOf(channelContext, "channelContext", S3ChannelContext.class);
            s3ChannelContext = (S3ChannelContext) channelContext;
        }
        return s3ChannelContext;
    }

    private int fragmentIndexForByteNumber(final long byteNumber) {
        return Math.toIntExact(byteNumber / s3Instructions.fragmentSize());
    }

    /**
     * If not already cached in the context, sends an async request to fetch the fragment at the provided index and
     * caches it in the context.
     */
    private void sendAsyncRequest(final int fragmentIndex, @NotNull final S3ChannelContext s3ChannelContext) {
        final S3ChannelContext.FragmentState fragmentState = s3ChannelContext.getFragmentState(fragmentIndex);
        if (fragmentState.matches(fragmentIndex)) {
            // We already have the fragment cached
            return;
        }
        // Cancel any outstanding requests for the fragment in cached slot
        fragmentState.cancelAndRelease();

        final int fragmentSize = s3Instructions.fragmentSize();
        final long readFrom = (long) fragmentIndex * fragmentSize;
        final long readTo = Math.min(readFrom + fragmentSize, size) - 1;
        final String range = "bytes=" + readFrom + "-" + readTo;

        final int numBytes = (int) (readTo - readFrom + 1);
        final BufferPool.BufferHolder bufferHolder = bufferPool.take(numBytes);
        final ByteBufferAsyncResponseTransformer<GetObjectResponse> asyncResponseTransformer =
                new ByteBufferAsyncResponseTransformer<>(Objects.requireNonNull(bufferHolder.get()));
        final CompletableFuture<ByteBuffer> future = s3AsyncClient
                .getObject(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .range(range)
                        .build(), asyncResponseTransformer)
                .whenComplete((response, throwable) -> asyncResponseTransformer.close());;
        fragmentState.set(fragmentIndex, future, bufferHolder);
    }

    private IOException handleS3Exception(final Exception e, final String operationDescription) {
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
                    s3Instructions.readTimeout()), e);
        }
        if (e instanceof CancellationException) {
            return new IOException(String.format("Cancelled an operation while %s", operationDescription), e);
        }
        return new IOException(String.format("Exception caught while %s", operationDescription), e);
    }

    @Override
    public int write(final ByteBuffer src) {
        throw new NonWritableChannelException();
    }

    @Override
    public long position() throws ClosedChannelException {
        final long localPosition = position;
        checkClosed(localPosition);
        return localPosition;
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws ClosedChannelException {
        if (newPosition < 0) {
            throw new IllegalArgumentException("newPosition cannot be < 0, provided newPosition=" + newPosition);
        }
        checkClosed(position);
        position = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        checkClosed(position);
        final S3ChannelContext s3ChannelContext = getS3ChannelContextFrom(channelContext);
        try (final SafeCloseable ignored = s3ChannelContext == channelContext ? null : s3ChannelContext) {
            populateSize(s3ChannelContext);
        }
        return size;
    }

    private void populateSize(final S3ChannelContext s3ChannelContext) throws IOException {
        if (size != UNINITIALIZED_SIZE) {
            // Store the size in the context if it is uninitialized
            if (s3ChannelContext.getSize() == UNINITIALIZED_SIZE) {
                s3ChannelContext.setSize(size);
            }
            return;
        }
        if (s3ChannelContext.getSize() == UNINITIALIZED_SIZE) {
            // Fetch the size of the file on the first read using a blocking HEAD request, and store it in the context
            // for future use
            final HeadObjectResponse headObjectResponse;
            try {
                headObjectResponse = s3AsyncClient
                        .headObject(HeadObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .build())
                        .get(s3Instructions.readTimeout().toNanos(), TimeUnit.NANOSECONDS);
            } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
                throw handleS3Exception(e, String.format("fetching HEAD for file %s in S3 bucket %s", key, bucket));
            }
            s3ChannelContext.setSize(headObjectResponse.contentLength());
        }
        this.size = s3ChannelContext.getSize();
        final int fragmentSize = s3Instructions.fragmentSize();
        this.numFragmentsInObject = (int) ((size + fragmentSize - 1) / fragmentSize); // = ceil(size / fragmentSize)
    }

    @Override
    public SeekableByteChannel truncate(final long size) {
        throw new NonWritableChannelException();
    }

    @Override
    public boolean isOpen() {
        return position != CLOSED_SENTINEL;
    }

    @Override
    public void close() {
        position = CLOSED_SENTINEL;
    }

    private static void checkClosed(final long position) throws ClosedChannelException {
        if (position == CLOSED_SENTINEL) {
            throw new ClosedChannelException();
        }
    }
}
