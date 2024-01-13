/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.base.verify.Assert;
import java.util.concurrent.CancellationException;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * {@link SeekableByteChannel} class used to fetch objects from AWS S3 buckets using an async client with the ability to
 * read ahead and cache fragments of the object.
 */
final class S3SeekableByteChannel implements SeekableByteChannel, SeekableChannelsProvider.ContextHolder {

    private static final long CLOSED_SENTINEL = -1;

    private static final long UNINITIALIZED_SIZE = -1;

    /**
     * Channel context object used to store read-ahead buffers for efficiently reading from S3.
     */
    static final class S3ChannelContext implements SeekableChannelsProvider.ChannelContext {

        /**
         * Used to store information related to a single fragment
         */
        private static final class FragmentState {

            /**
             * The index of the fragment in the object
             */
            private int fragmentIndex;

            /**
             * The future that will be completed with the fragment's bytes
             */
            private CompletableFuture<ByteBuffer> future;

            private FragmentState(final int fragmentIndex, final CompletableFuture<ByteBuffer> future) {
                this.fragmentIndex = fragmentIndex;
                this.future = future;
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
            return fragmentIndex % bufferCache.length;
        }

        void setFragmentState(final int fragmentIndex, final CompletableFuture<ByteBuffer> future) {
            final int cacheIdx = getIndex(fragmentIndex);
            final FragmentState cachedEntry = bufferCache[cacheIdx];
            if (cachedEntry == null) {
                bufferCache[cacheIdx] = new FragmentState(fragmentIndex, future);
            } else {
                // We should not cache an already cached fragment
                Assert.neq(cachedEntry.fragmentIndex, "cachedEntry.fragmentIndex", fragmentIndex, "fragmentIdx");

                // Cancel any outstanding requests for this cached fragment
                cachedEntry.future.cancel(true);

                // Reuse the existing entry
                cachedEntry.fragmentIndex = fragmentIndex;
                cachedEntry.future = future;
            }
        }

        /**
         * Will return the {@link CompletableFuture} corresponding to provided fragment index if present in the cache,
         * else will return {@code null}
         */
        @Nullable
        CompletableFuture<ByteBuffer> getCachedFuture(final int fragmentIndex) {
            final FragmentState cachedFragment = bufferCache[getIndex(fragmentIndex)];
            if (cachedFragment != null && cachedFragment.fragmentIndex == fragmentIndex) {
                return cachedFragment.future;
            }
            return null;
        }

        long getSize() {
            return size;
        }

        void setSize(final long size) {
            this.size = size;
        }
    }

    private final S3AsyncClient s3AsyncClient;
    private final String bucket;
    private final String key;
    private final S3Instructions s3Instructions;

    /**
     * The size of the object in bytes, fetched at the time of first read
     */
    private long size;
    private int numFragmentsInObject;

    private S3ChannelContext s3ChannelContext;

    private long position;

    S3SeekableByteChannel(@NotNull final SeekableChannelsProvider.ChannelContext channelContext, @NotNull final URI uri,
            @NotNull final S3AsyncClient s3AsyncClient, final S3Instructions s3Instructions) {
        final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
        this.bucket = s3Uri.bucket().orElse(null);
        this.key = s3Uri.key().orElse(null);
        this.s3AsyncClient = s3AsyncClient;
        this.s3Instructions = s3Instructions;
        Assert.instanceOf(channelContext, "channelContext", S3ChannelContext.class);
        this.s3ChannelContext = (S3ChannelContext) channelContext;
        this.size = UNINITIALIZED_SIZE;
        this.position = 0;
    }

    @Override
    public void setContext(@Nullable final SeekableChannelsProvider.ChannelContext channelContext) {
        // null context equivalent to clearing the context
        if (channelContext != null && !(channelContext instanceof S3ChannelContext)) {
            throw new IllegalArgumentException(
                    "Context must be null or an instance of S3ChannelContext, provided context of class " +
                            channelContext.getClass().getName());
        }
        this.s3ChannelContext = (S3ChannelContext) channelContext;
    }

    @Override
    public int read(@NotNull final ByteBuffer destination) throws IOException {
        Assert.neqNull(s3ChannelContext, "s3ChannelContext");
        Assert.neq(s3ChannelContext, "s3ChannelContext", SeekableChannelsProvider.ChannelContext.NULL,
                "SeekableChannelsProvider.ChannelContext.NULL");
        if (!destination.hasRemaining()) {
            return 0;
        }
        final long localPosition = position;
        checkClosed(localPosition);

        // Fetch the file size if this is the first read
        populateSize();
        if (localPosition >= size) {
            // We are finished reading
            return -1;
        }

        // Send async read requests for current fragment as well as read ahead fragments, if not already in cache
        final int currFragmentIndex = fragmentIndexForByteNumber(localPosition);
        final int numReadAheadFragments =
                Math.min(s3Instructions.readAheadCount(), numFragmentsInObject - currFragmentIndex - 1);
        for (int idx = currFragmentIndex; idx <= currFragmentIndex + numReadAheadFragments; idx++) {
            final CompletableFuture<ByteBuffer> future = s3ChannelContext.getCachedFuture(idx);
            if (future == null) {
                s3ChannelContext.setFragmentState(idx, sendAsyncRequest(idx));
            }
        }

        // Wait till the current fragment is fetched
        final CompletableFuture<ByteBuffer> currFragmentFuture = s3ChannelContext.getCachedFuture(currFragmentIndex);
        final ByteBuffer currentFragment;
        try {
            currentFragment = currFragmentFuture.get(s3Instructions.readTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
            throw handleS3Exception(e,
                    String.format("fetching fragment %d for file %s in S3 bucket %s", currFragmentIndex, key, bucket));
        }

        // Copy the bytes from fragment from the offset up to the min of remaining fragment and destination bytes.
        // Therefore, the number of bytes read by this method can be less than the number of bytes remaining in the
        // destination buffer.
        final int fragmentOffset = (int) (localPosition - (currFragmentIndex * s3Instructions.fragmentSize()));
        currentFragment.position(fragmentOffset);
        final int sizeToCopy = Math.min(currentFragment.remaining(), destination.remaining());
        final int originalBufferLimit = currentFragment.limit();
        currentFragment.limit(currentFragment.position() + sizeToCopy);
        destination.put(currentFragment);
        // Need to reset buffer limit, so we can read from the same buffer again in future
        currentFragment.limit(originalBufferLimit);
        position = localPosition + sizeToCopy;
        return sizeToCopy;
    }

    private int fragmentIndexForByteNumber(final long byteNumber) {
        return Math.toIntExact(byteNumber / s3Instructions.fragmentSize());
    }

    /**
     * @return A {@link CompletableFuture} that will be completed with the bytes of the fragment
     */
    private CompletableFuture<ByteBuffer> sendAsyncRequest(final int fragmentIndex) {
        final int fragmentSize = s3Instructions.fragmentSize();
        final long readFrom = (long) fragmentIndex * fragmentSize;
        final long readTo = Math.min(readFrom + fragmentSize, size) - 1;
        final String range = "bytes=" + readFrom + "-" + readTo;
        return s3AsyncClient.getObject(builder -> builder.bucket(bucket).key(key).range(range),
                new ByteBufferAsyncResponseTransformer<>((int) (readTo - readFrom + 1)));
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
        populateSize();
        return size;
    }

    private void populateSize() throws IOException {
        if (size != UNINITIALIZED_SIZE) {
            return;
        }
        if (s3ChannelContext.getSize() < 0) {
            // Fetch the size of the file on the first read using a blocking HEAD request, and store it in the context
            // for future use
            final HeadObjectResponse headObjectResponse;
            try {
                headObjectResponse = s3AsyncClient.headObject(builder -> builder.bucket(bucket).key(key))
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
