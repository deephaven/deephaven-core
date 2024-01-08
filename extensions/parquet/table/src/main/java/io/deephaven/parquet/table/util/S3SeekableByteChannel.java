package io.deephaven.parquet.table.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * {@link SeekableByteChannel} class used to fetch objects from AWS S3 buckets using an async client with the ability
 * to read ahead and cache fragments of the object.
 */
public final class S3SeekableByteChannel implements SeekableByteChannel, SeekableChannelsProvider.ContextHolder {

    private static final long CLOSED_SENTINEL = -1;

    /**
     * Context object used to store read-ahead buffers for efficiently reading from S3.
     */
    static final class S3ChannelContext implements SeekableChannelsProvider.ChannelContext {

        /**
         * Used to store information related to a single fragment
         */
        static class FragmentContext {
            /**
             * The index of the fragment in the object
             */
            private int fragmentIndex;

            /**
             * The future that will be completed with the fragment's bytes
             */
            private CompletableFuture<ByteBuffer> future;

            private FragmentContext(final int fragmentIndex, final CompletableFuture<ByteBuffer> future) {
                this.fragmentIndex = fragmentIndex;
                this.future = future;
            }
        }

        /**
         * Used to cache recently fetched fragments for faster lookup
         */
        private final List<FragmentContext> bufferCache;

        private long size;

        S3ChannelContext(final int maxCacheSize) {
            bufferCache = new ArrayList<>(maxCacheSize);
            for (int i = 0; i < maxCacheSize; i++) {
                bufferCache.add(null);
            }
            size = -1;
        }

        private int getIndex(final int fragmentIndex) {
            return fragmentIndex % bufferCache.size();
        }

        void setFragmentContext(final int fragmentIndex, final CompletableFuture<ByteBuffer> future) {
            final int cacheIdx = getIndex(fragmentIndex);
            final FragmentContext cachedEntry = bufferCache.get(cacheIdx);
            if (cachedEntry == null) {
                bufferCache.set(cacheIdx, new FragmentContext(fragmentIndex, future));
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
            final FragmentContext cachedFragment = bufferCache.get(getIndex(fragmentIndex));
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

        @Override
        public void close() {
            bufferCache.clear();
        }
    }

    private final S3AsyncClient s3AsyncClient;
    private final String bucket;
    private final String key;
    private final int fragmentSize;
    private final int readAheadCount;
    private final Duration readTimeout;

    /**
     * The size of the object in bytes, fetched at the time of first read
     */
    private long size;
    private int numFragmentsInObject;

    private S3ChannelContext context;

    private long position;

    S3SeekableByteChannel(@NotNull final SeekableChannelsProvider.ChannelContext context, @NotNull final URI uri,
                          @NotNull final S3AsyncClient s3AsyncClient, final int fragmentSize, final int readAheadCount,
                          final Duration readTimeout) {
        final S3Uri s3Uri = s3AsyncClient.utilities().parseUri(uri);
        this.bucket = s3Uri.bucket().orElse(null);
        this.key = s3Uri.key().orElse(null);
        this.s3AsyncClient = s3AsyncClient;
        this.fragmentSize = fragmentSize;
        this.readAheadCount = readAheadCount;
        this.readTimeout = readTimeout;
        Assert.instanceOf(context, "context", S3ChannelContext.class);
        this.context = (S3ChannelContext) context;
        this.size = -1;
        this.position = 0;
    }

    @Override
    public void setContext(@Nullable final SeekableChannelsProvider.ChannelContext context) {
        // null context equivalent to clearing the context
        if (context != null && !(context instanceof S3ChannelContext)) {
            throw new IllegalArgumentException("context must be null or an instance of ChannelContext, provided context " +
                    " of class " + context.getClass().getName());
        }
        this.context = (S3ChannelContext) context;
    }

    @Override
    public int read(@NotNull final ByteBuffer destination) throws ClosedChannelException {
        Assert.neqNull(context, "context");
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

        // Send async read request the current fragment, if it's not already in the cache
        final int currFragmentIndex = fragmentIndexForByteNumber(localPosition);
        CompletableFuture<ByteBuffer> currFragmentFuture = context.getCachedFuture(currFragmentIndex);
        if (currFragmentFuture == null) {
            currFragmentFuture = computeFragmentFuture(currFragmentIndex);
            context.setFragmentContext(currFragmentIndex, currFragmentFuture);
        }

        // Send async requests for read-ahead buffers and store them in the cache
        final int numFragmentsToLoad = Math.min(readAheadCount, numFragmentsInObject - currFragmentIndex - 1);
        for (int i = 0; i < numFragmentsToLoad; i++) {
            final int readAheadFragmentIndex = i + currFragmentIndex + 1;
            final CompletableFuture<ByteBuffer> readAheadFragmentFuture = context.getCachedFuture(readAheadFragmentIndex);
            if (readAheadFragmentFuture == null) {
                context.setFragmentContext(readAheadFragmentIndex, computeFragmentFuture(readAheadFragmentIndex));
            }
        }

        // Wait till the current fragment is fetched
        final ByteBuffer currentFragment;
        try {
            currentFragment = currFragmentFuture.get(readTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }  catch (final InterruptedException | ExecutionException | TimeoutException e) {
            final String operation = "fetching fragment " + currFragmentIndex + " for file " + key + " in S3 bucket " + bucket;
            handleS3Exception(e, operation);
            throw new UncheckedDeephavenException("Exception caught while " + operation, e);
        }

        // Copy the bytes from fragment from the offset up to the min of remaining fragment and destination bytes.
        // Therefore, the number of bytes read by this method can be less than the number of bytes remaining in the
        // destination buffer.
        final int fragmentOffset = (int) (localPosition - (currFragmentIndex * fragmentSize));
        currentFragment.position(fragmentOffset);
        final int limit = Math.min(currentFragment.remaining(), destination.remaining());
        final int originalBufferLimit = currentFragment.limit();
        currentFragment.limit(currentFragment.position() + limit);
        destination.put(currentFragment);
        // Need to reset buffer limit so we can read from the same buffer again in future
        currentFragment.limit(originalBufferLimit);
        position = localPosition + limit;
        return limit;
    }

    private int fragmentIndexForByteNumber(final long byteNumber) {
        return Math.toIntExact(byteNumber / fragmentSize);
    }

    private CompletableFuture<ByteBuffer> computeFragmentFuture(final int fragmentIndex) {
        final long readFrom = (long) fragmentIndex * fragmentSize;
        final long readTo = Math.min(readFrom + fragmentSize, size) - 1;
        final String range = "bytes=" + readFrom + "-" + readTo;
        return s3AsyncClient.getObject(builder -> builder.bucket(bucket).key(key).range(range),
                new ByteBufferAsyncResponseTransformer<>(fragmentSize));
    }

    private void handleS3Exception(final Exception e, final String operationDescription) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Thread interrupted while " + operationDescription, e);
        } else if (e instanceof ExecutionException) {
            throw new UncheckedDeephavenException("Execution exception occurred while " + operationDescription, e);
        } else if (e instanceof TimeoutException) {
            throw new UncheckedDeephavenException("Operation timeout while " + operationDescription + " after waiting " +
                    "for duration " + readTimeout, e);
        }
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
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
    public long size() throws ClosedChannelException {
        checkClosed(position);
        if (size < 0){
            populateSize();
        }
        return size;
    }

    private void populateSize() {
        if (context.getSize() < 0) {
            // Fetch the size of the file on the first read using a blocking HEAD request
            final HeadObjectResponse headObjectResponse;
            try {
                headObjectResponse = s3AsyncClient.headObject(builder -> builder.bucket(bucket).key(key))
                        .get(readTimeout.toNanos(), TimeUnit.NANOSECONDS);
            }  catch (final InterruptedException | ExecutionException | TimeoutException e) {
                final String operation = "fetching HEAD for file " + key + " in S3 bucket " + bucket;
                handleS3Exception(e, operation);
                throw new UncheckedDeephavenException("Exception caught while " + operation, e);
            }
            context.setSize(headObjectResponse.contentLength());
        }
        this.size = context.getSize();
        this.numFragmentsInObject = (int) ((size + fragmentSize - 1) / fragmentSize);  // = ceil(size / fragmentSize)
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
    public void close() throws IOException {
        position = CLOSED_SENTINEL;
    }

    private static void checkClosed(final long position) throws ClosedChannelException {
        if (position == CLOSED_SENTINEL) {
            throw new ClosedChannelException();
        }
    }
}
