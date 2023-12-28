package io.deephaven.parquet.table.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.security.InvalidParameterException;
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
     * Maximum time to wait while fetching fragments from S3.
     */
    static final Long CONNECTION_TIMEOUT_MINUTES = 1L;
    static final TimeUnit CONNECTION_TIMEOUT_UNIT = TimeUnit.MINUTES;

    /**
     * Context object used to store read-ahead buffers for efficiently reading from S3.
     */
    static final class ChannelContext implements SeekableChannelsProvider.ChannelContext {

        /**
         * Used to store information related to a single fragment
         */
        static class FragmentContext {
            /**
             * The index of the fragment in the object
             */
            private final int fragmentIndex;

            /**
             * The future that will be completed with the fragment's bytes
             */
            private final CompletableFuture<ByteBuffer> future;

            private FragmentContext(final int fragmentIndex, final CompletableFuture<ByteBuffer> future) {
                this.fragmentIndex = fragmentIndex;
                this.future = future;
            }
        }

        private final List<FragmentContext> readAheadBuffers;

        ChannelContext(final int readAheadCount, final int maxCacheSize) {
            if (maxCacheSize < 1 + readAheadCount) {
                throw new InvalidParameterException("maxCacheSize must be >= 1 + readAheadCount");
            }
            readAheadBuffers = new ArrayList<>(maxCacheSize);
            for (int i = 0; i < maxCacheSize; i++) {
                readAheadBuffers.add(null);
            }
        }

        private int getIndex(final int fragmentIndex) {
            return fragmentIndex % readAheadBuffers.size();
        }

        void setFragmentContext(final int fragmentIndex, final CompletableFuture<ByteBuffer> future) {
            readAheadBuffers.set(getIndex(fragmentIndex), new FragmentContext(fragmentIndex, future));
        }

        FragmentContext getFragmentContext(final int fragmentIndex) {
            return readAheadBuffers.get(getIndex(fragmentIndex));
        }

        @Override
        public void close() {
            readAheadBuffers.clear();
        }
    }

    private ChannelContext context;

    private volatile long position;

    private final S3AsyncClient s3Client;
    private final String bucket, key;
    private final int numFragmentsInObject;
    private final int fragmentSize;
    private final int readAheadCount;

    /**
     * The size of the object in bytes
     */
    private final long size;

    /**
     * The maximum time and units to wait while fetching an object
     **/
    private final Long timeout;
    private final TimeUnit timeUnit;


    S3SeekableByteChannel(@NotNull final SeekableChannelsProvider.ChannelContext context, @NotNull final String bucket,
                          @NotNull final String key, @NotNull final S3AsyncClient s3Client, final long size,
                          final int fragmentSize, final int readAheadCount) {
        this.position = 0;
        this.bucket = bucket;
        this.key = key;
        this.s3Client = s3Client;

        this.fragmentSize = fragmentSize;
        this.readAheadCount = readAheadCount;
        this.timeout = CONNECTION_TIMEOUT_MINUTES;
        this.timeUnit = CONNECTION_TIMEOUT_UNIT;
        this.size = size;
        this.numFragmentsInObject = (int) Math.ceil((double) size / fragmentSize);
        this.context = (ChannelContext) context;
    }

    @Override
    public void setContext(@Nullable final SeekableChannelsProvider.ChannelContext context) {
        // null context equivalent to clearing the context
        if (context != null && !(context instanceof ChannelContext)) {
            throw new IllegalArgumentException("context must be null or an instance of ChannelContext");
        }
        this.context = (ChannelContext) context;
    }

    @Override
    public int read(@NotNull final ByteBuffer destination) throws ClosedChannelException {
        Assert.neqNull(context, "context");
        if (!destination.hasRemaining()) {
            return 0;
        }
        final long localPosition = position;
        checkClosed(localPosition);
        if (localPosition >= size) {
            // We are finished reading
            return -1;
        }

        final int currFragmentIndex = fragmentIndexForByteNumber(localPosition);
        final int fragmentOffset = (int) (localPosition - (currFragmentIndex * fragmentSize));

        // Send async read request the current fragment, if it's not already in the cache
        final ChannelContext.FragmentContext fragmentContext = context.getFragmentContext(currFragmentIndex);
        final CompletableFuture<ByteBuffer> fetchCurrFragment;
        if (fragmentContext != null && fragmentContext.fragmentIndex == currFragmentIndex) {
            fetchCurrFragment = fragmentContext.future;
        } else {
            fetchCurrFragment = computeFragmentFuture(currFragmentIndex);
            context.setFragmentContext(currFragmentIndex, fetchCurrFragment);
        }

        // Send async requests for read-ahead buffers and store them in the cache
        final int numFragmentsToLoad = Math.min(readAheadCount, numFragmentsInObject - currFragmentIndex - 1);
        for (int i = 0; i < numFragmentsToLoad; i++) {
            final int readAheadFragmentIndex = i + currFragmentIndex + 1;
            final ChannelContext.FragmentContext readAheadFragmentContext = context.getFragmentContext(readAheadFragmentIndex);
            if (readAheadFragmentContext == null || readAheadFragmentContext.fragmentIndex != readAheadFragmentIndex) {
                context.setFragmentContext(readAheadFragmentIndex, computeFragmentFuture(readAheadFragmentIndex));
            }
        }

        // Wait till the current fragment is fetched
        final ByteBuffer currentFragment;
        try {
            currentFragment = fetchCurrFragment.get(timeout, timeUnit).asReadOnlyBuffer();
        } catch (final InterruptedException | ExecutionException | TimeoutException | RuntimeException e) {
            throw new UncheckedDeephavenException("Failed to fetch fragment " + currFragmentIndex + " at byte offset "
                    + fragmentOffset + " for file " + key + " in S3 bucket " + bucket, e);
        }

        // Copy the bytes from fragment from the offset up to the min of remaining fragment and destination bytes.
        // Therefore, the number of bytes read by this method can be less than the number of bytes remaining in the
        // destination buffer.
        currentFragment.position(fragmentOffset);
        final int limit = Math.min(currentFragment.remaining(), destination.remaining());
        currentFragment.limit(currentFragment.position() + limit);
        destination.put(currentFragment);
        position = localPosition + limit;
        return limit;
    }

    private int fragmentIndexForByteNumber(final long byteNumber) {
        return Math.toIntExact(Math.floorDiv(byteNumber, (long) fragmentSize));
    }

    private CompletableFuture<ByteBuffer> computeFragmentFuture(final int fragmentIndex) {
        final long readFrom = (long) fragmentIndex * fragmentSize;
        final long readTo = Math.min(readFrom + fragmentSize, size) - 1;
        final String range = "bytes=" + readFrom + "-" + readTo;
        return s3Client.getObject(builder -> builder.bucket(bucket).key(key).range(range),
                new ByteBufferAsyncResponseTransformer<>(fragmentSize));
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException("Don't support writing to S3 yet");
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
        return size;
    }

    @Override
    public SeekableByteChannel truncate(final long size) {
        throw new UnsupportedOperationException("Currently not supported");
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
