package io.deephaven.parquet.table.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class S3SeekableByteChannel implements SeekableByteChannel, SeekableChannelsProvider.ContextHolder {

    /**
     * Context object used to store read-ahead buffers for efficiently reading from S3.
     */
    static final class ChannelContext implements SeekableChannelsProvider.ChannelContext {

        /**
         * Used to store context information for fetching a single fragment from S3
         */
        static class FragmentContext {
            private final int fragmentIndex;
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
    }

    private long position;
    private final S3AsyncClient s3Client;
    private volatile boolean closed;
    private final String bucket, key;
    private final int fragmentSize;
    private final int readAheadCount;
    private final int numFragmentsInObject;
    private final long size;
    private final Long timeout;
    private final TimeUnit timeUnit;
    private ChannelContext context;

    S3SeekableByteChannel(SeekableChannelsProvider.ChannelContext context, String bucket, String key, S3AsyncClient s3Client, long size, int fragmentSize, int readAheadCount) {
        this.position = 0;
        this.bucket = bucket;
        this.key = key;
        this.closed = false;
        this.s3Client = s3Client;

        this.fragmentSize = fragmentSize;
        this.readAheadCount = readAheadCount;
        this.timeout = 5L;
        this.timeUnit = TimeUnit.MINUTES;
        this.size = size;
        this.numFragmentsInObject = (int) Math.ceil((double) size / fragmentSize);
        this.context = (ChannelContext) context;
    }

    @Override
    public void setContext(@Nullable SeekableChannelsProvider.ChannelContext context) {
        // null context is allowed for clearing the context
        if (context != null && !(context instanceof ChannelContext)) {
            throw new IllegalArgumentException("context must be null or an instance of ChannelContext");
        }
        this.context = (ChannelContext) context;
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * <p> Bytes are read starting at this channel's current position, and
     * then the position is updated with the number of bytes actually read.
     * Otherwise, this method behaves exactly as specified in the {@link
     * ReadableByteChannel} interface.
     *
     * @param dst the destination buffer
     * @return the number of bytes read or -1 if no more bytes can be read.
     */
    @Override
    public int read(final ByteBuffer dst) throws IOException {
        validateOpen();

        Objects.requireNonNull(dst);

        final long channelPosition = this.position();

        // if the position of the delegator is at the end (>= size) return -1. we're finished reading.
        if (channelPosition >= size) {
            return -1;
        }

        // Figure out the index of the fragment the bytes would start in
        final int currFragmentIndex = fragmentIndexForByteNumber(channelPosition);
        final int fragmentOffset = (int) (channelPosition - ((long) currFragmentIndex * fragmentSize));
        Assert.neqNull(context, "context");

        // Blocking fetch the current fragment if it's not already in the cache
        final ChannelContext.FragmentContext fragmentContext = context.getFragmentContext(currFragmentIndex);
        final CompletableFuture<ByteBuffer> fetchCurrFragment;
        if (fragmentContext != null && fragmentContext.fragmentIndex == currFragmentIndex) {
            fetchCurrFragment = fragmentContext.future;
        } else {
            fetchCurrFragment = computeFragmentFuture(currFragmentIndex);
            context.setFragmentContext(currFragmentIndex, fetchCurrFragment);
        }
        final ByteBuffer currentFragment;
        try {
            currentFragment = fetchCurrFragment.get(timeout, timeUnit).asReadOnlyBuffer();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (final ExecutionException e) {
            throw new IOException(e);
        } catch (final TimeoutException e) {
            throw new RuntimeException(e);
        }

        // Put the bytes from fragment from the offset up to the min of fragment remaining or dst remaining
        currentFragment.position(fragmentOffset);
        final int limit = Math.min(currentFragment.remaining(), dst.remaining());
        currentFragment.limit(currentFragment.position() + limit);
        dst.put(currentFragment);

        // Send requests for read-ahead buffers
        final int numFragmentsToLoad = Math.min(readAheadCount, numFragmentsInObject - currFragmentIndex - 1);
        for (int i = 0; i < numFragmentsToLoad; i++) {
            final int readAheadFragmentIndex = i + currFragmentIndex + 1;
            final ChannelContext.FragmentContext readAheadFragmentContext = context.getFragmentContext(readAheadFragmentIndex);
            if (readAheadFragmentContext == null || readAheadFragmentContext.fragmentIndex != readAheadFragmentIndex) {
                context.setFragmentContext(readAheadFragmentIndex, computeFragmentFuture(readAheadFragmentIndex));
            }
        }

        position(channelPosition + limit);
        return limit;
    }

    /**
     * Compute which fragment a byte should be in
     *
     * @param byteNumber the number of the byte in the object accessed by this channel
     * @return the index of the fragment in which {@code byteNumber} will be found.
     */
    private int fragmentIndexForByteNumber(final long byteNumber) {
        return Math.toIntExact(Math.floorDiv(byteNumber, (long) fragmentSize));
    }

    private CompletableFuture<ByteBuffer> computeFragmentFuture(final int fragmentIndex) {
        final long readFrom = (long) fragmentIndex * fragmentSize;
        final long readTo = Math.min(readFrom + fragmentSize, size) - 1;
        final String range = "bytes=" + readFrom + "-" + readTo;

        return s3Client.getObject(
                        builder -> builder
                                .bucket(bucket)
                                .key(key)
                                .range(range),
                        new ByteBufferAsyncResponseTransformer<>(fragmentSize));
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException("Don't support writing to S3 yet");
    }

    @Override
    public long position() throws ClosedChannelException {
        validateOpen();

        synchronized (this) {
            return position;
        }
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws ClosedChannelException {
        if (newPosition < 0)
            throw new IllegalArgumentException("newPosition cannot be < 0");

        if (!isOpen()) {
            throw new ClosedChannelException();
        }

        synchronized (this) {
            position = newPosition;
            return this;
        }
    }

    @Override
    public long size() throws ClosedChannelException {
        validateOpen();
        return this.size;
    }

    @Override
    public SeekableByteChannel truncate(final long size) {
        throw new UnsupportedOperationException("Currently not supported");
    }

    @Override
    public boolean isOpen() {
        synchronized (this) {
            return !this.closed;
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            closed = true;
        }
    }

    private void validateOpen() throws ClosedChannelException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
    }
}
