package io.deephaven.parquet.table.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.deephaven.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class S3SeekableByteChannel implements SeekableByteChannel {
    private long position;
    private final S3AsyncClient s3Client;
    private volatile boolean closed;
    private final String s3uri, bucket, key;
    private final int maxFragmentSize;
    private final int maxNumberFragments;
    private final int numFragmentsInObject;
    private final long size;
    private final Long timeout;
    private final TimeUnit timeUnit;
    private final Cache<Integer, CompletableFuture<ByteBuffer>> readAheadBuffersCache;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final int MAX_FRAGMENT_SIZE =
            Configuration.getInstance().getIntegerWithDefault("s3.spi.read.max-fragment-size", 512 * 1024); // 512 KB
    private static final int MAX_FRAGMENT_NUMBER =
            Configuration.getInstance().getIntegerWithDefault("s3.spi.read.max-fragment-number", 2);


    S3SeekableByteChannel(String s3uri, String bucket, String key, S3AsyncClient s3Client, long startAt, long size,
                          Long timeout, TimeUnit timeUnit, final Cache<Integer, CompletableFuture<ByteBuffer>> readAheadBuffersCache) throws IOException {
        Objects.requireNonNull(s3Client);
        if (MAX_FRAGMENT_SIZE < 1)
            throw new IllegalArgumentException("maxFragmentSize must be >= 1");
        if (MAX_FRAGMENT_NUMBER < 1)
            throw new IllegalArgumentException("maxNumberFragments must be >= 1");
        if (size < 1)
            throw new IllegalArgumentException("size must be >= 1");

        this.position = startAt;
        this.bucket = bucket;
        this.key = key;
        this.closed = false;
        this.s3Client = s3Client;

        this.s3uri = s3uri;
        this.maxFragmentSize = MAX_FRAGMENT_SIZE;
        this.readAheadBuffersCache = readAheadBuffersCache;
        this.maxNumberFragments = MAX_FRAGMENT_NUMBER;
        this.timeout = timeout != null ? timeout : 5L;
        this.timeUnit = timeUnit != null ? timeUnit : TimeUnit.MINUTES;
        this.size = size;
        this.numFragmentsInObject = (int) Math.ceil((double) size / maxFragmentSize);
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

        //figure out the index of the fragment the bytes would start in
        final int fragmentIndex = fragmentIndexForByteNumber(channelPosition);
        final int fragmentOffset = (int) (channelPosition - ((long) fragmentIndex * maxFragmentSize));
        try {
            final ByteBuffer fragment = Objects.requireNonNull(readAheadBuffersCache.get(fragmentIndex, this::computeFragmentFuture))
                    .get(timeout, timeUnit)
                    .asReadOnlyBuffer();

            //put the bytes from fragment from the offset upto the min of fragment remaining or dst remaining
            fragment.position(fragmentOffset);
            final int limit = Math.min(fragment.remaining(), dst.remaining());
            fragment.limit(fragment.position() + limit);
            dst.put(fragment);

            if (fragment.position() >= fragment.limit() / 2) {

                // until available cache slots are filled or number of fragments in file
                final int maxFragmentsToLoad = Math.min(maxNumberFragments - 1, numFragmentsInObject - fragmentIndex - 1);

                for (int i = 0; i < maxFragmentsToLoad; i++) {
                    final int idxToLoad = i + fragmentIndex + 1;

                    //  add the index if it's not already there
                    if (readAheadBuffersCache.asMap().containsKey(idxToLoad))
                        continue;

                    readAheadBuffersCache.put(idxToLoad, computeFragmentFuture(idxToLoad));
                }
            }

            position(channelPosition + limit);
            return limit;

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (final ExecutionException e) {
            // the async execution completed exceptionally.
            // not currently obvious when this will happen or if we can recover
            logger.error("an exception occurred while reading bytes from {} that was not recovered by the S3 Client RetryCondition(s)", s3uri);
            throw new IOException(e);
        } catch (final TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compute which buffer a byte should be in
     *
     * @param byteNumber the number of the byte in the object accessed by this channel
     * @return the index of the fragment in which {@code byteNumber} will be found.
     */
    private int fragmentIndexForByteNumber(final long byteNumber) {
        return Math.toIntExact(Math.floorDiv(byteNumber, (long) maxFragmentSize));
    }

    private CompletableFuture<ByteBuffer> computeFragmentFuture(final int fragmentIndex) {
        final long readFrom = (long) fragmentIndex * maxFragmentSize;
        final long readTo = Math.min(readFrom + maxFragmentSize, size) - 1;
        final String range = "bytes=" + readFrom + "-" + readTo;
        logger.debug("byte range for {} is '{}'", key, range);

        return s3Client.getObject(
                        builder -> builder
                                .bucket(bucket)
                                .key(key)
                                .range(range),
                        new AsyncAWSResponseTransformer<>(maxFragmentSize));
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p> Bytes are written starting at this channel's current position, unless
     * the channel is connected to an entity such as a file that is opened with
     * the {@link StandardOpenOption#APPEND APPEND} option, in
     * which case the position is first advanced to the end. The entity to which
     * the channel is connected will grow to accommodate the
     * written bytes, and the position updates with the number of bytes
     * actually written. Otherwise, this method behaves exactly as specified by
     * the {@link WritableByteChannel} interface.
     *
     * @param src the src of the bytes to write to this channel
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException("Don't support writing to S3 yet");
    }

    /**
     * Returns this channel's position.
     *
     * @return This channel's position,
     * a non-negative integer counting the number of bytes
     * from the beginning of the entity to the current position
     */
    @Override
    public long position() throws IOException {
        validateOpen();

        synchronized (this) {
            return position;
        }
    }

    /**
     * Sets this channel's position.
     *
     * <p> Setting the position to a value that is greater than the current size
     * is legal but does not change the size of the entity.  A later attempt to
     * read bytes at such a position will immediately return an end-of-file
     * indication.  A later attempt to write bytes at such a position will cause
     * the entity to grow to accommodate the new bytes; the values of any bytes
     * between the previous end-of-file and the newly-written bytes are
     * unspecified.
     *
     * <p> Setting the channel's position is not recommended when connected to
     * an entity, typically a file, that is opened with the {@link
     * StandardOpenOption#APPEND APPEND} option. When opened for
     * append, the position is first advanced to the end before writing.
     *
     * @param newPosition The new position, a non-negative integer counting
     *                    the number of bytes from the beginning of the entity
     * @return This channel
     * @throws ClosedChannelException   If this channel is closed
     * @throws IllegalArgumentException If the new position is negative
     * @throws IOException              If some other I/O error occurs
     */
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
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

    /**
     * Returns the current size of entity to which this channel is connected.
     *
     * @return The current size, measured in bytes
     * @throws IOException If some other I/O error occurs
     */
    @Override
    public long size() throws IOException {
        validateOpen();
        return this.size;
    }

    /**
     * Truncates the entity, to which this channel is connected, to the given
     * size.
     *
     * <p> If the given size is less than the current size then the entity is
     * truncated, discarding any bytes beyond the new end. If the given size is
     * greater than or equal to the current size then the entity is not modified.
     * In either case, if the current position is greater than the given size
     * then it is set to that size.
     *
     * <p> An implementation of this interface may prohibit truncation when
     * connected to an entity, typically a file, opened with the {@link
     * StandardOpenOption#APPEND APPEND} option.
     *
     * @param size The new size, a non-negative byte count
     * @return This channel
     */
    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException("Currently not supported");
    }

    /**
     * Tells whether this channel is open.
     *
     * @return {@code true} if, and only if, this channels delegate is open
     */
    @Override
    public boolean isOpen() {
        synchronized (this) {
            return !this.closed;
        }
    }

    /**
     * The number of fragments currently in the cache.
     *
     * @return the size of the cache after any async evictions or reloads have happened.
     */
    int numberOfCachedFragments() {
        readAheadBuffersCache.cleanUp();
        return (int) readAheadBuffersCache.estimatedSize();
    }

    /**
     * Obtain a snapshot of the statistics of the internal cache, provides information about hits, misses, requests, evictions etc.
     * that are useful for tuning.
     *
     * @return the statistics of the internal cache.
     */
    CacheStats cacheStatistics() {
        return readAheadBuffersCache.stats();
    }

    /**
     * Closes this channel.
     *
     * <p> After a channel is closed, any further attempt to invoke I/O
     * operations upon it will cause a {@link ClosedChannelException} to be
     * thrown.
     *
     * <p> If this channel is already closed then invoking this method has no
     * effect.
     *
     * <p> This method may be invoked at any time.  If some other thread has
     * already invoked it, however, then another invocation will block until
     * the first invocation is complete, after which it will return without
     * effect. </p>
     */
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
