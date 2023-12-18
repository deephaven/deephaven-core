package io.deephaven.parquet.table.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * A {@code ReadableByteChannel} delegate for an {@code S3SeekableByteChannel} that maintains internal read ahead
 * buffers to reduce the amount of high latency
 * requests to S3. If the bytes required by a read are already in the buffer, they will be fulfilled from the buffer
 * rather than making another S3 request.
 * <p>As reads are made this object will update the current read position of the delegating {@code S3SeekableByteChannel}</p>
 */
public class S3ReadAheadByteChannel implements ReadableByteChannel {

    private final S3AsyncClient client;
    private final String s3uri, bucket, key;
    private final S3SeekableByteChannel delegator;
    private final int maxFragmentSize;
    private final int maxNumberFragments;
    private final int numFragmentsInObject;
    private final long size;
    private final Long timeout;
    private final TimeUnit timeUnit;
    private boolean open;
    private final Cache<Integer, CompletableFuture<ByteBuffer>> readAheadBuffersCache;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    /**
     * Construct a new {@code S3ReadAheadByteChannel} which is used by its parent delegator to perform read operations.
     * The channel is backed by a cache that holds the buffered fragments of the object identified
     * by the {@code path}.
     *
     * @param maxFragmentSize    the maximum amount of bytes in a read ahead fragment. Must be {@code >= 1}.
     * @param maxNumberFragments the maximum number of read ahead fragments to hold. Must be {@code >= 2}.
     * @param client             the client used to read from the {@code path}
     * @param delegator          the {@code S3SeekableByteChannel} that delegates reading to this object.
     * @param timeout            the amount of time to wait for a read ahead fragment to be available.
     * @param timeUnit           the {@code TimeUnit} for the {@code timeout}.
     * @throws IOException if a problem occurs initializing the cached fragments
     */
    public S3ReadAheadByteChannel(String s3uri, String bucket, String key, int maxFragmentSize, int maxNumberFragments, S3AsyncClient client, S3SeekableByteChannel delegator, Long timeout, TimeUnit timeUnit) throws IOException {
        Objects.requireNonNull(client);
        Objects.requireNonNull(delegator);
        if (maxFragmentSize < 1)
            throw new IllegalArgumentException("maxFragmentSize must be >= 1");
        if (maxNumberFragments < 2)
            throw new IllegalArgumentException("maxNumberFragments must be >= 2");

        logger.debug("max read ahead fragments '{}' with size '{}' bytes", maxNumberFragments, maxFragmentSize);
        this.client = client;
        this.s3uri = s3uri;
        this.bucket = bucket;
        this.key = key;
        this.delegator = delegator;
        this.size = delegator.size();
        this.maxFragmentSize = maxFragmentSize;
        this.numFragmentsInObject = (int) Math.ceil((float) size / (float) maxFragmentSize);
        this.readAheadBuffersCache = Caffeine.newBuilder().maximumSize(maxNumberFragments).recordStats().build();
        this.maxNumberFragments = maxNumberFragments;
        this.open = true;
        this.timeout = timeout != null ? timeout : 5L;
        this.timeUnit = timeUnit != null ? timeUnit : TimeUnit.MINUTES;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        Objects.requireNonNull(dst);

        long channelPosition = delegator.position();
        logger.debug("delegator position: {}", channelPosition);

        // if the position of the delegator is at the end (>= size) return -1. we're finished reading.
        if (channelPosition >= size) return -1;

        //figure out the index of the fragment the bytes would start in
        Integer fragmentIndex = fragmentIndexForByteNumber(channelPosition);
        logger.debug("fragment index: {}", fragmentIndex);

        int fragmentOffset = (int) (channelPosition - (fragmentIndex.longValue() * maxFragmentSize));
        logger.debug("fragment {} offset: {}", fragmentIndex, fragmentOffset);

        try {
            final ByteBuffer fragment = Objects.requireNonNull(readAheadBuffersCache.get(fragmentIndex, this::computeFragmentFuture))
                    .get(timeout, timeUnit)
                    .asReadOnlyBuffer();

            fragment.position(fragmentOffset);
            logger.debug("fragment remaining: {}", fragment.remaining());
            logger.debug("dst remaining: {}",  dst.remaining());

            //put the bytes from fragment from the offset upto the min of fragment remaining or dst remaining
            int limit = Math.min(fragment.remaining(), dst.remaining());
            logger.debug("byte limit: {}", limit);

            byte[] copiedBytes = new byte[limit];
            fragment.get(copiedBytes, 0, limit);
            dst.put(copiedBytes);

            if (fragment.position() >= fragment.limit() / 2) {

                // clear any fragments in cache that are lower index than this one   // <-- This can be an issue for us
                clearPriorFragments(fragmentIndex);

                // until available cache slots are filled or number of fragments in file
                int maxFragmentsToLoad = Math.min(maxNumberFragments - 1, numFragmentsInObject - fragmentIndex - 1);

                for (int i = 0; i < maxFragmentsToLoad; i++) {
                    final int idxToLoad = i + fragmentIndex + 1;

                    //  add the index if it's not already there
                    if (readAheadBuffersCache.asMap().containsKey(idxToLoad))
                        continue;

                    logger.debug("initiate pre-loading fragment with index '{}' from '{}'", idxToLoad, s3uri);
                    readAheadBuffersCache.put(idxToLoad, computeFragmentFuture(idxToLoad));
                }
            }

            delegator.position(channelPosition + copiedBytes.length);
            return copiedBytes.length;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // the async execution completed exceptionally.
            // not currently obvious when this will happen or if we can recover
            logger.error("an exception occurred while reading bytes from {} that was not recovered by the S3 Client RetryCondition(s)", s3uri);
            throw new IOException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearPriorFragments(int currentFragIndx) {
        final Set<@NonNull Integer> priorIndexes = readAheadBuffersCache
                .asMap()
                .keySet().stream()
                .filter(idx -> idx < currentFragIndx)
                .collect(Collectors.toSet());

        if (priorIndexes.size() > 0) {
            logger.debug("invalidating fragment(s) '{}' from '{}'",
                    priorIndexes.stream().map(Objects::toString).collect(Collectors.joining(", ")), s3uri);

            readAheadBuffersCache.invalidateAll(priorIndexes);
        }
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
        readAheadBuffersCache.invalidateAll();
        readAheadBuffersCache.cleanUp();
    }

    /**
     * The number of fragments currently in the cache.
     *
     * @return the size of the cache after any async evictions or reloads have happened.
     */
    protected int numberOfCachedFragments() {
        readAheadBuffersCache.cleanUp();
        return (int) readAheadBuffersCache.estimatedSize();
    }

    /**
     * Obtain a snapshot of the statistics of the internal cache, provides information about hits, misses, requests, evictions etc.
     * that are useful for tuning.
     *
     * @return the statistics of the internal cache.
     */
    protected CacheStats cacheStatistics() {
        return readAheadBuffersCache.stats();
    }

    private CompletableFuture<ByteBuffer> computeFragmentFuture(int fragmentIndex) {
        long readFrom = (long) fragmentIndex * maxFragmentSize;
        long readTo = Math.min(readFrom + maxFragmentSize, size) - 1;
        String range = "bytes=" + readFrom + "-" + readTo;
        logger.debug("byte range for {} is '{}'", key, range);

        return client.getObject(
                        builder -> builder
                                .bucket(bucket)
                                .key(key)
                                .range(range),
                        AsyncResponseTransformer.toBytes())
                .thenApply(BytesWrapper::asByteBuffer);
    }

    /**
     * Compute which buffer a byte should be in
     *
     * @param byteNumber the number of the byte in the object accessed by this channel
     * @return the index of the fragment in which {@code byteNumber} will be found.
     */
    protected Integer fragmentIndexForByteNumber(long byteNumber) {
        return Math.toIntExact(Math.floorDiv(byteNumber, (long) maxFragmentSize));
    }
}
