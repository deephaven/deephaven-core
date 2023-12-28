package io.deephaven.parquet.table;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

/**
 * This class provides instructions intended for reading and writing parquet files to AWS S3 instances.
 */
@Value.Immutable
@BuildableStyle
public abstract class S3ParquetInstructions {

    private final static int DEFAULT_MAX_CONCURRENT_REQUESTS = 50;
    private final static int DEFAULT_READ_AHEAD_COUNT = 1;
    private final static int DEFAULT_FRAGMENT_SIZE = 512 << 20; // 5 MB
    private final static int MIN_FRAGMENT_SIZE = 8 << 10; // 8 KB
    private final static int DEFAULT_MAX_CACHE_SIZE = 50;

    public static Builder builder() {
        return ImmutableS3ParquetInstructions.builder();
    }

    /**
     * The AWS region name to use when reading or writing to S3.
     */
    public abstract String awsRegionName();

    /**
     * The maximum number of concurrent requests to make to S3.
     */
    @Value.Default
    public int maxConcurrentRequests() {
        return DEFAULT_MAX_CONCURRENT_REQUESTS;
    }

    /**
     * The number of fragments to send asynchronous read requests for while reading the current fragment.
     */
    @Value.Default
    public int readAheadCount() {
        return DEFAULT_READ_AHEAD_COUNT;
    }

    /**
     * The maximum size of each fragment to read from S3. The fetched fragment can be smaller than this in case fewer
     * bytes remaining in the file.
     */
    @Value.Default
    public int fragmentSize() {
        return DEFAULT_FRAGMENT_SIZE;
    }

    /**
     * The maximum number of fragments to cache in memory.
     */
    @Value.Default
    public int maxCacheSize() {
        return DEFAULT_MAX_CACHE_SIZE;
    }

    @Value.Check
    final void boundsCheckMaxConcurrentRequests() {
        if (maxConcurrentRequests() < 1) {
            throw new IllegalArgumentException("maxConcurrentRequests(=" + maxConcurrentRequests() + ") must be >= 1");
        }
    }

    @Value.Check
    final void boundsCheckReadAheadCount() {
        if (readAheadCount() < 0) {
            throw new IllegalArgumentException("readAheadCount(=" + readAheadCount() + ") must be >= 0");
        }
    }

    @Value.Check
    final void boundsCheckMaxFragmentSize() {
        if (fragmentSize() < MIN_FRAGMENT_SIZE) {
            throw new IllegalArgumentException("fragmentSize(=" + fragmentSize() + ") must be >= 8*1024 or 8 KB");
        }
    }

    @Value.Check
    final void boundsCheckMaxCacheSize() {
        if (maxCacheSize() < readAheadCount() + 1) {
            throw new IllegalArgumentException("maxCacheSize(=" + maxCacheSize() + ") must be >= 1 + readAheadCount");
        }
    }

    public interface Builder {
        Builder awsRegionName(String awsRegionName);

        Builder maxConcurrentRequests(int maxConcurrentRequests);

        Builder readAheadCount(int readAheadCount);

        Builder fragmentSize(int fragmentSize);

        Builder maxCacheSize(int maxCacheSize);

        S3ParquetInstructions build();
    }
}
