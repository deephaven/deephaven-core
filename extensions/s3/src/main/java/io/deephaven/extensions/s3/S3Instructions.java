/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.configuration.Configuration;
import org.immutables.value.Value;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.time.Duration;

/**
 * This class provides instructions intended for reading and writing data to AWS S3 instances.
 */
@Value.Immutable
@BuildableStyle
public abstract class S3Instructions {

    private final static int DEFAULT_MAX_CONCURRENT_REQUESTS = 50;
    private final static int DEFAULT_READ_AHEAD_COUNT = 1;

    private final static String MAX_FRAGMENT_SIZE_CONFIG_PARAM = "S3.maxFragmentSize";
    final static int MAX_FRAGMENT_SIZE =
            Configuration.getInstance().getIntegerWithDefault(MAX_FRAGMENT_SIZE_CONFIG_PARAM, 5 << 20); // 5 MB
    private final static int DEFAULT_FRAGMENT_SIZE = MAX_FRAGMENT_SIZE;

    private final static int MIN_FRAGMENT_SIZE = 8 << 10; // 8 KB
    private final static int DEFAULT_MAX_CACHE_SIZE = 32;
    private final static Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(2);
    private final static Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(2);

    public static Builder builder() {
        return ImmutableS3Instructions.builder();
    }

    /**
     * The AWS region name to use when reading or writing to S3.
     */
    public abstract String awsRegionName();

    /**
     * The maximum number of concurrent requests to make to S3, defaults to {@value #DEFAULT_MAX_CONCURRENT_REQUESTS}.
     */
    @Value.Default
    public int maxConcurrentRequests() {
        return DEFAULT_MAX_CONCURRENT_REQUESTS;
    }

    /**
     * The number of fragments to send asynchronous read requests for while reading the current fragment. Defaults to
     * {@value #DEFAULT_READ_AHEAD_COUNT}, which means by default, we will fetch {@value #DEFAULT_READ_AHEAD_COUNT}
     * fragments in advance when reading current fragment.
     */
    @Value.Default
    public int readAheadCount() {
        return DEFAULT_READ_AHEAD_COUNT;
    }

    /**
     * The maximum size of each fragment to read from S3, defaults to the value of config parameter
     * {@value MAX_FRAGMENT_SIZE_CONFIG_PARAM}. If there are fewer bytes remaining in the file, the fetched fragment can
     * be smaller.
     */
    @Value.Default
    public int fragmentSize() {
        return DEFAULT_FRAGMENT_SIZE;
    }

    /**
     * The maximum number of fragments to cache in memory, defaults to {@value #DEFAULT_MAX_CACHE_SIZE}. This caching is
     * done at the deephaven layer for faster access to recently read fragments.
     */
    @Value.Default
    public int maxCacheSize() {
        return DEFAULT_MAX_CACHE_SIZE;
    }

    /**
     * The amount of time to wait when initially establishing a connection before giving up and timing out, defaults to
     * 2 seconds.
     */
    @Value.Default
    public Duration connectionTimeout() {
        return DEFAULT_CONNECTION_TIMEOUT;
    }

    /**
     * The amount of time to wait when reading a fragment before giving up and timing out, defaults to 2 seconds
     */
    @Value.Default
    public Duration readTimeout() {
        return DEFAULT_READ_TIMEOUT;
    }

    /**
     * The credentials to use when reading or writing to S3. By default, uses {@link DefaultCredentialsProvider}.
     */
    @Value.Default
    public AwsCredentials credentials() {
        return AwsCredentials.defaultCredentials();
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
            throw new IllegalArgumentException("fragmentSize(=" + fragmentSize() + ") must be >= " + MIN_FRAGMENT_SIZE +
                    " bytes");
        }
        if (fragmentSize() > MAX_FRAGMENT_SIZE) {
            throw new IllegalArgumentException("fragmentSize(=" + fragmentSize() + ") must be <= " + MAX_FRAGMENT_SIZE +
                    " bytes");
        }
    }

    @Value.Check
    final void boundsCheckMaxCacheSize() {
        if (maxCacheSize() < readAheadCount() + 1) {
            throw new IllegalArgumentException("maxCacheSize(=" + maxCacheSize() + ") must be >= 1 + " +
                    "readAheadCount(=" + readAheadCount() + ")");
        }
    }

    public interface Builder {
        Builder awsRegionName(String awsRegionName);

        Builder maxConcurrentRequests(int maxConcurrentRequests);

        Builder readAheadCount(int readAheadCount);

        Builder fragmentSize(int fragmentSize);

        Builder maxCacheSize(int maxCacheSize);

        Builder connectionTimeout(Duration connectionTimeout);

        Builder readTimeout(Duration connectionTimeout);

        Builder credentials(AwsCredentials credentials);

        S3Instructions build();
    }
}
