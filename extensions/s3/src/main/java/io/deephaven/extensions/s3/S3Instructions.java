//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.configuration.Configuration;
import org.immutables.value.Value;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;

import java.time.Duration;

/**
 * This class provides instructions intended for reading from and writing to S3-compatible APIs. The default values
 * documented in this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@CopyableStyle
public abstract class S3Instructions implements LogOutputAppendable {

    private final static int DEFAULT_READ_AHEAD_COUNT = 1;
    private final static String MAX_FRAGMENT_SIZE_CONFIG_PARAM = "S3.maxFragmentSize";
    final static int MAX_FRAGMENT_SIZE =
            Configuration.getInstance().getIntegerWithDefault(MAX_FRAGMENT_SIZE_CONFIG_PARAM, 5 << 20); // 5 MiB
    private final static int DEFAULT_FRAGMENT_SIZE = MAX_FRAGMENT_SIZE;
    private final static int SINGLE_USE_FRAGMENT_SIZE_DEFAULT = Math.min(65536, MAX_FRAGMENT_SIZE); // 64 KiB
    private final static int MIN_FRAGMENT_SIZE = 8 << 10; // 8 KiB
    private final static int DEFAULT_MAX_CACHE_SIZE = 32;
    private final static Duration DEFAULT_READ_TIMEOUT = DeephavenS3AsyncClientFactory.DEFAULT_READ_TIMEOUT;

    public static Builder builder() {
        return ImmutableS3Instructions.builder();
    }

    // TODO(Ryan) Right now readTimeout is duplicated between S3Instructions and DeephavenS3AsyncClientFactory.
    // readTimeout is used both inside the client and in outside code.
    // I moved regionName inside and regionName is a manadatory parameter. But that would mean all users would now have
    // to make a client and supply a regionName.
    // Also, readTImeout would also need to be set in the client builder.
    // TODO(malhotras) Fix and test the python code based on what we decide here

    /**
     * Check {@link DeephavenS3AsyncClientFactory#readTimeout()}
     */
    @Value.Default
    Duration readTimeout() {
        if (asyncClientFactory() instanceof DeephavenS3AsyncClientFactory) {
            return ((DeephavenS3AsyncClientFactory) asyncClientFactory()).readTimeout();
        }
        return DEFAULT_READ_TIMEOUT;
    }

    @Value.Default
    public S3AsyncClientFactory asyncClientFactory() {
        return DeephavenS3AsyncClientFactory.builder()
                .readTimeout(readTimeout())
                .build();
    }

    /**
     * The number of fragments to send asynchronous read requests for while reading the current fragment. Defaults to
     * {@value #DEFAULT_READ_AHEAD_COUNT}, which means by default, we will fetch {@value #DEFAULT_READ_AHEAD_COUNT}
     * fragments in advance when reading current fragment.
     */
    @Default
    public int readAheadCount() {
        return DEFAULT_READ_AHEAD_COUNT;
    }

    /**
     * The maximum byte size of each fragment to read from S3, defaults to the value of config parameter
     * {@value MAX_FRAGMENT_SIZE_CONFIG_PARAM}, or 5 MiB if unset. Must be between 8 KiB and the value of config
     * parameter {@value MAX_FRAGMENT_SIZE_CONFIG_PARAM}. If there are fewer bytes remaining in the file, the fetched
     * fragment can be smaller.
     */
    @Default
    public int fragmentSize() {
        return DEFAULT_FRAGMENT_SIZE;
    }

    /**
     * The maximum number of fragments to cache in memory, defaults to
     * {@code Math.max(1 + readAheadCount(), DEFAULT_MAX_CACHE_SIZE)}, which is at least
     * {@value #DEFAULT_MAX_CACHE_SIZE}. This caching is done at the deephaven layer for faster access to recently read
     * fragments. Must be greater than or equal to {@code 1 + readAheadCount()}.
     */
    @Default
    public int maxCacheSize() {
        return Math.max(1 + readAheadCount(), DEFAULT_MAX_CACHE_SIZE);
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append(toString());
    }

    public interface Builder {
        Builder asyncClientFactory(S3AsyncClientFactory asyncClientFactory);

        Builder readTimeout(Duration readTimeout);

        Builder readAheadCount(int readAheadCount);

        Builder fragmentSize(int fragmentSize);

        Builder maxCacheSize(int maxCacheSize);

        S3Instructions build();
    }

    abstract S3Instructions withReadAheadCount(int readAheadCount);

    abstract S3Instructions withFragmentSize(int fragmentSize);

    abstract S3Instructions withMaxCacheSize(int maxCacheSize);

    @Lazy
    S3Instructions singleUse() {
        final int readAheadCount = Math.min(DEFAULT_READ_AHEAD_COUNT, readAheadCount());
        return withReadAheadCount(readAheadCount)
                .withFragmentSize(Math.min(SINGLE_USE_FRAGMENT_SIZE_DEFAULT, fragmentSize()))
                .withMaxCacheSize(readAheadCount + 1);
    }

    @Check
    final void boundsCheckReadAheadCount() {
        if (readAheadCount() < 0) {
            throw new IllegalArgumentException("readAheadCount(=" + readAheadCount() + ") must be >= 0");
        }
    }

    @Check
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

    @Check
    final void boundsCheckMaxCacheSize() {
        if (maxCacheSize() < readAheadCount() + 1) {
            throw new IllegalArgumentException("maxCacheSize(=" + maxCacheSize() + ") must be >= 1 + " +
                    "readAheadCount(=" + readAheadCount() + ")");
        }
    }

    @Check
    final void consistencyCheckReadTimeout() {
        if (asyncClientFactory() instanceof DeephavenS3AsyncClientFactory &&
                readTimeout() != ((DeephavenS3AsyncClientFactory) asyncClientFactory()).readTimeout()) {
            throw new IllegalArgumentException(
                    "readTimeout(=" + readTimeout() + ") must match asyncClientFactory.readTimeout(=" +
                            ((DeephavenS3AsyncClientFactory) asyncClientFactory()).readTimeout() + ")");
        }
    }

    // If necessary, we _could_ plumb support for "S3-compatible" services which don't support virtual-host style
    // requests via software.amazon.awssdk.services.s3.S3BaseClientBuilder.forcePathStyle. Originally, AWS planned to
    // deprecate path-style requests, but that has been delayed an indefinite amount of time. In the meantime, we'll
    // keep S3Instructions simpler.
    // https://aws.amazon.com/blogs/storage/update-to-amazon-s3-path-deprecation-plan/
    // @Default
    // public boolean forcePathStyle() {
    // return false;
    // }
}
