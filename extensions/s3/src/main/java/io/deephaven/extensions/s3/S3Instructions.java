//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

/**
 * This class provides instructions intended for reading from and writing to S3-compatible APIs. The default values
 * documented in this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@CopyableStyle
public abstract class S3Instructions implements LogOutputAppendable {

    private final static int DEFAULT_MAX_CONCURRENT_REQUESTS = 256;
    private final static int DEFAULT_READ_AHEAD_COUNT = 32;
    private final static int DEFAULT_FRAGMENT_SIZE = 1 << 16; // 64 KiB
    private final static int MIN_FRAGMENT_SIZE = 8 << 10; // 8 KiB
    private final static int DEFAULT_MAX_CACHE_SIZE = 256;
    private final static Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(2);
    private final static Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(2);

    static final S3Instructions DEFAULT = builder().build();

    public static Builder builder() {
        return ImmutableS3Instructions.builder();
    }

    /**
     * The region name to use when reading or writing to S3. If not provided, the region name is picked by the AWS SDK
     * from 'aws.region' system property, "AWS_REGION" environment variable, the {user.home}/.aws/credentials or
     * {user.home}/.aws/config files, or from EC2 metadata service, if running in EC2.
     */
    public abstract Optional<String> regionName();

    /**
     * The maximum number of concurrent requests to make to S3, defaults to {@value #DEFAULT_MAX_CONCURRENT_REQUESTS}.
     */
    @Default
    public int maxConcurrentRequests() {
        return DEFAULT_MAX_CONCURRENT_REQUESTS;
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
     * The maximum byte size of each fragment to read from S3, defaults to {@value DEFAULT_FRAGMENT_SIZE}, must be
     * larger than {@value MIN_FRAGMENT_SIZE}. If there are fewer bytes remaining in the file, the fetched fragment can
     * be smaller.
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

    /**
     * The amount of time to wait when initially establishing a connection before giving up and timing out, defaults to
     * 2 seconds.
     */
    @Default
    public Duration connectionTimeout() {
        return DEFAULT_CONNECTION_TIMEOUT;
    }

    /**
     * The amount of time to wait when reading a fragment before giving up and timing out, defaults to 2 seconds. The
     * implementation may choose to internally retry the request multiple times, so long as the total time does not
     * exceed this timeout.
     */
    @Default
    public Duration readTimeout() {
        return DEFAULT_READ_TIMEOUT;
    }

    /**
     * The credentials to use when reading or writing to S3. By default, uses {@link Credentials#defaultCredentials()}.
     */
    @Default
    public Credentials credentials() {
        return Credentials.defaultCredentials();
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append(toString());
    }

    /**
     * The endpoint to connect to. Callers connecting to AWS do not typically need to set this; it is most useful when
     * connecting to non-AWS, S3-compatible APIs.
     *
     * @see <a href="https://docs.aws.amazon.com/general/latest/gr/s3.html">Amazon Simple Storage Service endpoints</a>
     */
    public abstract Optional<URI> endpointOverride();

    public interface Builder {
        Builder regionName(String regionName);

        Builder maxConcurrentRequests(int maxConcurrentRequests);

        Builder readAheadCount(int readAheadCount);

        Builder fragmentSize(int fragmentSize);

        Builder maxCacheSize(int maxCacheSize);

        Builder connectionTimeout(Duration connectionTimeout);

        Builder readTimeout(Duration connectionTimeout);

        Builder credentials(Credentials credentials);

        Builder endpointOverride(URI endpointOverride);

        default Builder endpointOverride(String endpointOverride) {
            return endpointOverride(URI.create(endpointOverride));
        }

        S3Instructions build();
    }

    abstract S3Instructions withReadAheadCount(int readAheadCount);

    abstract S3Instructions withMaxCacheSize(int maxCacheSize);

    @Lazy
    S3Instructions singleUse() {
        final int readAheadCount = Math.min(DEFAULT_READ_AHEAD_COUNT, readAheadCount());
        return withReadAheadCount(readAheadCount)
                .withMaxCacheSize(readAheadCount + 1);
    }

    @Check
    final void boundsCheckMaxConcurrentRequests() {
        if (maxConcurrentRequests() < 1) {
            throw new IllegalArgumentException("maxConcurrentRequests(=" + maxConcurrentRequests() + ") must be >= 1");
        }
    }

    @Check
    final void boundsCheckReadAheadCount() {
        if (readAheadCount() < 0) {
            throw new IllegalArgumentException("readAheadCount(=" + readAheadCount() + ") must be >= 0");
        }
    }

    @Check
    final void boundsCheckMinFragmentSize() {
        if (fragmentSize() < MIN_FRAGMENT_SIZE) {
            throw new IllegalArgumentException("fragmentSize(=" + fragmentSize() + ") must be >= " + MIN_FRAGMENT_SIZE +
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
    final void awsSdkV2Credentials() {
        if (!(credentials() instanceof AwsSdkV2Credentials)) {
            throw new IllegalArgumentException(
                    "credentials() must be created via provided io.deephaven.extensions.s3.Credentials methods");
        }
    }

    final AwsCredentialsProvider awsV2CredentialsProvider() {
        return ((AwsSdkV2Credentials) credentials()).awsV2CredentialsProvider();
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
