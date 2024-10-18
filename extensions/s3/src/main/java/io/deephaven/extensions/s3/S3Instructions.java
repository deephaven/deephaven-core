//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.annotations.VisibleForTesting;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.profiles.ProfileFile;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

/**
 * This class provides instructions intended for reading from and writing to S3-compatible APIs. The default values
 * documented in this class may change in the future. As such, callers may wish to explicitly set the values.
 */
@Immutable
@CopyableStyle
public abstract class S3Instructions implements LogOutputAppendable {

    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 256;
    private static final int DEFAULT_READ_AHEAD_COUNT = 32;
    private static final int DEFAULT_FRAGMENT_SIZE = 1 << 16; // 64 KiB
    private static final int MIN_FRAGMENT_SIZE = 8 << 10; // 8 KiB
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(2);
    private static final int DEFAULT_NUM_CONCURRENT_WRITE_PARTS = 64;

    /**
     * We set default part size to 10 MiB. The maximum number of parts allowed is 10,000. This means maximum size of a
     * single file that we can write is roughly 100k MiB (or about 98 GiB). For uploading larger files, user would need
     * to set a larger part size.
     *
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html">Amazon S3 User Guide</a>
     */
    private static final int DEFAULT_WRITE_PART_SIZE = 10 << 20; // 10 MiB
    static final int MIN_WRITE_PART_SIZE = 5 << 20; // 5 MiB

    static final S3Instructions DEFAULT = builder().build();

    public static Builder builder() {
        return ImmutableS3Instructions.builder();
    }

    /**
     * The region name to use when reading or writing to S3. If not provided, the region name is picked by the AWS SDK
     * from 'aws.region' system property, "AWS_REGION" environment variable, the {user.home}/.aws/credentials or
     * {user.home}/.aws/config files, or from EC2 metadata service, if running in EC2. If no region name is derived from
     * the above chain or derived the region name derived is incorrect for the bucket accessed, the correct region name
     * will be derived internally, at the cost of one additional request.
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
     * The maximum byte size of each fragment to read from S3 in bytes, defaults to {@value DEFAULT_FRAGMENT_SIZE}, must
     * be larger than {@value MIN_FRAGMENT_SIZE}. If there are fewer bytes remaining in the file, the fetched fragment
     * can be smaller.
     */
    @Default
    public int fragmentSize() {
        return DEFAULT_FRAGMENT_SIZE;
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
     * The credentials to use when reading or writing to S3. By default, uses {@link Credentials#resolving()}.
     */
    @Default
    public Credentials credentials() {
        return Credentials.resolving();
    }

    /**
     * The size of each part (in bytes) to upload when writing to S3, defaults to {@value #DEFAULT_WRITE_PART_SIZE}. The
     * minimum allowed part size is {@value #MIN_WRITE_PART_SIZE}. Setting a higher value may increase throughput, but
     * may also increase memory usage. Note that the maximum number of parts allowed for a single file is 10,000.
     * Therefore, for {@value #DEFAULT_WRITE_PART_SIZE} part size, the maximum size of a single file that can be written
     * is {@value #DEFAULT_WRITE_PART_SIZE} * 10,000 bytes.
     */
    @Default
    public int writePartSize() {
        return DEFAULT_WRITE_PART_SIZE;
    }

    /**
     * The maximum number of parts that can be uploaded concurrently when writing to S3 without blocking. Setting a
     * higher value may increase throughput, but may also increase memory usage. Defaults to
     * {@value #DEFAULT_NUM_CONCURRENT_WRITE_PARTS}.
     */
    @Default
    public int numConcurrentWriteParts() {
        return DEFAULT_NUM_CONCURRENT_WRITE_PARTS;
    }

    /**
     * The default profile name used for configuring the default region, credentials, etc., when reading or writing to
     * S3. If not provided, the AWS SDK picks the profile name from the 'aws.profile' system property, the "AWS_PROFILE"
     * environment variable, or defaults to "default".
     * <p>
     * Setting a profile name assumes that the credentials are provided via this profile; if that is not the case, you
     * must explicitly set {@link #credentials() credentials}.
     *
     * @see ClientOverrideConfiguration.Builder#defaultProfileName(String)
     */
    public abstract Optional<String> profileName();

    /**
     * The path to the configuration file to use for configuring the default region, credentials, etc. when reading or
     * writing to S3. If not provided, the AWS SDK picks the configuration file from the 'aws.configFile' system
     * property, the "AWS_CONFIG_FILE" environment variable, or defaults to "{user.home}/.aws/config".
     * <p>
     * Setting a configuration file path assumes that the credentials are provided via the configuration and credentials
     * files; if that is not the case, you must explicitly set {@link #credentials() credentials}.
     *
     * @see ClientOverrideConfiguration.Builder#defaultProfileFile(ProfileFile)
     */
    public abstract Optional<Path> configFilePath();

    /**
     * The path to the credentials file to use for configuring the default region, credentials, etc. when reading or
     * writing to S3. If not provided, the AWS SDK picks the credentials file from the 'aws.credentialsFile' system
     * property, the "AWS_CREDENTIALS_FILE" environment variable, or defaults to "{user.home}/.aws/credentials".
     * <p>
     * Setting a credentials file path assumes that the credentials are provided via the config and credentials files;
     * if that is not the case, you must explicitly set {@link #credentials() credentials}.
     *
     * @see ClientOverrideConfiguration.Builder#defaultProfileFile(ProfileFile)
     */
    public abstract Optional<Path> credentialsFilePath();

    /**
     * The aggregated profile file that combines the configuration and credentials files.
     */
    @Lazy
    Optional<ProfileFile> aggregatedProfileFile() {
        if (configFilePath().isPresent() || credentialsFilePath().isPresent()) {
            return Optional.of(S3Utils.aggregateProfileFile(configFilePath(), credentialsFilePath()));
        }
        return Optional.empty();
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

    public abstract S3Instructions withEndpointOverride(final URI endpointOverride);

    public interface Builder {
        Builder regionName(String regionName);

        Builder maxConcurrentRequests(int maxConcurrentRequests);

        Builder readAheadCount(int readAheadCount);

        Builder fragmentSize(int fragmentSize);

        Builder connectionTimeout(Duration connectionTimeout);

        Builder readTimeout(Duration connectionTimeout);

        Builder credentials(Credentials credentials);

        Builder endpointOverride(URI endpointOverride);

        Builder writePartSize(int writePartSize);

        Builder numConcurrentWriteParts(int numConcurrentWriteParts);

        Builder profileName(String profileName);

        Builder configFilePath(Path configFilePath);

        Builder credentialsFilePath(Path credentialsFilePath);

        default Builder endpointOverride(final String endpointOverride) {
            return endpointOverride(URI.create(endpointOverride));
        }

        default Builder configFilePath(final String configFilePath) {
            return configFilePath(Path.of(configFilePath));
        }

        default Builder credentialsFilePath(final String credentialsFilePath) {
            return credentialsFilePath(Path.of(credentialsFilePath));
        }

        S3Instructions build();
    }

    abstract S3Instructions withReadAheadCount(int readAheadCount);

    @VisibleForTesting
    public abstract S3Instructions withRegionName(Optional<String> regionName);

    @Lazy
    S3Instructions singleUse() {
        final int readAheadCount = Math.min(DEFAULT_READ_AHEAD_COUNT, readAheadCount());
        return withReadAheadCount(readAheadCount);
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
    final void awsSdkV2Credentials() {
        if (!(credentials() instanceof AwsSdkV2Credentials)) {
            throw new IllegalArgumentException(
                    "credentials() must be created via provided io.deephaven.extensions.s3.Credentials methods");
        }
    }

    @Check
    final void boundsCheckWritePartSize() {
        if (writePartSize() < MIN_WRITE_PART_SIZE) {
            throw new IllegalArgumentException(
                    "writePartSize(=" + writePartSize() + ") must be >= " + MIN_WRITE_PART_SIZE + " MiB");
        }
    }

    @Check
    final void boundsCheckMinNumConcurrentWriteParts() {
        if (numConcurrentWriteParts() < 1) {
            throw new IllegalArgumentException(
                    "numConcurrentWriteParts(=" + numConcurrentWriteParts() + ") must be >= 1");
        }
    }

    @Check
    final void boundsCheckMaxNumConcurrentWriteParts() {
        if (numConcurrentWriteParts() > maxConcurrentRequests()) {
            throw new IllegalArgumentException(
                    "numConcurrentWriteParts(=" + numConcurrentWriteParts() + ") must be <= " +
                            "maxConcurrentRequests(=" + maxConcurrentRequests() + ")");
        }
    }

    final AwsCredentialsProvider awsV2CredentialsProvider() {
        return ((AwsSdkV2Credentials) credentials()).awsV2CredentialsProvider(this);
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
