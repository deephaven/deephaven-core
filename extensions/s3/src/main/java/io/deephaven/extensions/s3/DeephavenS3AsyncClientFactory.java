//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Immutable
@CopyableStyle
public abstract class DeephavenS3AsyncClientFactory implements S3AsyncClientFactory {

    private final static int DEFAULT_MAX_CONCURRENT_REQUESTS = 50;
    private final static Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(2);
    final static Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(2);

    public static DeephavenS3AsyncClientFactory.Builder builder() {
        return ImmutableDeephavenS3AsyncClientFactory.builder();
    }

    /**
     * The region name to use when reading or writing to S3.
     */
    public abstract String regionName();

    /**
     * The amount of time to wait when reading a fragment before giving up and timing out, defaults to 2 seconds. The
     * implementation may choose to internally retry the request multiple times, so long as the total time does not
     * exceed this timeout.
     */
    @Value.Default
    public Duration readTimeout() {
        return DEFAULT_READ_TIMEOUT;
    }

    /**
     * The maximum number of concurrent requests to make to S3, defaults to {@value #DEFAULT_MAX_CONCURRENT_REQUESTS}.
     */
    @Value.Default
    public int maxConcurrentRequests() {
        return DEFAULT_MAX_CONCURRENT_REQUESTS;
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
     * The credentials to use when reading or writing to S3. By default, uses {@link Credentials#defaultCredentials()}.
     */
    @Value.Default
    public Credentials credentials() {
        return Credentials.defaultCredentials();
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

        Builder connectionTimeout(Duration connectionTimeout);

        Builder readTimeout(Duration connectionTimeout);

        Builder credentials(Credentials credentials);

        Builder endpointOverride(URI endpointOverride);

        default Builder endpointOverride(String endpointOverride) {
            return endpointOverride(URI.create(endpointOverride));
        }

        DeephavenS3AsyncClientFactory build();
    }

    @Value.Check
    final void boundsCheckMaxConcurrentRequests() {
        if (maxConcurrentRequests() < 1) {
            throw new IllegalArgumentException("maxConcurrentRequests(=" + maxConcurrentRequests() + ") must be >= 1");
        }
    }

    @Value.Check
    final void awsSdkV2Credentials() {
        if (!(credentials() instanceof AwsSdkV2Credentials)) {
            throw new IllegalArgumentException(
                    "credentials() must be created via provided io.deephaven.extensions.s3.Credentials methods");
        }
    }

    final AwsCredentialsProvider awsV2CredentialsProvider() {
        return ((AwsSdkV2Credentials) credentials()).awsV2CredentialsProvider();
    }

    // Client builder logic

    private static class HttpClientConfig {
        private final int maxConcurrentRequests;
        private final Duration connectionTimeout;

        HttpClientConfig(final int maxConcurrentRequests, final Duration connectionTimeout) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            this.connectionTimeout = connectionTimeout;
        }

        int maxConcurrentRequests() {
            return maxConcurrentRequests;
        }

        Duration connectionTimeout() {
            return connectionTimeout;
        }

        @Override
        public int hashCode() {
            int result = maxConcurrentRequests;
            result = 31 * result + connectionTimeout.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final HttpClientConfig that = (HttpClientConfig) o;
            return maxConcurrentRequests == that.maxConcurrentRequests
                    && connectionTimeout.equals(that.connectionTimeout);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DeephavenS3AsyncClientFactory.class);
    private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private final Map<HttpClientConfig, SdkAsyncHttpClient> httpClientCache = new ConcurrentHashMap<>();

    // TODO(Ryan) I am getting a warning
    // warning: (immutables:incompat) Avoid introduction of fields (except constants) in abstract value types
    // Should I move this to a separate class?
    private volatile Executor futureCompletionExecutor;
    private volatile ScheduledExecutorService scheduledExecutor;

    private SdkAsyncHttpClient getOrBuildClient() {
        final HttpClientConfig config = new HttpClientConfig(maxConcurrentRequests(),
                connectionTimeout());
        return httpClientCache.computeIfAbsent(config, key -> AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(config.maxConcurrentRequests())
                .connectionTimeout(config.connectionTimeout())
                .build());
    }

    public S3AsyncClient create() {
        final S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .asyncConfiguration(
                        b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                                ensureAsyncFutureCompletionExecutor()))
                .httpClient(getOrBuildClient())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        // If we find that the STANDARD retry policy does not work well in all situations, we might
                        // try experimenting with ADAPTIVE retry policy, potentially with fast fail.
                        // .retryPolicy(RetryPolicy.builder(RetryMode.ADAPTIVE).fastFailRateLimiting(true).build())
                        .retryPolicy(RetryMode.STANDARD)
                        .apiCallAttemptTimeout(readTimeout().dividedBy(3))
                        .apiCallTimeout(readTimeout())
                        // Adding a metrics publisher may be useful for debugging, but it's very verbose.
                        // .addMetricPublisher(LoggingMetricPublisher.create(Level.INFO, Format.PRETTY))
                        .scheduledExecutorService(ensureScheduledExecutor())
                        .build())
                .region(Region.of(regionName()))
                .credentialsProvider(awsV2CredentialsProvider());
        endpointOverride().ifPresent(builder::endpointOverride);
        final S3AsyncClient ret = builder.build();
        if (log.isDebugEnabled()) {
            log.debug().append("Creating S3AsyncClient for region: ").append(regionName()).endl();
        }
        return ret;
    }

    /**
     * The following executor will be used to complete the futures returned by the async client. This is a shared
     * executor across all clients with fixed number of threads. This pattern is inspired by the default executor used
     * by the SDK ({@code SdkAsyncHttpClient#resolveAsyncFutureCompletionExecutor}).
     *
     */
    private Executor ensureAsyncFutureCompletionExecutor() {
        if (futureCompletionExecutor == null) {
            synchronized (DeephavenS3AsyncClientFactory.class) {
                if (futureCompletionExecutor == null) {
                    futureCompletionExecutor = Executors.newFixedThreadPool(NUM_PROCESSORS, new ThreadFactoryBuilder()
                            .threadNamePrefix("async-future-completion").build());
                }
            }
        }
        return futureCompletionExecutor;
    }

    /**
     * The following executor will be used to schedule tasks for the async client, such as timeouts and retries. This is
     * a shared executor across all clients with fixed number of threads. This pattern is inspired by the default
     * executor used by the SDK ({@code ClientOverrideConfiguration#resolveScheduledExecutorService}).
     */
    private ScheduledExecutorService ensureScheduledExecutor() {
        final int NUM_SCHEDULED_EXECUTOR_THREADS = 5;
        if (scheduledExecutor == null) {
            synchronized (DeephavenS3AsyncClientFactory.class) {
                if (scheduledExecutor == null) {
                    scheduledExecutor = Executors.newScheduledThreadPool(NUM_SCHEDULED_EXECUTOR_THREADS,
                            new ThreadFactoryBuilder().threadNamePrefix("scheduled-executor").build());
                }
            }
        }
        return scheduledExecutor;
    }
}
