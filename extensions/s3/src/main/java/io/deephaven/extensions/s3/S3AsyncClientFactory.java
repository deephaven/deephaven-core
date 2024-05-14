//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.deephaven.util.thread.ThreadHelpers.getOrComputeThreadCountProperty;

class S3AsyncClientFactory {

    private static final int NUM_FUTURE_COMPLETION_THREADS =
            getOrComputeThreadCountProperty("S3.numFutureCompletionThreads", -1);
    private static final int NUM_SCHEDULED_EXECUTOR_THREADS =
            getOrComputeThreadCountProperty("S3.numScheduledExecutorThreads", 5);

    private static final Logger log = LoggerFactory.getLogger(S3AsyncClientFactory.class);
    private static final Map<HttpClientConfig, SdkAsyncHttpClient> httpClientCache = new ConcurrentHashMap<>();

    private static volatile Executor futureCompletionExecutor;
    private static volatile ScheduledExecutorService scheduledExecutor;

    static S3AsyncClient getAsyncClient(@NotNull final S3Instructions instructions) {
        final S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .asyncConfiguration(
                        b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                                ensureAsyncFutureCompletionExecutor()))
                .httpClient(getOrBuildHttpClient(instructions))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        // If we find that the STANDARD retry policy does not work well in all situations, we might
                        // try experimenting with ADAPTIVE retry policy, potentially with fast fail.
                        // .retryPolicy(RetryPolicy.builder(RetryMode.ADAPTIVE).fastFailRateLimiting(true).build())
                        .retryPolicy(RetryMode.STANDARD)
                        .apiCallAttemptTimeout(instructions.readTimeout().dividedBy(3))
                        .apiCallTimeout(instructions.readTimeout())
                        // Adding a metrics publisher may be useful for debugging, but it's very verbose.
                        // .addMetricPublisher(LoggingMetricPublisher.create(Level.INFO, Format.PRETTY))
                        .scheduledExecutorService(ensureScheduledExecutor())
                        .build())
                .region(Region.of(instructions.regionName()))
                .credentialsProvider(instructions.awsV2CredentialsProvider());
        instructions.endpointOverride().ifPresent(builder::endpointOverride);
        final S3AsyncClient ret = builder.build();
        if (log.isDebugEnabled()) {
            log.debug().append("Building S3AsyncClient with instructions: ").append(instructions).endl();
        }
        return ret;
    }

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
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final HttpClientConfig that = (HttpClientConfig) other;
            return maxConcurrentRequests == that.maxConcurrentRequests
                    && connectionTimeout.equals(that.connectionTimeout);
        }
    }

    private static SdkAsyncHttpClient getOrBuildHttpClient(@NotNull final S3Instructions instructions) {
        final HttpClientConfig config = new HttpClientConfig(instructions.maxConcurrentRequests(),
                instructions.connectionTimeout());
        return httpClientCache.computeIfAbsent(config, key -> AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(config.maxConcurrentRequests())
                .connectionTimeout(config.connectionTimeout())
                .build());
    }

    /**
     * The following executor will be used to complete the futures returned by the async client. This is a shared
     * executor across all clients with fixed number of threads. This pattern is inspired by the default executor used
     * by the SDK
     * ({@code software.amazon.awssdk.core.client.builder.SdkDefaultClientBuilder#resolveAsyncFutureCompletionExecutor})
     *
     */
    private static Executor ensureAsyncFutureCompletionExecutor() {
        if (futureCompletionExecutor == null) {
            synchronized (S3AsyncClientFactory.class) {
                if (futureCompletionExecutor == null) {
                    futureCompletionExecutor = Executors.newFixedThreadPool(NUM_FUTURE_COMPLETION_THREADS,
                            new ThreadFactoryBuilder().threadNamePrefix("s3-async-future-completion").build());
                }
            }
        }
        return futureCompletionExecutor;
    }

    /**
     * The following executor will be used to schedule tasks for the async client, such as timeouts and retries. This is
     * a shared executor across all clients with fixed number of threads. This pattern is inspired by the default
     * executor used by the SDK
     * ({@code software.amazon.awssdk.core.client.builder.SdkDefaultClientBuilder#resolveScheduledExecutorService}).
     */
    private static ScheduledExecutorService ensureScheduledExecutor() {
        if (scheduledExecutor == null) {
            synchronized (S3AsyncClientFactory.class) {
                if (scheduledExecutor == null) {
                    scheduledExecutor = Executors.newScheduledThreadPool(NUM_SCHEDULED_EXECUTOR_THREADS,
                            new ThreadFactoryBuilder().threadNamePrefix("s3-scheduled-executor").build());
                }
            }
        }
        return scheduledExecutor;
    }
}
