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

class S3AsyncClientFactory {

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

    private static final Logger log = LoggerFactory.getLogger(S3SeekableChannelProvider.class);
    private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final Map<HttpClientConfig, SdkAsyncHttpClient> httpClientCache = new ConcurrentHashMap<>();

    private static volatile Executor futureCompletionExecutor;
    private static volatile ScheduledExecutorService scheduledExecutor;

    private static SdkAsyncHttpClient getOrBuildClient(@NotNull final S3Instructions s3Instructions) {
        final HttpClientConfig config = new HttpClientConfig(s3Instructions.maxConcurrentRequests(),
                s3Instructions.connectionTimeout());
        return httpClientCache.computeIfAbsent(config, key -> AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(config.maxConcurrentRequests())
                .connectionTimeout(config.connectionTimeout())
                .build());
    }

    static S3AsyncClient create(@NotNull final S3Instructions s3Instructions) {
        final S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .asyncConfiguration(
                        b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                                ensureAsyncFutureCompletionExecutor()))
                .httpClient(getOrBuildClient(s3Instructions))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        // If we find that the STANDARD retry policy does not work well in all situations, we might
                        // try experimenting with ADAPTIVE retry policy, potentially with fast fail.
                        // .retryPolicy(RetryPolicy.builder(RetryMode.ADAPTIVE).fastFailRateLimiting(true).build())
                        .retryPolicy(RetryMode.STANDARD)
                        .apiCallAttemptTimeout(s3Instructions.readTimeout().dividedBy(3))
                        .apiCallTimeout(s3Instructions.readTimeout())
                        // Adding a metrics publisher may be useful for debugging, but it's very verbose.
                        // .addMetricPublisher(LoggingMetricPublisher.create(Level.INFO, Format.PRETTY))
                        .scheduledExecutorService(ensureScheduledExecutor())
                        .build())
                .region(Region.of(s3Instructions.regionName()))
                .credentialsProvider(s3Instructions.awsV2CredentialsProvider());
        s3Instructions.endpointOverride().ifPresent(builder::endpointOverride);
        if (log.isDebugEnabled()) {
            log.debug().append("Building client with instructions: ").append(s3Instructions).endl();
        }
        return builder.build();
    }

    /**
     * The following executor will be used to complete the futures returned by the async client. This is a shared
     * executor across all clients with fixed number of threads. This pattern is inspired by the default executor used
     * by the SDK ({@code SdkAsyncHttpClient#resolveAsyncFutureCompletionExecutor}).
     *
     */
    private static Executor ensureAsyncFutureCompletionExecutor() {
        if (futureCompletionExecutor == null) {
            synchronized (S3AsyncClientFactory.class) {
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
    private static ScheduledExecutorService ensureScheduledExecutor() {
        final int NUM_SCHEDULED_EXECUTOR_THREADS = 5;
        if (scheduledExecutor == null) {
            synchronized (S3AsyncClientFactory.class) {
                if (scheduledExecutor == null) {
                    scheduledExecutor = Executors.newScheduledThreadPool(NUM_SCHEDULED_EXECUTOR_THREADS,
                            new ThreadFactoryBuilder().threadNamePrefix("scheduled-executor").build());
                }
            }
        }
        return scheduledExecutor;
    }
}
