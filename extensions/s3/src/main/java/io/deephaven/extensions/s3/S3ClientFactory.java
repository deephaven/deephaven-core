//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkAsyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static io.deephaven.util.thread.ThreadHelpers.getOrComputeThreadCountProperty;

final class S3ClientFactory {

    private static final int NUM_FUTURE_COMPLETION_THREADS =
            getOrComputeThreadCountProperty("S3.numFutureCompletionThreads", -1);
    private static final int NUM_SCHEDULED_EXECUTOR_THREADS =
            getOrComputeThreadCountProperty("S3.numScheduledExecutorThreads", 5);

    private static final Logger log = LoggerFactory.getLogger(S3ClientFactory.class);
    private static final Map<HttpClientConfig, SdkAsyncHttpClient> httpAsyncClientCache = new ConcurrentHashMap<>();

    private static volatile Executor futureCompletionExecutor;
    private static volatile ScheduledExecutorService scheduledExecutor;

    static S3AsyncClient getAsyncClient(@NotNull final S3Instructions instructions) {
        return buildForS3(S3ClientFactory::getAsyncClientBuilder, instructions, S3AsyncClient.class);
    }

    static S3Client getSyncClient(@NotNull final S3Instructions instructions) {
        return buildForS3(S3ClientFactory::getSyncClientBuilder, instructions, S3Client.class);
    }

    static <B extends S3BaseClientBuilder<B, C>, C extends AwsClient> C buildForS3(
            Function<S3Instructions, B> builderFactory,
            S3Instructions instructions,
            Class<C> clientClass) {
        if (log.isDebugEnabled()) {
            log.debug().append("Building ").append(clientClass.getSimpleName()).append(" with instructions: ")
                    .append(instructions).endl();
        }
        try {
            return builderFactory.apply(instructions).build();
        } catch (final SdkClientException e) {
            if (instructions.crossRegionAccessEnabled() && e.getMessage().contains("Unable to load region")) {
                // We might have failed because region was not provided and could not be determined by the SDK.
                // We will try again with a default region.
                return builderFactory.apply(instructions).region(Region.US_EAST_1).build();
            } else {
                throw e;
            }
        }
    }

    static S3AsyncClientBuilder getAsyncClientBuilder(@NotNull final S3Instructions instructions) {
        return S3AsyncClient.builder()
                .applyMutation(b -> applyAllSharedAsync(b, instructions))
                .applyMutation(b -> applyCrossRegionAccess(b, instructions));
    }

    static S3ClientBuilder getSyncClientBuilder(@NotNull final S3Instructions instructions) {
        return S3Client.builder()
                .applyMutation(b -> applyAllSharedSync(b, instructions))
                .applyMutation(b -> applyCrossRegionAccess(b, instructions));
    }

    static <B extends AwsSyncClientBuilder<B, C> & AwsClientBuilder<B, C>, C> void applyAllSharedSync(B builder,
            S3Instructions instructions) {
        builder
                .applyMutation(b -> applySyncHttpClient(b, instructions))
                .applyMutation(b -> applyAllSharedCommon(b, instructions));
    }

    static <B extends AwsAsyncClientBuilder<B, C> & AwsClientBuilder<B, C>, C> void applyAllSharedAsync(B builder,
            S3Instructions instructions) {
        builder
                .applyMutation(b -> applyAsyncHttpClient(b, instructions))
                .applyMutation(b -> applyAsyncConfiguration(b, instructions))
                .applyMutation(b -> applyAllSharedCommon(b, instructions));
    }

    static <B extends AwsClientBuilder<B, C>, C> void applyAllSharedCommon(B builder, S3Instructions instructions) {
        builder
                .applyMutation(b -> applyOverrideConfiguration(b, instructions))
                .applyMutation(b -> applyCredentialsProvider(b, instructions))
                .applyMutation(b -> applyRegion(b, instructions))
                .applyMutation(b -> applyEndpointOverride(b, instructions));
    }

    static <B extends SdkSyncClientBuilder<B, C>, C> void applySyncHttpClient(B builder, S3Instructions instructions) {
        builder.httpClient(buildHttpSyncClient(instructions));
    }

    static <B extends SdkAsyncClientBuilder<B, C>, C> void applyAsyncHttpClient(B builder,
            S3Instructions instructions) {
        builder.httpClient(getOrBuildHttpAsyncClient(instructions));
    }

    static <B extends SdkAsyncClientBuilder<B, C>, C> void applyAsyncConfiguration(B builder,
            @SuppressWarnings("unused") S3Instructions instructions) {
        builder.asyncConfiguration(ClientAsyncConfiguration.builder()
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                        ensureAsyncFutureCompletionExecutor())
                .build());
    }

    static <B extends SdkClientBuilder<B, C>, C> void applyOverrideConfiguration(B builder,
            S3Instructions instructions) {
        builder.overrideConfiguration(ClientOverrideConfiguration.builder()
                // If we find that the STANDARD retry policy does not work well in all situations, we might
                // try experimenting with ADAPTIVE retry policy, potentially with fast fail.
                // .retryPolicy(RetryPolicy.builder(RetryMode.ADAPTIVE).fastFailRateLimiting(true).build())
                .retryPolicy(RetryMode.STANDARD)
                .apiCallAttemptTimeout(instructions.readTimeout().dividedBy(3))
                .apiCallTimeout(instructions.readTimeout())
                // Adding a metrics publisher may be useful for debugging, but it's very verbose.
                // .addMetricPublisher(LoggingMetricPublisher.create(Level.INFO, Format.PRETTY))
                .scheduledExecutorService(ensureScheduledExecutor())
                .build());
    }

    static <B extends AwsClientBuilder<B, C>, C> void applyCredentialsProvider(B builder, S3Instructions instructions) {
        builder.credentialsProvider(instructions.awsV2CredentialsProvider());
    }

    static <B extends AwsClientBuilder<B, C>, C> void applyRegion(B builder, S3Instructions instructions) {
        instructions.regionName().map(Region::of).ifPresent(builder::region);
    }

    static <B extends S3BaseClientBuilder<B, C>, C> void applyCrossRegionAccess(B builder,
            S3Instructions instructions) {
        if (instructions.crossRegionAccessEnabled()) {
            // If region is not provided, we enable cross-region access to allow the SDK to determine the region
            // based on the bucket location and cache it for future requests.
            builder.crossRegionAccessEnabled(true);
        }
    }

    static <B extends SdkClientBuilder<B, C>, C> void applyEndpointOverride(B builder, S3Instructions instructions) {
        instructions.endpointOverride().ifPresent(builder::endpointOverride);
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

    private static SdkAsyncHttpClient getOrBuildHttpAsyncClient(@NotNull final S3Instructions instructions) {
        final HttpClientConfig config = new HttpClientConfig(instructions.maxConcurrentRequests(),
                instructions.connectionTimeout());
        return httpAsyncClientCache.computeIfAbsent(config, key -> AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(config.maxConcurrentRequests())
                .connectionTimeout(config.connectionTimeout())
                .build());
    }

    private static SdkHttpClient buildHttpSyncClient(@NotNull final S3Instructions instructions) {
        // TODO: cache?
        return AwsCrtHttpClient.builder()
                .maxConcurrency(instructions.maxConcurrentRequests())
                .connectionTimeout(instructions.connectionTimeout())
                .build();
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
            synchronized (S3ClientFactory.class) {
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
            synchronized (S3ClientFactory.class) {
                if (scheduledExecutor == null) {
                    scheduledExecutor = Executors.newScheduledThreadPool(NUM_SCHEDULED_EXECUTOR_THREADS,
                            new ThreadFactoryBuilder().threadNamePrefix("s3-scheduled-executor").build());
                }
            }
        }
        return scheduledExecutor;
    }
}
