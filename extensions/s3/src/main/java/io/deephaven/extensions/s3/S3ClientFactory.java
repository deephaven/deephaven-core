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
    private static final Map<HttpClientConfig, SdkHttpClient> httpSyncClientCache = new ConcurrentHashMap<>();

    private static volatile Executor futureCompletionExecutor;
    private static volatile ScheduledExecutorService scheduledExecutor;

    static S3AsyncClient getAsyncClient(@NotNull final S3Instructions instructions) {
        return buildForS3(S3ClientFactory::getAsyncClientBuilder, instructions, S3AsyncClient.class);
    }

    static S3Client getSyncClient(@NotNull final S3Instructions instructions) {
        return buildForS3(S3ClientFactory::getSyncClientBuilder, instructions, S3Client.class);
    }

    static <Builder extends AwsAsyncClientBuilder<Builder, Client> & AwsClientBuilder<Builder, Client>, Client> void applyAllSharedAsync(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        builder
                .applyMutation(b -> applyAsyncHttpClient(b, instructions))
                .applyMutation(b -> applyAsyncConfiguration(b, instructions))
                .applyMutation(b -> applyAllSharedCommon(b, instructions));
    }

    static <Builder extends AwsSyncClientBuilder<Builder, Client> & AwsClientBuilder<Builder, Client>, Client> void applyAllSharedSync(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        builder
                .applyMutation(b -> applySyncHttpClient(b, instructions))
                .applyMutation(b -> applyAllSharedCommon(b, instructions));
    }

    private static <Builder extends S3BaseClientBuilder<Builder, Client>, Client extends AwsClient> Client buildForS3(
            @NotNull final Function<S3Instructions, Builder> builderFactory,
            @NotNull final S3Instructions instructions,
            @NotNull final Class<Client> clientClass) {
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

    private static S3AsyncClientBuilder getAsyncClientBuilder(@NotNull final S3Instructions instructions) {
        return S3AsyncClient.builder()
                .applyMutation(b -> applyAllSharedAsync(b, instructions))
                .applyMutation(b -> applyCrossRegionAccess(b, instructions));
    }

    private static S3ClientBuilder getSyncClientBuilder(@NotNull final S3Instructions instructions) {
        return S3Client.builder()
                .applyMutation(b -> applyAllSharedSync(b, instructions))
                .applyMutation(b -> applyCrossRegionAccess(b, instructions));
    }

    private static <Builder extends AwsClientBuilder<Builder, Client>, Client> void applyAllSharedCommon(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        builder
                .applyMutation(b -> applyOverrideConfiguration(b, instructions))
                .applyMutation(b -> applyCredentialsProvider(b, instructions))
                .applyMutation(b -> applyRegion(b, instructions))
                .applyMutation(b -> applyEndpointOverride(b, instructions));
    }

    private static <Builder extends SdkSyncClientBuilder<Builder, Client>, Client> void applySyncHttpClient(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        builder.httpClient(getOrBuildHttpSyncClient(instructions));
    }

    private static <Builder extends SdkAsyncClientBuilder<Builder, Client>, Client> void applyAsyncHttpClient(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        builder.httpClient(getOrBuildHttpAsyncClient(instructions));
    }

    private static <Builder extends SdkAsyncClientBuilder<Builder, Client>, Client> void applyAsyncConfiguration(
            @NotNull final Builder builder,
            @SuppressWarnings("unused") @NotNull final S3Instructions instructions) {
        builder.asyncConfiguration(ClientAsyncConfiguration.builder()
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                        ensureAsyncFutureCompletionExecutor())
                .build());
    }

    private static <Builder extends SdkClientBuilder<Builder, Client>, Client> void applyOverrideConfiguration(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        final ClientOverrideConfiguration.Builder overideConfigurationBuilder = ClientOverrideConfiguration.builder()
                // If we find that the STANDARD retry policy does not work well in all situations, we might
                // try experimenting with ADAPTIVE retry policy, potentially with fast fail.
                // .retryPolicy(RetryPolicy.builder(RetryMode.ADAPTIVE).fastFailRateLimiting(true).build())
                .retryPolicy(RetryMode.STANDARD)
                .apiCallAttemptTimeout(instructions.readTimeout().dividedBy(3))
                .apiCallTimeout(instructions.readTimeout())
                // Adding a metrics publisher may be useful for debugging, but it's very verbose.
                // .addMetricPublisher(LoggingMetricPublisher.create(Level.INFO, Format.PRETTY))
                .scheduledExecutorService(ensureScheduledExecutor());
        instructions.profileName().ifPresent(overideConfigurationBuilder::defaultProfileName);
        instructions.aggregatedProfileFile().ifPresent(overideConfigurationBuilder::defaultProfileFile);
        builder.overrideConfiguration(overideConfigurationBuilder.build());
    }

    private static <Builder extends AwsClientBuilder<Builder, Client>, Client> void applyCredentialsProvider(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        builder.credentialsProvider(instructions.awsV2CredentialsProvider());
    }

    private static <Builder extends AwsClientBuilder<Builder, Client>, Client> void applyRegion(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        instructions.regionName().map(Region::of).ifPresent(builder::region);
    }

    private static <Builder extends S3BaseClientBuilder<Builder, Client>, Client> void applyCrossRegionAccess(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        if (instructions.crossRegionAccessEnabled()) {
            // If region is not provided, we enable cross-region access to allow the SDK to determine the region
            // based on the bucket location and cache it for future requests.
            builder.crossRegionAccessEnabled(true);
        }
    }

    private static <Builder extends SdkClientBuilder<Builder, Client>, Client> void applyEndpointOverride(
            @NotNull final Builder builder,
            @NotNull final S3Instructions instructions) {
        instructions.endpointOverride().ifPresent(builder::endpointOverride);
    }

    private static class HttpClientConfig {

        static HttpClientConfig of(S3Instructions instructions) {
            return new HttpClientConfig(instructions.maxConcurrentRequests(), instructions.connectionTimeout());
        }

        private final int maxConcurrentRequests;
        private final Duration connectionTimeout;

        HttpClientConfig(final int maxConcurrentRequests, final Duration connectionTimeout) {
            this.maxConcurrentRequests = maxConcurrentRequests;
            this.connectionTimeout = connectionTimeout;
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

        SdkAsyncHttpClient buildAsync() {
            return AwsCrtAsyncHttpClient.builder()
                    .maxConcurrency(maxConcurrentRequests)
                    .connectionTimeout(connectionTimeout)
                    .build();
        }

        SdkHttpClient buildSync() {
            return AwsCrtHttpClient.builder()
                    .maxConcurrency(maxConcurrentRequests)
                    .connectionTimeout(connectionTimeout)
                    .build();
        }
    }

    private static SdkAsyncHttpClient getOrBuildHttpAsyncClient(@NotNull final S3Instructions instructions) {
        return httpAsyncClientCache.computeIfAbsent(HttpClientConfig.of(instructions), HttpClientConfig::buildAsync);
    }

    private static SdkHttpClient getOrBuildHttpSyncClient(@NotNull final S3Instructions instructions) {
        return httpSyncClientCache.computeIfAbsent(HttpClientConfig.of(instructions), HttpClientConfig::buildSync);
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
