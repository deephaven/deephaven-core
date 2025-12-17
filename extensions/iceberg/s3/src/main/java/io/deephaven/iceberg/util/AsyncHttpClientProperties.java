//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.util.PropertyUtil;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

class AsyncHttpClientProperties {

    public static final String ASYNC_HTTP_CLIENT_PREFIX = "http-client-async.";

    // Add option for CRT and inject this property
    public static final String ASYNC_CLIENT_TYPE = "http-client-async.client-type";
    public static final String ASYNC_CLIENT_TYPE_CRT = "crt";
    public static final String ASYNC_CLIENT_TYPE_NETTY = "netty";
    public static final String DEFAULT_ASYNC_CLIENT_TYPE = ASYNC_CLIENT_TYPE_NETTY;

    // Netty NIO specific properties
    public static final String NETTY_READ_TIMEOUT_MS = "http-client-async.netty.read-timeout-ms";
    public static final String NETTY_WRITE_TIMEOUT_MS = "http-client-async.netty.write-timeout-ms";
    public static final String NETTY_CONNECTION_TIMEOUT_MS = "http-client-async.netty.connection-timeout-ms";
    public static final String NETTY_MAX_CONCURRENCY = "http-client-async.netty.max-concurrency";

    // CRT specific properties
    public static final String CRT_CONNECTION_TIMEOUT_MS = "http-client-async.crt.connection-timeout-ms";
    public static final String CRT_MAX_CONCURRENCY = "http-client-async.crt.max-concurrency";

    // Deephaven specific properties which are used by the AWS layer when using the async HTTP client.
    public static final String READ_FRAGMENT_SIZE = "http-client-async.read-fragment-size";
    public static final String WRITE_PART_SIZE = "http-client-async.write-part-size";
    public static final String READ_AHEAD_COUNT = "http-client-async.read-ahead-count";

    // When adding new http-client-async properties here, remember to update S3InstructionsProviderPlugin
    // so they are parsed and applied when constructing S3Instructions.

    private final String httpClientType;
    private final Map<String, String> httpClientProperties;

    AsyncHttpClientProperties(@NotNull final Map<String, String> properties) {
        this.httpClientType = PropertyUtil.propertyAsString(properties, ASYNC_CLIENT_TYPE, DEFAULT_ASYNC_CLIENT_TYPE);
        if (!httpClientType.equals(ASYNC_CLIENT_TYPE_NETTY) && !httpClientType.equals(ASYNC_CLIENT_TYPE_CRT)) {
            throw new IllegalArgumentException("Unknown HTTP client type: " + httpClientType
                    + ". Expected one of: " + ASYNC_CLIENT_TYPE_NETTY + ", " + ASYNC_CLIENT_TYPE_CRT);
        }
        this.httpClientProperties =
                PropertyUtil.filterProperties(properties, key -> key.startsWith(ASYNC_HTTP_CLIENT_PREFIX));
    }

    /**
     * Applies the asynchronous HTTP client configurations to the given S3AsyncClientBuilder.
     */
    void applyAsyncHttpClientConfigurations(final S3AsyncClientBuilder awsClientBuilder) {
        if (httpClientType.equals(ASYNC_CLIENT_TYPE_NETTY)) {
            final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder();
            this.applyAsyncNettyClientConfiguration(httpClientBuilder);
            awsClientBuilder.httpClientBuilder(httpClientBuilder);
        } else if (httpClientType.equals(ASYNC_CLIENT_TYPE_CRT)) {
            final AwsCrtAsyncHttpClient.Builder httpClientBuilder = AwsCrtAsyncHttpClient.builder();
            this.applyAsyncCRTClientConfiguration(httpClientBuilder);
            awsClientBuilder.httpClientBuilder(httpClientBuilder);
        } else {
            throw new IllegalStateException("Unknown HTTP client type: " + httpClientType);
        }

    }

    private void applyAsyncNettyClientConfiguration(
            @NotNull final NettyNioAsyncHttpClient.Builder builder) {
        final Long readTimeoutMs = PropertyUtil.propertyAsNullableLong(httpClientProperties, NETTY_READ_TIMEOUT_MS);
        if (readTimeoutMs != null) {
            builder.readTimeout(Duration.ofMillis(readTimeoutMs));
        }
        final Long writeTimeoutMs = PropertyUtil.propertyAsNullableLong(httpClientProperties, NETTY_WRITE_TIMEOUT_MS);
        if (writeTimeoutMs != null) {
            builder.writeTimeout(Duration.ofMillis(writeTimeoutMs));
        }
        final Long connectionTimeoutMs =
                PropertyUtil.propertyAsNullableLong(httpClientProperties, NETTY_CONNECTION_TIMEOUT_MS);
        if (connectionTimeoutMs != null) {
            builder.connectionTimeout(Duration.ofMillis(connectionTimeoutMs));
        }
        final Integer maxConcurrency = PropertyUtil.propertyAsNullableInt(httpClientProperties, NETTY_MAX_CONCURRENCY);
        if (maxConcurrency != null) {
            builder.maxConcurrency(maxConcurrency);
        }
    }

    private void applyAsyncCRTClientConfiguration(
            @NotNull final AwsCrtAsyncHttpClient.Builder builder) {
        final Long connectionTimeoutMs =
                PropertyUtil.propertyAsNullableLong(httpClientProperties, CRT_CONNECTION_TIMEOUT_MS);
        if (connectionTimeoutMs != null) {
            builder.connectionTimeout(Duration.ofMillis(connectionTimeoutMs));
        }
        final Integer maxConcurrency = PropertyUtil.propertyAsNullableInt(httpClientProperties, CRT_MAX_CONCURRENCY);
        if (maxConcurrency != null) {
            builder.maxConcurrency(maxConcurrency);
        }
    }
}
