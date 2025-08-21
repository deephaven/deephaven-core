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

    // Add option for CRT and inject this property
    public static final String CLIENT_TYPE = "http-client.async.client-type";
    public static final String CLIENT_TYPE_CRT = "aws-crt";
    public static final String CLIENT_TYPE_NIO = "netty-nio";
    public static final String CLIENT_TYPE_DEFAULT = CLIENT_TYPE_NIO;

    public static final String READ_TIMEOUT_MS = "http-client.async.read-timeout-ms";
    public static final String WRITE_TIMEOUT_MS = "http-client.async.write-timeout-ms";
    public static final String CONNECTION_TIMEOUT_MS = "http-client.async.connection-timeout-ms";
    public static final String MAX_CONCURRENCY = "http-client.async.max-concurrency";

    // Deephaven specific properties which are used by the AWS layer when using the async HTTP client.
    public static final String READ_FRAGMENT_SIZE = "http-client.async.read-fragment-size";
    public static final String WRITE_PART_SIZE = "http-client.async.write-part-size";
    public static final String READ_AHEAD_COUNT = "http-client.async.read-ahead-count";

    final String httpClientType;
    final Long readTimeoutMs;
    final Long writeTimeoutMs;
    final Long connectionTimeoutMs;
    final Integer maxConcurrency;

    AsyncHttpClientProperties(@NotNull final Map<String, String> properties) {
        this.httpClientType = PropertyUtil.propertyAsString(properties, CLIENT_TYPE, CLIENT_TYPE_DEFAULT);
        if (!httpClientType.equals(CLIENT_TYPE_NIO) && !httpClientType.equals(CLIENT_TYPE_CRT)) {
            throw new IllegalArgumentException("Unknown HTTP client type: " + httpClientType
                    + ". Expected one of: " + CLIENT_TYPE_NIO + ", " + CLIENT_TYPE_CRT);
        }
        this.readTimeoutMs = PropertyUtil.propertyAsNullableLong(properties, READ_TIMEOUT_MS);
        this.writeTimeoutMs = PropertyUtil.propertyAsNullableLong(properties, WRITE_TIMEOUT_MS);
        this.connectionTimeoutMs = PropertyUtil.propertyAsNullableLong(properties, CONNECTION_TIMEOUT_MS);
        this.maxConcurrency = PropertyUtil.propertyAsNullableInt(properties, MAX_CONCURRENCY);
    }

    /**
     * Applies the asynchronous HTTP client configurations to the given S3AsyncClientBuilder.
     */
    void applyAsyncHttpClientConfigurations(final S3AsyncClientBuilder awsClientBuilder) {
        if (httpClientType.equals(CLIENT_TYPE_NIO)) {
            final NettyNioAsyncHttpClient.Builder httpClientBuilder = NettyNioAsyncHttpClient.builder();
            this.applyAsyncNettyClientConfiguration(httpClientBuilder);
            awsClientBuilder.httpClientBuilder(httpClientBuilder);
        } else if (httpClientType.equals(CLIENT_TYPE_CRT)) {
            final AwsCrtAsyncHttpClient.Builder httpClientBuilder = AwsCrtAsyncHttpClient.builder();
            this.applyAsyncCRTClientConfiguration(httpClientBuilder);

            awsClientBuilder.httpClientBuilder(httpClientBuilder);
        } else {
            throw new IllegalStateException("Unknown HTTP client type: " + httpClientType);
        }

    }

    private void applyAsyncNettyClientConfiguration(
            @NotNull final NettyNioAsyncHttpClient.Builder builder) {
        if (readTimeoutMs != null) {
            builder.readTimeout(Duration.ofMillis(readTimeoutMs));
        }
        if (writeTimeoutMs != null) {
            builder.writeTimeout(Duration.ofMillis(writeTimeoutMs));
        }
        if (connectionTimeoutMs != null) {
            builder.connectionTimeout(Duration.ofMillis(connectionTimeoutMs));
        }
        if (maxConcurrency != null) {
            builder.maxConcurrency(maxConcurrency);
        }
    }

    private void applyAsyncCRTClientConfiguration(
            @NotNull final AwsCrtAsyncHttpClient.Builder builder) {
        if (connectionTimeoutMs != null) {
            builder.connectionTimeout(Duration.ofMillis(connectionTimeoutMs));
        }
        if (maxConcurrency != null) {
            builder.maxConcurrency(maxConcurrency);
        }
    }
}
