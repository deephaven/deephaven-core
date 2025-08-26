//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.auto.service.AutoService;
import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3InstructionsBasedAwsClientFactory;
import io.deephaven.extensions.s3.S3Constants;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.internal.DataInstructionsProviderPlugin;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.util.PropertyUtil;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;

/**
 * {@link DataInstructionsProviderPlugin} implementation for producing a {@link S3Instructions}. The produced
 * instructions will be from {@link S3InstructionsBasedAwsClientFactory#getInstructions(Map)} if present, and otherwise
 * will make a best-effort attempt to create an equivalent instructions based on properties from
 * {@link AwsClientProperties} and {@link S3FileIOProperties}.
 */
@AutoService(DataInstructionsProviderPlugin.class)
@SuppressWarnings("unused")
public final class S3InstructionsProviderPlugin implements DataInstructionsProviderPlugin {
    @Override
    public S3Instructions createInstructions(
            @NotNull final String uriScheme,
            @NotNull final Map<String, String> properties) {
        final S3Instructions s3Instructions =
                S3InstructionsBasedAwsClientFactory.getInstructions(properties).orElse(null);
        if (s3Instructions != null) {
            return s3Instructions;
        }

        // If the URI scheme is "s3","s3a","s3n" or if the properties contain one of these specific keys, we can
        // create a useful S3Instructions object.
        if (S3Constants.S3_URI_SCHEME.equals(uriScheme)
                || S3Constants.S3A_URI_SCHEME.equals(uriScheme)
                || S3Constants.S3N_URI_SCHEME.equals(uriScheme)
                || properties.containsKey(AwsClientProperties.CLIENT_REGION)
                || properties.containsKey(S3FileIOProperties.ACCESS_KEY_ID)
                || properties.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)
                || properties.containsKey(S3FileIOProperties.ENDPOINT)) {

            final S3Instructions.Builder builder = S3Instructions.builder();
            if (properties.containsKey(AwsClientProperties.CLIENT_REGION)) {
                builder.regionName(properties.get(AwsClientProperties.CLIENT_REGION));
            }
            if (properties.containsKey(S3FileIOProperties.ENDPOINT)) {
                builder.endpointOverride(properties.get(S3FileIOProperties.ENDPOINT));
            }
            if (properties.containsKey(S3FileIOProperties.ACCESS_KEY_ID)
                    && properties.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)) {
                builder.credentials(
                        Credentials.basic(properties.get(S3FileIOProperties.ACCESS_KEY_ID),
                                properties.get(S3FileIOProperties.SECRET_ACCESS_KEY)));
            }

            final String httpClientType = PropertyUtil.propertyAsString(properties,
                    AsyncHttpClientProperties.ASYNC_CLIENT_TYPE, AsyncHttpClientProperties.DEFAULT_ASYNC_CLIENT_TYPE);
            if (AsyncHttpClientProperties.ASYNC_CLIENT_TYPE_NETTY.equals(httpClientType)) {
                if (properties.containsKey(AsyncHttpClientProperties.NETTY_READ_TIMEOUT_MS)) {
                    builder.readTimeout(Duration.ofMillis(PropertyUtil.propertyAsNullableLong(
                            properties, AsyncHttpClientProperties.NETTY_READ_TIMEOUT_MS)));
                }
                if (properties.containsKey(AsyncHttpClientProperties.NETTY_WRITE_TIMEOUT_MS)) {
                    builder.writeTimeout(Duration.ofMillis(PropertyUtil.propertyAsNullableLong(
                            properties, AsyncHttpClientProperties.NETTY_WRITE_TIMEOUT_MS)));
                }
                if (properties.containsKey(AsyncHttpClientProperties.NETTY_CONNECTION_TIMEOUT_MS)) {
                    builder.connectionTimeout(Duration.ofMillis(PropertyUtil.propertyAsNullableLong(
                            properties, AsyncHttpClientProperties.NETTY_CONNECTION_TIMEOUT_MS)));
                }
                if (properties.containsKey(AsyncHttpClientProperties.NETTY_MAX_CONCURRENCY)) {
                    final int maxConcurrency = PropertyUtil.propertyAsNullableInt(
                            properties, AsyncHttpClientProperties.NETTY_MAX_CONCURRENCY);
                    builder.maxConcurrentRequests(maxConcurrency);
                    builder.numConcurrentWriteParts(maxConcurrency);
                }
            } else if (AsyncHttpClientProperties.ASYNC_CLIENT_TYPE_CRT.equals(httpClientType)) {
                if (properties.containsKey(AsyncHttpClientProperties.CRT_CONNECTION_TIMEOUT_MS)) {
                    builder.connectionTimeout(Duration.ofMillis(PropertyUtil.propertyAsNullableLong(
                            properties, AsyncHttpClientProperties.CRT_CONNECTION_TIMEOUT_MS)));
                }
                if (properties.containsKey(AsyncHttpClientProperties.CRT_MAX_CONCURRENCY)) {
                    final int maxConcurrency = PropertyUtil.propertyAsNullableInt(
                            properties, AsyncHttpClientProperties.CRT_MAX_CONCURRENCY);
                    builder.maxConcurrentRequests(maxConcurrency);
                    builder.numConcurrentWriteParts(maxConcurrency);
                }
            }

            if (properties.containsKey(AsyncHttpClientProperties.READ_FRAGMENT_SIZE)) {
                builder.fragmentSize(PropertyUtil.propertyAsNullableInt(
                        properties, AsyncHttpClientProperties.READ_FRAGMENT_SIZE));
            }
            if (properties.containsKey(AsyncHttpClientProperties.WRITE_PART_SIZE)) {
                builder.writePartSize(PropertyUtil.propertyAsNullableInt(
                        properties, AsyncHttpClientProperties.WRITE_PART_SIZE));
            }
            if (properties.containsKey(AsyncHttpClientProperties.READ_AHEAD_COUNT)) {
                builder.readAheadCount(PropertyUtil.propertyAsNullableInt(
                        properties, AsyncHttpClientProperties.READ_AHEAD_COUNT));
            }
            return builder.build();
        }

        // We have no useful properties for creating an S3Instructions object.
        return null;
    }
}
