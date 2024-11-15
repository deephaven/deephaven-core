//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.auto.service.AutoService;
import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.DeephavenAwsClientFactory;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.internal.DataInstructionsProviderPlugin;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * {@link DataInstructionsProviderPlugin} implementation for producing a {@link S3Instructions}. The produced
 * instructions will be from {@link DeephavenAwsClientFactory#getInstructions(Map)} if present, and otherwise will make
 * a best-effort attempt to create an equivalent instructions based on properties from {@link AwsClientProperties} and
 * {@link S3FileIOProperties}.
 */
@AutoService(DataInstructionsProviderPlugin.class)
@SuppressWarnings("unused")
public final class S3InstructionsProviderPlugin implements DataInstructionsProviderPlugin {
    @Override
    public S3Instructions createInstructions(@NotNull final String uriScheme,
            @NotNull final Map<String, String> properties) {
        final S3Instructions s3Instructions = DeephavenAwsClientFactory.getInstructions(properties).orElse(null);
        if (s3Instructions != null) {
            return s3Instructions;
        }

        // If the URI scheme is "s3","s3a","s3n" or if the properties contain one of these specific keys, we can
        // create a useful S3Instructions object.
        if (uriScheme.equals("s3")
                || uriScheme.equals("s3a")
                || uriScheme.equals("s3n")
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
            return builder.build();
        }

        // We have no useful properties for creating an S3Instructions object.
        return null;
    }
}
