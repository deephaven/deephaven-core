//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.util.channel.DataInstructionsProviderPlugin;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.Map;

/**
 * {@link DataInstructionsProviderPlugin} implementation used for reading files from S3.
 */
@AutoService(DataInstructionsProviderPlugin.class)
@SuppressWarnings("unused")
public final class S3InstructionsProviderPlugin implements DataInstructionsProviderPlugin {
    private static final String CLIENT_REGION = "client.region";
    private static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
    private static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
    private static final String S3_ENDPOINT = "s3.endpoint";

    @Override
    public Object createInstructions(@NotNull URI uri, @NotNull Map<String, String> properties) {
        // If the properties contain any of these keys, we can create a useful S3Instructions object.
        if (properties.containsKey(CLIENT_REGION)
                || properties.containsKey(S3_ACCESS_KEY_ID)
                || properties.containsKey(S3_SECRET_ACCESS_KEY)
                || properties.containsKey(S3_ENDPOINT)) {

            final S3Instructions.Builder builder = S3Instructions.builder();
            if (properties.containsKey(CLIENT_REGION)) {
                builder.regionName(properties.get(CLIENT_REGION));
            }
            if (properties.containsKey(S3_ENDPOINT)) {
                builder.endpointOverride(properties.get(S3_ENDPOINT));
            }
            if (properties.containsKey(S3_ACCESS_KEY_ID) && properties.containsKey(S3_SECRET_ACCESS_KEY)) {
                builder.credentials(
                        Credentials.basic(properties.get(S3_ACCESS_KEY_ID), properties.get(S3_SECRET_ACCESS_KEY)));
            }
            return builder.build();
        }

        // We have no useful properties for creating an S3Instructions object.
        return null;
    }
}
