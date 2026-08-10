//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Static helper that injects Deephaven-specific AWS/S3 settings into the property map passed to
 * {@link BuildCatalogOptions}. The keys are duplicated from Iceberg’s <em>iceberg-aws</em> modules to avoid adding an
 * extra dependency.
 */
class InjectAWSProperties {

    /** -- Duplicated from S3FileIOProperties -- **/
    private static final String S3_CRT_ENABLED = "s3.crt.enabled";

    /** -- Duplicated from AwsProperties -- **/
    private static final String CLIENT_FACTORY = "client.factory";



    /** -- Deephaven defaults -- **/
    // Same as DeephavenAwsClientFactory.class.getName()
    private static final String DEFAULT_CLIENT_FACTORY = "io.deephaven.iceberg.util.DeephavenAwsClientFactory";

    // TODO (DH-19253): Add support for S3CrtAsyncClient
    private static final String S3_CRT_ENABLED_DEFAULT = "false";

    /**
     * Creates a new map with Deephaven-specific properties injected. The input map is not modified.
     */
    static Map<String, String> injectDeephavenProperties(@NotNull final Map<String, String> inputProperties) {
        final Map<String, String> updatedProperties = new LinkedHashMap<>(inputProperties);

        // TODO (DH-19253): Add support for S3CrtAsyncClient
        updatedProperties.putIfAbsent(S3_CRT_ENABLED, S3_CRT_ENABLED_DEFAULT);

        if (!updatedProperties.containsKey(CLIENT_FACTORY)) {
            updatedProperties.put(CLIENT_FACTORY, DEFAULT_CLIENT_FACTORY);
        }
        return updatedProperties;
    }
}
