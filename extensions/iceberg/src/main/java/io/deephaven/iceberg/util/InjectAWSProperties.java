//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.CatalogProperties;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Static helper that injects Deephaven-specific AWS/S3 settings into the property map passed to
 * {@link BuildCatalogOptions}. The keys are duplicated from Iceberg’s <em>iceberg-aws</em> modules to avoid adding an
 * extra dependency.
 */
class InjectAWSProperties {

    /** -- Duplicated from AwsClientProperties -- **/
    private static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
    private static final String CLIENT_CREDENTIAL_PROVIDER_PREFIX = "client.credentials-provider.";
    private static final String REFRESH_CREDENTIALS_ENABLED = "client.refresh-credentials-enabled";
    private static final String REFRESH_CREDENTIALS_ENDPOINT = "client.refresh-credentials-endpoint";

    /** -- Duplicated from S3FileIOProperties -- **/
    private static final String S3_CRT_ENABLED = "s3.crt.enabled";
    private static final String ACCESS_KEY_ID = "s3.access-key-id";
    private static final String SECRET_ACCESS_KEY = "s3.secret-access-key";
    private static final String SESSION_TOKEN = "s3.session-token";
    private static final String REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";

    /** -- Duplicated from VendedCredentialsProvider -- **/
    private static final String URI = "credentials.uri";

    /** -- Duplicated from AwsProperties -- **/
    private static final String CLIENT_FACTORY = "client.factory";

    /**
     * The following properties are forwarded to the credentials provider, if not set already. This consists of all
     * properties that could be accessed in DeephavenS3ClientCredentialsProvider.resolveCredentials
     */
    private static final Set<String> CREDENTIALS_PROVIDER_PROPERTIES_TO_FORWARD = Set.of(
            REFRESH_CREDENTIALS_ENABLED,
            REFRESH_CREDENTIALS_ENDPOINT,
            ACCESS_KEY_ID,
            SECRET_ACCESS_KEY,
            SESSION_TOKEN,
            REMOTE_SIGNING_ENABLED,
            URI,
            CatalogProperties.URI);


    /** -- Deephaven defaults -- **/
    // Same as DeephavenS3ClientCredentialsProvider.class.getName()
    private static final String CLIENT_CREDENTIALS_PROVIDER_DEFAULT =
            "io.deephaven.iceberg.util.DeephavenS3ClientCredentialsProvider";

    // Same as DeephavenAwsClientFactory.class.getName()
    private static final String DEFAULT_CLIENT_FACTORY = "io.deephaven.iceberg.util.DeephavenAwsClientFactory";

    // TODO (DH-19253): Add support for S3CrtAsyncClient
    private static final String S3_CRT_ENABLED_DEFAULT = "false";

    private static void injectCredentialsProperties(
            @NotNull final Map<String, String> properties,
            @NotNull final String key) {
        if (properties.containsKey(key)) {
            final String injectedKey = CLIENT_CREDENTIAL_PROVIDER_PREFIX + key;
            if (!properties.containsKey(injectedKey)) {
                properties.put(injectedKey, properties.get(key));
            }
        }
    }

    /**
     * Creates a new map with Deephaven-specific properties injected. The input map is not modified.
     */
    static Map<String, String> injectDeephavenProperties(@NotNull final Map<String, String> inputProperties) {
        final Map<String, String> updatedProperties = new LinkedHashMap<>(inputProperties);

        // TODO (DH-19253): Add support for S3CrtAsyncClient
        updatedProperties.putIfAbsent(S3_CRT_ENABLED, S3_CRT_ENABLED_DEFAULT);

        // TODO (DH-19508): Remove this once Iceberg fix for #13131 is released
        if (!updatedProperties.containsKey(CLIENT_CREDENTIALS_PROVIDER)) {
            updatedProperties.put(CLIENT_CREDENTIALS_PROVIDER, CLIENT_CREDENTIALS_PROVIDER_DEFAULT);
            for (final String key : CREDENTIALS_PROVIDER_PROPERTIES_TO_FORWARD) {
                injectCredentialsProperties(updatedProperties, key);
            }
        }

        if (!updatedProperties.containsKey(CLIENT_FACTORY)) {
            updatedProperties.put(CLIENT_FACTORY, DEFAULT_CLIENT_FACTORY);
        }
        return updatedProperties;
    }
}
