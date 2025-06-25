//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IcebergTools#injectDeephavenProperties(Map)}.
 */
class InjectDeephavenPropertiesTest {

    private static final String DEFAULT_PROVIDER = "io.deephaven.iceberg.util.DeephavenS3ClientCredentialsProvider";

    @Test
    void defaultsInjectionTest() {
        final Map<String, String> props = new HashMap<>();
        final Map<String, String> updated = IcebergTools.injectDeephavenProperties(props);

        // The original map must remain untouched
        assertThat(props).isEmpty();

        // Returned map must contain defaults
        assertThat(updated)
                .containsEntry("s3.crt.enabled", "false")
                .containsEntry("client.credentials-provider", DEFAULT_PROVIDER);
        assertThat(updated.keySet()).noneMatch(k -> k.startsWith("client.credentials-provider.s3."));
    }

    @Test
    void forwardsInjectionTest() {
        final Map<String, String> props = new HashMap<>(Map.of(
                "s3.access-key-id", "AK",
                "s3.secret-access-key", "SK",
                "s3.session-token", "TOKEN",
                "s3.remote-signing-enabled", "true",
                "client.refresh-credentials-enabled", "false",
                "client.refresh-credentials-endpoint", "http://example.com/creds",
                "credentials.uri", "http://example.com/role",
                CatalogProperties.URI, "s3://bucket/catalog"));

        final Map<String, String> updated = IcebergTools.injectDeephavenProperties(props);

        // Deephaven defaults and forwarded counterparts were added
        assertThat(updated).containsAllEntriesOf(props);
        assertThat(updated)
                .containsEntry("client.credentials-provider", DEFAULT_PROVIDER)
                .containsEntry("client.credentials-provider.s3.access-key-id", "AK")
                .containsEntry("client.credentials-provider.s3.secret-access-key", "SK")
                .containsEntry("client.credentials-provider.s3.session-token", "TOKEN")
                .containsEntry("client.credentials-provider.s3.remote-signing-enabled", "true")
                .containsEntry("client.credentials-provider.client.refresh-credentials-enabled", "false")
                .containsEntry("client.credentials-provider.client.refresh-credentials-endpoint",
                        "http://example.com/creds")
                .containsEntry("client.credentials-provider.credentials.uri", "http://example.com/role")
                .containsEntry("client.credentials-provider." + CatalogProperties.URI,
                        "s3://bucket/catalog");
    }

    @Test
    void doesNotOverrideproviderTest() {
        final Map<String, String> props = new HashMap<>(Map.of(
                "client.credentials-provider", "com.example.CustomProvider",
                "s3.access-key-id", "AK"));

        final Map<String, String> updated = IcebergTools.injectDeephavenProperties(props);

        // Provider was not overridden
        assertThat(updated.get("client.credentials-provider")).isEqualTo("com.example.CustomProvider");

        // Still injects the CRT default
        assertThat(updated.get("s3.crt.enabled")).isEqualTo("false");

        // Does not forward any properties
        assertThat(updated.keySet()).noneMatch(k -> k.startsWith("client.credentials-provider.s3."));
    }
}
