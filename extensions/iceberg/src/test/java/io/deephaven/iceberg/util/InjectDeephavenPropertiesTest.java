//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IcebergTools#injectDeephavenProperties(Map)}.
 */
class InjectDeephavenPropertiesTest {

    @Test
    void defaultsInjectionTest() {
        final Map<String, String> props = new HashMap<>();
        final Map<String, String> updated = IcebergTools.injectDeephavenProperties(props);

        // The original map must remain untouched
        assertThat(props).isEmpty();

        // Returned map must contain defaults
        assertThat(updated)
                .containsEntry("s3.crt.enabled", "false");
        assertThat(updated.keySet()).noneMatch(k -> k.startsWith("client.credentials-provider.s3."));
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
