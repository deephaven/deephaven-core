//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

class BuildCatalogOptionsTest {

    private static final Map<String, String> MINIMAL_PROPS = Map.of(CatalogUtil.ICEBERG_CATALOG_TYPE, "minimal");

    @Test
    void minimalOptions() {
        final BuildCatalogOptions options = BuildCatalogOptions.builder()
                .putAllProperties(MINIMAL_PROPS)
                .build();
        assertThat(options.name()).isEqualTo("IcebergCatalog");
        assertThat(options.properties()).isEqualTo(MINIMAL_PROPS);
        assertThat(options.hadoopConfig()).isEmpty();
    }

    @Test
    void named() {
        final BuildCatalogOptions options = BuildCatalogOptions.builder()
                .name("Test")
                .putAllProperties(MINIMAL_PROPS)
                .build();
        assertThat(options.name()).isEqualTo("Test");
        assertThat(options.properties()).isEqualTo(MINIMAL_PROPS);
        assertThat(options.hadoopConfig()).isEmpty();
    }

    @Test
    void hadoopConfig() {
        final BuildCatalogOptions options = BuildCatalogOptions.builder()
                .putAllProperties(MINIMAL_PROPS)
                .putHadoopConfig("Foo", "Bar")
                .build();
        assertThat(options.name()).isEqualTo("IcebergCatalog");
        assertThat(options.properties()).isEqualTo(MINIMAL_PROPS);
        assertThat(options.hadoopConfig()).isEqualTo(Map.of("Foo", "Bar"));
    }

    @Test
    void nameWithUri() {
        final BuildCatalogOptions options = BuildCatalogOptions.builder()
                .putAllProperties(MINIMAL_PROPS)
                .putProperties(CatalogProperties.URI, "foo")
                .build();
        assertThat(options.name()).isEqualTo("IcebergCatalog-foo");
    }

    @Test
    void missingType() {
        try {
            BuildCatalogOptions.builder().build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e)
                    .hasMessageContaining("Catalog type 'type' or implementation class 'catalog-impl' is required");
        }
    }

    @Test
    void testBasicPropertyInjection() {
        final BuildCatalogOptions options = BuildCatalogOptions.builder()
                .putAllProperties(MINIMAL_PROPS)
                .build();
        assertThat(options.enablePropertyInjection()).isTrue();
        assertThat(options.properties()).isEqualTo(MINIMAL_PROPS);

        final Map<String, String> updatedProperties = options.updatedProperties();
        assertThat(updatedProperties).containsAllEntriesOf(MINIMAL_PROPS);
        assertThat(updatedProperties.get("s3.crt.enabled")).isEqualTo("false");
        assertThat(updatedProperties.get("client.credentials-provider")).isEqualTo(
                "io.deephaven.iceberg.util.DeephavenS3ClientCredentialsProvider");
    }

    @Test
    void testPropertyForwardingWithInjection() {
        final Map<String, String> props = Map.of(
                CatalogUtil.ICEBERG_CATALOG_TYPE, "minimal",
                "s3.access-key-id", "accessKey",
                "s3.session-token", "sessionToken",
                "s3.crt.enabled", "true");
        final BuildCatalogOptions options = BuildCatalogOptions.builder()
                .putAllProperties(props)
                .build();
        assertThat(options.properties()).isEqualTo(props);

        final Map<String, String> updatedProperties = options.updatedProperties();
        assertThat(updatedProperties).containsAllEntriesOf(props);
        assertThat(updatedProperties.get("s3.crt.enabled")).isEqualTo("true");
        assertThat(updatedProperties.get("client.credentials-provider.s3.access-key-id")).isEqualTo("accessKey");
        assertThat(updatedProperties.get("client.credentials-provider.s3.session-token")).isEqualTo("sessionToken");
    }

    @Test
    void disablePropertyInjection() {
        final BuildCatalogOptions options = BuildCatalogOptions.builder()
                .putAllProperties(MINIMAL_PROPS)
                .enablePropertyInjection(false)
                .build();
        assertThat(options.properties()).isEqualTo(MINIMAL_PROPS);
        assertThat(options.updatedProperties()).isEqualTo(MINIMAL_PROPS);
        assertThat(options.enablePropertyInjection()).isFalse();
    }
}
