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
        assertThat(options.enablePropertyInjection()).isTrue();
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
    void disablePropertyInjection() {
        final BuildCatalogOptions options = BuildCatalogOptions.builder()
                .putAllProperties(MINIMAL_PROPS)
                .enablePropertyInjection(false)
                .build();
        assertThat(options.enablePropertyInjection()).isFalse();
    }
}
