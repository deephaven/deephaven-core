//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.immutables.value.Value;

import java.util.Map;

/**
 * The options to use with {@link IcebergTools#createAdapter(BuildCatalogOptions)}.
 */
@Value.Immutable
public abstract class BuildCatalogOptions {

    public static Builder builder() {
        return ImmutableBuildCatalogOptions.builder();
    }

    /**
     * The catalog name. By default, is "IcebergCatalog-{uri}" if {@value CatalogProperties#URI} is set in
     * {@link #properties()}, otherwise is "IcebergCatalog".
     */
    @Value.Default
    public String name() {
        final String catalogUri = properties().get(CatalogProperties.URI);
        return "IcebergCatalog" + (catalogUri == null ? "" : "-" + catalogUri);
    }

    /**
     * The catalog properties provided by the user. Must contain {@value CatalogUtil#ICEBERG_CATALOG_TYPE} or
     * {@value CatalogProperties#CATALOG_IMPL}.
     */
    public abstract Map<String, String> properties();

    abstract BuildCatalogOptions withProperties(Map<String, ? extends String> entries);

    /**
     * Enables Deephaven’s automatic injection of properties that work around upstream issues and supply defaults needed
     * for Deephaven’s Iceberg usage. Disable to manage all properties yourself.
     *
     * @see IcebergTools#injectDeephavenProperties(Map)
     */
    @Value.Default
    public boolean enablePropertyInjection() {
        return true;
    }

    /**
     * The Hadoop configuration properties.
     */
    public abstract Map<String, String> hadoopConfig();

    public interface Builder {

        Builder name(String name);

        Builder putProperties(String key, String value);

        Builder putAllProperties(Map<String, ? extends String> entries);

        Builder putHadoopConfig(String key, String value);

        Builder putAllHadoopConfig(Map<String, ? extends String> entries);

        Builder enablePropertyInjection(boolean enable);

        BuildCatalogOptions build();
    }

    @Value.Check
    final void checkProperties() {
        // Validate the minimum required properties are set
        if (!properties().containsKey(CatalogProperties.CATALOG_IMPL)
                && !properties().containsKey(CatalogUtil.ICEBERG_CATALOG_TYPE)) {
            throw new IllegalArgumentException(
                    String.format("Catalog type '%s' or implementation class '%s' is required",
                            CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogProperties.CATALOG_IMPL));
        }
    }
}
