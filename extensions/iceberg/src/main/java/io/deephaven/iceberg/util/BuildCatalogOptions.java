//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.immutables.value.Value;

import java.util.Map;
import java.util.Optional;

/**
 * The options to use with {@link IcebergTools#createAdapter(BuildCatalogOptions)}.
 */
@Value.Immutable
public abstract class BuildCatalogOptions {

    public static Builder builder() {
        return ImmutableBuildCatalogOptions.builder();
    }

    /**
     * The catalog name.
     */
    public abstract Optional<String> name();

    /**
     * The catalog properties.
     */
    public abstract Map<String, String> properties();

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
