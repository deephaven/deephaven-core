//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Tools for accessing tables in the Iceberg table format.
 */
public abstract class IcebergTools {
    @SuppressWarnings("unused")
    public static IcebergCatalogAdapter createAdapter(
            final Catalog catalog) {
        return IcebergCatalogAdapter.of(catalog, Map.of());
    }

    /**
     * <p>
     * Create an Iceberg catalog adapter for an Iceberg catalog created from configuration properties. These properties
     * map to the Iceberg catalog Java API properties and are used to create the catalog and file IO implementations.
     * </p>
     * <p>
     * The minimal set of properties required to create an Iceberg catalog are:
     * <ul>
     * <li>{@code "catalog-impl"} or {@code "type"} - the Java catalog implementation to use. When providing
     * {@code "catalog-impl"}, the implementing Java class should be provided (e.g.
     * {@code "org.apache.iceberg.rest.RESTCatalog"} or {@code "org.apache.iceberg.aws.glue.GlueCatalog")}. Choices for
     * {@code "type"} include {@code "hive"}, {@code "hadoop"}, {@code "rest"}, {@code "glue"}, {@code "nessie"},
     * {@code "jdbc"}.</li>
     * <li>{@code "uri"} - the URI of the catalog.</li>
     * </ul>
     * <p>
     * Other common properties include:
     * </p>
     * <ul>
     * <li>{@code "warehouse"} - the location of the data warehouse.</li>
     * <li>{@code "client.region"} - the region of the AWS client.</li>
     * <li>{@code "s3.access-key-id"} - the S3 access key for reading files.</li>
     * <li>{@code "s3.secret-access-key"} - the S3 secret access key for reading files.</li>
     * <li>{@code "s3.endpoint"} - the S3 endpoint to connect to.</li>
     * </ul>
     * <p>
     * Additional properties for the specific catalog should also be included, such as as S3-specific properties for
     * authentication or endpoint overriding.
     * </p>
     * 
     * @param name the name of the catalog; if omitted, the catalog URI will be used to generate a name
     * @param properties a map containing the Iceberg catalog properties to use
     * @return the Iceberg catalog adapter
     */
    @SuppressWarnings("unused")
    public static IcebergCatalogAdapter createAdapter(
            @Nullable final String name,
            @NotNull final Map<String, String> properties) {
        return createAdapter(name, properties, Map.of());
    }

    /**
     * <p>
     * Create an Iceberg catalog adapter for an Iceberg catalog created from configuration properties. These properties
     * map to the Iceberg catalog Java API properties and are used to create the catalog and file IO implementations.
     * </p>
     * <p>
     * The minimal set of properties required to create an Iceberg catalog are:
     * <ul>
     * <li>{@code "catalog-impl"} or {@code "type"} - the Java catalog implementation to use. When providing
     * {@code "catalog-impl"}, the implementing Java class should be provided (e.g.
     * {@code "org.apache.iceberg.rest.RESTCatalog"} or {@code "org.apache.iceberg.aws.glue.GlueCatalog")}. Choices for
     * {@code "type"} include {@code "hive"}, {@code "hadoop"}, {@code "rest"}, {@code "glue"}, {@code "nessie"},
     * {@code "jdbc"}.</li>
     * <li>{@code "uri"} - the URI of the catalog.</li>
     * </ul>
     * <p>
     * Other common properties include:
     * </p>
     * <ul>
     * <li>{@code "warehouse"} - the location of the data warehouse.</li>
     * <li>{@code "client.region"} - the region of the AWS client.</li>
     * <li>{@code "s3.access-key-id"} - the S3 access key for reading files.</li>
     * <li>{@code "s3.secret-access-key"} - the S3 secret access key for reading files.</li>
     * <li>{@code "s3.endpoint"} - the S3 endpoint to connect to.</li>
     * </ul>
     * <p>
     * Additional properties for the specific catalog should also be included, such as as S3-specific properties for
     * authentication or endpoint overriding.
     * </p>
     *
     * @param name the name of the catalog; if omitted, the catalog URI will be used to generate a name
     * @param properties a map containing the Iceberg catalog properties to use
     * @param hadoopConfig a map containing Hadoop configuration properties to use
     * @return the Iceberg catalog adapter
     */
    @SuppressWarnings("unused")
    public static IcebergCatalogAdapter createAdapter(
            @Nullable final String name,
            @NotNull final Map<String, String> properties,
            @NotNull final Map<String, String> hadoopConfig) {
        // Validate the minimum required properties are set
        if (!properties.containsKey(CatalogProperties.CATALOG_IMPL)
                && !properties.containsKey(CatalogUtil.ICEBERG_CATALOG_TYPE)) {
            throw new IllegalArgumentException(
                    String.format("Catalog type '%s' or implementation class '%s' is required",
                            CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogProperties.CATALOG_IMPL));
        }
        if (!properties.containsKey(CatalogProperties.URI)) {
            throw new IllegalArgumentException(String.format("Catalog URI property '%s' is required",
                    CatalogProperties.URI));
        }

        final String catalogUri = properties.get(CatalogProperties.URI);
        final String catalogName = name != null ? name : "IcebergCatalog-" + catalogUri;

        // Load the Hadoop configuration with the provided properties
        final Configuration hadoopConf = new Configuration();
        hadoopConfig.forEach(hadoopConf::set);

        // Create the Iceberg catalog from the properties
        final Catalog catalog = CatalogUtil.buildIcebergCatalog(catalogName, properties, hadoopConf);

        return IcebergCatalogAdapter.of(catalog, properties);
    }

}
