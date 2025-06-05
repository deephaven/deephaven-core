//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
public final class IcebergTools {
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
     * <li>{@value CatalogProperties#CATALOG_IMPL} or {@value CatalogUtil#ICEBERG_CATALOG_TYPE} - the Java catalog
     * implementation to use. When providing {@value CatalogProperties#CATALOG_IMPL}, the implementing Java class should
     * be provided (e.g. {@code "org.apache.iceberg.rest.RESTCatalog"} or
     * {@code "org.apache.iceberg.aws.glue.GlueCatalog")}. Choices for {@value CatalogUtil#ICEBERG_CATALOG_TYPE} include
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_HIVE}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_HADOOP},
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_REST}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_GLUE},
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_NESSIE}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_JDBC}.</li>
     * </ul>
     * <p>
     * Other common properties include:
     * </p>
     * <ul>
     * <li>{@value CatalogProperties#URI} - the URI of the catalog.</li>
     * <li>{@value CatalogProperties#WAREHOUSE_LOCATION} - the location of the data warehouse.</li>
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
     * <li>{@value CatalogProperties#CATALOG_IMPL} or {@value CatalogUtil#ICEBERG_CATALOG_TYPE} - the Java catalog
     * implementation to use. When providing {@value CatalogProperties#CATALOG_IMPL}, the implementing Java class should
     * be provided (e.g. {@code "org.apache.iceberg.rest.RESTCatalog"} or
     * {@code "org.apache.iceberg.aws.glue.GlueCatalog")}. Choices for {@value CatalogUtil#ICEBERG_CATALOG_TYPE} include
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_HIVE}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_HADOOP},
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_REST}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_GLUE},
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_NESSIE}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_JDBC}.</li>
     * </ul>
     * <p>
     * Other common properties include:
     * </p>
     * <ul>
     * <li>{@value CatalogProperties#URI} - the URI of the catalog.</li>
     * <li>{@value CatalogProperties#WAREHOUSE_LOCATION} - the location of the data warehouse.</li>
     * <li>{@code "client.region"} - the region of the AWS client.</li>
     * <li>{@code "s3.access-key-id"} - the S3 access key for reading files.</li>
     * <li>{@code "s3.secret-access-key"} - the S3 secret access key for reading files.</li>
     * <li>{@code "s3.endpoint"} - the S3 endpoint to connect to.</li>
     * </ul>
     * <p>
     * Additional properties for the specific catalog should also be included, such as S3-specific properties for
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
        final BuildCatalogOptions.Builder builder = BuildCatalogOptions.builder()
                .putAllProperties(properties)
                .putAllHadoopConfig(hadoopConfig);
        if (name != null) {
            builder.name(name);
        }
        return createAdapter(builder.build());
    }

    /**
     * <p>
     * Create an Iceberg catalog adapter for an Iceberg catalog created from configuration properties. These properties
     * map to the Iceberg catalog Java API properties and are used to create the catalog and file IO implementations.
     * </p>
     * <p>
     * The minimal set of properties required to create an Iceberg catalog are:
     * <ul>
     * <li>{@value CatalogProperties#CATALOG_IMPL} or {@value CatalogUtil#ICEBERG_CATALOG_TYPE} - the Java catalog
     * implementation to use. When providing {@value CatalogProperties#CATALOG_IMPL}, the implementing Java class should
     * be provided (e.g. {@code "org.apache.iceberg.rest.RESTCatalog"} or
     * {@code "org.apache.iceberg.aws.glue.GlueCatalog")}. Choices for {@value CatalogUtil#ICEBERG_CATALOG_TYPE} include
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_HIVE}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_HADOOP},
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_REST}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_GLUE},
     * {@value CatalogUtil#ICEBERG_CATALOG_TYPE_NESSIE}, {@value CatalogUtil#ICEBERG_CATALOG_TYPE_JDBC}.</li>
     * </ul>
     * <p>
     * Other common properties include:
     * </p>
     * <ul>
     * <li>{@value CatalogProperties#URI} - the URI of the catalog.</li>
     * <li>{@value CatalogProperties#WAREHOUSE_LOCATION} - the location of the data warehouse.</li>
     * <li>{@code "client.region"} - the region of the AWS client.</li>
     * <li>{@code "s3.access-key-id"} - the S3 access key for reading files.</li>
     * <li>{@code "s3.secret-access-key"} - the S3 secret access key for reading files.</li>
     * <li>{@code "s3.endpoint"} - the S3 endpoint to connect to.</li>
     * </ul>
     * <p>
     * Additional properties for the specific catalog should also be included, such as S3-specific properties for
     * authentication or endpoint overriding.
     * </p>
     *
     * @param options the options
     * @return the Iceberg catalog adapter
     */
    public static IcebergCatalogAdapter createAdapter(@NotNull final BuildCatalogOptions options) {
        // Load the Hadoop configuration with the provided properties
        final Configuration hadoopConf = new Configuration();
        options.hadoopConfig().forEach(hadoopConf::set);

        final Map<String, String> properties;
        if (options.enablePropertyInjection()) {
            properties = injectDeephavenProperties(options.properties());
        } else {
            properties = options.properties();
        }

        // Create the Iceberg catalog from the properties
        final Catalog catalog = CatalogUtil.buildIcebergCatalog(options.name(), properties, hadoopConf);
        return IcebergCatalogAdapter.of(catalog, properties);
    }

    /**
     * Create a new {@link java.util.Map} containing the caller-supplied properties plus additional properties that work
     * around upstream issues and supply defaults needed for Deephaven’s Iceberg usage.
     *
     * @param inputProperties the input properties to inject into
     * @return a new map with the injected properties
     */
    public static Map<String, String> injectDeephavenProperties(@NotNull final Map<String, String> inputProperties) {
        return InjectAWSProperties.injectDeephavenProperties(inputProperties);
    }
}
