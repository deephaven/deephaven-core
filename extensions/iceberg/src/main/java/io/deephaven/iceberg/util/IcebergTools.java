//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Tools for accessing tables in the Iceberg table format.
 */
public abstract class IcebergTools {
    @SuppressWarnings("unused")
    public static IcebergCatalogAdapter createAdapter(
            final Catalog catalog,
            final FileIO fileIO) {
        return new IcebergCatalogAdapter(catalog, fileIO);
    }

    /**
     * <p>
     * Create an Iceberg catalog adapter for an Iceberg catalog created from configuration properties. These properties
     * map to the Iceberg catalog Java API properties and are used to create the catalog and file IO implementations.
     * </p>
     * <p>
     * This is a wrapper around {@link CatalogUtil#buildIcebergCatalog(String, Map, Object)} and accepts the same
     * properties. The minimal set of properties required to create an Iceberg catalog are:
     * <ul>
     * <li>{@link CatalogProperties#CATALOG_IMPL}</li>
     * <li>{@link CatalogProperties#URI}</li>
     * <li>{@link CatalogProperties#WAREHOUSE_LOCATION}</li>
     * <li>{@link CatalogProperties#FILE_IO_IMPL}</li>
     * </ul>
     * <p>
     * Additional properties for the specific catalog should also be included, such as as S3-specific properties for
     * authentication or endpoint overriding.
     * </p>
     * 
     * @param name the name of the catalog; if omitted, the catalog URI will be used to generate a name
     * @param properties the map containing the Iceberg catalog properties to use
     * @return the Iceberg catalog adapter
     */
    public static IcebergCatalogAdapter createAdapter(
            @Nullable final String name,
            @NotNull final Map<String, String> properties) {

        // Validate the minimum required properties are set.
        if (!properties.containsKey(CatalogProperties.CATALOG_IMPL)) {
            throw new IllegalArgumentException(String.format("Catalog implementation property '%s' is required",
                    CatalogProperties.CATALOG_IMPL));
        }
        if (!properties.containsKey(CatalogProperties.URI)) {
            throw new IllegalArgumentException(String.format("Catalog URI property '%s' is required",
                    CatalogProperties.URI));
        }
        if (!properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
            throw new IllegalArgumentException(String.format("Warehouse location property '%s' is required",
                    CatalogProperties.WAREHOUSE_LOCATION));
        }
        if (!properties.containsKey(CatalogProperties.FILE_IO_IMPL)) {
            throw new IllegalArgumentException(String.format("File IO implementation property '%s' is required",
                    CatalogProperties.FILE_IO_IMPL));
        }

        final String catalogUri = properties.get(CatalogProperties.URI);
        final String catalogName = name != null ? name : "IcebergCatalog-" + catalogUri;

        // Create the Iceberg catalog from the properties.
        final Catalog catalog = CatalogUtil.buildIcebergCatalog(catalogName, properties, null);

        // Create the file IO implementation from the provided File IO property.
        final String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        final FileIO fileIO = CatalogUtil.loadFileIO(fileIOImpl, properties, null);

        return new IcebergCatalogAdapter(catalog, fileIO);
    }
}
