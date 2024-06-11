//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.common.base.Strings;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Tools for accessing tables in the Iceberg table format.
 */
@SuppressWarnings("unused")
public class IcebergToolsS3 extends IcebergTools {
    private static final String S3_FILE_IO_CLASS = "org.apache.iceberg.aws.s3.S3FileIO";

    public static IcebergCatalogAdapter createS3Rest(
            @Nullable final String name,
            @NotNull final String catalogURI,
            @NotNull final String warehouseLocation,
            @Nullable final String region,
            @Nullable final String accessKeyId,
            @Nullable final String secretAccessKey,
            @Nullable final String endpointOverride) {

        // Set up the properties map for the Iceberg catalog
        final Map<String, String> properties = new HashMap<>();

        final RESTCatalog catalog = new RESTCatalog();

        properties.put(CatalogProperties.CATALOG_IMPL, catalog.getClass().getName());
        properties.put(CatalogProperties.URI, catalogURI);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        // Configure the properties map from the Iceberg instructions.
        if (!Strings.isNullOrEmpty(accessKeyId) && !Strings.isNullOrEmpty(secretAccessKey)) {
            properties.put(S3FileIOProperties.ACCESS_KEY_ID, accessKeyId);
            properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, secretAccessKey);
        }
        if (!Strings.isNullOrEmpty(region)) {
            properties.put(AwsClientProperties.CLIENT_REGION, region);
        }
        if (!Strings.isNullOrEmpty(endpointOverride)) {
            properties.put(S3FileIOProperties.ENDPOINT, endpointOverride);
        }

        // TODO: create a FileIO interface wrapping the Deephaven S3SeekableByteChannel/Provider
        final FileIO fileIO = CatalogUtil.loadFileIO(S3_FILE_IO_CLASS, properties, null);

        final String catalogName = name != null ? name : "IcebergCatalog-" + catalogURI;
        catalog.initialize(catalogName, properties);

        return new IcebergCatalogAdapter(catalog, fileIO);
    }

}
