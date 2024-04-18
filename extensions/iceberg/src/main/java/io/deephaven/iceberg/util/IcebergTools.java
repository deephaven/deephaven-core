//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.parquet.Strings;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Tools for accessing tables in the Iceberg table format.
 */
public class IcebergTools {

    @SuppressWarnings("unused")
    public static IcebergCatalogAdapter createAdapter(
            final Catalog catalog,
            final FileIO fileIO,
            final IcebergInstructions instructions) {
        return new IcebergCatalogAdapter(catalog, fileIO, instructions);
    }

    private IcebergTools() {}


    public static IcebergCatalogAdapter createS3Rest(
            @Nullable final String name,
            @NotNull final String catalogURI,
            @NotNull final String warehouseLocation,
            @Nullable final String region,
            @Nullable final String accessKeyId,
            @Nullable final String secretAccessKey,
            @Nullable final String endpointOverride,
            @Nullable final IcebergInstructions specialInstructions) {


        // Set up the properties map for the Iceberg catalog
        final Map<String, String> properties = new HashMap<>();

        final RESTCatalog catalog = new RESTCatalog();

        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, catalogURI);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

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
        final FileIO fileIO = CatalogUtil.loadFileIO("org.apache.iceberg.aws.s3.S3FileIO", properties, null);

        final String catalogName = name != null ? name : "IcebergTableDataService-" + catalogURI;
        catalog.initialize(catalogName, properties);

        // If the user did not supply custom read instructions, let's create some defaults.
        final IcebergInstructions instructions = specialInstructions != null
                ? specialInstructions
                : buildInstructions(properties);

        return new IcebergCatalogAdapter(catalog, fileIO, instructions);
    }

    private static IcebergInstructions buildInstructions(final Map<String, String> properties) {
        final S3Instructions.Builder builder = S3Instructions.builder();
        if (properties.containsKey(S3FileIOProperties.ACCESS_KEY_ID)
                && properties.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)) {
            builder.credentials(Credentials.basic(properties.get(S3FileIOProperties.ACCESS_KEY_ID),
                    properties.get(S3FileIOProperties.SECRET_ACCESS_KEY)));
        }
        if (properties.containsKey(AwsClientProperties.CLIENT_REGION)) {
            builder.regionName(properties.get(AwsClientProperties.CLIENT_REGION));
        }
        if (properties.containsKey(S3FileIOProperties.ENDPOINT)) {
            builder.endpointOverride(properties.get(S3FileIOProperties.ENDPOINT));
        }
        return IcebergInstructions.builder()
                .s3Instructions(builder.build())
                .build();
    }
}
