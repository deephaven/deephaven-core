//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.common.base.Strings;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
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

    /**
     * Create an Iceberg catalog adapter for a REST catalog backed by S3 storage. If {@code null} is provided for a
     * value, the system defaults will be used.
     *
     * @param name the name of the catalog; if omitted, the catalog URI will be used to generate a name
     * @param catalogURI the URI of the Iceberg REST catalog
     * @param warehouseLocation the location of the S3 datafiles backing the catalog
     * @param region the AWS region; if omitted, system defaults will be used
     * @param accessKeyId the AWS access key ID; if omitted, system defaults will be used
     * @param secretAccessKey the AWS secret access key; if omitted, system defaults will be used
     * @param endpointOverride the S3 endpoint override; this is useful for testing with a S3-compatible local service
     *        such as MinIO or LocalStack
     * @return the Iceberg catalog adapter
     */
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

        final String catalogName = name != null ? name : "IcebergCatalog-" + catalogURI;
        catalog.initialize(catalogName, properties);
        return IcebergCatalogAdapter.of(catalog);
    }

    /**
     * Create an Iceberg catalog adapter for an AWS Glue catalog. System defaults will be used to populate the region
     * and credentials. These can be configured by following
     * <a href="https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html">AWS Authentication and
     * access credentials</a> guide.
     *
     * @param name the name of the catalog; if omitted, the catalog URI will be used to generate a name
     * @param catalogURI the URI of the AWS Glue catalog
     * @param warehouseLocation the location of the S3 datafiles backing the catalog
     * @return the Iceberg catalog adapter
     */
    public static IcebergCatalogAdapter createGlue(
            @Nullable final String name,
            @NotNull final String catalogURI,
            @NotNull final String warehouseLocation) {

        // Set up the properties map for the Iceberg catalog
        final Map<String, String> properties = new HashMap<>();

        final GlueCatalog catalog = new GlueCatalog();

        properties.put(CatalogProperties.CATALOG_IMPL, catalog.getClass().getName());
        properties.put(CatalogProperties.URI, catalogURI);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        final String catalogName = name != null ? name : "IcebergCatalog-" + catalogURI;
        catalog.initialize(catalogName, properties);

        return new IcebergCatalogAdapter(catalog, properties);
    }
}
