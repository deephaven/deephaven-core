//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.common.base.Strings;
import io.deephaven.extensions.s3.DeephavenAwsClientFactory;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.util.reference.CleanupReferenceProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Tools for accessing tables in the Iceberg table format from S3.
 */
public final class IcebergToolsS3 {

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

        final RESTCatalog catalog = new RESTCatalog();
        catalog.setConf(new Configuration());
        return createAdapterCommon(name, catalogURI, warehouseLocation, catalog, properties);
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
        catalog.setConf(new Configuration());
        return createAdapterCommon(name, catalogURI, warehouseLocation, catalog, properties);
    }

    private static IcebergCatalogAdapter createAdapterCommon(
            @Nullable final String name,
            @NotNull final String catalogURI,
            @NotNull final String warehouseLocation,
            @NotNull final Catalog catalog,
            @NotNull final Map<String, String> properties) {
        properties.put(CatalogProperties.CATALOG_IMPL, catalog.getClass().getName());
        properties.put(CatalogProperties.URI, catalogURI);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        // Following is needed to write new manifest files when writing new data.
        // Not setting this will result in using ResolvingFileIO.
        properties.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());

        final String catalogName = name != null ? name : "IcebergCatalog-" + catalogURI;
        catalog.initialize(catalogName, properties);

        return IcebergCatalogAdapter.of(catalog, properties);
    }

    /**
     * Create an Iceberg catalog adapter.
     *
     * <p>
     * This is the preferred way to configure an Iceberg catalog adapter when the caller is responsible for providing
     * AWS / S3 connectivity details; specifically, this allows for the parity of construction logic between
     * Iceberg-managed and Deephaven-managed AWS clients. For advanced use-cases, users are encouraged to use
     * {@link S3Instructions#profileName() profiles} which allows a rich degree of configurability. The
     * {@code instructions} will automatically be used as special instructions if
     * {@link IcebergReadInstructions#dataInstructions()} is not explicitly set. The caller is still responsible for
     * providing any other properties necessary to configure their {@link org.apache.iceberg.catalog.Catalog}
     * implementation.
     *
     * <p>
     * In cases where the caller prefers to use Iceberg's AWS properties (found amongst {@link AwsProperties},
     * {@link S3FileIOProperties}, and {@link HttpClientProperties}), they should use
     * {@link IcebergTools#createAdapter(String, Map, Map) IcebergTools} directly. In this case, parity will be limited
     * to what {@link S3InstructionsProviderPlugin} is able to infer; in advanced cases, it's possible that there will
     * be a difference in construction logic between the Iceberg-managed and Deephaven-managed AWS clients which
     * manifests itself as being able to browse {@link org.apache.iceberg.catalog.Catalog} metadata, but not retrieve
     * {@link org.apache.iceberg.Table} data.
     *
     * <p>
     * Note: this method does not explicitly set, nor impose, that {@link org.apache.iceberg.aws.s3.S3FileIO} be used.
     * It's possible that a {@link org.apache.iceberg.catalog.Catalog} implementations depends on an AWS client for
     * purposes unrelated to storing the warehouse data via S3.
     *
     * @param name the name of the catalog; if omitted, the catalog URI will be used to generate a name
     * @param properties a map containing the Iceberg catalog properties to use
     * @param hadoopConfig a map containing Hadoop configuration properties to use
     * @param instructions the s3 instructions
     * @return the Iceberg catalog adapter
     */
    public static IcebergCatalogAdapter createAdapter(
            @Nullable final String name,
            @NotNull final Map<String, String> properties,
            @NotNull final Map<String, String> hadoopConfig,
            @NotNull final S3Instructions instructions) {
        final Map<String, String> newProperties = new HashMap<>(properties);
        final Runnable cleanup = DeephavenAwsClientFactory.addToProperties(instructions, newProperties);
        final IcebergCatalogAdapter adapter = IcebergTools.createAdapter(name, newProperties, hadoopConfig);
        // When the Catalog becomes phantom reachable, we can invoke the DeephavenAwsClientFactory cleanup.
        // Note: it would be incorrect to register the cleanup against the adapter since the Catalog can outlive the
        // adapter (and the DeephavenAwsClientFactory properties are needed by the Catalog).
        CleanupReferenceProcessor.getDefault().registerPhantom(adapter.catalog(), cleanup);
        return adapter;
    }
}
