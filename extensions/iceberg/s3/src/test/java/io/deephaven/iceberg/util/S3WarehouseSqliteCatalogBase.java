//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.s3.S3Constants;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.base.IcebergTestUtils;
import io.deephaven.iceberg.junit5.SqliteCatalogBase;
import io.deephaven.iceberg.sqlite.SqliteHelper;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.apache.iceberg.catalog.TableIdentifier;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.extensions.s3.testlib.S3Helper.TIMEOUT_SECONDS;
import static io.deephaven.iceberg.base.IcebergUtils.dataFileUri;
import static io.deephaven.iceberg.base.IcebergUtils.locationUri;
import static org.assertj.core.api.Assertions.assertThat;

abstract class S3WarehouseSqliteCatalogBase extends SqliteCatalogBase {

    public abstract S3Instructions s3Instructions();

    public abstract S3AsyncClient s3AsyncClient();

    @Override
    public final Object dataInstructions() {
        return s3Instructions();
    }

    @Override
    protected IcebergCatalogAdapter catalogAdapter(
            final TestInfo testInfo,
            final Path rootDir,
            final Map<String, String> properties)
            throws ExecutionException, InterruptedException, TimeoutException {
        return catalogAdapterForScheme(testInfo, properties, "s3");
    }

    private IcebergCatalogAdapter catalogAdapterForScheme(
            final TestInfo testInfo,
            final Map<String, String> properties,
            final String scheme)
            throws ExecutionException, InterruptedException, TimeoutException {
        final String methodName = testInfo.getTestMethod().orElseThrow().getName();
        final String catalogName = methodName + "-catalog";
        final String bucket = methodName.toLowerCase(Locale.US) + "-bucket";
        try (final S3AsyncClient client = s3AsyncClient()) {
            if (!doesBucketExist(client, bucket)) {
                client.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, scheme + "://" + bucket + "/warehouse");
        properties.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        return IcebergToolsS3.createAdapter(catalogName, properties, Map.of(), s3Instructions());
    }

    private boolean doesBucketExist(final S3AsyncClient client, final String bucketName)
            throws ExecutionException, InterruptedException, TimeoutException {
        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoSuchBucketException) {
                return false;
            }
            throw e;
        }
    }

    @Test
    void testIcebergTablesWithS3AScheme(TestInfo testInfo, @TempDir Path rootDir)
            throws ExecutionException, InterruptedException, TimeoutException {
        testIcebergTablesWithCustomScheme(S3Constants.S3A_URI_SCHEME, testInfo, rootDir);
    }

    @Test
    void testIcebergTablesWithS3NScheme(TestInfo testInfo, @TempDir Path rootDir)
            throws ExecutionException, InterruptedException, TimeoutException {
        testIcebergTablesWithCustomScheme(S3Constants.S3N_URI_SCHEME, testInfo, rootDir);
    }

    private void testIcebergTablesWithCustomScheme(final String scheme, TestInfo testInfo, @TempDir Path rootDir)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Map<String, String> properties = new HashMap<>();
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        final IcebergCatalogAdapter catalogAdapter = catalogAdapterForScheme(testInfo, properties, scheme);

        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");

        final Table data = TableTools.newTable(
                intCol("intCol", 2, 4, 6, 8, 10),
                doubleCol("doubleCol", 2.5, 5.0, 7.5, 10.0, 12.5));

        // Create a new iceberg table
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, data.getDefinition());
        final org.apache.iceberg.Table icebergTable = tableAdapter.icebergTable();

        // Verify that the table location has the right scheme
        assertThat(locationUri(icebergTable).getScheme()).isEqualTo(scheme);

        // Add data to the table
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(data.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(data, data)
                .build());

        // Verify all data files have the right scheme
        final List<DataFile> dataFiles = IcebergTestUtils.allDataFiles(icebergTable, icebergTable.currentSnapshot())
                .collect(Collectors.toList());
        assertThat(dataFiles).hasSize(2);
        assertThat(dataFiles).allMatch(dataFile -> dataFileUri(icebergTable, dataFile).getScheme().equals(scheme));

        // Verify the data is correct
        Table fromIceberg = tableAdapter.table();
        Table expected = TableTools.merge(data, data);
        assertTableEquals(expected, fromIceberg);

        // Create a new data file but with s3 scheme
        final DataFile existingDataFile = dataFiles.get(0);
        final String existingDataFileLocation = existingDataFile.location();
        assertThat(existingDataFileLocation).startsWith(scheme);
        final String newLocation = existingDataFileLocation.replace(scheme + "://", "s3://");
        final DataFile newDataFile = DataFiles.builder(icebergTable.spec())
                .withPath(newLocation)
                .withFormat(existingDataFile.format())
                .withRecordCount(existingDataFile.recordCount())
                .withFileSizeInBytes(existingDataFile.fileSizeInBytes())
                .build();

        // Append the new data files to the table
        icebergTable.newAppend().appendFile(newDataFile).commit();

        // Verify the new data file has the right scheme
        final List<DataFile> newDataFiles = IcebergTestUtils.allDataFiles(icebergTable, icebergTable.currentSnapshot())
                .collect(Collectors.toList());
        int s3DataFiles = 0;
        int nonS3DataFiles = 0;
        for (final DataFile dataFile : newDataFiles) {
            if (dataFileUri(icebergTable, dataFile).getScheme().equals(scheme)) {
                nonS3DataFiles++;
            } else {
                assertThat(dataFileUri(icebergTable, dataFile).getScheme()).isEqualTo("s3");
                s3DataFiles++;
            }
        }
        assertThat(s3DataFiles).isEqualTo(1);
        assertThat(nonS3DataFiles).isEqualTo(2);

        // Verify the data is correct
        fromIceberg = tableAdapter.table();
        expected = TableTools.merge(expected, data);
        assertTableEquals(expected, fromIceberg);
    }
}
