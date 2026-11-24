//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.extensions.s3.testlib.S3Helper.TIMEOUT_SECONDS;
import static io.deephaven.iceberg.base.IcebergUtils.dataFileUri;
import static io.deephaven.iceberg.base.IcebergUtils.locationUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

abstract class S3WarehouseSqliteCatalogBase extends SqliteCatalogBase {

    public abstract Map<String, String> s3ConnectionProperties();

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
        // By default, the http clients are managed by Iceberg
        return catalogAdapterForScheme(testInfo, properties, "s3", false);
    }

    private IcebergCatalogAdapter catalogAdapterForScheme(
            final TestInfo testInfo,
            final Map<String, String> properties,
            final String scheme,
            final boolean useDeephavenManagedS3Clients)
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
        if (useDeephavenManagedS3Clients) {
            return IcebergToolsS3.createAdapter(
                    BuildCatalogOptions.builder()
                            .name(catalogName)
                            .putAllProperties(properties)
                            .putAllHadoopConfig(Map.of())
                            .build(),
                    s3Instructions());
        }
        final Map<String, String> s3ConnectionProperties = s3ConnectionProperties();
        for (final Map.Entry<String, String> entry : s3ConnectionProperties.entrySet()) {
            final String key = entry.getKey();
            if (!properties.containsKey(key)) {
                properties.put(key, entry.getValue());
            }
        }
        return IcebergTools.createAdapter(
                BuildCatalogOptions.builder()
                        .name(catalogName)
                        .putAllProperties(properties)
                        .putAllHadoopConfig(Map.of())
                        .build());
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
        final IcebergCatalogAdapter catalogAdapter = catalogAdapterForScheme(testInfo, properties, scheme, false);

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

    @Test
    void testReadWriteUsingAsyncHttpClientsManagedByDeephaven(TestInfo testInfo, @TempDir Path rootDir)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Map<String, String> properties = new HashMap<>();
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        testReadWriteImpl(catalogAdapterForScheme(testInfo, properties, "s3", true),
                "MyNamespace.TestTableWithClientManagedByDeephaven");
    }

    @Test
    void testReadWriteWithCrtAsyncHttpClientsManagedByIceberg(TestInfo testInfo, @TempDir Path rootDir)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Map<String, String> properties = new HashMap<>();

        // Use the incorrect client type to verify that the test fails with the expected error message
        properties.put("http-client-async.client-type", "aws-crt");
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        try {
            testReadWriteImpl(catalogAdapterForScheme(testInfo, properties, "s3", false),
                    "MyNamespace.TestTableWithCRTClientManagedByIceberg1");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("Unknown HTTP client type: aws-crt. Expected one of: netty, crt");
        }

        // Use the correct client type to verify that the test passes
        properties.put("http-client-async.client-type", "crt");
        testReadWriteImpl(catalogAdapterForScheme(testInfo, properties, "s3", false),
                "MyNamespace.TestTableWithCRTClientManagedByIceberg2");
    }

    @Test
    void testReadWriteWithNettyAsyncHttpClientsManagedByIceberg(TestInfo testInfo, @TempDir Path rootDir)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Map<String, String> properties = new HashMap<>();

        // Use the incorrect client type to verify that the test fails with the expected error message
        properties.put("http-client-async.client-type", "netty-nio");
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        try {
            testReadWriteImpl(catalogAdapterForScheme(testInfo, properties, "s3", false),
                    "MyNamespace.TestTableWithNettyClientManagedByIceberg");
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("Unknown HTTP client type: netty-nio. Expected one of: netty, crt");
        }

        // Use the correct client type to verify that the test passes
        properties.put("http-client-async.client-type", "netty");
        testReadWriteImpl(catalogAdapterForScheme(testInfo, properties, "s3", false),
                "MyNamespace.TestTableWithNettyClientManagedByIceberg");
    }


    /**
     * Each catalog has its own FileIO, as each REST-catalog table does when its load response carries config or
     * credentials. Reading through more FileIOs must not add AWS SDK thread pools.
     */
    @Test
    void testFileIOsShareSdkThreadPools(TestInfo testInfo, @TempDir Path rootDir)
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
        final int numReaders = 3;
        final Map<String, String> properties = new HashMap<>();
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        final TableIdentifier tableIdentifier = TableIdentifier.parse("MyNamespace.MyTable");
        final Table data = TableTools.newTable(
                intCol("intCol", 2, 4, 6, 8, 10),
                doubleCol("doubleCol", 2.5, 5.0, 7.5, 10.0, 12.5));
        final IcebergReadInstructions readInstructions = IcebergReadInstructions.builder()
                .dataInstructions(s3Instructions())
                .build();

        final List<IcebergCatalogAdapter> adapters = new ArrayList<>();
        try {
            final IcebergCatalogAdapter writer = catalogAdapterForScheme(testInfo, properties, "s3", false);
            adapters.add(writer);
            writer.createTable(tableIdentifier, data.getDefinition())
                    .tableWriter(writerOptionsBuilder().tableDefinition(data.getDefinition()).build())
                    .append(IcebergWriteInstructions.builder().addTables(data).build());
            // Read once first, so that any pools shared by design already exist.
            assertTableEquals(data, writer.loadTable(tableIdentifier).table(readInstructions));

            final Set<String> before = sdkThreadPools();
            final Set<String> added = new TreeSet<>();
            for (int ii = 0; ii < numReaders; ++ii) {
                final IcebergCatalogAdapter reader = catalogAdapterForScheme(testInfo, properties, "s3", false);
                adapters.add(reader);
                assertTableEquals(data, reader.loadTable(tableIdentifier).table(readInstructions));
                // Sample after every read; idle SDK threads can time out before the loop ends.
                added.addAll(sdkThreadPools());
            }
            added.removeAll(before);
            assertThat(added).as("AWS SDK thread pools added by %d more FileIOs", numReaders).isEmpty();

            // Closing one FileIO's clients must leave the shared executors usable by the others.
            adapters.remove(1).close();
            assertTableEquals(data,
                    adapters.get(adapters.size() - 1).loadTable(tableIdentifier).table(readInstructions));
        } finally {
            for (final IcebergCatalogAdapter adapter : adapters) {
                adapter.close();
            }
        }
    }

    /**
     * The per-client AWS SDK thread pools that are alive, as {@code <prefix>-<pool number>}.
     */
    private static Set<String> sdkThreadPools() {
        final Set<String> pools = new TreeSet<>();
        for (final Thread thread : Thread.getAllStackTraces().keySet()) {
            final Matcher matcher = SDK_THREAD_NAME.matcher(thread.getName());
            if (matcher.matches()) {
                pools.add(matcher.group(1));
            }
        }
        return pools;
    }

    // The SDK's ThreadFactoryBuilder names threads <prefix>-<pool number>-<thread number>.
    private static final Pattern SDK_THREAD_NAME = Pattern.compile(
            "((?:sdk-ScheduledExecutor|sdk-async-response|aws-java-sdk-NettyEventLoop)-\\d+)-\\d+");

    private void testReadWriteImpl(final IcebergCatalogAdapter catalogAdapter, final String tableIdentifierStr) {
        final TableIdentifier tableIdentifier = TableIdentifier.parse(tableIdentifierStr);
        final Table data = TableTools.newTable(
                intCol("intCol", 2, 4, 6, 8, 10),
                doubleCol("doubleCol", 2.5, 5.0, 7.5, 10.0, 12.5));

        // Create a new iceberg table
        final IcebergTableAdapter tableAdapter = catalogAdapter.createTable(tableIdentifier, data.getDefinition());
        final IcebergTableWriter tableWriter = tableAdapter.tableWriter(writerOptionsBuilder()
                .tableDefinition(data.getDefinition())
                .build());
        tableWriter.append(IcebergWriteInstructions.builder()
                .addTables(data, data)
                .build());

        // Verify the data is correct
        final Table fromIceberg = tableAdapter.table();
        final Table expected = TableTools.merge(data, data);
        assertTableEquals(expected, fromIceberg);
    }

}
