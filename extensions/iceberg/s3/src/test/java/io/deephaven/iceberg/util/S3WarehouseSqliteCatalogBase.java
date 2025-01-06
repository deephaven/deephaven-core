//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.junit5.SqliteCatalogBase;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.junit.jupiter.api.TestInfo;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.deephaven.extensions.s3.testlib.S3Helper.TIMEOUT_SECONDS;

abstract class S3WarehouseSqliteCatalogBase extends SqliteCatalogBase {

    public abstract S3Instructions s3Instructions();

    public abstract S3AsyncClient s3AsyncClient();

    @Override
    public final Object dataInstructions() {
        return s3Instructions();
    }

    @Override
    protected IcebergCatalogAdapter catalogAdapter(TestInfo testInfo, Path rootDir, Map<String, String> properties)
            throws ExecutionException, InterruptedException, TimeoutException {
        final String methodName = testInfo.getTestMethod().orElseThrow().getName();
        final String catalogName = methodName + "-catalog";
        final String bucket = methodName.toLowerCase(Locale.US) + "-bucket";
        try (final S3AsyncClient client = s3AsyncClient()) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + bucket + "/warehouse");
        properties.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        return IcebergToolsS3.createAdapter(catalogName, properties, Map.of(), s3Instructions());
    }
}
