//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.extensions.s3.testlib.SingletonContainers.MinIO;
import io.deephaven.base.OSUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.time.Duration;
import java.util.Map;

import static io.deephaven.extensions.s3.testlib.S3Helper.TIMEOUT_SECONDS;
import static org.apache.iceberg.aws.AwsClientProperties.CLIENT_REGION;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ACCESS_KEY_ID;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ENDPOINT;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.SECRET_ACCESS_KEY;

@Tag("testcontainers")
final class MinioWarehouseSqliteCatalogTest extends S3WarehouseSqliteCatalogBase {
    @BeforeAll
    static void initContainer() {
        // TODO(deephaven-core#5116): MinIO testcontainers does not work on OS X
        Assumptions.assumeFalse(OSUtil.runningMacOS(), "OSUtil.runningMacOS()");
        // ensure container is started so container startup time isn't associated with a specific test
        MinIO.init();
    }

    @Override
    public S3Instructions s3Instructions() {
        return MinIO.s3Instructions(S3Instructions.builder()
                .readTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))).build();
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return MinIO.s3AsyncClient();
    }

    @Override
    public Map<String, String> s3ConnectionProperties() {
        return Map.of(
                ENDPOINT, MinIO.s3Endpoint(),
                CLIENT_REGION, MinIO.region(),
                ACCESS_KEY_ID, MinIO.accessKey(),
                SECRET_ACCESS_KEY, MinIO.secretAccessKey(),
                AsyncHttpClientProperties.ASYNC_CLIENT_TYPE, AsyncHttpClientProperties.ASYNC_CLIENT_TYPE_NETTY,
                AsyncHttpClientProperties.NETTY_READ_TIMEOUT_MS, String.valueOf(TIMEOUT_SECONDS * 1_000L));
    }
}
