//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.extensions.s3.testlib.SingletonContainers.LocalStack;
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
final class LocalstackWarehouseSqliteCatalogTest extends S3WarehouseSqliteCatalogBase {
    @BeforeAll
    static void initContainer() {
        // ensure container is started so container startup time isn't associated with a specific test
        LocalStack.init();
    }

    @Override
    public S3Instructions s3Instructions() {
        return LocalStack.s3Instructions(S3Instructions.builder()
                .readTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))).build();
    }

    @Override
    public Map<String, String> s3ConnectionProperties() {
        return Map.of(
                ENDPOINT, LocalStack.s3Endpoint(),
                CLIENT_REGION, LocalStack.region(),
                ACCESS_KEY_ID, LocalStack.accessKey(),
                SECRET_ACCESS_KEY, LocalStack.secretAccessKey(),
                AsyncHttpClientProperties.ASYNC_CLIENT_TYPE, AsyncHttpClientProperties.ASYNC_CLIENT_TYPE_NETTY,
                AsyncHttpClientProperties.NETTY_READ_TIMEOUT_MS, String.valueOf(TIMEOUT_SECONDS * 1_000L));
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return LocalStack.s3AsyncClient();
    }
}
