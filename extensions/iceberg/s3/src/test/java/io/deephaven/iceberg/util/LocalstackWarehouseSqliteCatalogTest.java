//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.extensions.s3.testlib.SingletonContainers.LocalStack;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.time.Duration;

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
                .readTimeout(Duration.ofSeconds(10))).build();
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return LocalStack.s3AsyncClient();
    }
}
