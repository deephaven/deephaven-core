//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
                .readTimeout(Duration.ofSeconds(10))).build();
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return MinIO.s3AsyncClient();
    }
}
