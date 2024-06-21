//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;


import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers;
import io.deephaven.stats.util.OSUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Tag("testcontainers")
public class IcebergMinIOTest extends IcebergToolsTest {

    @BeforeAll
    static void initContainer() {
        // TODO(deephaven-core#5116): MinIO testcontainers does not work on OS X
        Assumptions.assumeFalse(OSUtil.runningMacOS(), "OSUtil.runningMacOS()");
        // ensure container is started so container startup time isn't associated with a specific test
        SingletonContainers.MinIO.init();
    }

    @Override
    public Builder s3Instructions(Builder builder) {
        return SingletonContainers.MinIO.s3Instructions(builder);
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return SingletonContainers.MinIO.s3AsyncClient();
    }
}
