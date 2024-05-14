//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.MinIO;
import io.deephaven.extensions.s3.testlib.SingletonContainers;
import io.deephaven.stats.util.OSUtil;
import org.junit.BeforeClass;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import static org.junit.Assert.assertFalse;

public class ParquetS3SimpleMinIOTest extends ParquetS3SimpleTestBase {

    @BeforeClass
    public static void initContainer() {
        // TODO(deephaven-core#5116): MinIO testcontainers does not work on OS X
        assertFalse("OSUtil.runningMacOS()", OSUtil.runningMacOS());
        // ensure container is started so container startup time isn't associated with a specific test
        MinIO.init();
    }

    @Override
    public Builder s3Instructions(final Builder builder) {
        return MinIO.s3Instructions(builder);
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return SingletonContainers.MinIO.s3AsyncClient();
    }
}
