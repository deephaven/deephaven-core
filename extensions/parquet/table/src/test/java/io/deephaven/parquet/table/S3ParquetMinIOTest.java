//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.MinIO;
import io.deephaven.stats.util.OSUtil;
import org.junit.Assume;
import org.junit.BeforeClass;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3ParquetMinIOTest extends S3ParquetTestBase {

    @BeforeClass
    public static void initContainer() {
        // TODO(deephaven-core#5116): MinIO testcontainers does not work on OS X
        Assume.assumeFalse("OSUtil.runningMacOS()", OSUtil.runningMacOS());
        // ensure container is started so container startup time isn't associated with a specific test
        MinIO.init();
    }

    @Override
    public Builder s3Instructions(final Builder builder) {
        return MinIO.s3Instructions(builder);
    }

    @Override
    public String s3Endpoint() {
        return MinIO.s3Endpoint();
    }

    @Override
    public String region() {
        return MinIO.region();
    }

    @Override
    public String accessKey() {
        return MinIO.accessKey();
    }

    @Override
    public String secretAccessKey() {
        return MinIO.secretAccessKey();
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return MinIO.s3AsyncClient();
    }
}
