//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.MinIO;
import io.deephaven.stats.util.OSUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.BeforeClass;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Map;

import static org.apache.iceberg.aws.AwsClientProperties.CLIENT_REGION;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ACCESS_KEY_ID;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ENDPOINT;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.SECRET_ACCESS_KEY;

public class IcebergMinIOTest extends IcebergToolsTest {

    @BeforeClass
    public static void initContainer() {
        // TODO(deephaven-core#5116): MinIO testcontainers does not work on OS X
        Assumptions.assumeFalse(OSUtil.runningMacOS(), "OSUtil.runningMacOS()");
        // ensure container is started so container startup time isn't associated with a specific test
        MinIO.init();
    }

    @Override
    public Builder s3Instructions(final Builder builder) {
        return MinIO.s3Instructions(builder);
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return MinIO.s3AsyncClient();
    }

    @Override
    public Map<String, String> s3Properties() {
        return Map.of(
                ENDPOINT, MinIO.s3Endpoint(),
                CLIENT_REGION, MinIO.region(),
                ACCESS_KEY_ID, MinIO.accessKey(),
                SECRET_ACCESS_KEY, MinIO.secretAccessKey());
    }

}
