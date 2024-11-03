//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.LocalStack;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Map;

import static org.apache.iceberg.aws.AwsClientProperties.CLIENT_REGION;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ACCESS_KEY_ID;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ENDPOINT;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.SECRET_ACCESS_KEY;

@Tag("testcontainers")
@Deprecated
public class IcebergLocalStackTest extends IcebergToolsTest {

    @BeforeAll
    public static void initContainer() {
        // ensure container is started so container startup time isn't associated with a specific test
        LocalStack.init();
    }

    @Override
    public Builder s3Instructions(final Builder builder) {
        return LocalStack.s3Instructions(builder);
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return LocalStack.s3AsyncClient();
    }

    @Override
    public Map<String, String> properties() {
        return Map.of(
                ENDPOINT, LocalStack.s3Endpoint(),
                CLIENT_REGION, LocalStack.region(),
                ACCESS_KEY_ID, LocalStack.accessKey(),
                SECRET_ACCESS_KEY, LocalStack.secretAccessKey());
    }
}
