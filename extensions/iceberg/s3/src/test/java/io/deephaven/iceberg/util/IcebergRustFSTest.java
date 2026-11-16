//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.RustFS;
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
public class IcebergRustFSTest extends IcebergToolsTest {

    @BeforeAll
    public static void initContainer() {
        // ensure container is started so container startup time isn't associated with a specific test
        RustFS.init();
    }

    @Override
    public Builder s3Instructions(final Builder builder) {
        return RustFS.s3Instructions(builder);
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return RustFS.s3AsyncClient();
    }

    @Override
    public Map<String, String> properties() {
        return Map.of(
                ENDPOINT, RustFS.s3Endpoint(),
                CLIENT_REGION, RustFS.region(),
                ACCESS_KEY_ID, RustFS.accessKey(),
                SECRET_ACCESS_KEY, RustFS.secretAccessKey());
    }
}
