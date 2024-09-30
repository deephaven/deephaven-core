//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Map;

@Tag("testcontainers")
public class IcebergLocalStackTest extends IcebergToolsTest {

    @BeforeAll
    public static void initContainer() {
        // ensure container is started so container startup time isn't associated with a specific test
        SingletonContainers.LocalStack.init();
    }

    @Override
    public Builder s3Instructions(final Builder builder) {
        return SingletonContainers.LocalStack.s3Instructions(builder);
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return SingletonContainers.LocalStack.s3AsyncClient();
    }

    @Override
    public Map<String, String> s3Properties() {
        return SingletonContainers.LocalStack.s3Properties();
    }
}
