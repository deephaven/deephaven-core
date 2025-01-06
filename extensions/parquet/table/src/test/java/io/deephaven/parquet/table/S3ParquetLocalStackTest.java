//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.LocalStack;
import org.junit.Assume;
import org.junit.BeforeClass;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;

public class S3ParquetLocalStackTest extends S3ParquetTestBase {

    @BeforeClass
    public static void initContainer() {
        // ensure container is started so container startup time isn't associated with a specific test
        LocalStack.init();
    }

    @Override
    public Builder s3Instructions(Builder builder) {
        return LocalStack.s3Instructions(builder);
    }

    @Override
    public String s3Endpoint() {
        return LocalStack.s3Endpoint();
    }

    @Override
    public String region() {
        return LocalStack.region();
    }

    @Override
    public String accessKey() {
        return LocalStack.accessKey();
    }

    @Override
    public String secretAccessKey() {
        return LocalStack.secretAccessKey();
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return LocalStack.s3AsyncClient();
    }

    // Ignore annotation does not seem to work with Override
    // @Ignore
    @Override
    public void testInvalidKeysFromProfile() throws IOException {
        Assume.assumeTrue(
                "localstack 3.7 behavior changed, an 'invalid' key does not seem to cause an error anymore (potentially silently fails?)",
                false);
        super.testInvalidKeysFromProfile();
    }
}
