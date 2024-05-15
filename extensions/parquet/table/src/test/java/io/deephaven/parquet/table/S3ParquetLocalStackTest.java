//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.LocalStack;
import io.deephaven.extensions.s3.testlib.SingletonContainers;
import org.junit.BeforeClass;
import software.amazon.awssdk.services.s3.S3AsyncClient;

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
    public S3AsyncClient s3AsyncClient() {
        return SingletonContainers.LocalStack.s3AsyncClient();
    }
}
