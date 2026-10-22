//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.RustFS;
import org.junit.BeforeClass;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3ParquetRustFSTest extends S3ParquetTestBase {

    @BeforeClass
    public static void initContainer() {
        // ensure container is started so container startup time isn't associated with a specific test
        RustFS.init();
    }

    @Override
    public Builder s3Instructions(Builder builder) {
        return RustFS.s3Instructions(builder);
    }

    @Override
    public String s3Endpoint() {
        return RustFS.s3Endpoint();
    }

    @Override
    public String region() {
        return RustFS.region();
    }

    @Override
    public String accessKey() {
        return RustFS.accessKey();
    }

    @Override
    public String secretAccessKey() {
        return RustFS.secretAccessKey();
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return RustFS.s3AsyncClient();
    }
}
