//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.RustFS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Tag("testcontainers")
public class S3SeekableChannelSimpleRustFSTest extends S3SeekableChannelSimpleTestBase {

    @BeforeAll
    static void initContainer() {
        // ensure container is started so container startup time isn't associated with a specific test
        RustFS.init();
    }

    @Override
    public Builder s3Instructions(Builder builder) {
        return RustFS.s3Instructions(builder);
    }

    @Override
    public S3AsyncClient s3AsyncClient() {
        return RustFS.s3AsyncClient();
    }
}
