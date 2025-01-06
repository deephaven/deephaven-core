//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.testlib.SingletonContainers.MinIO;
import io.deephaven.base.OSUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Tag("testcontainers")
public class S3SeekableChannelSimpleMinIOTest extends S3SeekableChannelSimpleTestBase {

    @BeforeAll
    static void initContainer() {
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
}
