/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;


import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.SingletonContainers.MinIO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3Client;

@Tag("testcontainers")
public class S3SeekableChannelMinIOTest extends S3SeekableChannelTestBase {

    @BeforeAll
    static void initContainer() {
        // ensure container is started so container startup time isn't associated with a specific test
        MinIO.init();
    }

    @Override
    public Builder s3Instructions(Builder builder) {
        return MinIO.s3Instructions(builder);
    }

    @Override
    public S3Client s3Client() {
        return MinIO.s3Client();
    }
}
