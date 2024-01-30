/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;


import io.deephaven.extensions.s3.S3Instructions.Builder;
import io.deephaven.extensions.s3.SingletonContainers.LocalStack;
import org.junit.jupiter.api.Tag;
import software.amazon.awssdk.services.s3.S3Client;

@Tag("testcontainers")
public class S3SeekableChannelLocalStackTest extends S3SeekableChannelTestBase {

    @Override
    public Builder s3Instructions(Builder builder) {
        return LocalStack.s3Instructions(builder);
    }

    @Override
    public S3Client s3Client() {
        return LocalStack.s3Client();
    }
}
