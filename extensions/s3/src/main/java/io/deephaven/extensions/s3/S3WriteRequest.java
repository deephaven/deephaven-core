//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.pool.Pool;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

class S3WriteRequest {

    private static final int INVALID_PART_NUMBER = -1;

    private Pool<ByteBuffer> pool;

    /**
     * The buffer for this request
     */
    ByteBuffer buffer;

    /**
     * The part number for the part to be uploaded
     */
    int partNumber;

    /**
     * The future for the part upload, returned by the AWS SDK. This future should be used to cancel the upload, if
     * necessary.
     */
    CompletableFuture<UploadPartResponse> sdkUploadFuture;

    S3WriteRequest(@NotNull final Pool<ByteBuffer> pool, final int writePartSize) {
        this.pool = pool;
        buffer = pool.take();
        if (buffer.position() != 0) {
            throw new IllegalStateException("Buffer from pool has incorrect position, position = " + buffer.position() +
                    ", expected 0");
        }
        if (buffer.limit() != writePartSize) {
            throw new IllegalStateException("Buffer from pool has incorrect limit, limit = " + buffer.limit() +
                    ", expected " + writePartSize);
        }
        partNumber = INVALID_PART_NUMBER;
    }

    void releaseBuffer() {
        pool.give(buffer);
        buffer = null;
    }
}
