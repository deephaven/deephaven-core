//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;


import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

class S3WriteRequest {

    private static final int INVALID_PART_NUMBER = -1;

    /**
     * The buffer for this request
     */
    final ByteBuffer buffer;

    /**
     * The part number for the part to be uploaded
     */
    int partNumber;

    /**
     * The future for the part upload
     */
    CompletableFuture<UploadPartResponse> future;

    S3WriteRequest(final int writePartSize) {
        buffer = ByteBuffer.allocate(writePartSize);
        partNumber = INVALID_PART_NUMBER;
    }

    void reset() {
        buffer.clear();
        partNumber = INVALID_PART_NUMBER;
        future = null;
    }
}
