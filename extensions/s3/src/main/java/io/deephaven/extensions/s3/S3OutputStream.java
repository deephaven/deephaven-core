//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.internal.multipart.SdkPojoConversionUtils;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.deephaven.extensions.s3.S3ChannelContext.handleS3Exception;

class S3OutputStream extends OutputStream {

    /**
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html">Amazon S3 User Guide</a>
     */
    private static final int MIN_PART_NUMBER = 1;
    private static final int MAX_PART_NUMBER = 10000;
    private static final int INVALID_PART_NUMBER = -1;

    private final S3Uri uri;
    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    private final int partSize;
    private final int numConcurrentParts; // TODO Better name for this

    private final List<CompletedPart> completedParts;
    private final List<OutgoingRequest> pendingRequests;

    private int nextPartNumber;
    private String uploadId;

    S3OutputStream(
            @NotNull final URI uri,
            @NotNull final S3AsyncClient s3AsyncClient,
            @NotNull final S3Instructions s3Instructions) {
        this.uri = s3AsyncClient.utilities().parseUri(uri);
        this.s3AsyncClient = s3AsyncClient;
        this.s3Instructions = s3Instructions;

        this.partSize = s3Instructions.partSize();
        this.numConcurrentParts = s3Instructions.numConcurrentParts();
        this.pendingRequests = new ArrayList<>(numConcurrentParts);

        this.nextPartNumber = MIN_PART_NUMBER;
        this.completedParts = new ArrayList<>();
    }

    public void write(int b) throws IOException {
        write(new byte[] {(byte) b}, 0, 1);
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public void write(final byte @NotNull [] b, int off, int len) throws IOException {
        while (len != 0) {
            if (uploadId == null) {
                // Initialize the upload ID for the multipart upload
                uploadId = initiateMultipartUpload();
            }

            // We use request slots in a circular queue fashion
            final int nextSlotId = (nextPartNumber - 1) % numConcurrentParts;
            if (pendingRequests.size() == nextSlotId) {
                pendingRequests.add(new OutgoingRequest(partSize));
            } else if (pendingRequests.size() < nextSlotId) {
                throw new IllegalStateException("Unexpected slot ID " + nextSlotId + " for uri " + uri + " with " +
                        pendingRequests.size() + " pending requests.");
            }

            // Wait for the oldest upload to complete if no space is available
            final OutgoingRequest useRequest = pendingRequests.get(nextSlotId);
            if (useRequest.future != null) {
                waitForCompletion(useRequest);
            }

            // Write as much as possible to this buffer
            final ByteBuffer buffer = useRequest.buffer;
            final int remaining = buffer.remaining();
            if (remaining >= len) {
                buffer.put(b, off, len);
                if (!buffer.hasRemaining()) {
                    sendPartRequest(useRequest);
                }
                break; // done
            }
            buffer.put(b, off, remaining);
            sendPartRequest(useRequest);
            off += remaining;
            len -= remaining;
        }
    }

    public void flush() throws IOException {
        final int nextSlotId = (nextPartNumber - 1) % numConcurrentParts;
        if (pendingRequests.size() == nextSlotId) {
            // Nothing to flush
            return;
        }
        final OutgoingRequest request = pendingRequests.get(nextSlotId);
        if (request.buffer.position() != 0 && request.future == null) {
            sendPartRequest(request);
        }
    }

    /**
     * Try to finish the multipart upload and close the stream. Cancel the upload if an error occurs.
     *
     * @throws IOException if an error occurs while closing the stream
     */
    public void close() throws IOException {
        if (uploadId == null) {
            return;
        }
        try {
            flush();
            completeMultipartUpload();
        } catch (final IOException e) {
            abort();
            throw new IOException(String.format("Error closing S3OutputStream for uri %s, aborting upload.", uri), e);
        }
        uploadId = null;
    }

    /**
     * Abort the multipart upload if it is in progress and close the stream.
     */
    void abort() throws IOException {
        if (uploadId == null) {
            return;
        }
        final AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId)
                .build();
        try {
            s3AsyncClient.abortMultipartUpload(abortRequest).get();
        } catch (final InterruptedException | ExecutionException e) {
            throw handleS3Exception(e, String.format("aborting multipart upload for uri %s", uri), s3Instructions);
        }
        uploadId = null;
    }

    ////////// Helper methods and classes //////////

    private static class OutgoingRequest {
        /**
         * The buffer for this request
         */
        private final ByteBuffer buffer;

        /**
         * The part number for the part to be uploaded
         */
        private int partNumber;

        /**
         * The future for the part upload
         */
        private CompletableFuture<UploadPartResponse> future;

        OutgoingRequest(final int partSize) {
            buffer = ByteBuffer.allocate(partSize);
            partNumber = INVALID_PART_NUMBER;
        }
    }

    private String initiateMultipartUpload() throws IOException {
        final CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .build();
        // Note: We can add support for other parameters like tagging, storage class, encryption, permissions, etc. in
        // future
        final CompletableFuture<CreateMultipartUploadResponse> future =
                s3AsyncClient.createMultipartUpload(createMultipartUploadRequest);
        final CreateMultipartUploadResponse response;
        try {
            response = future.get();
        } catch (final InterruptedException | ExecutionException e) {
            throw handleS3Exception(e, String.format("initiating multipart upload for uri %s", uri), s3Instructions);
        }
        return response.uploadId();
    }

    /**
     * Send a part request for the given buffer. This method assumes that the buffer is non-empty.
     */
    private void sendPartRequest(final OutgoingRequest request) throws IOException {
        if (nextPartNumber > MAX_PART_NUMBER) {
            throw new IOException("Cannot upload more than " + MAX_PART_NUMBER + " parts for uri " + uri + ", please" +
                    " try again with a larger part size");
        }
        if (request.future != null) {
            throw new IllegalStateException("Request already in progress for uri " + uri + " with part number " +
                    nextPartNumber);
        }
        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId)
                .partNumber(nextPartNumber)
                .build();
        request.buffer.flip();
        request.future = s3AsyncClient.uploadPart(uploadPartRequest,
                AsyncRequestBody.fromByteBufferUnsafe(request.buffer));
        request.partNumber = nextPartNumber;
        nextPartNumber++;
    }

    private void waitForCompletion(final OutgoingRequest request) throws IOException {
        final UploadPartResponse uploadPartResponse;
        try {
            uploadPartResponse = request.future.get();
        } catch (final InterruptedException | ExecutionException e) {
            throw handleS3Exception(e, String.format("waiting for part %d for uri %s to complete uploading",
                    request.partNumber, uri), s3Instructions);
        }
        completedParts.add(SdkPojoConversionUtils.toCompletedPart(uploadPartResponse, request.partNumber));
        request.buffer.clear();
        request.future = null;
        request.partNumber = INVALID_PART_NUMBER;
    }

    private void completeMultipartUpload() throws IOException {
        // Complete all pending requests in the exact order they were sent
        for (int partNumber = completedParts.size() + 1; partNumber < nextPartNumber; partNumber++) {
            final OutgoingRequest request = pendingRequests.get((partNumber - 1) % numConcurrentParts);
            waitForCompletion(request);
        }
        final CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(completedParts)
                        .build())
                .build();
        try {
            s3AsyncClient.completeMultipartUpload(completeRequest).get();
        } catch (final InterruptedException | ExecutionException e) {
            throw handleS3Exception(e, String.format("completing multipart upload for uri %s", uri), s3Instructions);
        }
    }
}
