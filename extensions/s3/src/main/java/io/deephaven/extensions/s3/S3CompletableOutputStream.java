//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.util.channel.CompletableOutputStream;
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
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.deephaven.extensions.s3.S3ChannelContext.handleS3Exception;
import static io.deephaven.extensions.s3.S3Instructions.MIN_WRITE_PART_SIZE;

class S3CompletableOutputStream extends CompletableOutputStream {

    /**
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html">Amazon S3 User Guide</a>
     */
    private static final int MIN_PART_NUMBER = 1;
    private static final int MAX_PART_NUMBER = 10000;
    private static final int INVALID_PART_NUMBER = -1;

    private enum State {
        OPEN, DONE, COMPLETED, ABORTED
    }

    private final S3Uri uri;
    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    private final int writePartSize;
    private final int numConcurrentWriteParts;

    private final List<CompletedPart> completedParts;
    private final List<OutgoingRequest> pendingRequests;

    private int nextPartNumber;
    private String uploadId; // Initialized on first write, changed back to null when multipart upload completed/aborted
    private State state;

    S3CompletableOutputStream(
            @NotNull final URI uri,
            @NotNull final S3AsyncClient s3AsyncClient,
            @NotNull final S3Instructions s3Instructions) {
        this.uri = s3AsyncClient.utilities().parseUri(uri);
        this.s3AsyncClient = s3AsyncClient;
        this.s3Instructions = s3Instructions;

        this.writePartSize = s3Instructions.writePartSize();
        this.numConcurrentWriteParts = s3Instructions.numConcurrentWriteParts();
        this.pendingRequests = new ArrayList<>(numConcurrentWriteParts);

        this.nextPartNumber = MIN_PART_NUMBER;
        this.completedParts = new ArrayList<>();
        this.state = State.OPEN;
    }

    @Override
    public void write(final int b) throws IOException {
        write((dest, destOff, destCount) -> {
            dest.put((byte) b);
            return 1;
        }, 0, 1);
    }

    @Override
    public void write(final byte @NotNull [] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte @NotNull [] b, final int off, final int len) throws IOException {
        write((dest, currentOffset, remainingLength) -> {
            final int lengthToWrite = Math.min(remainingLength, dest.remaining());
            dest.put(b, currentOffset, lengthToWrite);
            return lengthToWrite;
        }, off, len);
    }

    @FunctionalInterface
    private interface DataWriter {
        /**
         * Writes source data from a single {@code outputStream.write} call to the given destination buffer, starting
         * from the current offset in the source data.
         *
         * @param dest the destination buffer to write data to
         * @param currentOffset the current offset in the source data
         * @param remainingLength the remaining number of bytes of source data to write
         * @return the number of bytes written to the destination buffer
         *
         * @throws IOException if an I/O error occurs during the write operation
         */
        int write(ByteBuffer dest, int currentOffset, int remainingLength) throws IOException;
    }

    /**
     * Writes source data from a single {@code outputStream.write} call to S3 using the provided {@link DataWriter}.
     *
     * @param writer the {@link DataWriter} used to write data to the destination buffer
     * @param off the offset in the source data from which to start writing
     * @param len the length of the data to be written
     *
     * @throws IOException if an I/O error occurs during the write operation or if the stream is not {@link State#OPEN}
     */
    private void write(@NotNull final DataWriter writer, int off, int len) throws IOException {
        if (state != State.OPEN) {
            throw new IOException("Cannot write to stream for uri " + uri + " because stream in state " + state +
                    " instead of OPEN");
        }
        while (len != 0) {
            if (uploadId == null) {
                // Initialize the upload ID for the multipart upload
                uploadId = initiateMultipartUpload();
            }

            // We use request slots in a circular queue fashion
            final int nextSlotId = (nextPartNumber - 1) % numConcurrentWriteParts;
            final OutgoingRequest useRequest;
            if (pendingRequests.size() == nextSlotId) {
                pendingRequests.add(useRequest = new OutgoingRequest(writePartSize));
            } else if (pendingRequests.size() < nextSlotId) {
                throw new IllegalStateException("Unexpected slot ID " + nextSlotId + " for uri " + uri + " with " +
                        pendingRequests.size() + " pending requests.");
            } else {
                useRequest = pendingRequests.get(nextSlotId);
                // Wait for the oldest upload to complete if no space is available
                if (useRequest.future != null) {
                    waitForCompletion(useRequest);
                }
            }

            // Write as much as possible to this buffer
            final ByteBuffer buffer = useRequest.buffer;
            final int lengthWritten = writer.write(buffer, off, len);
            if (!buffer.hasRemaining()) {
                sendPartRequest(useRequest);
            }
            off += lengthWritten;
            len -= lengthWritten;
        }
    }

    @Override
    public void flush() throws IOException {
        // Flush the next part if it is larger than the minimum part size
        flushImpl(false);
    }

    @Override
    public void done() throws IOException {
        if (state == State.DONE) {
            return;
        }
        if (state != State.OPEN) {
            throw new IOException("Cannot mark stream as done for uri " + uri + " because stream in state " + state +
                    " instead of OPEN");
        }
        flushImpl(true);
        state = State.DONE;
    }

    @Override
    public void complete() throws IOException {
        if (state == State.COMPLETED) {
            return;
        }
        done();
        completeMultipartUpload();
        state = State.COMPLETED;
    }

    @Override
    public void rollback() throws IOException {
        if (state == State.COMPLETED || state == State.ABORTED) {
            // Cannot roll back a completed or aborted multipart upload
            return;
        }
        abortMultipartUpload();
        state = State.ABORTED;
    }

    @Override
    public void close() throws IOException {
        if (state == State.COMPLETED || state == State.ABORTED) {
            return;
        }
        abortMultipartUpload();
        state = State.ABORTED;
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

        OutgoingRequest(final int writePartSize) {
            // TODO(deephaven-core#5935): Experiment with buffer pool here
            buffer = ByteBuffer.allocate(writePartSize);
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

    /**
     * Flushes the current buffer to S3.
     *
     * @param force if true, forces the buffer to be flushed even if it is smaller than the minimum
     *        {@value S3Instructions#MIN_WRITE_PART_SIZE} MiB threshold, which should only be done for the very last
     *        part.
     * @throws IOException if an I/O error occurs during the flush operation
     */
    private void flushImpl(final boolean force) throws IOException {
        final int nextSlotId = (nextPartNumber - 1) % numConcurrentWriteParts;
        if (pendingRequests.size() == nextSlotId) {
            // Nothing to flush
            return;
        }
        final OutgoingRequest request = pendingRequests.get(nextSlotId);
        if (request.buffer.position() != 0
                && request.future == null
                && (force || request.buffer.position() >= MIN_WRITE_PART_SIZE)) {
            sendPartRequest(request);
        }
    }

    private void completeMultipartUpload() throws IOException {
        if (uploadId == null) {
            throw new IllegalStateException("Cannot complete multipart upload for uri " + uri + " because upload ID " +
                    "is null");
        }
        // Complete all pending requests in the exact order they were sent
        final int partCount = nextPartNumber - 1;
        for (int partNumber = completedParts.size() + 1; partNumber <= partCount; partNumber++) {
            // Part numbers start from 1, therefore, we use (partNumber - 1) for the slot ID
            final int slotId = (partNumber - 1) % numConcurrentWriteParts;
            final OutgoingRequest request = pendingRequests.get(slotId);
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
        uploadId = null;
    }

    /**
     * Abort the multipart upload if it is in progress.
     */
    private void abortMultipartUpload() throws IOException {
        if (uploadId == null) {
            // We didn't start the upload, so nothing to abort
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
}
