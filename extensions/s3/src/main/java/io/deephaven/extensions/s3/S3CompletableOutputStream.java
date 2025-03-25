//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.pool.Pool;
import io.deephaven.util.channel.CompletableOutputStream;
import io.deephaven.util.channel.SeekableChannelContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
import software.amazon.awssdk.utils.CompletableFutureUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.deephaven.extensions.s3.S3ReadContext.handleS3Exception;
import static io.deephaven.extensions.s3.S3Instructions.MIN_WRITE_PART_SIZE;

class S3CompletableOutputStream extends CompletableOutputStream {

    /**
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html">Amazon S3 User Guide</a>
     */
    private static final int MIN_PART_NUMBER = 1;
    private static final int MAX_PART_NUMBER = 10000;

    private final S3Uri uri;
    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    private int nextPartNumber;

    /**
     * A list of all write requests that have been initiated. This list is used to wait for all parts to complete before
     * completing the multipart upload.
     */
    private final List<S3WriteRequest> writeRequests;

    /**
     * A list of all parts that have been successfully uploaded. This list is used to complete the multipart upload.
     */
    private final List<CompletedPart> completedParts;

    /**
     * The pool of byte buffers used by the write requests. This field is set by the {@link S3WriteContext} before
     * initiating a write operation and does not change during the lifetime of the stream.
     * <p>
     * This pool needs to be thread safe.
     */
    @Nullable
    private Pool<ByteBuffer> bufferPool;

    /**
     * The request containing data buffered for the next part to be uploaded. S3 requires that all parts, except the
     * last one, must be at least {@value S3Instructions#MIN_WRITE_PART_SIZE} in size. Therefore, we buffer the data
     * until we have enough to send a part request, or until the stream is closed.
     * <p>
     * This object can be {@code null} if there is no pending data (e.g., at the beginning of a stream) or if the data
     * has already been uploaded in a previous part.
     */
    @Nullable
    private S3WriteRequest bufferedPartRequest;


    /**
     * The upload ID for the multipart upload. This is initialized on the first write operation and is set to
     * {@code null} when the multipart upload is completed or aborted.
     */
    @Nullable
    private String uploadId;

    private enum State {
        OPEN, DONE, COMPLETED, ABORTED
    }

    /**
     * The current state of the stream.
     */
    private State state;

    /**
     * An {@code IOException} that represents a fatal upload failure in write request. If non-null, this indicates that
     * at least one part upload has failed. Any subsequent operations on the stream should fail immediately.
     */
    private volatile IOException uploadFailure;

    S3CompletableOutputStream(
            @NotNull final URI uri,
            @NotNull final S3AsyncClient s3AsyncClient,
            @NotNull final S3Instructions s3Instructions,
            @NotNull final SeekableChannelContext channelContext) {
        this.uri = s3AsyncClient.utilities().parseUri(uri);
        this.s3AsyncClient = s3AsyncClient;
        this.s3Instructions = s3Instructions;

        this.nextPartNumber = MIN_PART_NUMBER;

        this.writeRequests = Collections.synchronizedList(new ArrayList<>());
        this.completedParts = Collections.synchronizedList(new ArrayList<>());

        if (!(channelContext instanceof S3WriteContext)) {
            throw new IllegalArgumentException("Unsupported channel context " + channelContext);
        }
        this.bufferPool = ((S3WriteContext) channelContext).bufferPool;

        this.bufferedPartRequest = null;
        this.uploadId = null;

        this.state = State.OPEN;
    }

    @Override
    public void write(final int b) throws IOException {
        checkForFailure();
        write((dest, destOff, destCount) -> {
            dest.put((byte) b);
            return 1;
        }, 0, 1);
    }

    @Override
    public void write(final byte @NotNull [] b) throws IOException {
        checkForFailure();
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte @NotNull [] b, final int off, final int len) throws IOException {
        checkForFailure();
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
        if (bufferPool == null) {
            throw new IllegalStateException("Cannot write to stream for uri " + uri + " because request pool is not " +
                    "initialized using a valid context");
        }
        if (state != State.OPEN) {
            throw new IOException("Cannot write to stream for uri " + uri + " because stream in state " + state +
                    " instead of OPEN");
        }
        while (len != 0) {
            if (uploadId == null) {
                // Initialize the upload ID for the multipart upload
                uploadId = initiateMultipartUpload();
            }

            final S3WriteRequest useRequest;
            if (bufferedPartRequest == null) {
                // Get a new request from the pool
                useRequest = new S3WriteRequest(bufferPool);
            } else {
                // Re-use the pending request
                useRequest = bufferedPartRequest;
                bufferedPartRequest = null;
            }

            // Write as much as possible to this buffer
            final ByteBuffer buffer = useRequest.buffer;
            final int lengthWritten = writer.write(buffer, off, len);
            if (!buffer.hasRemaining()) {
                sendPartRequest(useRequest);
            } else {
                // Save the request for the next write
                bufferedPartRequest = useRequest;
            }
            off += lengthWritten;
            len -= lengthWritten;
        }
    }

    @Override
    public void flush() throws IOException {
        checkForFailure();
        // Flush the next part if it is larger than the minimum part size
        flushImpl(false);
    }

    @Override
    public void done() throws IOException {
        checkForFailure();
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
        checkForFailure();
        if (state == State.COMPLETED) {
            return;
        }
        done();
        completeMultipartUpload();
        state = State.COMPLETED;
    }

    @Override
    public void rollback() throws IOException {
        checkForFailure();
        if (state == State.COMPLETED || state == State.ABORTED) {
            // Cannot roll back a completed or aborted multipart upload
            return;
        }
        abortMultipartUpload();
        state = State.ABORTED;
    }

    @Override
    public void close() throws IOException {
        checkForFailure();
        if (state == State.COMPLETED || state == State.ABORTED) {
            return;
        }
        abortMultipartUpload();
        state = State.ABORTED;
    }

    ////////// Helper methods and classes //////////

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
    private void sendPartRequest(final S3WriteRequest request) throws IOException {
        if (nextPartNumber > MAX_PART_NUMBER) {
            throw new IOException("Cannot upload more than " + MAX_PART_NUMBER + " parts for uri " + uri + ", please" +
                    " try again with a larger part size");
        }
        if (request.sdkUploadFuture != null) {
            throw new IllegalStateException("Request already in progress for uri " + uri + " with part number " +
                    request.partNumber);
        }
        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId)
                .partNumber(nextPartNumber)
                .build();
        request.buffer.flip();
        request.partNumber = nextPartNumber;
        request.sdkUploadFuture =
                s3AsyncClient.uploadPart(uploadPartRequest, AsyncRequestBody.fromByteBufferUnsafe(request.buffer));
        request.completionFuture = request.sdkUploadFuture
                .whenComplete((uploadPartResponse, throwable) -> {
                    try {
                        if (throwable == null) {
                            // Success
                            completedParts.add(
                                    SdkPojoConversionUtils.toCompletedPart(uploadPartResponse, request.partNumber));
                        } else {
                            setUploadFailure(request.partNumber, throwable);
                        }
                    } finally {
                        // We no longer need the buffer
                        request.releaseBuffer();
                    }
                });

        // Propagate the cancellation to the SDK future
        CompletableFutureUtils.forwardExceptionTo(request.completionFuture, request.sdkUploadFuture);

        writeRequests.add(request);
        nextPartNumber++;
    }

    /**
     * Record an upload failure if one is not already set. Only the first failure is recorded to avoid overwriting the
     * initial root cause.
     *
     * @param partNumber the S3 part number that failed
     * @param throwable the exception that was thrown during the upload
     */
    private synchronized void setUploadFailure(int partNumber, Throwable throwable) {
        if (uploadFailure == null) {
            uploadFailure = new IOException("Failed to upload part " + partNumber + " for uri " + uri, throwable);
        }
    }

    /**
     * Check if a failure in upload has been recorded, and if so, throw it.
     * <p>
     * All essential operations (write, flush, complete, etc.) should call this method before proceeding. Once a failure
     * is recorded, the stream is essentially broken, and no further writes should be attempted.
     *
     * @throws IOException if an upload failure has been recorded
     */
    private void checkForFailure() throws IOException {
        if (uploadFailure != null) {
            throw uploadFailure;
        }
    }


    private void waitForCompletion(final S3WriteRequest request) throws IOException {
        try {
            // The response is processed in the whenComplete callback
            request.completionFuture.get();
        } catch (final InterruptedException | ExecutionException e) {
            throw handleS3Exception(e, String.format("waiting for part %d for uri %s to complete uploading",
                    request.partNumber, uri), s3Instructions);
        }
        // If the part failed to upload, we should fail here
        checkForFailure();
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
        if (bufferedPartRequest == null) {
            // Nothing to flush
            return;
        }
        final S3WriteRequest request = bufferedPartRequest;
        if (request.buffer.position() != 0
                && (force || request.buffer.position() >= MIN_WRITE_PART_SIZE)) {
            sendPartRequest(request);
            bufferedPartRequest = null;
        }
    }

    private void completeMultipartUpload() throws IOException {
        if (uploadId == null) {
            throw new IllegalStateException("Cannot complete multipart upload for uri " + uri + " because upload ID " +
                    "is null");
        }

        for (final S3WriteRequest request : writeRequests) {
            waitForCompletion(request);
        }
        writeRequests.clear();

        // Sort the completed parts by part number, as required by S3
        completedParts.sort(Comparator.comparingInt(CompletedPart::partNumber));

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
