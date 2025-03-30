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
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import static io.deephaven.extensions.s3.S3ReadContext.handleS3Exception;

class S3CompletableOutputStream extends CompletableOutputStream {

    /**
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html">Amazon S3 User Guide</a>
     */
    private static final int MIN_PART_NUMBER = 1;
    private static final int MAX_PART_NUMBER = 10000;

    private final S3Uri uri;
    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;

    /**
     * The part number to be used for the next part upload. This is initialized to {@link #MIN_PART_NUMBER} and
     * incremented for each part The total number of parts uploaded can be calculated as {@code nextPartNumber - 1} and
     * cannot exceed {@link #MAX_PART_NUMBER}.
     */
    private int nextPartNumber;

    /**
     * Track how many parts have completed (success or failure). This is used to wait for all parts to finish before
     * completing the upload.
     */
    private final Semaphore numPartsCompleted;

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
    private final Pool<ByteBuffer> bufferPool;

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
     * A {@link CompletableFuture} that represents the overall health of this S3 upload stream. This future completes
     * normally (with {@code null}) when the entire upload succeeds, and completes exceptionally if any part fails or if
     * the operation is aborted due to an error. Once it fails, all outstanding requests will be cancelled, and
     * subsequent calls to methods like {@code write()} or {@code flush()} will throw an exception immediately.
     */
    private final CompletableFuture<Void> status;

    S3CompletableOutputStream(
            @NotNull final URI uri,
            @NotNull final S3AsyncClient s3AsyncClient,
            @NotNull final S3Instructions s3Instructions,
            @NotNull final SeekableChannelContext channelContext) {
        this.uri = s3AsyncClient.utilities().parseUri(uri);
        this.s3AsyncClient = s3AsyncClient;
        this.s3Instructions = s3Instructions;

        this.nextPartNumber = MIN_PART_NUMBER;
        this.numPartsCompleted = new Semaphore(0);
        this.completedParts = Collections.synchronizedList(new ArrayList<>());

        if (!(channelContext instanceof S3WriteContext)) {
            throw new IllegalArgumentException("Unsupported channel context " + channelContext);
        }
        this.bufferPool = ((S3WriteContext) channelContext).bufferPool;
        this.bufferedPartRequest = null;
        this.uploadId = null;

        this.state = State.OPEN;
        this.status = new CompletableFuture<>();
    }

    @Override
    public void write(final int b) throws IOException {
        checkStatus();
        write((dest, destOff, destCount) -> {
            dest.put((byte) b);
            return 1;
        }, 0, 1);
    }

    @Override
    public void write(final byte @NotNull [] b) throws IOException {
        checkStatus();
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte @NotNull [] b, final int off, final int len) throws IOException {
        checkStatus();
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
            throw new IllegalStateException("Cannot write to stream for uri " + uri + " because buffer pool is not " +
                    "initialized");
        }
        if (state != State.OPEN) {
            throw new IOException("Cannot write to stream for uri " + uri + " because stream is in state " + state +
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
                useRequest = new S3WriteRequest(bufferPool, s3Instructions.writePartSize());
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
        checkStatus();
        // Do nothing since we automatically flush parts when they reach the configured part size
    }

    @Override
    public void done() throws IOException {
        checkStatus();
        if (state == State.DONE) {
            return;
        }
        if (state != State.OPEN) {
            throw new IOException("Cannot mark stream as done for uri " + uri + " because stream is in state " + state +
                    " instead of OPEN");
        }
        sendBufferedData();
        state = State.DONE;
    }

    @Override
    public void complete() throws IOException {
        checkStatus();
        if (state == State.COMPLETED) {
            return;
        }
        done();
        completeMultipartUpload();
        state = State.COMPLETED;
    }

    @Override
    public void rollback() throws IOException {
        checkStatus();
        if (state == State.COMPLETED || state == State.ABORTED) {
            // Cannot roll back a completed or aborted multipart upload
            return;
        }
        abortMultipartUpload();
        state = State.ABORTED;
    }

    @Override
    public void close() throws IOException {
        checkStatus();
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
        final CompletableFuture<CreateMultipartUploadResponse> initiateUploadFuture =
                s3AsyncClient.createMultipartUpload(createMultipartUploadRequest);
        initiateUploadFuture.exceptionally(throwable -> {
            failAll(new IOException("Failed to initiate multipart upload for uri " + uri, throwable));
            return null;
        });
        final CreateMultipartUploadResponse response;
        try {
            response = initiateUploadFuture.get();
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
            final IOException ex = new IOException("Cannot upload more than " + MAX_PART_NUMBER +
                    " parts for uri " + uri + ", please try again with a larger part size");
            failAll(ex);
            throw ex;
        }
        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId)
                .partNumber(nextPartNumber)
                .build();
        request.buffer.flip();
        final int partNumber = nextPartNumber;
        final CompletableFuture<UploadPartResponse> uploadPartFuture = s3AsyncClient.uploadPart(uploadPartRequest,
                AsyncRequestBody.fromByteBufferUnsafe(request.buffer));

        // Release the buffer when the upload is complete, regardless of success or failure. So the future will hold a
        // reference to the request instance.
        uploadPartFuture.whenComplete((uploadPartResponse, throwable) -> request.releaseBuffer());

        // Process the upload part response
        uploadPartFuture.whenComplete((uploadPartResponse, throwable) -> {
            try {
                if (throwable == null) {
                    completedParts.add(
                            SdkPojoConversionUtils.toCompletedPart(uploadPartResponse, partNumber));
                } else {
                    failAll(new IOException("Failed to upload part " + partNumber + " for uri " + uri,
                            throwable));
                }
            } finally {
                numPartsCompleted.release();
            }
        });

        // Cancel the upload if the status future fails. So status will hold a reference to the upload future.
        status.exceptionally(t -> {
            uploadPartFuture.cancel(true);
            return null;
        });

        nextPartNumber++;
    }

    /**
     * Fail the entire upload. Any subsequent calls to {@link #checkStatus()} will throw.
     */
    private void failAll(final Throwable throwable) {
        status.completeExceptionally(throwable);
    }

    /**
     * Check if a failure in upload has been recorded, and if so, throw it.
     * <p>
     * All essential operations (write, flush, complete, etc.) should call this method before proceeding. Once a failure
     * is recorded, the stream is essentially broken, and no further writes should be attempted.
     *
     * @throws IOException if an upload failure has been recorded
     */
    private void checkStatus() throws IOException {
        if (status.isCompletedExceptionally()) {
            // join() will throw a CompletionException with the cause as the original exception
            try {
                status.join();
            } catch (final CompletionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                } else {
                    throw new IOException("Failed to upload to S3, check cause for more details", cause);
                }
            }
        }
    }

    /**
     * Send all buffered data to S3. This should only be done for the last part of the upload because we want to enforce
     * that all parts except the last one are of equal size, given by {@link S3Instructions#writePartSize()}
     */
    private void sendBufferedData() throws IOException {
        if (bufferedPartRequest == null) {
            // Nothing to send
            return;
        }
        final S3WriteRequest request = bufferedPartRequest;
        if (request.buffer.position() != 0) {
            sendPartRequest(request);
            bufferedPartRequest = null;
        }
    }

    private void completeMultipartUpload() throws IOException {
        if (uploadId == null) {
            // We didn't start the upload, so nothing to complete
            return;
        }

        final int totalPartCount = nextPartNumber - 1;
        try {
            numPartsCompleted.acquire(totalPartCount);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            final IOException ioe = new IOException("Failed to complete the upload since interrupted while waiting " +
                    "for all parts to finish", e);
            failAll(ioe);
            throw ioe;
        }

        // If any part failed to upload, we should fail here
        checkStatus();

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
            final IOException ex = handleS3Exception(e,
                    String.format("completing multipart upload for uri %s", uri), s3Instructions);
            failAll(ex);
            throw ex;
        }
        uploadId = null;

        // Mark the status as completed successfully if we get here with no errors
        status.complete(null);
    }

    /**
     * Abort the multipart upload if it is in progress.
     */
    private void abortMultipartUpload() throws IOException {
        if (uploadId == null) {
            // We didn't start the upload, so nothing to abort
            return;
        }

        // Initiate the abort request
        final AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId)
                .build();
        final CompletableFuture<AbortMultipartUploadResponse> future = s3AsyncClient.abortMultipartUpload(abortRequest);

        // Cancel all pending requests
        failAll(new IOException("Upload aborted for uri " + uri));

        // Wait for the abort to complete
        try {
            future.get();
        } catch (final InterruptedException | ExecutionException e) {
            throw handleS3Exception(e,
                    String.format("aborting multipart upload for uri %s", uri), s3Instructions);
        }
    }
}
