//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.channel.CompletableOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.internal.multipart.SdkPojoConversionUtils;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static io.deephaven.extensions.s3.S3ReadContext.handleS3Exception;
import static io.deephaven.extensions.s3.S3Utils.addTimeout;

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
     * completing or aborting the upload.
     */
    private final Semaphore numPartsCompleted;

    /**
     * A list of all parts that have been successfully uploaded. This list is used to complete the multipart upload.
     */
    private final List<CompletedPart> completedParts;

    /**
     * The context object used to manage the pool of buffers for write requests.
     */
    private final S3WriteContext writeContext;

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
     * Represents the handoff of responsibility for returning the buffer for the bufferedPartRequest. If completed
     * exceptionally, then the buffer will be returned as part of that exception handling. If completed successfully,
     * the buffer will be released downstream of the successful completion.
     */
    @Nullable
    private CompletableFuture<Void> handoff;

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
            @NotNull final SafeCloseable channelContext) {
        this.uri = s3AsyncClient.utilities().parseUri(uri);
        this.s3AsyncClient = s3AsyncClient;
        this.s3Instructions = s3Instructions;

        this.nextPartNumber = MIN_PART_NUMBER;
        this.numPartsCompleted = new Semaphore(0);
        this.completedParts = Collections.synchronizedList(new ArrayList<>());

        if (!(channelContext instanceof S3WriteContext)) {
            throw new IllegalArgumentException("Unsupported channel context " + channelContext);
        }
        this.writeContext = (S3WriteContext) channelContext;
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
        if (state != State.OPEN) {
            throw new IOException("Cannot write to stream for uri " + uri + " because stream is in state " + state +
                    " instead of OPEN");
        }
        while (len != 0) {
            if (uploadId == null) {
                // Initialize the upload ID for the multipart upload
                uploadId = initiateMultipartUpload();
            }
            final int lengthWritten = writeImpl(writer, off, len);
            off += lengthWritten;
            len -= lengthWritten;
        }
    }

    private int writeImpl(final DataWriter writer, int off, int len) throws IOException {
        if (bufferedPartRequest == null) {
            final S3WriteRequest request;
            try {
                request = new S3WriteRequest(writeContext, s3Instructions.writePartSize());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Thread interrupted while creating a write request for part " +
                        nextPartNumber, e);
            }
            handoff = new CompletableFuture<>();
            // If the status has an exception, cancel the handoff
            forwardExceptionAsCancel(status, handoff);
            // If we can't complete the handoff, release the buffer
            handoff.whenComplete((unused, throwable) -> {
                if (throwable != null) {
                    request.releaseBuffer();
                }
            });
            bufferedPartRequest = request;
        }
        final int lengthWritten = writer.write(bufferedPartRequest.buffer, off, len);
        if (!bufferedPartRequest.buffer.hasRemaining()) {
            sendPartRequest(false);
            bufferedPartRequest = null;
            handoff = null;
        }
        return lengthWritten;
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
        sendLastRequestIfPresent();
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
        final CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow());
        final CreateMultipartUploadRequest createMultipartUploadRequest = applyOverrideConfiguration(builder).build();
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
        } catch (final InterruptedException | ExecutionException | CancellationException e) {
            initiateUploadFuture.cancel(true);
            throw handleS3Exception(e, String.format("initiating multipart upload for uri %s", uri), s3Instructions);
        }
        return response.uploadId();
    }

    /**
     * Send a part request for the given buffer. This method assumes that the buffer is non-empty.
     */
    private void sendPartRequest(boolean onDone) throws IOException {
        if (nextPartNumber > MAX_PART_NUMBER) {
            final IOException ex = new IOException("Cannot upload more than " + MAX_PART_NUMBER +
                    " parts for uri " + uri + ", please try again with a larger part size");
            failAll(ex);
            throw ex;
        }
        if (!Objects.requireNonNull(handoff).complete(null)) {
            // handoff only completes exceptionally if status had an error
            throw statusError();
        }
        // After handoff, we are responsible for releasing buffer
        final S3WriteRequest request = Objects.requireNonNull(bufferedPartRequest);
        request.buffer.flip();
        if (onDone && !request.buffer.hasRemaining()) {
            // The only potential for an empty buffer is when onDone; we still need to release it.
            request.releaseBuffer();
            return;
        }

        final UploadPartRequest.Builder builder = UploadPartRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId)
                .partNumber(nextPartNumber);
        final UploadPartRequest uploadPartRequest = applyOverrideConfiguration(builder).build();

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
        forwardExceptionAsCancel(status, uploadPartFuture);
        nextPartNumber++;
    }

    private static void forwardExceptionAsCancel(CompletableFuture<?> src, CompletableFuture<?> dst) {
        src.whenComplete((ignored, ex) -> {
            if (ex != null) {
                dst.cancel(true);
            }
        });
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
            throw statusError();
        }
    }

    private IOException statusError() {
        // join() will throw a CompletionException with the cause as the original exception
        try {
            status.join();
        } catch (final CompletionException | CancellationException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                return (IOException) cause;
            } else {
                return new IOException("Failed to upload to S3, check cause for more details", cause);
            }
        }
        throw new IllegalStateException();
    }

    /**
     * Send all buffered data to S3. This should only be done for the last part of the upload because we want to enforce
     * that all parts except the last one are of equal size, given by {@link S3Instructions#writePartSize()}
     */
    private void sendLastRequestIfPresent() throws IOException {
        if (bufferedPartRequest == null) {
            // Nothing to send
            return;
        }
        sendPartRequest(true);
        bufferedPartRequest = null;
        handoff = null;
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

        final CompleteMultipartUploadRequest.Builder builder = CompleteMultipartUploadRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(completedParts)
                        .build());
        final CompleteMultipartUploadRequest completeRequest = applyOverrideConfiguration(builder).build();
        final CompletableFuture<CompleteMultipartUploadResponse> uploadFuture =
                s3AsyncClient.completeMultipartUpload(completeRequest);
        try {
            uploadFuture.get();
        } catch (final InterruptedException | ExecutionException | CancellationException e) {
            uploadFuture.cancel(true);
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
     * Abort the multipart upload if it is in progress. Note that aborting an upload is a best effort operation, since
     * according to the <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html">SDK</a>,
     * any in flight requests might still succeed. So its recommended to set expiration rules on buckets for incomplete
     * uploads.
     *
     *
     * @see <a href=
     *      "https://aws.amazon.com/blogs/aws-cloud-financial-management/discovering-and-deleting-incomplete-multipart-uploads-to-lower-amazon-s3-costs/">
     *      Discovering and Deleting Incomplete Multipart Uploads to Lower Amazon S3 Costs</a>
     */
    private void abortMultipartUpload() throws IOException {
        if (uploadId == null) {
            // We didn't start the upload, so nothing to abort
            return;
        }

        // Cancel all pending requests
        failAll(new IOException("Upload aborted for uri " + uri));

        // Wait for all parts to finish
        final int totalPartCount = nextPartNumber - 1;
        try {
            numPartsCompleted.acquire(totalPartCount);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            final IOException ioe = new IOException("Failed to abort the upload since interrupted while waiting " +
                    "for all parts to finish", e);
            throw ioe;
        }

        // Initiate the abort request
        final AbortMultipartUploadRequest.Builder builder = AbortMultipartUploadRequest.builder()
                .bucket(uri.bucket().orElseThrow())
                .key(uri.key().orElseThrow())
                .uploadId(uploadId);
        final AbortMultipartUploadRequest abortRequest = applyOverrideConfiguration(builder).build();
        final CompletableFuture<AbortMultipartUploadResponse> future = s3AsyncClient.abortMultipartUpload(abortRequest);

        // Wait for the abort to complete
        try {
            future.get();
        } catch (final InterruptedException | ExecutionException | CancellationException e) {
            future.cancel(true);
            throw handleS3Exception(e,
                    String.format("aborting multipart upload for uri %s", uri), s3Instructions);
        }
    }

    /**
     * Applies the write timeout, then generates a request for the given builder and request class.
     *
     * @param builder an instance of a {@link S3Request.Builder} class
     * @return the builder
     */
    private <B extends AwsRequest.Builder> B applyOverrideConfiguration(@NotNull B builder) {
        final Duration writeTimeout = s3Instructions.writeTimeout();
        builder.overrideConfiguration(b -> addTimeout(b, writeTimeout));
        return builder;
    }
}
