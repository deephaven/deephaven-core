//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.pool.Pool;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.hash.KeyedIntObjectKey;
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
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

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
    private final List<CompletedPart> completedParts;

    /**
     * The pool of write request objects used to buffer data for each part to be uploaded. This is set by the context
     * before initiating a write operation and does not change during the lifetime of the stream.
     * <p>
     * This pool needs to be thread safe.
     */
    @Nullable
    private Pool<S3WriteRequest> requestPool;

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
     * A map from part number to pending requests that have been sent to S3 but have not yet completed.
     */
    private final KeyedIntObjectHashMap<S3WriteRequest> pendingRequests =
            new KeyedIntObjectHashMap<>(new KeyedIntObjectKey.BasicStrict<>() {
                @Override
                public int getIntKey(@NotNull final S3WriteRequest s3WriteRequest) {
                    return s3WriteRequest.partNumber;
                }
            });

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

    S3CompletableOutputStream(
            @NotNull final URI uri,
            @NotNull final S3AsyncClient s3AsyncClient,
            @NotNull final S3Instructions s3Instructions,
            @NotNull final SeekableChannelContext channelContext) {
        this.uri = s3AsyncClient.utilities().parseUri(uri);
        this.s3AsyncClient = s3AsyncClient;
        this.s3Instructions = s3Instructions;

        this.nextPartNumber = MIN_PART_NUMBER;
        this.completedParts = Collections.synchronizedList(new ArrayList<>());

        if (!(channelContext instanceof S3WriteContext)) {
            throw new IllegalArgumentException("Unsupported channel context " + channelContext);
        }
        this.requestPool = ((S3WriteContext) channelContext).requestPool;

        this.bufferedPartRequest = null;
        this.uploadId = null;

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
        if (requestPool == null) {
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
                useRequest = requestPool.take();
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
        request.partNumber = nextPartNumber;
        request.future =
                s3AsyncClient.uploadPart(uploadPartRequest, AsyncRequestBody.fromByteBufferUnsafe(request.buffer))
                        .whenComplete((uploadPartResponse, throwable) -> {
                            if (throwable == null) {
                                // Success
                                completePartUpload(request, uploadPartResponse);
                            }
                            // Failure will be handled when waiting for completion
                        });
        pendingRequests.add(request);
        nextPartNumber++;
    }

    private void waitForCompletion(final S3WriteRequest request) throws IOException {
        try {
            // We already processed the response in the whenComplete callback
            request.future.get();
        } catch (final InterruptedException | ExecutionException e) {
            throw handleS3Exception(e, String.format("waiting for part %d for uri %s to complete uploading",
                    request.partNumber, uri), s3Instructions);
        }
    }

    /**
     * Complete a pending upload part request by converting the upload response into a completed part, removing the
     * request from pending requests, and returning the request back to the pool.
     */
    private void completePartUpload(final S3WriteRequest request, final UploadPartResponse response) {
        completedParts.add(SdkPojoConversionUtils.toCompletedPart(response, request.partNumber));
        pendingRequests.remove(request.partNumber);
        requestPool.give(request);
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

        for (final S3WriteRequest request : pendingRequests.values()) {
            waitForCompletion(request);
        }

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
