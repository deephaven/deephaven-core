//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.reference.CleanupReference;
import io.deephaven.base.verify.Require;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.reference.CleanupReferenceProcessor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * A request for a single fragment of an S3 object, which can be used concurrently.
 *
 * @implNote This class extends from a {@link SoftReference < ByteBuffer >} and implements {@link CleanupReference} to
 *           allow for cancelling the request once all references to the buffer have been released. Users should not
 *           access the buffer directly, but instead use the {@link #fill(long, ByteBuffer)} method. Also, users should
 *           hold instances of {@link AcquiredRequest} to keep the requests alive.
 */
final class S3Request extends SoftReference<ByteBuffer>
        implements AsyncResponseTransformer<GetObjectResponse, Boolean>, BiConsumer<Boolean, Throwable>,
        CleanupReference<ByteBuffer> {

    static class AcquiredRequest {
        final S3Request request;
        /**
         * The ownership token keeps the request alive. When the ownership token is GC'd, the request is no longer
         * usable and will be cleaned up.
         */
        final Object ownershipToken;

        AcquiredRequest(final S3Request request, final Object ownershipToken) {
            this.request = request;
            this.ownershipToken = ownershipToken;
        }
    }

    /**
     * A unique identifier for a request, consisting of the URI and fragment index.
     */
    static final class ID {
        private final S3Uri uri;
        private final long fragmentIndex;

        ID(final S3Uri s3Uri, final long fragmentIndex) {
            this.uri = Require.neqNull(s3Uri, "s3Uri");
            this.fragmentIndex = fragmentIndex;
        }

        @Override
        public int hashCode() {
            int result = 31 + Long.hashCode(fragmentIndex);
            result = 31 * result + uri.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final ID other = (ID) obj;
            return fragmentIndex == other.fragmentIndex && uri.equals(other.uri);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(S3Request.class);

    private final S3Uri s3Uri;
    private final ID id;
    private final S3Instructions instructions;
    private final S3AsyncClient client;
    private final long fragmentIndex;
    private final long from;
    private final long to;
    private final Instant createdAt;
    private volatile CompletableFuture<Boolean> consumerFuture;
    private volatile CompletableFuture<Boolean> producerFuture;
    private int fillCount;
    private long fillBytes;
    private final S3RequestCache sharedCache;

    /**
     * Create a new request for the given fragment index using the provided context object.
     *
     * @return A new {@link AcquiredRequest} object containing newly created request and an ownership token. The request
     *         will stay alive as long as the ownership token is held.
     *
     * @implNote This method does not cache the context because contexts are short-lived while a request may be cached.
     */
    @NotNull
    static AcquiredRequest createAndAcquire(final long fragmentIndex, @NotNull final S3ChannelContext context) {
        final long from = fragmentIndex * context.instructions.fragmentSize();
        final long to = Math.min(from + context.instructions.fragmentSize(), context.size) - 1;
        final long requestLength = to - from + 1;
        final ByteBuffer buffer = ByteBuffer.allocate((int) requestLength);
        final S3Request request = new S3Request(fragmentIndex, context, buffer, from, to);
        return new AcquiredRequest(request, buffer);
    }

    private S3Request(final long fragmentIndex, @NotNull final S3ChannelContext context,
            @NotNull final ByteBuffer buffer, final long from, final long to) {
        super(buffer, CleanupReferenceProcessor.getDefault().getReferenceQueue());
        this.fragmentIndex = fragmentIndex;
        this.s3Uri = context.uri;
        this.instructions = context.instructions;
        this.client = context.client;
        this.from = from;
        this.to = to;
        sharedCache = context.sharedCache;
        createdAt = Instant.now();
        id = new ID(s3Uri, fragmentIndex);
        if (log.isDebugEnabled()) {
            log.debug().append("Creating request: ").append(String.format("ctx=%d ",
                    System.identityHashCode(context))).append(requestStr()).endl();
        }
    }

    ID getId() {
        return id;
    }

    /**
     * Try to acquire a reference to this request and ownership token. Returns {@code null} if the token is already
     * released.
     */
    @Nullable
    AcquiredRequest tryAcquire() {
        final Object token = get();
        if (token == null) {
            return null;
        }
        return new AcquiredRequest(this, token);
    }

    /**
     * Send the request to the S3 service. This method is idempotent and can be called multiple times.
     */
    void sendRequest() {
        if (consumerFuture == null) {
            synchronized (this) {
                if (consumerFuture == null) {
                    if (log.isDebugEnabled()) {
                        log.debug().append("Sending: ").append(requestStr()).endl();
                    }
                    final CompletableFuture<Boolean> ret = client.getObject(getObjectRequest(), this);
                    ret.whenComplete(this);
                    consumerFuture = ret;
                }
            }
        }
    }

    boolean isDone() {
        return consumerFuture.isDone();
    }

    /**
     * Fill the provided buffer with data from this request, starting at the given local position. Returns the number of
     * bytes filled. Note that the request must be acquired before calling this method.
     */
    int fill(long localPosition, ByteBuffer dest) throws IOException {
        if (get() == null) {
            throw new IllegalStateException(String.format("Trying to fill data after release, %s", requestStr()));
        }
        final int resultOffset = (int) (localPosition - from);
        final int resultLength = Math.min((int) (to - localPosition + 1), dest.remaining());
        final ByteBuffer fullFragment;
        try {
            fullFragment = getFullFragment().asReadOnlyBuffer();
        } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
            throw S3ChannelContext.handleS3Exception(e, String.format("fetching fragment %s", requestStr()),
                    instructions);
        }
        // fullFragment has limit == capacity. This lets us have safety around math and the ability to simply
        // clear to reset.
        fullFragment.limit(resultOffset + resultLength);
        fullFragment.position(resultOffset);
        try {
            dest.put(fullFragment);
        } finally {
            fullFragment.clear();
        }
        ++fillCount;
        fillBytes += resultLength;
        return resultLength;
    }

    @Override
    public void cleanup() {
        final boolean didCancel = consumerFuture.cancel(true);
        sharedCache.remove(this);
        if (log.isDebugEnabled()) {
            final String cancelType = didCancel ? "fast" : (fillCount == 0 ? "unused" : "normal");
            log.debug()
                    .append("cancel ").append(cancelType)
                    .append(": ")
                    .append(requestStr())
                    .append(" fillCount=").append(fillCount)
                    .append(" fillBytes=").append(fillBytes).endl();
        }
    }

    // --------------------------------------------------------------------------------------------------

    @Override
    public void accept(final Boolean isComplete, final Throwable throwable) {
        if (log.isDebugEnabled()) {
            final Instant completedAt = Instant.now();
            if (Boolean.TRUE.equals(isComplete)) {
                log.debug().append("Send complete: ").append(requestStr()).append(' ')
                        .append(Duration.between(createdAt, completedAt).toString()).endl();
            } else {
                log.debug().append("Send error: ").append(requestStr()).append(' ')
                        .append(Duration.between(createdAt, completedAt).toString()).endl();
            }
        }
    }

    // --------------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> prepare() {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        producerFuture = future;
        return future;
    }

    @Override
    public void onResponse(GetObjectResponse response) {

    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        publisher.subscribe(new Sub());
    }

    @Override
    public void exceptionOccurred(Throwable error) {
        producerFuture.completeExceptionally(error);
    }

    // --------------------------------------------------------------------------------------------------

    private ByteBuffer getFullFragment() throws ExecutionException, InterruptedException, TimeoutException {
        // Giving our own get() a bit of overhead - the clients should already be constructed with appropriate
        // apiCallTimeout.
        final long readNanos = instructions.readTimeout().plusMillis(100).toNanos();
        final Boolean isComplete = consumerFuture.get(readNanos, TimeUnit.NANOSECONDS);
        if (!Boolean.TRUE.equals(isComplete)) {
            throw new IllegalStateException(String.format("Failed to complete request %s", requestStr()));
        }
        final ByteBuffer result = get();
        if (result == null) {
            throw new IllegalStateException(
                    String.format("Tried to read from no-longer-acquired Request, %s", requestStr()));
        }
        if (result.position() != 0 || result.limit() != result.capacity() || result.limit() != requestLength()) {
            throw new IllegalStateException(String.format(
                    "Expected: pos=0, limit=%d, capacity=%d. Actual: pos=%d, limit=%d, capacity=%d",
                    requestLength(), requestLength(), result.position(), result.limit(), result.capacity()));
        }
        return result;
    }

    boolean isFragment(final long fragmentIndex) {
        return this.fragmentIndex == fragmentIndex;
    }

    private int requestLength() {
        return (int) (to - from + 1);
    }

    private GetObjectRequest getObjectRequest() {
        return GetObjectRequest.builder()
                .bucket(s3Uri.bucket().orElseThrow())
                .key(s3Uri.key().orElseThrow())
                .range("bytes=" + from + "-" + to)
                .build();
    }

    String requestStr() {
        return String.format("ix=%d [%d, %d]/%d %s/%s", fragmentIndex, from, to, requestLength(),
                s3Uri.bucket().orElseThrow(), s3Uri.key().orElseThrow());
    }

    // --------------------------------------------------------------------------------------------------

    private final class Sub implements Subscriber<ByteBuffer> {

        private final CompletableFuture<Boolean> localProducer;
        private Subscription subscription;

        /**
         * Number of bytes stored in the buffer.
         */
        int offset;

        Sub() {
            localProducer = producerFuture;
            final ByteBuffer buffer = S3Request.this.get();
            if (buffer == null) {
                localProducer.complete(false);
                return;
            }
            if (buffer.position() != 0) {
                // We don't change the buffer position while writing to it, so this should never happen
                localProducer.completeExceptionally(new IllegalStateException(
                        String.format("Buffer not empty for new subscriber, %s", requestStr())));
            }
        }

        // ---------------------------------------------------- -------------------------

        @Override
        public void onSubscribe(Subscription s) {
            if (S3Request.this.get() == null) {
                localProducer.complete(false);
                s.cancel();
                return;
            }
            if (subscription != null) {
                s.cancel();
                return;
            }
            subscription = s;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(final ByteBuffer dataBuffer) {
            final ByteBuffer resultBuffer = S3Request.this.get();
            if (resultBuffer == null) {
                localProducer.complete(false);
                return;
            }
            final int numBytes = dataBuffer.remaining();
            resultBuffer.duplicate().position(offset).put(dataBuffer);
            offset += numBytes;
            subscription.request(1);
        }

        @Override
        public void onError(Throwable t) {
            localProducer.completeExceptionally(t);
        }

        @Override
        public void onComplete() {
            if (S3Request.this.get() == null) {
                localProducer.complete(false);
                return;
            }
            if (offset != requestLength()) {
                localProducer.completeExceptionally(new IllegalStateException(String.format(
                        "Expected %d bytes, received %d, %s", requestLength(), offset,
                        requestStr())));
                return;
            }
            localProducer.complete(true);
        }
    }
}
