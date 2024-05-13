//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Require;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.base.reference.CleanupReference;
import io.deephaven.engine.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.BaseSeekableChannelContext;
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
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

/**
 * Context object used to store read-ahead buffers for efficiently reading from S3. A single context object can only be
 * associated with a single URI at a time.
 */
final class S3ChannelContext extends BaseSeekableChannelContext implements SeekableChannelContext {
    private static final Logger log = LoggerFactory.getLogger(S3ChannelContext.class);
    static final long UNINITIALIZED_SIZE = -1;
    private static final long UNINITIALIZED_NUM_FRAGMENTS = -1;

    private final S3AsyncClient client;
    private final S3Instructions instructions;

    /**
     * The URI associated with this context. A single context object can only be associated with a single URI at a time.
     * But it can be re-associated with a different URI after {@link #reset() resetting}.
     */
    private S3Uri uri;

    /**
     * Used to cache recently fetched fragments from the {@link #uri} for faster lookup. Note that this cache can be
     * shared across multiple contexts and be accessed concurrently.
     */
    private final S3RequestCache sharedCache;

    /**
     * Used to cache recently fetched fragments as well as the ownership token for the request. This cache is local to
     * the context and is used to keep the requests alive as long as the context is alive.
     */
    private final Request.AcquiredRequest[] localCache;

    /**
     * The size of the object in bytes, stored in context to avoid fetching multiple times
     */
    private long size;

    /**
     * The number of fragments in the object, ceil(size / fragmentSize)
     */
    private long numFragments;

    S3ChannelContext(@NotNull final S3AsyncClient client, @NotNull final S3Instructions instructions,
            @NotNull final S3RequestCache sharedCache) {
        this.client = Objects.requireNonNull(client);
        this.instructions = Objects.requireNonNull(instructions);
        this.localCache = new Request.AcquiredRequest[instructions.maxCacheSize()];
        this.sharedCache = sharedCache;
        if (sharedCache.getFragmentSize() != instructions.fragmentSize()) {
            throw new IllegalArgumentException("Fragment size mismatch between shared cache and instructions, "
                    + sharedCache.getFragmentSize() + " != " + instructions.fragmentSize());
        }
        uri = null;
        size = UNINITIALIZED_SIZE;
        numFragments = UNINITIALIZED_NUM_FRAGMENTS;
        if (log.isDebugEnabled()) {
            log.debug().append("Creating context: ").append(ctxStr()).endl();
        }
    }

    void setURI(@NotNull final S3Uri uri) {
        if (!uri.equals(this.uri)) {
            reset();
        }
        this.uri = uri;
    }

    void verifyOrSetSize(long size) {
        if (this.size == UNINITIALIZED_SIZE) {
            setSize(size);
        } else if (this.size != size) {
            throw new IllegalStateException(
                    String.format("Inconsistent size. expected=%d, actual=%d, ctx=%s", size, this.size, ctxStr()));
        }
    }

    long size() throws IOException {
        ensureSize();
        return size;
    }

    int fill(final long position, ByteBuffer dest) throws IOException {
        final int destRemaining = dest.remaining();
        if (destRemaining == 0) {
            return 0;
        }
        ensureSize();
        // Send async read requests for current fragment as well as read ahead fragments
        final long firstFragmentIx = fragmentIndex(position);
        final long readAhead;
        {
            final long lastFragmentIx = fragmentIndex(position + destRemaining - 1);
            final int impliedReadAhead = (int) (lastFragmentIx - firstFragmentIx);
            final int desiredReadAhead = instructions.readAheadCount();
            final long totalRemainingFragments = numFragments - firstFragmentIx - 1;
            final int maxReadAhead = instructions.maxCacheSize() - 1;
            readAhead = Math.min(
                    Math.max(impliedReadAhead, desiredReadAhead),
                    (int) Math.min(maxReadAhead, totalRemainingFragments));
        }
        final Request firstRequest = getOrCreateRequest(firstFragmentIx);
        for (int i = 0; i < readAhead; ++i) {
            getOrCreateRequest(firstFragmentIx + i + 1);
        }
        // blocking
        int filled = firstRequest.fill(position, dest);
        for (int i = 0; dest.hasRemaining(); ++i) {
            // Since we have already created requests for read ahead fragments, we can retrieve them from the local
            // cache
            final Request request = getRequestFromLocalCache(firstFragmentIx + i + 1);
            if (request == null || !request.isDone()) {
                break;
            }
            // non-blocking since we know isDone
            filled += request.fill(position + filled, dest);
        }
        return filled;
    }

    private void reset() {
        releaseOutstanding();
        // Reset the internal state
        uri = null;
        size = UNINITIALIZED_SIZE;
        numFragments = UNINITIALIZED_NUM_FRAGMENTS;
    }

    /**
     * Close the context, releasing all outstanding requests and resources associated with it.
     */
    @Override
    public void close() {
        super.close();
        if (log.isDebugEnabled()) {
            log.debug().append("Closing context: ").append(ctxStr()).endl();
        }
        releaseOutstanding();
    }

    /**
     * Release all outstanding requests associated with this context. Eventually, the request will be canceled when the
     * objects are garbage collected.
     */
    private void releaseOutstanding() {
        Arrays.fill(localCache, null);
    }

    // --------------------------------------------------------------------------------------------------

    @Nullable
    private Request getRequestFromLocalCache(final long fragmentIndex) {
        return getRequestFromLocalCache(fragmentIndex, cacheIndex(fragmentIndex));
    }

    @Nullable
    private Request getRequestFromLocalCache(final long fragmentIndex, final int cacheIdx) {
        if (localCache[cacheIdx] != null && localCache[cacheIdx].request.isFragment(fragmentIndex)) {
            return localCache[cacheIdx].request;
        }
        return null;
    }

    @NotNull
    private Request getOrCreateRequest(final long fragmentIndex) {
        final int cacheIdx = cacheIndex(fragmentIndex);
        final Request locallyCached = getRequestFromLocalCache(fragmentIndex, cacheIdx);
        if (locallyCached != null) {
            return locallyCached;
        }
        final Request.AcquiredRequest sharedCacheRequest = sharedCache.getOrCreateRequest(uri, fragmentIndex, this);
        // Cache the request and the ownership token locally
        localCache[cacheIdx] = sharedCacheRequest;
        // Send the request, if not sent already. The following method is idempotent, so we always call it.
        sharedCacheRequest.request.sendRequest();
        return sharedCacheRequest.request;
    }

    private int cacheIndex(final long fragmentIndex) {
        return (int) (fragmentIndex % instructions.maxCacheSize());
    }

    private long fragmentIndex(final long pos) {
        return pos / instructions.fragmentSize();
    }

    private String ctxStr() {
        if (uri != null) {
            return String.format("ctx=%d %s/%s", System.identityHashCode(S3ChannelContext.this),
                    uri.bucket().orElseThrow(), uri.key().orElseThrow());
        } else {
            return String.format("ctx=%d", System.identityHashCode(S3ChannelContext.this));
        }
    }

    // TODO Move Request class into a separate file after the initial code review
    /**
     * A request for a single fragment of an S3 object, which can be used concurrently.
     *
     * @implNote This class extends from a {@link SoftReference<ByteBuffer>} and implements {@link CleanupReference} to
     *           allow for cancelling the request once all references to the buffer have been released. Users should not
     *           access the buffer directly, but instead use the {@link #fill(long, ByteBuffer)} method. Also, users
     *           should hold instances of {@link AcquiredRequest} to keep the requests alive.
     */
    static final class Request extends SoftReference<ByteBuffer>
            implements AsyncResponseTransformer<GetObjectResponse, Boolean>, BiConsumer<Boolean, Throwable>,
            CleanupReference<ByteBuffer> {

        static class AcquiredRequest {
            final Request request;
            /**
             * The ownership token keeps the request alive. When the ownership token is GC'd, the request is no longer
             * usable and will be cleaned up.
             */
            final Object ownershipToken;

            AcquiredRequest(final Request request, final Object ownershipToken) {
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

        private static final int REQUEST_NOT_SENT = 0;
        private static final int REQUEST_SENT = 1;
        private volatile int requestSent = REQUEST_NOT_SENT;
        private static final AtomicIntegerFieldUpdater<Request> REQUEST_SENT_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(Request.class, "requestSent");

        private final S3Uri s3Uri;
        private final ID id;
        private final S3Instructions instructions;
        private final S3AsyncClient client;
        private final long fragmentIndex;
        private final long from;
        private final long to;
        private final Instant createdAt;
        private CompletableFuture<Boolean> consumerFuture;
        private volatile CompletableFuture<Boolean> producerFuture;
        private int fillCount;
        private long fillBytes;
        private final S3RequestCache sharedCache;

        /**
         * Create a new request for the given fragment index using the provided context object.
         *
         * @return A new {@link AcquiredRequest} object containing newly created request and an ownership token. The
         *         request will stay alive as long as the ownership token is held.
         *
         * @implNote This method does not cache the context because contexts are short-lived while a request may be
         *           cached.
         */
        @NotNull
        static AcquiredRequest createAndAcquire(final long fragmentIndex, @NotNull final S3ChannelContext context) {
            final long from = fragmentIndex * context.instructions.fragmentSize();
            final long to = Math.min(from + context.instructions.fragmentSize(), context.size) - 1;
            final long requestLength = to - from + 1;
            final ByteBuffer buffer = ByteBuffer.allocate((int) requestLength);
            final Request request = new Request(fragmentIndex, context, buffer, from, to);
            return new AcquiredRequest(request, buffer);
        }

        private Request(final long fragmentIndex, @NotNull final S3ChannelContext context,
                @NotNull final ByteBuffer buffer, final long from, final long to) {
            super(buffer, CleanupReferenceProcessorInstance.DEFAULT.getReferenceQueue());
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
            if (!REQUEST_SENT_UPDATER.compareAndSet(this, REQUEST_NOT_SENT, REQUEST_SENT)) {
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug().append("Sending: ").append(requestStr()).endl();
            }
            consumerFuture = client.getObject(getObjectRequest(), this);
            consumerFuture.whenComplete(this);
        }

        boolean isDone() {
            return consumerFuture.isDone();
        }

        /**
         * Fill the provided buffer with data from this request, starting at the given local position. Returns the
         * number of bytes filled. Note that the request must be acquired before calling this method.
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
                throw handleS3Exception(e, String.format("fetching fragment %s", requestStr()), instructions);
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
            if (Boolean.FALSE.equals(isComplete)) {
                throw new UncheckedDeephavenException(String.format("Failed to complete request %s", requestStr()));
            }
            final ByteBuffer result = get();
            if (result == null) {
                throw new UncheckedDeephavenException(
                        String.format("Failed to acquire buffer after completion, %s", requestStr()));
            }
            if (result.position() != 0 || result.limit() != result.capacity() || result.limit() != requestLength()) {
                throw new IllegalStateException(String.format(
                        "Expected: pos=0, limit=%d, capacity=%d. Actual: pos=%d, limit=%d, capacity=%d",
                        requestLength(), requestLength(), result.position(), result.limit(), result.capacity()));
            }
            return result;
        }

        private boolean isFragment(final long fragmentIndex) {
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
                final ByteBuffer buffer = Request.this.get();
                if (buffer == null) {
                    localProducer.completeExceptionally(new IllegalStateException(
                            String.format("Failed to acquire buffer for new subscriber, %s", requestStr())));
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
                if (Request.this.get() == null) {
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
                final ByteBuffer resultBuffer = Request.this.get();
                if (resultBuffer == null) {
                    localProducer.completeExceptionally(new IllegalStateException(
                            String.format("Failed to acquire buffer for data, %s", requestStr())));
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
                if (Request.this.get() == null) {
                    localProducer.completeExceptionally(new IllegalStateException(
                            String.format("Failed to acquire buffer for completion, %s", requestStr())));
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

    static IOException handleS3Exception(final Exception e, final String operationDescription,
            final S3Instructions instructions) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new IOException(String.format("Thread interrupted while %s", operationDescription), e);
        }
        if (e instanceof ExecutionException) {
            return new IOException(String.format("Execution exception occurred while %s", operationDescription), e);
        }
        if (e instanceof TimeoutException) {
            return new IOException(String.format(
                    "Operation timeout while %s after waiting for duration %s", operationDescription,
                    instructions.readTimeout()), e);
        }
        if (e instanceof CancellationException) {
            return new IOException(String.format("Cancelled an operation while %s", operationDescription), e);
        }
        return new IOException(String.format("Exception caught while %s", operationDescription), e);
    }

    private void ensureSize() throws IOException {
        if (size != UNINITIALIZED_SIZE) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug().append("Head: ").append(ctxStr()).endl();
        }
        // Fetch the size of the file on the first read using a blocking HEAD request, and store it in the context
        // for future use
        final HeadObjectResponse headObjectResponse;
        try {
            headObjectResponse = client
                    .headObject(HeadObjectRequest.builder()
                            .bucket(uri.bucket().orElseThrow())
                            .key(uri.key().orElseThrow())
                            .build())
                    .get(instructions.readTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
            throw handleS3Exception(e, String.format("fetching HEAD for file %s, %s", uri, ctxStr()), instructions);
        }
        setSize(headObjectResponse.contentLength());
    }

    private void setSize(long size) {
        this.size = size;
        // ceil(size / fragmentSize)
        this.numFragments = (size + instructions.fragmentSize() - 1) / instructions.fragmentSize();
    }
}
