package io.deephaven.extensions.s3;

import io.deephaven.util.channel.SeekableChannelContext;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * Context object used to store read-ahead buffers for efficiently reading from S3.
 */
final class S3ChannelContext implements SeekableChannelContext {
    private static final Logger log = LoggerFactory.getLogger(S3ChannelContext.class);
    private static final long UNINITIALIZED_SIZE = -1;

    private final S3AsyncClient client;
    final S3Instructions instructions;
    private final BufferPool bufferPool;

    private S3Uri uri;

    /**
     * Used to cache recently fetched fragments for faster lookup
     */
    private final Request[] requests;

    /**
     * The size of the object in bytes, stored in context to avoid fetching multiple times
     */
    private long size;

    private long numFragmentsInObject;

    S3ChannelContext(S3AsyncClient client, S3Instructions instructions, BufferPool bufferPool) {
        this.client = Objects.requireNonNull(client);
        this.instructions = Objects.requireNonNull(instructions);
        this.bufferPool = Objects.requireNonNull(bufferPool);
        requests = new Request[instructions.maxCacheSize()];
        size = UNINITIALIZED_SIZE;
        if (log.isDebugEnabled()) {
            log.debug("creating context: {}", ctxStr());
        }
    }

    public long size(S3Uri uri) throws IOException {
        assume(uri);
        populateSize();
        return size;
    }

    public int fill(S3Uri uri, final long position, ByteBuffer dest) throws IOException {
        assume(uri);
        populateSize();
        final int destRemaining = dest.remaining();
        if (destRemaining == 0) {
            return 0;
        }
        // Send async read requests for current fragment as well as read ahead fragments
        final long firstFragmentIx = fragmentIndex(position);
        final long readAhead;
        {
            final long lastFragmentIx = fragmentIndex(position + destRemaining - 1);
            final int impliedReadAhead = (int) (lastFragmentIx - firstFragmentIx);
            final int desiredReadAhead = instructions.readAheadCount();
            final long totalRemainingFragments = numFragmentsInObject - firstFragmentIx - 1;
            final int maxReadAhead = requests.length - 1;
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
            final Request request = getRequest(firstFragmentIx + i + 1).orElse(null);
            if (request == null || !request.isDone()) {
                break;
            }
            // non-blocking since we know isDone
            filled += request.fill(position + filled, dest);
        }
        return filled;
    }

    @Override
    public void close() {
        if (log.isDebugEnabled()) {
            log.debug("closing context: {}", ctxStr());
        }
        // Cancel all outstanding requests
        for (int i = 0; i < requests.length; i++) {
            if (requests[i] != null) {
                requests[i].release();
                requests[i] = null;
            }
        }
    }

    // --------------------------------------------------------------------------------------------------

    void assume(S3Uri uri) {
        if (this.uri == null) {
            this.uri = Objects.requireNonNull(uri);
        } else {
            if (!this.uri.equals(uri)) {
                throw new IllegalStateException(
                        String.format("Inconsistent URIs. expected=%s, actual=%s", this.uri, uri));
            }
        }
    }

    private Optional<Request> getRequest(final long fragmentIndex) {
        final int cacheIdx = cacheIndex(fragmentIndex);
        final Request request = requests[cacheIdx];
        return request == null || !request.isFragment(fragmentIndex)
                ? Optional.empty()
                : Optional.of(request);
    }

    private Request getOrCreateRequest(final long fragmentIndex) {
        final int cacheIdx = cacheIndex(fragmentIndex);
        Request request = requests[cacheIdx];
        if (request != null) {
            if (!request.isFragment(fragmentIndex)) {
                request.release();
                requests[cacheIdx] = (request = new Request(fragmentIndex));
                request.init();
            }
        } else {
            requests[cacheIdx] = (request = new Request(fragmentIndex));
            request.init();
        }
        return request;
    }

    private int cacheIndex(final long fragmentIndex) {
        // TODO(deephaven-core#5061): Experiment with LRU caching
        return (int) (fragmentIndex % requests.length);
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

    final class Request
            implements AsyncResponseTransformer<GetObjectResponse, ByteBuffer>, BiConsumer<ByteBuffer, Throwable> {

        // implicitly + URI
        private final long fragmentIndex;
        private final long from;
        private final long to;
        private final Instant createdAt;
        private Instant completedAt;
        private final CompletableFuture<Void> released;
        private CompletableFuture<ByteBuffer> consumerFuture;
        private volatile CompletableFuture<ByteBuffer> producerFuture;
        private GetObjectResponse response;
        private int fillCount;
        private long fillBytes;

        private Request(long fragmentIndex) {
            createdAt = Instant.now();
            this.fragmentIndex = fragmentIndex;
            from = fragmentIndex * instructions.fragmentSize();
            to = Math.min(from + instructions.fragmentSize(), size) - 1;
            released = new CompletableFuture<>();
        }

        void init() {
            if (log.isDebugEnabled()) {
                log.debug("send: {}", requestStr());
            }
            consumerFuture = client.getObject(getObjectRequest(), this);
            consumerFuture.whenComplete(this);
        }

        public boolean isDone() {
            return consumerFuture.isDone();
        }

        public int fill(long localPosition, ByteBuffer dest) throws IOException {
            final int outOffset = (int) (localPosition - from);
            final int outLength = Math.min((int) (to - localPosition + 1), dest.remaining());
            final ByteBuffer fullFragment;
            try {
                fullFragment = get();
            } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
                throw handleS3Exception(e, String.format("fetching fragment %s", requestStr()));
            }
            dest.put(fullFragment.duplicate().position(outOffset).limit(outOffset + outLength));
            ++fillCount;
            fillBytes += outLength;
            return outLength;
        }

        public void release() {
            final boolean didCancel = consumerFuture.cancel(true);
            if (log.isDebugEnabled()) {
                final String cancelType = didCancel ? "fast" : (fillCount == 0 ? "unused" : "normal");
                log.debug("cancel {}: {} fillCount={}, fillBytes={}", cancelType, requestStr(), fillCount, fillBytes);
            }
            // Finishing via exception to ensure downstream subscribers can cleanup if SDK didn't complete them
            released.cancel(true);
        }

        // --------------------------------------------------------------------------------------------------

        @Override
        public void accept(ByteBuffer byteBuffer, Throwable throwable) {
            completedAt = Instant.now();
            if (log.isDebugEnabled()) {
                if (byteBuffer != null) {
                    log.debug("send complete: {} {}", requestStr(), Duration.between(createdAt, completedAt));
                } else {
                    log.debug("send error: {} {}", requestStr(), Duration.between(createdAt, completedAt));
                }
            }
        }

        // --------------------------------------------------------------------------------------------------

        @Override
        public CompletableFuture<ByteBuffer> prepare() {
            final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            producerFuture = future;
            return future;
        }

        @Override
        public void onResponse(GetObjectResponse response) {
            this.response = response;
        }

        @Override
        public void onStream(SdkPublisher<ByteBuffer> publisher) {
            publisher.subscribe(new Request.Sub());
        }

        @Override
        public void exceptionOccurred(Throwable error) {
            producerFuture.completeExceptionally(error);
        }

        // --------------------------------------------------------------------------------------------------

        private ByteBuffer get() throws ExecutionException, InterruptedException, TimeoutException {
            if (released.isDone()) {
                throw new IllegalStateException(String.format("Trying to get data after release, %s", requestStr()));
            }
            // Giving our own get() a bit of overhead - the clients should already be constructed with appropriate
            // apiCallTimeout.
            final long readNanos = instructions.readTimeout().plusMillis(100).toNanos();
            return consumerFuture.get(readNanos, TimeUnit.NANOSECONDS);
        }

        private boolean isFragment(long fragmentIndex) {
            return this.fragmentIndex == fragmentIndex;
        }

        private int requestLength() {
            return (int) (to - from + 1);
        }

        private GetObjectRequest getObjectRequest() {
            return GetObjectRequest.builder()
                    .bucket(uri.bucket().orElseThrow())
                    .key(uri.key().orElseThrow())
                    .range("bytes=" + from + "-" + to)
                    .build();
        }

        private String requestStr() {
            if (uri != null) {
                return String.format("ctx=%d ix=%d [%d, %d]/%d %s/%s", System.identityHashCode(S3ChannelContext.this),
                        fragmentIndex, from, to, requestLength(), uri.bucket().orElseThrow(), uri.key().orElseThrow());
            } else {
                return String.format("ctx=%d ix=%d [%d, %d]/%d", System.identityHashCode(S3ChannelContext.this),
                        fragmentIndex, from, to, requestLength());
            }
        }

        // --------------------------------------------------------------------------------------------------

        final class Sub implements Subscriber<ByteBuffer>, BiConsumer<Void, Throwable> {
            private final CompletableFuture<ByteBuffer> localProducer;
            private ByteBuffer buffer;
            private Subscription subscription;

            Sub() {
                localProducer = producerFuture;
                buffer = bufferPool.take(requestLength());
                // 1. localProducer succeeds: whenComplete will executed when released
                // 2. localProducer fails: whenComplete will executed asap
                // 3. localProducer limbo (not expected): whenComplete will executed when released
                CompletableFuture.allOf(localProducer, released).whenComplete(this);
            }

            // -----------------------------------------------------------------------------

            @Override
            public synchronized void accept(Void unused, Throwable throwable) {
                bufferPool.give(buffer);
                buffer = null;
            }

            // -----------------------------------------------------------------------------

            @Override
            public synchronized void onSubscribe(Subscription s) {
                if (subscription != null) {
                    s.cancel();
                    return;
                }
                subscription = s;
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public synchronized void onNext(ByteBuffer byteBuffer) {
                buffer.put(byteBuffer);
                subscription.request(1);
            }

            @Override
            public synchronized void onError(Throwable t) {
                localProducer.completeExceptionally(t);
            }

            @Override
            public synchronized void onComplete() {
                buffer.flip();
                if (buffer.remaining() != requestLength()) {
                    localProducer.completeExceptionally(new IllegalStateException(String.format(
                            "Expected %d bytes, received %d, %s", requestLength(), buffer.remaining(), requestStr())));
                } else {
                    localProducer.complete(buffer.asReadOnlyBuffer());
                }
            }
        }
    }

    private IOException handleS3Exception(final Exception e, final String operationDescription) {
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

    private void populateSize() throws IOException {
        if (size != UNINITIALIZED_SIZE) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("head: {}", ctxStr());
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
            throw handleS3Exception(e, String.format("fetching HEAD for file %s, %s", uri, ctxStr()));
        }
        setSize(headObjectResponse.contentLength());
    }

    void hackSize(long size) {
        if (this.size == UNINITIALIZED_SIZE) {
            setSize(size);
        } else if (this.size != size) {
            throw new IllegalStateException();
        }
    }

    private void setSize(long size) {
        this.size = size;
        // ceil(size / fragmentSize)
        this.numFragmentsInObject = (size + instructions.fragmentSize() - 1) / instructions.fragmentSize();
    }
}
