/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

final class ByteBufferAsyncResponseTransformer<ResponseT>
        implements AsyncResponseTransformer<ResponseT, ByteBuffer>, SafeCloseable {

    private final ByteBuffer byteBuffer;

    private volatile boolean released;
    private volatile CompletableFuture<ByteBuffer> currentFuture;

    /**
     * @param byteBuffer A {@link ByteBuffer} to store the response bytes.
     */
    ByteBufferAsyncResponseTransformer(@NotNull final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public CompletableFuture<ByteBuffer> prepare() {
        return currentFuture = new CompletableFuture<>();
    }

    @Override
    public void onResponse(final ResponseT response) {
        // No need to store the response object as we are only interested in the byte buffer
    }

    @Override
    public void onStream(final SdkPublisher<ByteBuffer> publisher) {
        publisher.subscribe(new ByteBufferSubscriber(currentFuture));
    }

    @Override
    public void exceptionOccurred(final Throwable throwable) {
        currentFuture.completeExceptionally(throwable);
    }

    /**
     * Prevent further mutation of the underlying buffer by this ByteBufferAsyncResponseTransformer and any of its
     * Subscribers.
     */
    @Override
    public synchronized void close() {
        released = true;
    }

    private final class ByteBufferSubscriber implements Subscriber<ByteBuffer> {

        private final CompletableFuture<ByteBuffer> resultFuture;
        /**
         * A duplicate of the underlying buffer used to store the response bytes without modifying the original reusable
         * buffer's position, limit, or mark.
         */
        private final ByteBuffer duplicate;

        private Subscription subscription;

        ByteBufferSubscriber(CompletableFuture<ByteBuffer> resultFuture) {
            this.resultFuture = resultFuture;
            this.duplicate = byteBuffer.duplicate();
        }

        @Override
        public void onSubscribe(final Subscription s) {
            if (subscription != null) {
                // Only maintain the first successful subscription
                s.cancel();
                return;
            }
            subscription = s;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(final ByteBuffer responseBytes) {
            // Assuming responseBytes will fit in the buffer
            Assert.assertion(responseBytes.remaining() <= duplicate.remaining(),
                    "responseBytes.remaining() <= duplicate.remaining()");
            if (released) {
                return;
            }
            synchronized (ByteBufferAsyncResponseTransformer.this) {
                if (released) {
                    return;
                }
                duplicate.put(responseBytes);
            }
            subscription.request(1);
        }

        @Override
        public void onError(final Throwable throwable) {
            resultFuture.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            resultFuture.complete(byteBuffer.asReadOnlyBuffer().limit(duplicate.position()));
        }
    }
}
