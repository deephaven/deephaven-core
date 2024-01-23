/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.base.verify.Assert;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

final class ByteBufferAsyncResponseTransformer<ResponseT> implements AsyncResponseTransformer<ResponseT, ByteBuffer> {

    private final BufferPool.BufferHolder bufferHolder;
    private final ByteBuffer byteBuffer;

    private volatile boolean released;
    private volatile CompletableFuture<ByteBuffer> currentFuture;

    /**
     * @param bufferHolder A {@link BufferPool.BufferHolder} that will provide a buffer to store the response bytes.
     *        This will be {@link BufferPool.BufferHolder#close}d when {@link #release()} is called.
     */
    ByteBufferAsyncResponseTransformer(@NotNull final BufferPool.BufferHolder bufferHolder) {
        this.bufferHolder = bufferHolder;
        this.byteBuffer = bufferHolder.get();
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
    public synchronized void release() {
        released = true;
        bufferHolder.close();
    }

    private final class ByteBufferSubscriber implements Subscriber<ByteBuffer> {

        private final CompletableFuture<ByteBuffer> resultFuture;
        /**
         * A duplicate of the underlying buffer used to store the response bytes without modifying the original reusable
         * buffer.
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
