//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.stream;

import io.deephaven.web.client.api.event.EventFn;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Java wrapper to deal with the distinct ResponseStream types that are emitted. Provides strongly typed methods for
 * cleaner Java consumption, that can be used to represent any of the structural types that are used for grpc methods.
 *
 * @param <T> payload that is emitted from the stream
 */
public class ResponseStreamWrapper<T> extends HasEventHandling {

    public static final String DATA_EVENT = "data";
    public static final String STATUS_EVENT = "status";
    public static final String END_EVENT = "end";

    public static <T> ResponseStreamWrapper<T> of(Consumer<StreamObserver<T>> openStream) {
        return new ResponseStreamWrapper<>(openStream);
    }

    private final Context.CancellableContext cancellation;
    private final StreamObserver<T> observer = new StreamObserver<>() {
        @Override
        public void onNext(T value) {
            fireEvent(DATA_EVENT, value);
        }

        @Override
        public void onError(Throwable t) {
            Status detail = Status.fromThrowable(t);
            fireEvent(STATUS_EVENT, detail);
            fireEvent(END_EVENT, detail);
        }

        @Override
        public void onCompleted() {
            fireEvent(END_EVENT, Status.OK);
        }
    };

    public ResponseStreamWrapper(Consumer<StreamObserver<T>> openStream) {
        cancellation = Context.current().withCancellation();
        cancellation.run(() -> openStream.accept(observer));
    }

    public void cancel() {
        cancellation.cancel(null);
    }

    public final void onStatus(Consumer<Status> handler) {
        addEventListener(STATUS_EVENT, wrapAsEventListener(handler));
    }

    @NotNull
    private static <T> EventFn<Object> wrapAsEventListener(Consumer<T> handler) {
        return e -> handler.accept((T) e.getDetail());
    }

    public final void onData(Consumer<T> handler) {
        addEventListener(DATA_EVENT, wrapAsEventListener(handler));
    }

    public final void onEnd(Consumer<Status> handler) {
        addEventListener(END_EVENT, wrapAsEventListener(handler));
    }

    public final void onHeaders(Consumer<Object> handler) {
        addEventListener("headers", wrapAsEventListener(handler));
    }
}
