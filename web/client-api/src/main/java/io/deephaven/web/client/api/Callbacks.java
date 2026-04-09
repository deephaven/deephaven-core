//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import elemental2.promise.Promise.PromiseExecutorCallbackFn.RejectCallbackFn;
import io.deephaven.web.client.api.barrage.stream.TrailersCapturingInterceptor;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A set of utilities for creating callbacks and promises using lambdas.
 */
public interface Callbacks {

    static <T> StreamObserver<T> ignore() {
        return new StreamObserver<T>() {
            @Override
            public void onNext(T value) {}

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {}
        };
    }

    static <S, T> Promise<S> promise(@Nullable HasEventHandling failHandler, Consumer<Callback<S, T>> t) {
        return new Promise<>((
                Promise.PromiseExecutorCallbackFn.ResolveCallbackFn<S> resolve,
                Promise.PromiseExecutorCallbackFn.RejectCallbackFn reject) -> t.accept(new Callback<S, T>() {
                    @Override
                    public void onFailure(T reason) {
                        notNull(failHandler, t, reject).onInvoke(reason);
                    }

                    @Override
                    public void onSuccess(S result) {
                        resolve.onInvoke(result);
                    }
                }));
    }

    static <S> RejectCallbackFn notNull(
            @Nullable HasEventHandling failHandler, // system provided failHandler
            Consumer<S> realCallback, // success handler
            RejectCallbackFn reject // promise-supplied failHandler
    ) {
        if (reject == null) {
            return f -> failLog(failHandler, realCallback, f);
        }
        return f -> {
            failLog(failHandler, realCallback, f);
            reject.onInvoke(f);
        };
    }

    static <S, F> void failLog(HasEventHandling failHandler, Consumer<S> from, F failure) {
        if (failHandler != null) {
            failHandler.failureHandled(failure == null ? null : String.valueOf(failure));
        } else {
            DomGlobal.console.error("Request ", from, " failed with reason ", failure);
        }
    }

    /**
     * Transform a bi-consumer into a callback. It is the caller's responsibility to fire "requestfailed" events as
     * appropriate.
     */
    static <S, F> Callback<S, F> of(BiConsumer<S, F> from) {
        return new Callback<>() {
            F fail;

            @Override
            public void onFailure(F reason) {
                fail = reason;
                from.accept(null, reason);
            }

            @Override
            public void onSuccess(S result) {
                from.accept(result, fail);
            }
        };
    }

    /**
     * Propagates the message and the context into a Promise, as a promise's microtask will result in losing the
     * context. Does not resolve until the unary stream is closed, in order to read the trailers, in contrast with
     * {@link #grpcUnaryPromise(Consumer)}.
     */
    static <S> Promise<Response<S>> grpcUnaryPromiseWrapped(Consumer<StreamObserver<S>> t) {
        return new Promise<>((resolve, reject) -> {
            t.accept(new StreamObserver<>() {
                private S success;

                @Override
                public void onNext(S s) {
                    success = s;
                }

                @Override
                public void onError(Throwable throwable) {
                    assert success == null;
                    reject.onInvoke(getError(throwable));
                }

                @Override
                public void onCompleted() {
                    if (success != null) {
                        // Capture the context here, when the call has finished
                        Context current = Context.current();
                        resolve.onInvoke(new Response<>(success, current,
                                TrailersCapturingInterceptor.getTrailersFromContext()));
                    }
                }
            });
        });
    }

    /**
     * Returns a promise that resolves when the first payload arrives from a unary request. Because promises resolve on
     * a microtask, the later called promise will not necessarily share the same Context as the call, use
     * {@link #grpcUnaryPromiseWrapped(Consumer)} for that.
     */
    static <S> Promise<S> grpcUnaryPromise(Consumer<StreamObserver<S>> t) {
        return new Promise<>((resolve, reject) -> {
            t.accept(new StreamObserver<>() {

                @Override
                public void onNext(S s) {
                    resolve.onInvoke(s);
                }

                @Override
                public void onError(Throwable throwable) {
                    reject.onInvoke(getError(throwable));
                }

                @Override
                public void onCompleted() {
                    // no-op since we don't need context/trailers
                }
            });
        });
    }

    private static Object getError(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException sre) {
            String description = sre.getStatus().getDescription();
            return "Error: " + (description == null ? sre.getMessage() : description);
        } else {
            return "Error: " + throwable;
        }
    }

    @SuppressWarnings("unusable-by-js")
    record Response<M>(M message, Context context, Metadata trailers) {
    }
}
