package io.deephaven.web.client.api;

import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import elemental2.promise.Promise.PromiseExecutorCallbackFn.RejectCallbackFn;
import io.deephaven.web.shared.fu.JsBiConsumer;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A set of utilities for creating callbacks and promises using lambdas.
 */
public interface Callbacks {

    static <S, T> Promise<S> promise(@Nullable HasEventHandling failHandler,
        Consumer<Callback<S, T>> t) {
        return new Promise<>((
            Promise.PromiseExecutorCallbackFn.ResolveCallbackFn<S> resolve,
            Promise.PromiseExecutorCallbackFn.RejectCallbackFn reject) -> t
                .accept(new Callback<S, T>() {
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

    static <S> Callback<S, String> of(@Nullable HasEventHandling failHandler, Consumer<S> from) {
        return of((v, f) -> {
            if (f == null) {
                from.accept(v);
            } else {
                failLog(failHandler, from, f);
            }
        });
    }

    static <S, F> void failLog(HasEventHandling failHandler, Consumer<S> from, F failure) {
        if (failHandler != null) {
            failHandler.failureHandled(failure == null ? null : String.valueOf(failure));
        } else {
            DomGlobal.console.error("Request ", from, " failed with reason ", failure);
        }

    }

    /**
     * Transform a bi-consumer into a callback. It is the caller's responsibility to fire
     * "requestfailed" events as appropriate.
     */
    static <S, F> Callback<S, F> of(BiConsumer<S, F> from) {
        return new Callback<S, F>() {
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

    static <S, F> Promise<S> grpcUnaryPromise(Consumer<JsBiConsumer<F, S>> t) {
        return new Promise<>((resolve, reject) -> {
            t.accept((fail, success) -> {
                if (fail == null) {
                    resolve.onInvoke(success);
                } else {
                    reject.onInvoke(fail);
                }
            });
        });
    }

    static <S, F> void translateCallback(Callback<S, String> callback,
        Consumer<JsBiConsumer<F, S>> t) {
        try {
            t.accept((fail, success) -> {
                if (fail != null) {
                    callback.onSuccess(success);
                } else {
                    callback.onFailure(fail.toString());
                }
            });
        } catch (Exception exception) {
            callback.onFailure(exception.getMessage());
        }
    }
}
