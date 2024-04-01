//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.fu;

import com.google.gwt.core.client.GWT;
import elemental2.core.JsArray;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import elemental2.promise.Promise.CatchOnRejectedCallbackFn;
import elemental2.promise.Promise.PromiseExecutorCallbackFn.RejectCallbackFn;
import elemental2.promise.Promise.PromiseExecutorCallbackFn.ResolveCallbackFn;
import io.deephaven.web.client.api.Callback;
import io.deephaven.web.shared.fu.*;
import jsinterop.annotations.JsIgnore;
import jsinterop.base.Js;

/**
 * Tired of needing to create annoying promise lambdas?
 *
 * If you want to create a promise early in a method's scope, and then configure it's handling of resolve/reject
 * callbacks later, or in a conditional nature, than LazyPromise is for you!
 *
 */
public class LazyPromise<T> implements PromiseLike<T> {
    public static native Throwable ofObject(Object obj) /*-{
      return @java.lang.Throwable::of(*)(obj);
    }-*/;

    private T succeeded;
    private boolean isSuccess;
    private Object failed;
    private final JsArray<JsConsumer<T>> onResolved = new JsArray<>();
    private final JsArray<JsConsumer<Object>> onRejected = new JsArray<>();

    private boolean isScheduled;

    @JsIgnore
    public static void runLater(JsRunnable task) {
        Promise.resolve((Object) null).then(ignored -> {
            task.run();
            return null;
        }).catch_(e -> {
            GWT.reportUncaughtException(ofObject(e));
            return null;
        });
    }

    /**
     * @param timeout How many millis to wait until failing the promise with a timeout.
     * @return a real promise that we will resolve when we are resolved.
     *
     *         This method overload is not strictly necessary to call when explicitly wiring up failure handling for
     *         this LazyPromise which you can guarantee will be eventually called.
     *
     *         To create a promise without a timeout, see {@link #asPromise()}.
     */
    public final Promise<T> asPromise(int timeout) {
        timeout(timeout);
        return asPromise();
    }

    /**
     * @return a real promise that we will resolve when we are resolved.
     *
     *         Use this method if you are safely wiring up failure handling for this LazyPromise. If you aren't
     *         explicitly wiring up calls to {@link #fail(Object)} this LazyPromise, then you should setup a timeout
     *         (see {@link #asPromise(int)} and {@link #timeout(int)}
     */
    public final Promise<T> asPromise() {
        return new Promise<>(this::spyResult);
    }

    public final CancellablePromise<T> asPromise(JsRunnable cancel) {
        return CancellablePromise.from(this::spyResult, cancel.beforeMe(this::cancel));
    }

    private void cancel() {
        if (isSuccess()) {
            // already succeeded.
            JsLog.debug("Cancelled promise after it succeeded", this);
        } else if (isFailure()) {
            // already failed
            JsLog.debug("Cancelled promise after it failed with", failed, this);
        } else {
            // not resolved yet, cancellation is treated as a failure with the cancellation message as the reason
            fail(CANCELLATION_MESSAGE);
        }
    }

    public final <V> CancellablePromise<V> asPromise(JsFunction<T, V> mapper, JsRunnable cancel) {
        return CancellablePromise.from(
                (resolve, reject) -> {
                    onSuccess(result -> {
                        final V mapped;
                        try {
                            mapped = mapper.apply(result);
                        } catch (Exception e) {
                            reject.onInvoke(e.getMessage());
                            return;
                        }
                        resolve.onInvoke(mapped);
                    });
                    onFailure(reject::onInvoke);
                }, cancel.andThen(this::cancel));
    }

    private void spyResult(ResolveCallbackFn<T> resolve, RejectCallbackFn reject) {
        onResolved.push(resolve::onInvoke);
        onRejected.push(reject::onInvoke);
        if (isFulfilled()) {
            this.flushCallbacks();
        }
    }

    public boolean isUnresolved() {
        return !isFailure() && !isSuccess();
    }

    public boolean isResolved() {
        return failed != null || succeeded != null;
    }

    public boolean isFailure() {
        return failed != null;
    }

    public boolean isSuccess() {
        return failed == null && isSuccess;
    }

    public boolean isFulfilled() {
        return !isUnresolved();
    }

    public void fail(Object reason) {
        if (isFulfilled()) {
            JsLog.debug("Got failure after fulfilled", this, reason, this.succeeded, failed);
        } else {
            this.failed = reason;
            runLater(this::flushCallbacks);
        }
    }

    public void succeed(T value) {
        if (isFulfilled()) {
            JsLog.debug("Got success after fulfilled", this, value, this.succeeded, failed);
        } else {
            this.isSuccess = true;
            // just storing value is not good enough, since we can be resolved w/ null
            this.succeeded = value;
            runLater(this::flushCallbacks);
        }
    }

    private void flushCallbacks() {
        if (isScheduled) {
            return;
        }
        isScheduled = true;
        // We'll always use micro tasks to handle promises;
        // this ensures that we predictably get async behavior for callbacks.
        runLater(() -> {
            isScheduled = false;
            if (isFailure()) {
                while (onRejected.length > 0) {
                    onRejected.shift().apply(failed);
                }
            } else if (isSuccess()) {
                while (onResolved.length > 0) {
                    onResolved.shift().apply(succeeded);
                }
            } else {
                throw new IllegalStateException("flushCallbacks called when promise is not fulfilled");
            }
            onResolved.length = 0;
            onRejected.length = 0;
        });
    }

    @Override
    public void onSuccess(JsConsumer<T> success) {
        if (isFailure()) {
            return;
        }
        onResolved.push(success);
        if (isSuccess()) {
            flushCallbacks();
        }
    }

    @Override
    public void onFailure(JsConsumer<Object> failure) {
        if (isSuccess()) {
            return;
        }
        onRejected.push(failure);
        if (isFailure()) {
            flushCallbacks();
        }
    }

    /**
     * Create a deferred promise from a known value.
     *
     * Rather than resolve immediately, this forces asynchronicity, to give the calling code time to unwind its stack
     * before running.
     */
    public static <T> Promise<T> promiseLater(T table) {
        // runs on next microtask
        return Promise.resolve((Object) null)
                .then(ignored -> Promise.resolve(table));
    }

    public static <V> IThenable<V> reject(Object failure) {
        return Js.uncheckedCast(Promise.reject(failure));
    }

    public LazyPromise<T> timeout(int wait) {
        final double pid = DomGlobal.setTimeout(a -> {
            if (!isSuccess && failed == null) {
                fail("Timeout after " + wait + "ms");
            }
        }, wait);
        onSuccess(ignored -> DomGlobal.clearTimeout(pid));
        onFailure(ignored -> DomGlobal.clearTimeout(pid));
        return this;
    }

    /**
     * Eats exceptions in exchange for messages logged to the console.
     *
     * Only use this when it is preferable to print a nice error message instead of leaving uncaught promises to bubble
     * (confusing) stack traces into console, especially if it is reasonable for a given promise to fail.
     *
     * This prevents "uncaught exceptions" when we have tests that expect failure.
     */
    public static <T> CatchOnRejectedCallbackFn<T> logError(JsProvider<String> msg) {
        return error -> {
            DomGlobal.console.error(msg.valueOf(), error);
            return Promise.resolve((T) null);
        };
    }

    public Callback<T, String> asCallback() {
        return new Callback<T, String>() {
            @Override
            public void onFailure(String reason) {
                fail(reason);
            }

            @Override
            public void onSuccess(T result) {
                succeed(result);
            }
        };
    }
}
