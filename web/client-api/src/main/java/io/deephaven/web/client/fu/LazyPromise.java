package io.deephaven.web.client.fu;

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
 * If you want to create a promise early in a method's scope, and then configure it's handling of
 * resolve/reject callbacks later, or in a conditional nature, than LazyPromise is for you!
 *
 */
public class LazyPromise<T> implements PromiseLike<T> {

    private T succeeded;
    private boolean isSuccess;
    private Object failed;
    private final JsArray<JsConsumer<T>> onResolved = new JsArray<>();
    private final JsArray<JsConsumer<Object>> onRejected = new JsArray<>();

    private boolean cancelled;
    private boolean isScheduled;

    @JsIgnore
    public static void runLater(JsRunnable task) {
        Promise.resolve((Object) null).then(ignored -> {
            task.run();
            return null;
        });
    }

    /**
     * @param timeout How many millis to wait until failing the promise with a timeout.
     * @return a real promise that we will resolve when we are resolved.
     *
     *         This method overload is not strictly necessary to call when explicitly wiring up
     *         failure handling for this LazyPromise which you can guarantee will be eventually
     *         called.
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
     *         Use this method if you are safely wiring up failure handling for this LazyPromise. If
     *         you aren't explicitly wiring up calls to {@link #fail(Object)} this LazyPromise, then
     *         you should setup a timeout (see {@link #asPromise(int)} and {@link #timeout(int)}
     */
    public final Promise<T> asPromise() {
        return new Promise<>(((resolve, reject) -> {
            if (spyReject(reject)) {
                spyResolve(resolve);
            }
        }));
    }

    public final CancellablePromise<T> asPromise(JsRunnable cancel) {
        return CancellablePromise.from((resolve, reject) -> {
            if (spyReject(reject)) {
                spyResolve(resolve);
            }
        }, cancel.beforeMe(this::cancel));
    }

    private void cancel() {
        if (isSuccess()) {
            // already succeeded.
            JsLog.debug("Cancelled promise after it succeeded", this);
        } else if (isFailure()) {
            // already failed
            JsLog.debug("Cancelled promise after it failed with", failed, this);
        } else {
            // not resolved yet
            fail(CANCELLATION_MESSAGE);
        }
        // we set this last because fail will drop the message if it's true when running.
        cancelled = true;
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

    protected boolean spyReject(RejectCallbackFn reject) {
        if (cancelled) {
            if (failed == null) {
                failed = CANCELLATION_MESSAGE;
            }
        }
        if (failed != null) {
            runLater(() -> reject.onInvoke(failed));
            return false;
        }
        onRejected.push(reject::onInvoke);
        return true;
    }

    protected void spyResolve(ResolveCallbackFn<T> resolve) {
        // we ignore cancellation here because we are only called if spyReject returns true,
        // and it will return false when cancelled.
        if (failed == null && succeeded != null) {
            runLater(() -> resolve.onInvoke(succeeded));
        } else {
            onResolved.push(resolve::onInvoke);
        }
    }

    public boolean isUnresolved() {
        return failed == null && succeeded == null;
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

    public void fail(Object reason) {
        if (cancelled) {
            // already cancelled...
            JsLog.debug("Got failure after cancellation", this, reason);
        } else if (isSuccess) {
            // Perhaps downgrade to a runtime exception, or allowing failure after success
            // (though that gets into dicey race conditions I want no part of).
            throw new AssertionError("Trying to fail after succeeding");
        } else {
            this.failed = reason;
        }
        runLater(this::flushCallbacks);
    }

    public void succeed(T value) {
        if (cancelled) {
            JsLog.debug("Got success after cancellation", this, value);
        } else if (failed != null) {
            // Perhaps downgrade to a runtime exception...
            throw new AssertionError("Trying to succeed after failing with message: " + failed);
        } else {
            this.isSuccess = true;
            // just storing value is not good enough, since we can be resolved w/ null
            this.succeeded = value;
        }
        runLater(this::flushCallbacks);
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
                    onRejected.pop().apply(failed);
                }
            } else if (isSuccess()) {
                while (onResolved.length > 0) {
                    onResolved.pop().apply(succeeded);
                }
            } else {
                assert isUnresolved();
            }
            onResolved.length = 0;
            onRejected.length = 0;
        });
    }

    @Override
    public void onSuccess(JsConsumer<T> success) {
        if (isFailure() || cancelled) {
            return;
        }
        onResolved.push(success);
        if (isSuccess()) {
            flushCallbacks();
        }
    }

    @Override
    public void onFailure(JsConsumer<Object> failure) {
        if (isSuccess() || cancelled) {
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
     * Rather than resolve immediately, this forces asynchronicity, to give the calling code time to
     * unwind its stack before running.
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
            if (!isSuccess && failed == null && !cancelled) {
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
     * Only use this when it is preferable to print a nice error message instead of leaving uncaught
     * promises to bubble (confusing) stack traces into console, especially if it is reasonable for
     * a given promise to fail.
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
