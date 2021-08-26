package io.deephaven.web.client.fu;

import elemental2.promise.Promise;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Used to get a promise that can be manually cancelled later.
 *
 * We do not extend Promise as js interop is not yet good enough to extend native types.
 *
 * So, instead, we just hack on a "cancel" property pointing to a function that can be invoked only from javascript.
 */
@JsType(namespace = JsPackage.GLOBAL, name = "Promise", isNative = true)
public class CancellablePromise<T> extends Promise<T> {

    @JsProperty
    private JsRunnable cancel;

    public CancellablePromise(PromiseExecutorCallbackFn<T> executor) {
        super(executor);
    }

    @JsOverlay
    public static <T> CancellablePromise<T> from(PromiseExecutorCallbackFn<T> exe, JsRunnable cancel) {
        CancellablePromise<T> promise = new CancellablePromise<>(exe);
        promise.cancel = cancel;
        return promise;
    }

}
