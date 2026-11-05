//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.fu;

import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Analogous to TypeScript's PromiseLike interface.
 */
@JsType(isNative = true, namespace = JsPackage.GLOBAL)
@NullMarked
public interface PromiseLike<T> {
    // Callbacks return elemental2's IThenable, so a Java caller can return a Promise from them as usual. A pending
    // table is returned from a callback through asPromise().
    @JsFunction
    interface ThenOnFulfilledCallbackFn<T extends @Nullable Object, V extends @Nullable Object> {
        @Nullable
        IThenable<V> onInvoke(T p0);
    }

    @JsFunction
    interface ThenOnRejectedCallbackFn<V extends @Nullable Object> {
        @Nullable
        IThenable<V> onInvoke(Object p0);
    }

    <V extends @Nullable Object> PromiseLike<V> then(
            @Nullable ThenOnFulfilledCallbackFn<? super T, ? extends V> onFulfilled,
            @Nullable ThenOnRejectedCallbackFn<? extends V> onRejected);

    /**
     * Java convenience matching the one-argument form JS callers use; not a separate JS member.
     */
    @JsOverlay
    default <V extends @Nullable Object> PromiseLike<V> then(
            @Nullable ThenOnFulfilledCallbackFn<? super T, ? extends V> onFulfilled) {
        return then(onFulfilled, null);
    }

    /**
     * Java convenience: this thenable adopted as a {@link Promise}, as {@code Promise.resolve(thenable)} does in JS,
     * for a caller that needs a promise rather than a further {@code then()}. Not a JS member.
     */
    @JsOverlay
    default Promise<T> asPromise() {
        return Promise.resolve(Js.<IThenable<T>>uncheckedCast(this));
    }
}
