//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.fu;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Analogous to TypeScript's PromiseLike interface.
 */
@JsType(isNative = true, namespace = JsPackage.GLOBAL)
@NullMarked
public interface PromiseLike<T> {
    @JsFunction
    interface ThenOnFulfilledCallbackFn<T extends @Nullable Object, V extends @Nullable Object> {
        @Nullable
        PromiseLike<V> onInvoke(T p0);
    }

    @JsFunction
    interface ThenOnRejectedCallbackFn<V extends @Nullable Object> {
        @Nullable
        PromiseLike<V> onInvoke(Object p0);
    }

    <V extends @Nullable Object> PromiseLike<V> then(
            @Nullable ThenOnFulfilledCallbackFn<? super T, ? extends V> onFulfilled,
            @Nullable ThenOnRejectedCallbackFn<? extends V> onRejected);
}
