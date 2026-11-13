//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.dom.AbortSignal;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

/**
 * Optional configuration that can be passed to {@link JsPendingTable#resolve(Object)}.
 */
@JsType(namespace = "dh")
public class ResolveOptions {
    /**
     * Aborting this signal while the operation is pending cancels it as {@code cancel()} would: the promise rejects
     * with the signal's reason as a string, and the table it would have produced is released. Every awaiter of the same
     * pending table is affected, since they share one operation. Unlike {@code cancel()}, aborting after the table was
     * delivered does nothing: the signal means "stop waiting", so a timeout signal cannot close a table already in use.
     */
    @JsNullable
    public AbortSignal signal;

    public ResolveOptions() {

    }

    @JsIgnore
    public ResolveOptions(Object resolveOptions) {
        this();
        JsPropertyMap<Object> map = Js.asPropertyMap(resolveOptions);
        if (map.has("signal")) {
            signal = map.getAsAny("signal").uncheckedCast();
        }
    }
}
