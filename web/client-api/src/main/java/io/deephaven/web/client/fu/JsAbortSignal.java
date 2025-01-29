package io.deephaven.web.client.fu;

import elemental2.dom.AbortSignal;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Backport of elemental2 1.2.3's change to include the reason field.
 */
@JsType(isNative = true, namespace = JsPackage.GLOBAL, name = "AbortSignal")
public interface JsAbortSignal extends AbortSignal {
    @JsProperty
    Object getReason();
}
