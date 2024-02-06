package io.deephaven.web.client.fu;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(namespace = JsPackage.GLOBAL, name = "WeakRef", isNative = true)
public class JsWeakRef<T> {
    public JsWeakRef(T target) {

    }

    public native T deref();
}
