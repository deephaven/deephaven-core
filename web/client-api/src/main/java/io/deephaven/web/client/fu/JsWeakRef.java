//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.fu;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(namespace = JsPackage.GLOBAL, name = "WeakRef", isNative = true)
public class JsWeakRef<T> {
    public JsWeakRef(T target) {

    }

    public native T deref();
}
