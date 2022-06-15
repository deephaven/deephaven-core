/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import jsinterop.annotations.JsMethod;

public class LongWrapper {
    private final long value;

    public static LongWrapper of(long value) {
        return new LongWrapper(value);
    }

    @JsMethod(namespace = "dh.LongWrapper")
    public static LongWrapper ofString(String str) {
        return of(Long.parseLong(str));
    }

    protected LongWrapper(long value) {
        this.value = value;
    }

    public long getWrapped() {
        return value;
    }

    @JsMethod
    public double asNumber() {
        return getWrapped();
    }

    @JsMethod
    public String valueOf() {
        return toString();
    }

    @JsMethod
    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
