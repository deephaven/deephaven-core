//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.util.QueryConstants;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
public class LongWrapper {
    private final long value;

    @JsIgnore
    public static LongWrapper of(long value) {
        if (value == QueryConstants.NULL_LONG) {
            return null;
        }
        return new LongWrapper(value);
    }

    public static LongWrapper ofString(String str) {
        return of(Long.parseLong(str));
    }

    @JsIgnore
    protected LongWrapper(long value) {
        this.value = value;
    }

    @JsIgnore
    public long getWrapped() {
        return value;
    }

    public double asNumber() {
        return getWrapped();
    }

    public String valueOf() {
        return toString();
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @JsIgnore
    @Override
    public boolean equals(Object obj) {
        return obj instanceof LongWrapper && ((LongWrapper) obj).value == value;
    }

    @JsIgnore
    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }
}
