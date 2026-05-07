//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.util.QueryConstants;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

/**
 * Wrap {@code long} values for use in JS. Provides text formatting for display and access to the underlying value.
 */
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

    /**
     * Creates a wrapper from an integer string.
     *
     * @param str A string value accepted by {@link Long#parseLong(String)}.
     * @return A wrapper for the parsed value, or {@code null} if the parsed value is {@link QueryConstants#NULL_LONG}.
     */
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

    /**
     * Returns the wrapped value as a number.
     *
     * @return The wrapped {@code long} converted to a {@code double}.
     */
    public double asNumber() {
        return getWrapped();
    }

    /**
     * Returns the string representation of this value.
     *
     * <p>
     * Provided to match JavaScript's {@code valueOf} convention.
     *
     * @return The string form of this value.
     */
    public String valueOf() {
        return toString();
    }

    /**
     * Returns the string representation of the wrapped value.
     */
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
