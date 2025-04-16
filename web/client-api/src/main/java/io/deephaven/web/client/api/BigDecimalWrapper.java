//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

/**
 * Wrap BigDecimal values for use in JS. Provides text formatting for display and access to the underlying value.
 */
@JsType(namespace = "dh")
public class BigDecimalWrapper {
    public static BigDecimalWrapper ofString(String value) {
        return new BigDecimalWrapper(new BigDecimal(value));
    }

    private final BigDecimal value;

    @JsIgnore
    public BigDecimalWrapper(@Nonnull BigDecimal value) {
        this.value = value;
    }

    @JsIgnore
    public BigDecimal getWrapped() {
        return value;
    }

    public double asNumber() {
        return getWrapped().doubleValue();
    }

    public String valueOf() {
        return toString();
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BigDecimalWrapper that = (BigDecimalWrapper) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
