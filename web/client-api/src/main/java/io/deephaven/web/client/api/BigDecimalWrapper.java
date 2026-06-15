//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
    /**
     * Creates a wrapper from a decimal string. Strings may optionally start with {@code +}/{@code -}, must have a
     * decimal value, and may end with {@code e}/{@code E} followed by an exponent.
     *
     * @param value A string representation of a decimal number.
     * @return A wrapper for the parsed value.
     */
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

    /**
     * Returns the wrapped value as a number.
     *
     * @return The {@link BigDecimal} value converted to a JS Number.
     */
    public double asNumber() {
        return getWrapped().doubleValue();
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
     * Returns the string representation of the wrapped {@link BigDecimal}.
     */
    @Override
    public String toString() {
        return value.toString();
    }

    /**
     * Compares this wrapper to another object.
     *
     * @param o The object to compare.
     * @return {@code true} if {@code o} is a {@link BigDecimalWrapper} with an equal wrapped value.
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BigDecimalWrapper that = (BigDecimalWrapper) o;
        return value.equals(that.value);
    }

    /**
     * Returns a hash code based on the wrapped value.
     */
    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
