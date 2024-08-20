//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import javax.annotation.Nonnull;
import java.math.BigInteger;

/**
 * Wrap BigInteger values for use in JS. Provides text formatting for display and access to the underlying value.
 */
@JsType(namespace = "dh")
public class BigIntegerWrapper {
    public static BigIntegerWrapper ofString(String str) {
        return new BigIntegerWrapper(new BigInteger(str));
    }

    private final BigInteger value;

    @JsIgnore
    public BigIntegerWrapper(@Nonnull BigInteger value) {
        this.value = value;
    }

    @JsIgnore
    public BigInteger getWrapped() {
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
}
