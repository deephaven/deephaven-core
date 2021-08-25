package io.deephaven.web.client.api;

import jsinterop.annotations.JsMethod;

import javax.annotation.Nonnull;
import java.math.BigInteger;

/**
 * Wrap BigInteger values for use in JS. Provides text formatting for display and access to the underlying value.
 */
public class BigIntegerWrapper {
    private final BigInteger value;

    public BigIntegerWrapper(@Nonnull BigInteger value) {
        this.value = value;
    }

    public BigInteger getWrapped() {
        return value;
    }

    @JsMethod
    public double asNumber() {
        return getWrapped().doubleValue();
    }

    @JsMethod
    public String valueOf() {
        return toString();
    }

    @JsMethod
    @Override
    public String toString() {
        return value.toString();
    }
}
