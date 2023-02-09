/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

import javax.annotation.Nonnull;
import java.math.BigInteger;

/**
 * Wrap BigInteger values for use in JS. Provides text formatting for display and access to the underlying value.
 */
@TsInterface
@JsType(namespace = "dh")//TODO instead make a factory method?
public class BigIntegerWrapper {
    private final BigInteger value;

    public static BigIntegerWrapper ofString(String str) {
        return new BigIntegerWrapper(new BigInteger(str));
    }

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
