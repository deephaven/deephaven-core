/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

/**
 * Wrap BigDecimal values for use in JS. Provides text formatting for display and access to the underlying value.
 */
@JsType(namespace = "dh")
public class BigDecimalWrapper {
    private final BigDecimal value;

    public BigDecimalWrapper(@Nonnull BigDecimal value) {
        this.value = value;
    }

    public BigDecimal getWrapped() {
        return value;
    }

    @JsIgnore
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
