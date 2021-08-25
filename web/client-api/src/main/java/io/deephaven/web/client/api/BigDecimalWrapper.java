package io.deephaven.web.client.api;

import jsinterop.annotations.JsMethod;

import javax.annotation.Nonnull;
import java.math.BigDecimal;

/**
 * Wrap BigDecimal values for use in JS. Provides text formatting for display and access to the underlying value.
 */
public class BigDecimalWrapper {
    private final BigDecimal value;

    public BigDecimalWrapper(@Nonnull BigDecimal value) {
        this.value = value;
    }

    public BigDecimal getWrapped() {
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
