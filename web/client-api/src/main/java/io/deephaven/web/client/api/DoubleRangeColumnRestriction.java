//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

/**
 * A {@link ColumnRestriction} that constrains a double column to a closed range {@code [min, max]}. Either bound may
 * be {@code null}, meaning the range is unbounded on that side.
 */
@TsName(namespace = "dh")
public class DoubleRangeColumnRestriction extends ColumnRestriction {

    private final Double min;
    private final Double max;

    public DoubleRangeColumnRestriction(@JsNullable Double min, @JsNullable Double max) {
        super("DoubleRangeRestriction", null);
        this.min = min;
        this.max = max;
    }

    /**
     * The inclusive minimum value allowed, or {@code null} if the range is unbounded below.
     *
     * @return The minimum value, or {@code null}
     */
    @JsProperty
    @JsNullable
    public Double getMin() {
        return min;
    }

    /**
     * The inclusive maximum value allowed, or {@code null} if the range is unbounded above.
     *
     * @return The maximum value, or {@code null}
     */
    @JsProperty
    @JsNullable
    public Double getMax() {
        return max;
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(Object value) {
        if (value == null) {
            return null;
        }
        double num = ((Number) value).doubleValue();
        if (min != null && num < min) {
            return "Value " + num + " is less than the minimum allowed value of " + min;
        }
        if (max != null && num > max) {
            return "Value " + num + " is greater than the maximum allowed value of " + max;
        }
        return null;
    }

    @Override
    public String toString() {
        return "DoubleRangeColumnRestriction{min=" + min + ", max=" + max + "}";
    }
}

