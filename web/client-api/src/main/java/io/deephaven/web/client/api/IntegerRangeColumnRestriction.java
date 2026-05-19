//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

/**
 * A {@link ColumnRestriction} that constrains an integer column to a closed range {@code [min, max]}. Either bound may
 * be {@code null}, meaning the range is unbounded on that side.
 */
@TsName(namespace = "dh")
public class IntegerRangeColumnRestriction extends ColumnRestriction {

    private final Double min;
    private final Double max;

    public IntegerRangeColumnRestriction(Double min, Double max) {
        super("IntegerRangeRestriction");
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
            return "Value " + (long) num + " is less than the minimum allowed value of " + min.longValue();
        }
        if (max != null && num > max) {
            return "Value " + (long) num + " is greater than the maximum allowed value of " + max.longValue();
        }
        return null;
    }

    @Override
    public String toString() {
        return "IntegerRangeColumnRestriction{min=" + min + ", max=" + max + "}";
    }
}

