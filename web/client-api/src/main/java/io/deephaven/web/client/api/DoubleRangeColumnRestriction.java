//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.Message;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.grpc.DoubleRangeRestriction;
import io.deephaven.util.annotations.TestUseOnly;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;

/**
 * A {@link ColumnRestriction} that constrains a double column to a closed range {@code [min, max]}. Either bound may be
 * {@code null}, meaning the range is unbounded on that side.
 *
 * <p>
 * <b>This class is intended for testing and demonstrating validation functionality, it is not production ready and may
 * be changed or removed at any time.</b>
 * </p>
 */
@TestUseOnly
@TsName(namespace = "dh")
public class DoubleRangeColumnRestriction extends ColumnRestriction {

    private final DoubleRangeRestriction restriction;

    public DoubleRangeColumnRestriction(DoubleRangeRestriction restriction) {
        this.restriction = restriction;
    }

    @Override
    protected Message getRestriction() {
        return restriction;
    }

    /**
     * The inclusive minimum value allowed, or {@code null} if the range is unbounded below.
     *
     * @return The minimum value, or {@code null}
     */
    @JsProperty
    @JsNullable
    public Double getMin() {
        return restriction.hasMinInclusive() ? restriction.getMinInclusive() : null;
    }

    /**
     * The inclusive maximum value allowed, or {@code null} if the range is unbounded above.
     *
     * @return The maximum value, or {@code null}
     */
    @JsProperty
    @JsNullable
    public Double getMax() {
        return restriction.hasMaxInclusive() ? restriction.getMaxInclusive() : null;
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(@JsNullable Any value) {
        if (value == null) {
            return null;
        }
        return validateImpl(value.asDouble());
    }

    @JsNullable
    String validateImpl(double num) {
        if (restriction.hasMinInclusive() && num < restriction.getMinInclusive()) {
            return "Value " + num + " is less than the minimum allowed value of " + restriction.getMinInclusive();
        }
        if (restriction.hasMaxInclusive() && num > restriction.getMaxInclusive()) {
            return "Value " + num + " is greater than the maximum allowed value of " + restriction.getMaxInclusive();
        }
        return null;
    }

    @Override
    public String toString() {
        return "DoubleRangeColumnRestriction{min=" + getMin() + ", max=" + getMax() + "}";
    }
}

