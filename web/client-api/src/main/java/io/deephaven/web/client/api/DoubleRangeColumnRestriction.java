//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.Any;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.grpc.DoubleRangeRestriction;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverterException;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

/**
 * A {@link ColumnRestriction} that constrains a double column to a closed range {@code [min, max]}. Either bound may be
 * {@code null}, meaning the range is unbounded on that side.
 */
@TsName(namespace = "dh")
public class DoubleRangeColumnRestriction extends ColumnRestriction {

    private final DoubleRangeRestriction restriction;

    @JsIgnore
    public DoubleRangeColumnRestriction(DoubleRangeRestriction restriction) {
        super("DoubleRangeRestriction");
        this.restriction = restriction;
    }

    @JsIgnore
    public static DoubleRangeColumnRestriction fromAny(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        return parseFromAny(restrictionAny, "DoubleRangeRestriction",
                buffer -> new DoubleRangeColumnRestriction(DoubleRangeRestriction.parseFrom(buffer)));
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
    public String validate(Object value) {
        if (value == null) {
            return "Value must not be null";
        }
        double num = ((Number) value).doubleValue();
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

