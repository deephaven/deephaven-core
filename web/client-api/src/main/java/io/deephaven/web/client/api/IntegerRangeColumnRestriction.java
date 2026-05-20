//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.Any;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.grpc.IntegerRangeRestriction;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverterException;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

/**
 * A {@link ColumnRestriction} that constrains an integer column to a closed range {@code [min, max]}. Either bound may
 * be {@code null}, meaning the range is unbounded on that side.
 */
@TsName(namespace = "dh")
public class IntegerRangeColumnRestriction extends ColumnRestriction {

    private final IntegerRangeRestriction restriction;

    @JsIgnore
    public IntegerRangeColumnRestriction(IntegerRangeRestriction restriction) {
        super("IntegerRangeRestriction");
        this.restriction = restriction;
    }

    @JsIgnore
    public static IntegerRangeColumnRestriction fromAny(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        return parseFromAny(restrictionAny, "IntegerRangeRestriction",
                buffer -> new IntegerRangeColumnRestriction(IntegerRangeRestriction.parseFrom(buffer)));
    }

    /**
     * The inclusive minimum value allowed, or {@code null} if the range is unbounded below.
     *
     * @return The minimum value as a {@link LongWrapper}, or {@code null}
     */
    @JsProperty
    @JsNullable
    public LongWrapper getMin() {
        return restriction.hasMinInclusive() ? LongWrapper.of(restriction.getMinInclusive()) : null;
    }

    /**
     * The inclusive maximum value allowed, or {@code null} if the range is unbounded above.
     *
     * @return The maximum value as a {@link LongWrapper}, or {@code null}
     */
    @JsProperty
    @JsNullable
    public LongWrapper getMax() {
        return restriction.hasMaxInclusive() ? LongWrapper.of(restriction.getMaxInclusive()) : null;
    }

    @Override
    @JsMethod
    @JsNullable
    public String validate(Object value) {
        if (value == null) {
            return "Value must not be null";
        }
        if (!(value instanceof LongWrapper)) {
            return "Value must be a LongWrapper";
        }
        long num = ((LongWrapper) value).getWrapped();
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
        LongWrapper min = getMin();
        LongWrapper max = getMax();
        return "IntegerRangeColumnRestriction{min=" + (min != null ? min : "null") + ", max="
                + (max != null ? max : "null") + "}";
    }
}

