//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

/**
 * Represents a restriction on an input table column. Restrictions define constraints that the server enforces on
 * column values. There are several types of restrictions:
 * <ul>
 * <li>IntegerRangeRestriction - validates integer values are within a range</li>
 * <li>DoubleRangeRestriction - validates double values are within a range</li>
 * <li>NotNullRestriction - validates values are not null</li>
 * <li>NonEmptyRestriction - validates string values are not empty</li>
 * <li>StringListRestriction - validates string values belong to a set of allowed values</li>
 * </ul>
 */
@TsName(namespace = "dh")
public class ColumnRestriction {
    private final String type;
    private final double minValue;
    private final double maxValue;
    private final JsArray<String> allowedValues;

    /**
     * Creates a range restriction (IntegerRangeRestriction or DoubleRangeRestriction).
     *
     * @param type The type of restriction (e.g., "IntegerRangeRestriction" or "DoubleRangeRestriction")
     * @param minValue The minimum value (inclusive), or NaN if no minimum
     * @param maxValue The maximum value (inclusive), or NaN if no maximum
     */
    public ColumnRestriction(String type, double minValue, double maxValue) {
        this.type = type;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.allowedValues = null;
    }

    /**
     * Creates a string list restriction (StringListRestriction).
     *
     * @param type The type of restriction (e.g. "StringListRestriction")
     * @param allowedValues The array of allowed values
     */
    public ColumnRestriction(String type, JsArray<String> allowedValues) {
        this.type = type;
        this.minValue = Double.NaN;
        this.maxValue = Double.NaN;
        this.allowedValues = allowedValues;
    }

    /**
     * Creates a simple restriction with no parameters (NotNullRestriction or NonEmptyRestriction).
     *
     * @param type The type of restriction (e.g., "NotNullRestriction" or "NonEmptyRestriction")
     */
    public ColumnRestriction(String type) {
        this.type = type;
        this.minValue = Double.NaN;
        this.maxValue = Double.NaN;
        this.allowedValues = null;
    }

    /**
     * The type of restriction. Possible values:
     * <ul>
     * <li>"IntegerRangeRestriction" - integer values must be within a range</li>
     * <li>"DoubleRangeRestriction" - double values must be within a range</li>
     * <li>"NotNullRestriction" - values must not be null</li>
     * <li>"NonEmptyRestriction" - string values must not be empty</li>
     * <li>"StringListRestriction" - string values must be in the allowed list</li>
     * </ul>
     *
     * @return The restriction type
     */
    @JsProperty
    public String getType() {
        return type;
    }

    /**
     * For range restrictions (IntegerRangeRestriction or DoubleRangeRestriction), returns an array with two elements:
     * [minValue, maxValue]. Either value may be NaN if not specified. Returns null for non-range restrictions.
     *
     * @return Array of [min, max] values for range restrictions, or null for other restriction types
     */
    @JsProperty
    @JsNullable
    public JsArray<Double> getRange() {
        if (Double.isNaN(minValue) && Double.isNaN(maxValue)) {
            return null;
        }
        JsArray<Double> range = new JsArray<>();
        range.push(minValue);
        range.push(maxValue);
        return range;
    }

    /**
     * For StringListRestriction, returns the array of allowed values. Returns null for other restriction types.
     *
     * @return Array of allowed values for StringListRestriction, or null for other restriction types
     */
    @JsProperty
    @JsNullable
    public JsArray<String> getValues() {
        return allowedValues;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ColumnRestriction{type='" + type + "'");
        if (!Double.isNaN(minValue) || !Double.isNaN(maxValue)) {
            sb.append(", range=[").append(minValue).append(", ").append(maxValue).append("]");
        }
        if (allowedValues != null) {
            sb.append(", values=").append(allowedValues);
        }
        sb.append("}");
        return sb.toString();
    }
}

