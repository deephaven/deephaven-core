/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatFpPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

/**
 * A set of commonly used doubleing point numeric functions that can be applied to Double types.
 */
@SuppressWarnings("WeakerAccess")
public class DoubleFpPrimitives {

    /**
     * Returns {@code true} if the value is NaN and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is NaN and {@code false} otherwise.
     */
    static public boolean isNaN(double value){
        return Double.isNaN(value);
    }

    /**
     * Returns {@code true} if the value is infinite and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is infinite and {@code false} otherwise.
     */
    static public boolean isInf(double value){
        return Double.isInfinite(value);
    }

    /**
     * Returns {@code true} if the value is normal, where "normal" is defined as not infinite, not NaN, and not null.
     *
     * @param value value.
     * @return {@code true} if the value is not infinite, NaN, nor null; {@code false} otherwise
     */
    static public boolean isNormal(double value) {
        return !Double.isInfinite(value) && !Double.isNaN(value) && !DoublePrimitives.isNull(value);
    }

    /**
     * Returns {@code true} if the values contains any non-normal value, where "normal" is defined as
     * not infinite, not NaN, and not null.
     *
     * @param values values.
     * @return {@code true} if any value is not {@link #isNormal(double) normal}; {@code false} otherwise.
     * @see #isNormal(double)
     */
    static public boolean containsNonNormal(Double ... values) {
        for (Double v1 : values) {
            if (v1 == null || !isNormal(v1)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns {@code true} if the values contains any non-normal value, where "normal" is defined as
     * not infinite, not NaN, and not null.
     *
     * @param values values.
     * @return {@code true} if any value is not {@link #isNormal(double) normal}; {@code false} otherwise.
     * @see #isNormal(double)
     */
    static public boolean containsNonNormal(double[] values) {
        for (double v1 : values) {
            if (!isNormal(v1)) {
                return true;
            }
        }

        return false;
    }
}