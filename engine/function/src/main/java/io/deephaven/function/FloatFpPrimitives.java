/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

/**
 * A set of commonly used floating point numeric functions that can be applied to Float types.
 */
@SuppressWarnings("WeakerAccess")
public class FloatFpPrimitives {

    /**
     * Returns {@code true} if the value is NaN and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is NaN and {@code false} otherwise.
     */
    static public boolean isNaN(float value){
        return Float.isNaN(value);
    }

    /**
     * Returns {@code true} if the value is infinite and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is infinite and {@code false} otherwise.
     */
    static public boolean isInf(float value){
        return Float.isInfinite(value);
    }

    /**
     * Returns {@code true} if the value is normal, where "normal" is defined as not infinite, not NaN, and not null.
     *
     * @param value value.
     * @return {@code true} if the value is not infinite, NaN, nor null; {@code false} otherwise
     */
    static public boolean isNormal(float value) {
        return !Float.isInfinite(value) && !Float.isNaN(value) && !FloatPrimitives.isNull(value);
    }

    /**
     * Returns {@code true} if the values contains any non-normal value, where "normal" is defined as
     * not infinite, not NaN, and not null.
     *
     * @param values values.
     * @return {@code true} if any value is not {@link #isNormal(float) normal}; {@code false} otherwise.
     * @see #isNormal(float)
     */
    static public boolean containsNonNormal(Float ... values) {
        for (Float v1 : values) {
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
     * @return {@code true} if any value is not {@link #isNormal(float) normal}; {@code false} otherwise.
     * @see #isNormal(float)
     */
    static public boolean containsNonNormal(float[] values) {
        for (float v1 : values) {
            if (!isNormal(v1)) {
                return true;
            }
        }

        return false;
    }
}