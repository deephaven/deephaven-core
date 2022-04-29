/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * Functions for casting between types.
 */
@SuppressWarnings("unused")
public class Cast {

    /**
     * Cast can not preserve the value.
     */
    public static class CastDoesNotPreserveValue extends UnsupportedOperationException {

        private static final long serialVersionUID = 5998276378465685974L;

        /**
         * Creates a new exception.
         *
         * @param message error message.
         */
        public CastDoesNotPreserveValue(final String message) {
            super(message);
        }
    }

    private static boolean isLosingPrecisionDouble(final long v) {
        return v > 9007199254740992L || v < -9007199254740992L;
    }

    /**
     * Casts a value to an {@code int}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static int castInt(byte value, boolean checkFidelity) {
        return value == NULL_BYTE ? NULL_INT : value;
    }

    /**
     * Casts a value to an {@code int}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static int castInt(short value, boolean checkFidelity) {
        return value == NULL_SHORT ? NULL_INT : value;
    }

    /**
     * Casts a value to an {@code int}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static int castInt(int value, boolean checkFidelity) {
        return value;
    }

    /**
     * Casts a value to an {@code int}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static int castInt(long value, boolean checkFidelity) {
        if( value == NULL_LONG ){
            return NULL_INT;
        }

        if (checkFidelity && value == NULL_INT) {
            throw new CastDoesNotPreserveValue("Casting is not supported because the value, " + value + ", represents the null integer sentinel and therefore cannot be cast to int.");
        }

        if(checkFidelity && (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)) {
            throw new CastDoesNotPreserveValue("Casting is not supported because the value, " + value + ", overflows while being cast to int.");
        }

        return (int) value;
    }

    /**
     * Casts a value to a {@code long}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static long castLong(byte value, boolean checkFidelity) {
        return value == NULL_BYTE ? NULL_LONG : value;
    }

    /**
     * Casts a value to a {@code long}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static long castLong(short value, boolean checkFidelity) {
        return value == NULL_SHORT ? NULL_LONG : value;
    }

    /**
     * Casts a value to a {@code long}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static long castLong(int value, boolean checkFidelity) {
        return value == NULL_INT ? NULL_LONG : value;
    }

    /**
     * Casts a value to a {@code long}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static long castLong(long value, boolean checkFidelity) {
        return value;
    }

    /**
     * Casts a value to a {@code double}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static double castDouble(byte value, boolean checkFidelity) {
        return value == NULL_BYTE ? NULL_DOUBLE : value;
    }

    /**
     * Casts a value to a {@code double}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static double castDouble(short value, boolean checkFidelity) {
        return value == NULL_SHORT ? NULL_DOUBLE : value;
    }

    /**
     * Casts a value to a {@code double}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static double castDouble(int value, boolean checkFidelity) {
        return value == NULL_INT ? NULL_DOUBLE : value;
    }

    /**
     * Casts a value to a {@code double}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static double castDouble(long value, boolean checkFidelity) {
        if (value == NULL_LONG) {
            return NULL_DOUBLE;
        }

        if (checkFidelity && isLosingPrecisionDouble(value)) {
            throw new CastDoesNotPreserveValue("Casting is not supported because the value, " + value + ", overflows while being cast to double.");
        }

        return value;
    }

    /**
     * Casts a value to a {@code double}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static double castDouble(float value, boolean checkFidelity) {
        return value == NULL_FLOAT ? NULL_DOUBLE : value;
    }

    /**
     * Casts a value to a {@code double}.
     *
     * @param value value
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast value.
     */
    public static double castDouble(double value, boolean checkFidelity) {
        return value;
    }


    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber >

    <#if pt.valueType.isInteger >

    /**
     * Casts a value to an {@code int}.
     *
     * @param value value
     * @return cast value.
     */
    public static int castInt(${pt.primitive} value) {
        return castInt(value, true);
    }

    /**
     * Casts an array to an {@code int} array.
     *
     * @param values values
     * @return cast array.
     */
    public static int[] castInt(${pt.primitive}... values){
        return castInt(values, true);
    }

    /**
     * Casts an array to an {@code int} array.
     *
     * @param values values
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast array.
     */
    public static int[] castInt(${pt.primitive}[] values, boolean checkFidelity) {
        return values == null ? null : castInt(new ${pt.dbArrayDirect}(values), checkFidelity);
    }

    /**
     * Casts an array to an {@code int} array.
     *
     * @param values values
     * @return cast array.
     */
    public static int[] castInt(${pt.dbArray} values){
        return castInt(values, true);
    }

    /**
     * Casts an array to an {@code int} array.
     *
     * @param values values
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast array.
     */
    public static int[] castInt(${pt.dbArray} values, boolean checkFidelity) {
        if (values == null) {
            return null;
        }

        final int s = values.intSize("castInt");
        int[] result = new int[s];

        for (int i = 0; i < result.length; i++) {
            result[i] = castInt(values.get(i), checkFidelity);
        }

        return result;
    }

    /**
     * Casts a value to a {@code long}.
     *
     * @param value value
     * @return cast value.
     */
    public static long castLong(${pt.primitive} value) {
        return castLong(value, true);
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(${pt.primitive}... values){
        return castLong(values, true);
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast array.
     */
    public static long[] castLong(${pt.primitive}[] values, boolean checkFidelity) {
        return values == null ? null : castLong(new ${pt.dbArrayDirect}(values), checkFidelity);
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(${pt.dbArray} values){
        return castLong(values, true);
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast array.
     */
    public static long[] castLong(${pt.dbArray} values, boolean checkFidelity) {
        if (values == null) {
            return null;
        }

        final int s = values.intSize("castLong");
        long[] result = new long[s];

        for (int i = 0; i < result.length; i++) {
            result[i] = castLong(values.get(i), checkFidelity);
        }

        return result;
    }

    </#if>

    /**
     * Casts a value to a {@code double}.
     *
     * @param value value
     * @return cast value.
     */
    public static double castDouble(${pt.primitive} value) {
        return castDouble(value, true);
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(${pt.primitive}... values) {
        return values == null ? null : castDouble(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast array.
     */
    public static double[] castDouble(${pt.primitive}[] values, boolean checkFidelity) {
        return values == null ? null : castDouble(new ${pt.dbArrayDirect}(values), checkFidelity);
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(${pt.dbArray} values) {
        return castDouble(values, true);
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @param checkFidelity check to see if the cast preserves the value.
     * @return cast array.
     */
    public static double[] castDouble(${pt.dbArray} values, boolean checkFidelity) {
        if (values == null) {
            return null;
        }

        final int s = values.intSize("castDouble");
        double[] result = new double[s];

        for (int i = 0; i < result.length; i++) {
            result[i] = castDouble(values.get(i), checkFidelity);
        }

        return result;
    }

    </#if>
    </#list>
}
