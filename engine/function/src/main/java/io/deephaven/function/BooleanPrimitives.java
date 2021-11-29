/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.BooleanVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.BooleanVectorDirect;

import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;

/**
 * A set of commonly used functions that can be applied to Boolean types.
 */
public class BooleanPrimitives {
    /**
     * Returns if a value is null.
     *
     * @param value value.
     * @return true if the value is null, and false otherwise.
     */
    static public boolean isNull(Boolean value){
        return value == null;
    }

    /**
     * Replaces null values with a default value.
     *
     * @param value value.
     * @param defaultValue default value to return for null values.
     * @return value, if value is not null, and defaultValue if value is null.
     */
    static public Boolean nullToValue(Boolean value, boolean defaultValue) {
        if (isNull(value)) {
            return defaultValue;
        } else {
            return value;
        }
    }

    /**
     * Replaces null values with a default value.
     *
     * @param values values.
     * @param defaultValue default value to return for null values.
     * @return value, if value is not null, and defaultValue if value is null.
     */
    static public Boolean[] nullToValue(BooleanVector values, boolean defaultValue) {
        Boolean[] result = new Boolean[values.intSize("nullToValue")];

        for (int i = 0; i < values.size(); i++) {
            result[i] = nullToValue(values.get(i), defaultValue);
        }

        return result;
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public int count(BooleanVector values){
        if (values == null){
            return 0;
        }
        int count = 0;
        for (int i = 0; i < values.size();i++) {
            if (!isNull(values.get(i))) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the last value from an array.
     *
     * @param values values.
     * @return last value from the array.
     */
    static public Boolean last(Boolean... values) {
        if (values.length == 0) {
            return NULL_BOOLEAN;
        } else {
            return values[values.length - 1];
        }
    }

    /**
     * Returns the last value from an array.
     *
     * @param values values.
     * @return last value from the array.
     */
    static public Boolean last(boolean... values) {
        if (values.length == 0) {
            return NULL_BOOLEAN;
        } else {
            return values[values.length - 1];
        }
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public Boolean first(Boolean... values) {
        if (values.length == 0) {
            return NULL_BOOLEAN;
        } else {
            return values[0];
        }
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public Boolean first(boolean... values) {
        if (values.length == 0) {
            return NULL_BOOLEAN;
        } else {
            return values[0];
        }
    }

    /**
     * Returns the nth value from an array.
     *
     * @param index index of the value to return.
     * @param values values.
     * @return nth value from the array or null, if the index is outside the array's index range.
     */
    static public Boolean nth(int index, BooleanVector values){
        if(index < 0 || index >= values.size()){
            return NULL_BOOLEAN;
        }

        return values.get(index);
    }

    /**
     * Returns the nth value from an array.
     *
     * @param index index of the value to return.
     * @param values values.
     * @return nth value from the array or null, if the index is outside the array's index range.
     */
    static public Boolean nth(int index, Boolean[] values){
        return nth(index, array(values));
    }

    /**
     * Converts a Vector to a primitive array.
     *
     * @param values Vector
     * @return primitive array.
     */
    public static Boolean[] vec(BooleanVector values) {
        return values.toArray();
    }

    /**
     * Converts a primitive array to a Vector.
     *
     * @param values primitive array
     * @return Vector.
     */
    public static BooleanVector array(Boolean[] values) {
        return new BooleanVectorDirect(values);
    }

    /**
     * Ands all of the values in the array together.  Null values are ignored.
     *
     * @param values values.
     * @return logical and of all the values in the array.
     */
    static public Boolean and(Boolean... values) {
        for (Boolean b : values) {
            if (!isNull(b) && !b) {
                return false;
            }
        }

        return true;
    }

    /**
     * Ands all of the values in the array together.
     *
     * @param values values.
     * @return logical and of all the values in the array.
     */
    static public Boolean and(boolean... values) {
        for (Boolean b : values) {
            if (!b) {
                return false;
            }
        }

        return true;
    }

    /**
     * Ands all of the values in the array together.  Null values are ignored.
     *
     * @param values values.
     * @return logical and of all the values in the array.
     */
    static public Boolean and(ObjectVector<Boolean> values) {
        for (int ii = 0; ii < values.size(); ++ii) {
            Boolean b = values.get(ii);
            if (!isNull(b) && !b) {
                return false;
            }
        }

        return true;
    }

    /**
     * Ands all of the values in the array together.
     *
     * @param values values.
     * @param nullValue value to use in place of null values.
     * @return logical and of all the values in the array.
     */
    static public Boolean and(Boolean[] values, Boolean nullValue) {
        for (Boolean b : values) {
            b = b==null ? nullValue : b;

            if (!b) {
                return false;
            }
        }

        return true;
    }

    /**
     * Ands all of the values in the array together.
     *
     * @param values values.
     * @param nullValue value to use in place of null values.
     * @return logical and of all the values in the array.
     */
    static public Boolean and(ObjectVector<Boolean> values, Boolean nullValue) {
        for (int ii = 0; ii < values.size(); ++ii) {
            Boolean b = values.get(ii);
            b = b==null ? nullValue : b;

            if (!b) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the sum of the values in an array.  For booleans, this is equivalent to "or".  Null values are ignored.
     *
     * @param values values.
     * @return sum of the values.  This is equivalent to "or".
     */
    static public Boolean sum(Boolean... values) {
        return or(values);
    }

    /**
     * Returns the sum of the values in an array.  For booleans, this is equivalent to "or".
     *
     * @param values values.
     * @return sum of the values.  This is equivalent to "or".
     */
    static public Boolean sum(boolean... values) {
        return or(values);
    }

    /**
     * Ors all of the values in the array together.  Null values are ignored.
     *
     * @param values values.
     * @return logical or of all the values in the array.
     */
    static public Boolean or(Boolean... values) {
        for (Boolean b : values) {
            if (!isNull(b) && b) {
                return true;
            }
        }

        return false;
    }

    /**
     * Ors all of the values in the array together.
     *
     * @param values values.
     * @return logical or of all the values in the array.
     */
    static public Boolean or(boolean... values) {
        for (Boolean b : values) {
            if (b) {
                return true;
            }
        }

        return false;
    }

    /**
     * Ors all of the values in the array together.
     *
     * @param values values.
     * @param nullValue value to use in place of null values.
     * @return logical or of all the values in the array.
     */
    static public Boolean or(Boolean[] values, Boolean nullValue) {
        for (Boolean b : values) {
            b = b==null ? nullValue : b;

            if (b) {
                return true;
            }
        }

        return false;
    }

    /**
     * Not of all values in an array.
     *
     * @param values values.
     * @return logical not of all the values in the array.
     */
    static public Boolean[] not(Boolean... values) {
        if (values == null) {
            return null;
        }

        Boolean[] result = new Boolean[values.length];

        for (int i = 0; i < values.length; i++) {
            result[i] = values[i] == null ? null : !values[i];
        }

        return result;
    }

    /**
     * Not of all values in an array.
     *
     * @param values values.
     * @return logical not of all the values in the array.
     */
    static public Boolean[] not(boolean... values) {
        if (values == null) {
            return null;
        }

        Boolean[] result = new Boolean[values.length];

        for (int i = 0; i < values.length; i++) {
            result[i] = !values[i];
        }

        return result;
    }

}