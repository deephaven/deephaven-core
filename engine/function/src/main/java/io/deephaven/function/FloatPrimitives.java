/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.FloatVector;
import io.deephaven.vector.FloatVectorDirect;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.set.TFloatSet;
import gnu.trove.set.hash.TFloatHashSet;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * A set of commonly used functions that can be applied to Float types.
 */
public class FloatPrimitives {
    /**
     * Unboxes a array of values.
     *
     * @param values values.
     * @return unboxed array of values.
     */
    public static float[] unbox(Float... values) {
        if(values == null){
            return null;
        }

        float[] result = new float[values.length];

        for(int i=0; i<values.length; i++){
            Float v = values[i];

            if(v == null || isNull(v)) {
                result[i] = NULL_FLOAT;
            } else {
                result[i] = v;
            }
        }

        return result;
    }

    /**
     * Determines if a value is null.
     *
     * @param value value.
     * @return true if the value is null, and false otherwise.
     */
    static public boolean isNull(float value){
        return value == NULL_FLOAT;
    }

    /**
     * Replaces null values with a default value.
     *
     * @param value value.
     * @param defaultValue default value to return for null values.
     * @return value, if value is not null, and defaultValue if value is null.
     */
    static public float nullToValue(float value, float defaultValue) {
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
     * @return values with nulls replaced by defaultValue.
     */
    static public float[] nullToValue(float[] values, float defaultValue) {
        return nullToValue(new FloatVectorDirect(values), defaultValue);
    }

    /**
     * Replaces null values with a default value.
     *
     * @param values values.
     * @param defaultValue default value to return for null values.
     * @return values with nulls replaced by defaultValue.
     */
    static public float[] nullToValue(FloatVector values, float defaultValue) {
        float[] result = new float[LongSizedDataStructure.intSize("nullToValue", values.size())];

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
    static public int count(Float[] values){
        if (values == null){
            return 0;
        }
        int count = 0;
        for (Float value : values) {
            if (value != null && !isNull(value)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public int count(float[] values){
        if (values == null){
            return 0;
        }

        return count(new FloatVectorDirect(values));
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public int count(FloatVector values){
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
    static public float last(FloatVector values){
        if(values == null || values.size() < 1){
            return NULL_FLOAT;
        }

        return values.get(values.size() - 1);
    }

    /**
     * Returns the last value from an array.
     *
     * @param values values.
     * @return last value from the array.
     */
    static public float last(float[] values){
        if(values == null || values.length < 1){
            return NULL_FLOAT;
        }

        return values[values.length - 1];
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public float first(FloatVector values){
        if(values == null || values.size() < 1){
            return NULL_FLOAT;
        }

        return values.get(0);
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public float first(float[] values){
        if(values == null || values.length < 1){
            return NULL_FLOAT;
        }

        return first(array(values));
    }

    /**
     * Returns the nth value from an array.
     *
     * @param index index of the value to return.
     * @param values values.
     * @return nth value from the array or null, if the index is outside of the array's index range.
     */
    static public float nth(int index, FloatVector values){
        if(index < 0 || index >= values.size()){
            return NULL_FLOAT;
        }

        return values.get(index);
    }

    /**
     * Returns the nth value from an array.
     *
     * @param index index of the value to return.
     * @param values values.
     * @return nth value from the array or null, if the index is outside of the array's index range.
     */
    static public float nth(int index, float[] values){
        return nth(index, array(values));
    }

    /**
     * Converts a Vector to a primitive array.
     *
     * @param values Vector
     * @return primitive array.
     */
    public static float[] vec(FloatVector values) {
        if(values == null){
            return null;
        }

        return values.toArray();
    }

    /**
     * Converts a primitive array to a Vector.
     *
     * @param values primitive array
     * @return Vector.
     */
    public static FloatVector array(float[] values) {
        if(values == null){
            return null;
        }

        return new FloatVectorDirect(values);
    }

    /**
     * Checks if a value is within a range.
     *
     * @param testedValue tested value.
     * @param lowInclusiveValue lower inclusive bound of the range.
     * @param highInclusiveValue upper inclusive bound of the range.
     * @return true if the tested value is within the range, and false if the tested value is not in the range or is null.
     */
    static public boolean inRange(float testedValue,float lowInclusiveValue,float highInclusiveValue){
        if (isNull(testedValue)) {
            return false;
        }
        if (testedValue >= lowInclusiveValue && testedValue <= highInclusiveValue) {
            return true;
        }
        return false;
    }

    /**
     * Checks if a value is within a discrete set of possible values.
     *
     * @param testedValues tested value.
     * @param possibleValues possible values.
     * @return true if the tested value is contained in the possible values, and false otherwise.
     */
    static public boolean in(float testedValues,float... possibleValues){
        for (int i = 0; i < possibleValues.length; i++) {
            if (testedValues == possibleValues[i]){
                return true;
            }
        }
        return false;
    }


    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @return number of distinct non-null values.
     */
    public static long countDistinct(final float[] values) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new FloatVectorDirect(values));
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @return number of distinct non-null values.
     */
    public static long countDistinct(final FloatVector values) {
        return countDistinct(values, false);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final float[] values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new FloatVectorDirect(values), countNull);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final FloatVector values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        if(values.size() == 0) {
            return 0;
        }

        if(values.size() == 1) {
            return !countNull && values.get(0) == QueryConstants.NULL_FLOAT ? 0 : 1;
        }

        final TFloatSet keys = new TFloatHashSet();
        for(int ii = 0; ii < values.size(); ii++) {
            keys.add(values.get(ii));
        }

        if(!countNull) {
            keys.remove(NULL_FLOAT);
        }

        return keys.size();
    }

    /**
     * Get the single unique value in the array, or null if there are none, or there are more than 1 distinct values.
     *
     * @param arr the array
     * @param countNull if nulls should count as values
     * @return the single unique value in the array, or null.
     */
    public static float uniqueValue(final FloatVector arr, boolean countNull) {
        if(arr == null || arr.isEmpty()) {
            return NULL_FLOAT;
        }

        if(arr.size() == 1) {
            return arr.get(0);
        }

        final TFloatSet keys = new TFloatHashSet();
        for(int ii = 0; ii < arr.size(); ii++) {
            keys.add(arr.get(ii));
        }

        if(!countNull) {
            keys.remove(NULL_FLOAT);
        }

        return keys.size() == 1 ? keys.iterator().next() : NULL_FLOAT;
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static float[] distinct(final float[] values) {
        if(values == null) {
            return null;
        }

        return distinct(new FloatVectorDirect(values)).toArray();
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static FloatVector distinct(final FloatVector values) {
        if(values == null) {
            return null;
        }

        return distinct(values, false, false);
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @param includeNull true to include null values, and false to exclude null values.
     * @param sort true to sort the resultant array
     * @return array containing only distinct items from arr.
     */
    public static float[] distinct(final float[] values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values == null) {
            return null;
        }

        if(values.length == 0) {
            return new float[0];
        }

        if(values.length == 1) {
            return !includeNull && values[0] == QueryConstants.NULL_FLOAT ? new float[0] : values;
        }

        final TFloatArrayList orderedList = new TFloatArrayList();
        final TFloatSet counts = new TFloatHashSet();
        for(int ii = 0; ii < values.length; ii++) {
            float val = values[ii];
            if((includeNull || val != NULL_FLOAT) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        final float[] data;
        if(sort) {
            orderedList.sort();
            data = orderedList.toArray();
            // region SortFixup
            // endregion SortFixup
        } else {
            data = orderedList.toArray();
        }

        return data;
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @param includeNull true to include null values, and false to exclude null values.
     * @param sort true to sort the resultant array
     * @return array containing only distinct items from arr.
     */
    public static FloatVector distinct(final FloatVector values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values.size() == 0) {
            return new FloatVectorDirect();
        }

        if(values.size() == 1) {
            return !includeNull && values.get(0) == QueryConstants.NULL_FLOAT ? new FloatVectorDirect() : values;
        }

        final TFloatArrayList orderedList = new TFloatArrayList();
        final TFloatSet counts = new TFloatHashSet();
        for(int ii = 0; ii < values.size(); ii++) {
            float val = values.get(ii);
            if((includeNull || val != NULL_FLOAT) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        final float[] data;
        if(sort) {
            orderedList.sort();
            data = orderedList.toArray();
            // region SortFixup
            // endregion SortFixup
        } else {
            data = orderedList.toArray();
        }

        return new FloatVectorDirect(data);
    }

    /**
     * Returns an array with a value repeated.
     *
     * @param value value.
     * @param size number of times to repeat the value.
     * @return array of repeated values.  If {@code size} is less than zero, an empty array is returned.
     */
    public static float[] repeat(float value, int size) {
        if(size < 0){
            return new float[0];
        }

        final float[] result = new float[size];

        for(int i=0; i<size; i++){
            result[i] = value;
        }

        return result;
    }

    /**
     * Returns a list containing its arguments.
     *
     * @param values values.
     * @return list containing values.
     */
    public static float[] enlist(float... values){
        if(values == null){
            return new float[0];
        }

        return values;
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static float[] concat(float[]... values){
        if(values == null){
            return new float[0];
        }

        return concat(Arrays.stream(values).map(e->e==null?null:new FloatVectorDirect(e)).toArray(FloatVector[]::new));
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static float[] concat(FloatVector... values){
        if(values == null){
            return new float[0];
        }

        int n = 0;

        for (FloatVector v : values) {
            if (v != null) {
                n += v.size();
            }
        }

        final float[] result = new float[n];
        int idx = 0;

        for (FloatVector v : values) {
            if (v != null) {
                for (int i = 0; i < v.size(); i++) {
                    result[idx] = v.get(i);
                    idx++;
                }
            }
        }

        return result;
    }

    /**
     * Returns an array with the values reversed.
     *
     * @param values values.
     * @return array with the values reversed.
     */
    public static float[] reverse(float... values){
        if(values == null){
            return null;
        }

        return reverse(new FloatVectorDirect(values));
    }

    /**
     * Returns an array with the values reversed.
     *
     * @param values values.
     * @return array with the values reversed.
     */
    public static float[] reverse(FloatVector values){
        if(values == null){
            return null;
        }

        final float[] result = new float[(int) values.size()];

        for(int i=0; i<values.size(); i++){
            result[i] = values.get(values.size()-1-i);
        }

        return result;
    }
}
