/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.ShortVector;
import io.deephaven.vector.ShortVectorDirect;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import gnu.trove.list.array.TShortArrayList;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * A set of commonly used functions that can be applied to Short types.
 */
public class ShortPrimitives {
    /**
     * Unboxes a array of values.
     *
     * @param values values.
     * @return unboxed array of values.
     */
    public static short[] unbox(Short... values) {
        if(values == null){
            return null;
        }

        short[] result = new short[values.length];

        for(int i=0; i<values.length; i++){
            Short v = values[i];

            if(v == null || isNull(v)) {
                result[i] = NULL_SHORT;
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
    static public boolean isNull(short value){
        return value == NULL_SHORT;
    }

    /**
     * Replaces null values with a default value.
     *
     * @param value value.
     * @param defaultValue default value to return for null values.
     * @return value, if value is not null, and defaultValue if value is null.
     */
    static public short nullToValue(short value, short defaultValue) {
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
    static public short[] nullToValue(short[] values, short defaultValue) {
        return nullToValue(new ShortVectorDirect(values), defaultValue);
    }

    /**
     * Replaces null values with a default value.
     *
     * @param values values.
     * @param defaultValue default value to return for null values.
     * @return values with nulls replaced by defaultValue.
     */
    static public short[] nullToValue(ShortVector values, short defaultValue) {
        short[] result = new short[LongSizedDataStructure.intSize("nullToValue", values.size())];

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
    static public int count(Short[] values){
        if (values == null){
            return 0;
        }
        int count = 0;
        for (Short value : values) {
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
    static public int count(short[] values){
        if (values == null){
            return 0;
        }

        return count(new ShortVectorDirect(values));
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public int count(ShortVector values){
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
    static public short last(ShortVector values){
        if(values == null || values.size() < 1){
            return NULL_SHORT;
        }

        return values.get(values.size() - 1);
    }

    /**
     * Returns the last value from an array.
     *
     * @param values values.
     * @return last value from the array.
     */
    static public short last(short[] values){
        if(values == null || values.length < 1){
            return NULL_SHORT;
        }

        return values[values.length - 1];
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public short first(ShortVector values){
        if(values == null || values.size() < 1){
            return NULL_SHORT;
        }

        return values.get(0);
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public short first(short[] values){
        if(values == null || values.length < 1){
            return NULL_SHORT;
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
    static public short nth(int index, ShortVector values){
        if(index < 0 || index >= values.size()){
            return NULL_SHORT;
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
    static public short nth(int index, short[] values){
        return nth(index, array(values));
    }

    /**
     * Converts a Vector to a primitive array.
     *
     * @param values Vector
     * @return primitive array.
     */
    public static short[] vec(ShortVector values) {
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
    public static ShortVector array(short[] values) {
        if(values == null){
            return null;
        }

        return new ShortVectorDirect(values);
    }

    /**
     * Checks if a value is within a range.
     *
     * @param testedValue tested value.
     * @param lowInclusiveValue lower inclusive bound of the range.
     * @param highInclusiveValue upper inclusive bound of the range.
     * @return true if the tested value is within the range, and false if the tested value is not in the range or is null.
     */
    static public boolean inRange(short testedValue,short lowInclusiveValue,short highInclusiveValue){
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
    static public boolean in(short testedValues,short... possibleValues){
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
    public static long countDistinct(final short[] values) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new ShortVectorDirect(values));
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @return number of distinct non-null values.
     */
    public static long countDistinct(final ShortVector values) {
        return countDistinct(values, false);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final short[] values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new ShortVectorDirect(values), countNull);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final ShortVector values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        if(values.size() == 0) {
            return 0;
        }

        if(values.size() == 1) {
            return !countNull && values.get(0) == QueryConstants.NULL_SHORT ? 0 : 1;
        }

        final TShortSet keys = new TShortHashSet();
        for(int ii = 0; ii < values.size(); ii++) {
            keys.add(values.get(ii));
        }

        if(!countNull) {
            keys.remove(NULL_SHORT);
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
    public static short uniqueValue(final ShortVector arr, boolean countNull) {
        if(arr == null || arr.isEmpty()) {
            return NULL_SHORT;
        }

        if(arr.size() == 1) {
            return arr.get(0);
        }

        final TShortSet keys = new TShortHashSet();
        for(int ii = 0; ii < arr.size(); ii++) {
            keys.add(arr.get(ii));
        }

        if(!countNull) {
            keys.remove(NULL_SHORT);
        }

        return keys.size() == 1 ? keys.iterator().next() : NULL_SHORT;
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static short[] distinct(final short[] values) {
        if(values == null) {
            return null;
        }

        return distinct(new ShortVectorDirect(values)).toArray();
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static ShortVector distinct(final ShortVector values) {
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
    public static short[] distinct(final short[] values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values == null) {
            return null;
        }

        if(values.length == 0) {
            return new short[0];
        }

        if(values.length == 1) {
            return !includeNull && values[0] == QueryConstants.NULL_SHORT ? new short[0] : values;
        }

        final TShortArrayList orderedList = new TShortArrayList();
        final TShortSet counts = new TShortHashSet();
        for(int ii = 0; ii < values.length; ii++) {
            short val = values[ii];
            if((includeNull || val != NULL_SHORT) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        final short[] data;
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
    public static ShortVector distinct(final ShortVector values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values.size() == 0) {
            return new ShortVectorDirect();
        }

        if(values.size() == 1) {
            return !includeNull && values.get(0) == QueryConstants.NULL_SHORT ? new ShortVectorDirect() : values;
        }

        final TShortArrayList orderedList = new TShortArrayList();
        final TShortSet counts = new TShortHashSet();
        for(int ii = 0; ii < values.size(); ii++) {
            short val = values.get(ii);
            if((includeNull || val != NULL_SHORT) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        final short[] data;
        if(sort) {
            orderedList.sort();
            data = orderedList.toArray();
            // region SortFixup
            // endregion SortFixup
        } else {
            data = orderedList.toArray();
        }

        return new ShortVectorDirect(data);
    }

    /**
     * Returns an array with a value repeated.
     *
     * @param value value.
     * @param size number of times to repeat the value.
     * @return array of repeated values.  If {@code size} is less than zero, an empty array is returned.
     */
    public static short[] repeat(short value, int size) {
        if(size < 0){
            return new short[0];
        }

        final short[] result = new short[size];

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
    public static short[] enlist(short... values){
        if(values == null){
            return new short[0];
        }

        return values;
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static short[] concat(short[]... values){
        if(values == null){
            return new short[0];
        }

        return concat(Arrays.stream(values).map(e->e==null?null:new ShortVectorDirect(e)).toArray(ShortVector[]::new));
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static short[] concat(ShortVector... values){
        if(values == null){
            return new short[0];
        }

        int n = 0;

        for (ShortVector v : values) {
            if (v != null) {
                n += v.size();
            }
        }

        final short[] result = new short[n];
        int idx = 0;

        for (ShortVector v : values) {
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
    public static short[] reverse(short... values){
        if(values == null){
            return null;
        }

        return reverse(new ShortVectorDirect(values));
    }

    /**
     * Returns an array with the values reversed.
     *
     * @param values values.
     * @return array with the values reversed.
     */
    public static short[] reverse(ShortVector values){
        if(values == null){
            return null;
        }

        final short[] result = new short[(int) values.size()];

        for(int i=0; i<values.size(); i++){
            result[i] = values.get(values.size()-1-i);
        }

        return result;
    }
}
