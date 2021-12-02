/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * A set of commonly used functions that can be applied to Integer types.
 */
public class IntegerPrimitives {
    /**
     * Unboxes a array of values.
     *
     * @param values values.
     * @return unboxed array of values.
     */
    public static int[] unbox(Integer... values) {
        if(values == null){
            return null;
        }

        int[] result = new int[values.length];

        for(int i=0; i<values.length; i++){
            Integer v = values[i];

            if(v == null || isNull(v)) {
                result[i] = NULL_INT;
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
    static public boolean isNull(int value){
        return value == NULL_INT;
    }

    /**
     * Replaces null values with a default value.
     *
     * @param value value.
     * @param defaultValue default value to return for null values.
     * @return value, if value is not null, and defaultValue if value is null.
     */
    static public int nullToValue(int value, int defaultValue) {
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
    static public int[] nullToValue(int[] values, int defaultValue) {
        return nullToValue(new IntVectorDirect(values), defaultValue);
    }

    /**
     * Replaces null values with a default value.
     *
     * @param values values.
     * @param defaultValue default value to return for null values.
     * @return values with nulls replaced by defaultValue.
     */
    static public int[] nullToValue(IntVector values, int defaultValue) {
        int[] result = new int[LongSizedDataStructure.intSize("nullToValue", values.size())];

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
    static public int count(Integer[] values){
        if (values == null){
            return 0;
        }
        int count = 0;
        for (Integer value : values) {
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
    static public int count(int[] values){
        if (values == null){
            return 0;
        }

        return count(new IntVectorDirect(values));
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public int count(IntVector values){
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
    static public int last(IntVector values){
        if(values == null || values.size() < 1){
            return NULL_INT;
        }

        return values.get(values.size() - 1);
    }

    /**
     * Returns the last value from an array.
     *
     * @param values values.
     * @return last value from the array.
     */
    static public int last(int[] values){
        if(values == null || values.length < 1){
            return NULL_INT;
        }

        return values[values.length - 1];
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public int first(IntVector values){
        if(values == null || values.size() < 1){
            return NULL_INT;
        }

        return values.get(0);
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public int first(int[] values){
        if(values == null || values.length < 1){
            return NULL_INT;
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
    static public int nth(int index, IntVector values){
        if(index < 0 || index >= values.size()){
            return NULL_INT;
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
    static public int nth(int index, int[] values){
        return nth(index, array(values));
    }

    /**
     * Converts a Vector to a primitive array.
     *
     * @param values Vector
     * @return primitive array.
     */
    public static int[] vec(IntVector values) {
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
    public static IntVector array(int[] values) {
        if(values == null){
            return null;
        }

        return new IntVectorDirect(values);
    }

    /**
     * Checks if a value is within a range.
     *
     * @param testedValue tested value.
     * @param lowInclusiveValue lower inclusive bound of the range.
     * @param highInclusiveValue upper inclusive bound of the range.
     * @return true if the tested value is within the range, and false if the tested value is not in the range or is null.
     */
    static public boolean inRange(int testedValue,int lowInclusiveValue,int highInclusiveValue){
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
    static public boolean in(int testedValues,int... possibleValues){
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
    public static long countDistinct(final int[] values) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new IntVectorDirect(values));
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @return number of distinct non-null values.
     */
    public static long countDistinct(final IntVector values) {
        return countDistinct(values, false);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final int[] values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new IntVectorDirect(values), countNull);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final IntVector values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        if(values.size() == 0) {
            return 0;
        }

        if(values.size() == 1) {
            return !countNull && values.get(0) == QueryConstants.NULL_INT ? 0 : 1;
        }

        final TIntSet keys = new TIntHashSet();
        for(int ii = 0; ii < values.size(); ii++) {
            keys.add(values.get(ii));
        }

        if(!countNull) {
            keys.remove(NULL_INT);
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
    public static int uniqueValue(final IntVector arr, boolean countNull) {
        if(arr == null || arr.isEmpty()) {
            return NULL_INT;
        }

        if(arr.size() == 1) {
            return arr.get(0);
        }

        final TIntSet keys = new TIntHashSet();
        for(int ii = 0; ii < arr.size(); ii++) {
            keys.add(arr.get(ii));
        }

        if(!countNull) {
            keys.remove(NULL_INT);
        }

        return keys.size() == 1 ? keys.iterator().next() : NULL_INT;
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static int[] distinct(final int[] values) {
        if(values == null) {
            return null;
        }

        return distinct(new IntVectorDirect(values)).toArray();
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static IntVector distinct(final IntVector values) {
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
    public static int[] distinct(final int[] values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values == null) {
            return null;
        }

        if(values.length == 0) {
            return new int[0];
        }

        if(values.length == 1) {
            return !includeNull && values[0] == QueryConstants.NULL_INT ? new int[0] : values;
        }

        final TIntArrayList orderedList = new TIntArrayList();
        final TIntSet counts = new TIntHashSet();
        for(int ii = 0; ii < values.length; ii++) {
            int val = values[ii];
            if((includeNull || val != NULL_INT) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        final int[] data;
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
    public static IntVector distinct(final IntVector values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values.size() == 0) {
            return new IntVectorDirect();
        }

        if(values.size() == 1) {
            return !includeNull && values.get(0) == QueryConstants.NULL_INT ? new IntVectorDirect() : values;
        }

        final TIntArrayList orderedList = new TIntArrayList();
        final TIntSet counts = new TIntHashSet();
        for(int ii = 0; ii < values.size(); ii++) {
            int val = values.get(ii);
            if((includeNull || val != NULL_INT) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        final int[] data;
        if(sort) {
            orderedList.sort();
            data = orderedList.toArray();
            // region SortFixup
            // endregion SortFixup
        } else {
            data = orderedList.toArray();
        }

        return new IntVectorDirect(data);
    }

    /**
     * Returns an array with a value repeated.
     *
     * @param value value.
     * @param size number of times to repeat the value.
     * @return array of repeated values.  If {@code size} is less than zero, an empty array is returned.
     */
    public static int[] repeat(int value, int size) {
        if(size < 0){
            return new int[0];
        }

        final int[] result = new int[size];

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
    public static int[] enlist(int... values){
        if(values == null){
            return new int[0];
        }

        return values;
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static int[] concat(int[]... values){
        if(values == null){
            return new int[0];
        }

        return concat(Arrays.stream(values).map(e->e==null?null:new IntVectorDirect(e)).toArray(IntVector[]::new));
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static int[] concat(IntVector... values){
        if(values == null){
            return new int[0];
        }

        int n = 0;

        for (IntVector v : values) {
            if (v != null) {
                n += v.size();
            }
        }

        final int[] result = new int[n];
        int idx = 0;

        for (IntVector v : values) {
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
    public static int[] reverse(int... values){
        if(values == null){
            return null;
        }

        return reverse(new IntVectorDirect(values));
    }

    /**
     * Returns an array with the values reversed.
     *
     * @param values values.
     * @return array with the values reversed.
     */
    public static int[] reverse(IntVector values){
        if(values == null){
            return null;
        }

        final int[] result = new int[(int) values.size()];

        for(int i=0; i<values.size(); i++){
            result[i] = values.get(values.size()-1-i);
        }

        return result;
    }
}
