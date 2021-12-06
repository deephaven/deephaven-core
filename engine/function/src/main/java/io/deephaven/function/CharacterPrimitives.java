/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import gnu.trove.list.array.TCharArrayList;
import gnu.trove.set.TCharSet;
import gnu.trove.set.hash.TCharHashSet;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * A set of commonly used functions that can be applied to Character types.
 */
public class CharacterPrimitives {
    /**
     * Unboxes a array of values.
     *
     * @param values values.
     * @return unboxed array of values.
     */
    public static char[] unbox(Character... values) {
        if(values == null){
            return null;
        }

        char[] result = new char[values.length];

        for(int i=0; i<values.length; i++){
            Character v = values[i];

            if(v == null || isNull(v)) {
                result[i] = NULL_CHAR;
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
    static public boolean isNull(char value){
        return value == NULL_CHAR;
    }

    /**
     * Replaces null values with a default value.
     *
     * @param value value.
     * @param defaultValue default value to return for null values.
     * @return value, if value is not null, and defaultValue if value is null.
     */
    static public char nullToValue(char value, char defaultValue) {
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
    static public char[] nullToValue(char[] values, char defaultValue) {
        return nullToValue(new CharVectorDirect(values), defaultValue);
    }

    /**
     * Replaces null values with a default value.
     *
     * @param values values.
     * @param defaultValue default value to return for null values.
     * @return values with nulls replaced by defaultValue.
     */
    static public char[] nullToValue(CharVector values, char defaultValue) {
        char[] result = new char[LongSizedDataStructure.intSize("nullToValue", values.size())];

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
    static public int count(Character[] values){
        if (values == null){
            return 0;
        }
        int count = 0;
        for (Character value : values) {
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
    static public int count(char[] values){
        if (values == null){
            return 0;
        }

        return count(new CharVectorDirect(values));
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public int count(CharVector values){
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
    static public char last(CharVector values){
        if(values == null || values.size() < 1){
            return NULL_CHAR;
        }

        return values.get(values.size() - 1);
    }

    /**
     * Returns the last value from an array.
     *
     * @param values values.
     * @return last value from the array.
     */
    static public char last(char[] values){
        if(values == null || values.length < 1){
            return NULL_CHAR;
        }

        return values[values.length - 1];
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public char first(CharVector values){
        if(values == null || values.size() < 1){
            return NULL_CHAR;
        }

        return values.get(0);
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public char first(char[] values){
        if(values == null || values.length < 1){
            return NULL_CHAR;
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
    static public char nth(int index, CharVector values){
        if(index < 0 || index >= values.size()){
            return NULL_CHAR;
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
    static public char nth(int index, char[] values){
        return nth(index, array(values));
    }

    /**
     * Converts a Vector to a primitive array.
     *
     * @param values Vector
     * @return primitive array.
     */
    public static char[] vec(CharVector values) {
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
    public static CharVector array(char[] values) {
        if(values == null){
            return null;
        }

        return new CharVectorDirect(values);
    }

    /**
     * Checks if a value is within a range.
     *
     * @param testedValue tested value.
     * @param lowInclusiveValue lower inclusive bound of the range.
     * @param highInclusiveValue upper inclusive bound of the range.
     * @return true if the tested value is within the range, and false if the tested value is not in the range or is null.
     */
    static public boolean inRange(char testedValue,char lowInclusiveValue,char highInclusiveValue){
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
    static public boolean in(char testedValues,char... possibleValues){
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
    public static long countDistinct(final char[] values) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new CharVectorDirect(values));
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @return number of distinct non-null values.
     */
    public static long countDistinct(final CharVector values) {
        return countDistinct(values, false);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final char[] values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new CharVectorDirect(values), countNull);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final CharVector values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        if(values.size() == 0) {
            return 0;
        }

        if(values.size() == 1) {
            return !countNull && values.get(0) == QueryConstants.NULL_CHAR ? 0 : 1;
        }

        final TCharSet keys = new TCharHashSet();
        for(int ii = 0; ii < values.size(); ii++) {
            keys.add(values.get(ii));
        }

        if(!countNull) {
            keys.remove(NULL_CHAR);
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
    public static char uniqueValue(final CharVector arr, boolean countNull) {
        if(arr == null || arr.isEmpty()) {
            return NULL_CHAR;
        }

        if(arr.size() == 1) {
            return arr.get(0);
        }

        final TCharSet keys = new TCharHashSet();
        for(int ii = 0; ii < arr.size(); ii++) {
            keys.add(arr.get(ii));
        }

        if(!countNull) {
            keys.remove(NULL_CHAR);
        }

        return keys.size() == 1 ? keys.iterator().next() : NULL_CHAR;
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static char[] distinct(final char[] values) {
        if(values == null) {
            return null;
        }

        return distinct(new CharVectorDirect(values)).toArray();
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static CharVector distinct(final CharVector values) {
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
    public static char[] distinct(final char[] values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values == null) {
            return null;
        }

        if(values.length == 0) {
            return new char[0];
        }

        if(values.length == 1) {
            return !includeNull && values[0] == QueryConstants.NULL_CHAR ? new char[0] : values;
        }

        final TCharArrayList orderedList = new TCharArrayList();
        final TCharSet counts = new TCharHashSet();
        for(int ii = 0; ii < values.length; ii++) {
            char val = values[ii];
            if((includeNull || val != NULL_CHAR) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        final char[] data;
        if(sort) {
            orderedList.sort();
            data = orderedList.toArray();
            // region SortFixup
            int foundLeft = Arrays.binarySearch(data, 0, data.length, NULL_CHAR);
            if (foundLeft >= 0) {
                int foundRight = foundLeft;
                while (foundLeft > 0 && data[foundLeft - 1] == NULL_CHAR) {
                    foundLeft--;
                }
                while (foundRight < data.length - 1 && data[foundRight + 1] == NULL_CHAR) {
                    foundRight++;
                }

                final int nullCount = foundRight - foundLeft + 1;
                System.arraycopy(data, 0, data, nullCount, data.length - nullCount);
                Arrays.fill(data, 0, nullCount, NULL_CHAR);
            }
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
    public static CharVector distinct(final CharVector values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values.size() == 0) {
            return new CharVectorDirect();
        }

        if(values.size() == 1) {
            return !includeNull && values.get(0) == QueryConstants.NULL_CHAR ? new CharVectorDirect() : values;
        }

        final TCharArrayList orderedList = new TCharArrayList();
        final TCharSet counts = new TCharHashSet();
        for(int ii = 0; ii < values.size(); ii++) {
            char val = values.get(ii);
            if((includeNull || val != NULL_CHAR) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        final char[] data;
        if(sort) {
            orderedList.sort();
            data = orderedList.toArray();
            // region SortFixup
            int foundLeft = Arrays.binarySearch(data, 0, data.length, NULL_CHAR);
            if (foundLeft >= 0) {
                int foundRight = foundLeft;
                while (foundLeft > 0 && data[foundLeft - 1] == NULL_CHAR) {
                    foundLeft--;
                }
                while (foundRight < data.length - 1 && data[foundRight + 1] == NULL_CHAR) {
                    foundRight++;
                }

                final int nullCount = foundRight - foundLeft + 1;
                System.arraycopy(data, 0, data, nullCount, data.length - nullCount);
                Arrays.fill(data, 0, nullCount, NULL_CHAR);
            }
            // endregion SortFixup
        } else {
            data = orderedList.toArray();
        }

        return new CharVectorDirect(data);
    }

    /**
     * Returns an array with a value repeated.
     *
     * @param value value.
     * @param size number of times to repeat the value.
     * @return array of repeated values.  If {@code size} is less than zero, an empty array is returned.
     */
    public static char[] repeat(char value, int size) {
        if(size < 0){
            return new char[0];
        }

        final char[] result = new char[size];

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
    public static char[] enlist(char... values){
        if(values == null){
            return new char[0];
        }

        return values;
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static char[] concat(char[]... values){
        if(values == null){
            return new char[0];
        }

        return concat(Arrays.stream(values).map(e->e==null?null:new CharVectorDirect(e)).toArray(CharVector[]::new));
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static char[] concat(CharVector... values){
        if(values == null){
            return new char[0];
        }

        int n = 0;

        for (CharVector v : values) {
            if (v != null) {
                n += v.size();
            }
        }

        final char[] result = new char[n];
        int idx = 0;

        for (CharVector v : values) {
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
    public static char[] reverse(char... values){
        if(values == null){
            return null;
        }

        return reverse(new CharVectorDirect(values));
    }

    /**
     * Returns an array with the values reversed.
     *
     * @param values values.
     * @return array with the values reversed.
     */
    public static char[] reverse(CharVector values){
        if(values == null){
            return null;
        }

        final char[] result = new char[(int) values.size()];

        for(int i=0; i<values.size(); i++){
            result[i] = values.get(values.size()-1-i);
        }

        return result;
    }
}
