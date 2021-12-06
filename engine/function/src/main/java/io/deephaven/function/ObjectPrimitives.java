/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import gnu.trove.set.hash.THashSet;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.*;

/**
 * A set of commonly used functions that can be applied to Object types.
 */
@SuppressWarnings("SameParameterValue")
public class ObjectPrimitives {

    /**
     * Returns true if a sequence is in the target string.
     *
     * @param target string to search for the sequence.
     * @param sequence      sequence to search for.
     * @return true if {@code target} is not null and contains {@code sequence}; false otherwise.
     * @throws NullPointerException if <code>sequence</code> is <code>null</code>
     */
    static public boolean contains(String target, CharSequence sequence) {
        return target != null && target.contains(sequence);
    }

    /**
     * Determines if a value is null.
     *
     * @param value value.
     * @return true if the value is null, and false otherwise.
     */
    static public <T> boolean isNull(T value) {
        return value == null;
    }

    /**
     * Determines if a value is null.  Beyond checking for simple null values, this method checks to see if the input
     * object is equal to a null value.  For example, if a boxed {@link Long} value is passed in, this method first
     * checks to see if the value is null.  If the value is not null, it checks to see if the value is {@link QueryConstants#NULL_LONG}.
     *
     * @param value value.
     * @return true if the value is null, and false otherwise.
     */
    static public <T> boolean isDBNull(T value) {
        return isDeephavenNull(value);
    }

    static <T> boolean isDeephavenNull(final T value) {
        return (value == null || (Byte.class.isAssignableFrom(value.getClass()) && value.equals(NULL_BYTE))
                || (Character.class.isAssignableFrom(value.getClass()) && value.equals(NULL_CHAR))
                || (Short.class.isAssignableFrom(value.getClass()) && value.equals(NULL_SHORT))
                || (Integer.class.isAssignableFrom(value.getClass()) && value.equals(NULL_INT))
                || (Float.class.isAssignableFrom(value.getClass()) && value.equals(NULL_FLOAT))
                || (Long.class.isAssignableFrom(value.getClass()) && value.equals(NULL_LONG))
                || (Double.class.isAssignableFrom(value.getClass()) && value.equals(NULL_DOUBLE)));
    }

    /**
     * Replaces null values with a default value.
     *
     * @param value value.
     * @param defaultValue default value to return for null values.
     * @return value, if value is not null, and defaultValue if value is null.
     */
    static public <T> T nullToValue(T value, T defaultValue) {
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
    static public <T> T[] nullToValue(ObjectVector<T> values, T defaultValue) {
        T[] result = (T[]) Array.newInstance(values.getComponentType(), LongSizedDataStructure.intSize("nullToValue", values.size()));

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
    static public <T> int count(ObjectVector<T> values) {
        if(values == null){
            return 0;
        }

        int count = 0;

        for (int i = 0; i < values.size(); i++) {
            T c = values.get(i);

            if (!isNull(c)) {
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
    static public <T> T last(ObjectVector<T> values) {
        return values.get(values.size() - 1);
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public <T> T first(ObjectVector<T> values) {
        return values.get(0);
    }

    /**
     * Returns the nth value from an array.
     *
     * @param index index of the value to return.
     * @param values values.
     * @return nth value from the array or null, if the index is outside of the array's index range.
     */
    static public <T> T nth(int index, ObjectVector<T> values) {
        if (index < 0 || index >= values.size()) {
            return null;
        }

        return values.get(index);
    }

//    public static <T> T[] vec(T... values) {
//        return values;
//    }

    /**
     * Converts a Vector to a primitive array.
     *
     * @param values Vector
     * @return primitive array.
     */
    public static <T> T[] vec(ObjectVector<T> values) {
        return values.toArray();
    }

    /**
     * Checks if a value is within a discrete set of possible values.
     *
     * @param testedValue tested value.
     * @param possibleValues possible values.
     * @return true if the tested value is contained in the possible values, and false otherwise.
     */
    static public <T> boolean in(T testedValue, T... possibleValues) {
        for (int i = 0; i < possibleValues.length; i++) {
            if (testedValue == possibleValues[i] || (testedValue != null && testedValue.equals(possibleValues[i]))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if a value is within a range.
     *
     * @param testedValue tested value.
     * @param lowInclusiveValue lower inclusive bound of the range.
     * @param highInclusiveValue upper inclusive bound of the range.
     * @return true if the tested value is within the range, and false if the tested value is not in the range or is null.
     */
    static public boolean inRange(Comparable testedValue, Comparable lowInclusiveValue, Comparable highInclusiveValue) {
        if (testedValue == null) {
            return false;
        }
        return testedValue.compareTo(lowInclusiveValue) >= 0 && testedValue.compareTo(highInclusiveValue) <= 0;
    }

    /**
     * Returns the minimum.  Null values are excluded.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    static public <T extends Comparable> T min(ObjectVector<T> values) {
        T min = null;

        for (int i = 0; i < values.size(); i++) {
            T c = values.get(i);

            if (c != null) {
                if (min == null)
                    min = c;
                else if (c.compareTo(min) < 0)
                    min = c;
            }
        }

        return min;
    }

    /**
     * Returns the maximum.  Null values are excluded.
     *
     * @param values values.
     * @return maximum of non-null values.
     */
    static public <T extends Comparable> T max(ObjectVector<T> values) {
        T max = null;

        for (int i = 0; i < values.size(); i++) {
            T c = values.get(i);

            if (c != null) {
                if (max == null)
                    max = c;
                else if (c.compareTo(max) > 0)
                    max = c;
            }
        }

        return max;
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.
     * @param key key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index of where the key would be inserted.
     */
    static public <T extends Comparable<? super T>> int binSearchIndex(ObjectVector<T> values, T key, BinSearch choiceWhenEquals) {
        int index = rawBinSearchIndex(values, key, choiceWhenEquals);
        if (index == NULL_INT) {
            return index;
        }

        if (index < 0) {
            return -index - 2;
        } else {
            return index;
        }
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.
     * @param key key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    static public <T extends Comparable<? super T>> int rawBinSearchIndex(ObjectVector<T> values, T key, BinSearch choiceWhenEquals) {
        if (values == null || key == null) {
            return NULL_INT;
        }

        if (choiceWhenEquals != BinSearch.BS_ANY) {
            return binarySearch0Modified(values, 0, LongSizedDataStructure.intSize("rawBinSearchIndex", values.size()), key, choiceWhenEquals == BinSearch.BS_HIGHEST);
        } else {
            return binarySearch0(values, 0, LongSizedDataStructure.intSize("rawBinSearchIndex", values.size()), key);
        }
    }

    static private <T extends Comparable<? super T>> int binarySearch0(ObjectVector<T> array, int fromIndex, int toIndex, T key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = array.get(mid);
            if (midVal == null) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            int cmp = key.compareTo(midVal);
            if (cmp > 0) {
                low = mid + 1;
            } else if (cmp < 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);  // key not found.
    }

    static private <T extends Comparable<? super T>> int binarySearch0Modified(ObjectVector<T> array, int fromIndex, int toIndex, T key, boolean highestOrLowest) {
        int low = fromIndex;
        int high = toIndex - 1;

        if (highestOrLowest) {
            if (high >= low && key.compareTo(array.get(high)) == 0) {
                return high;
            }
        } else if (low <= high && key.compareTo(array.get(low)) == 0) {
            return low;
        }


        while (low <= high) {
            int mid = highestOrLowest ? (low + high + 1) >>> 1 : (low + high) >>> 1;
            T midVal = array.get(mid);
            if (midVal == null) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            int cmp = key.compareTo(midVal);
            if (cmp > 0) {
                low = mid + 1;
                if (low <= high) {
                    T lowVal = array.get(low);
                    if (lowVal == null) {
                        throw new RuntimeException("Can't have a null in the array!");
                    }
                    if (!highestOrLowest && key.compareTo(lowVal) == 0) {
                        return low;
                    }
                }
            } else if (cmp < 0) {
                high = mid - 1;
                if (high >= low) {
                    T highVal = array.get(high);
                    if (highVal == null) {
                        throw new RuntimeException("Can't have a null in the array!");
                    }
                    if (highestOrLowest && key.compareTo(highVal) == 0) {
                        return high;
                    }
                }
            } else {
                if (highestOrLowest) {
                    low = mid;
                } else {
                    high = mid;
                }
            }
        }
        return -(low + 1);  // key not found.
    }

    /**
     * Returns the minimum.  Null values are excluded.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    public static <NUM extends Number> NUM min(final NUM... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        NUM min = null;

        for (int i = 0; i < values.length; i++) {
            final NUM num = values[i];

            if (!isDeephavenNull(num) && !Double.isNaN(num.doubleValue())) {
                if (min == null) {
                    min = num;
                } else {
                    if (Comparators.compare(num, min) < 0) {
                        min = num;
                    }
                }
            }
        }
        return min;
    }

    /**
     * Returns the minimum.  Null values are excluded.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    static public <T> T min(final T... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        T min = null;
        int minIndex = 0;

        for (int i = 0; i < values.length; i++) {
            final T t = values[i];

            if (!(isDeephavenNull(t) || (t instanceof Double && Double.isNaN((Double) t)) || (t instanceof Float && Float.isNaN((Float) t)))) {
                try {
                    if (min == null) {
                        min = t;
                        minIndex = i;
                    } else {
                        if (Comparators.compare(t, min) < 0) {
                            min = t;
                            minIndex = i;
                        }
                    }
                } catch (final IllegalArgumentException cce) {
                    throw new IllegalArgumentException("Can not compare element at rowSet: " + i + ", type: " + t.getClass() + " with element at rowSet:" + minIndex + ", type: " + min.getClass() + "\n"
                            + cce.getMessage());
                }
            }
        }
        return min;
    }

    /**
     * Returns the maximum.  Null values are excluded.
     *
     * @param values values.
     * @return maximum of non-null values.
     */
    static public <NUM extends Number> NUM max(final NUM... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        NUM max = null;

        for (int i = 0; i < values.length; i++) {
            final NUM num = values[i];

            if (!isDeephavenNull(num) && !Double.isNaN(num.doubleValue())) {
                if (max == null) {
                    max = num;
                } else {
                    if (Comparators.compare(num, max) > 0) {
                        max = num;
                    }
                }
            }
        }
        return max;
    }

    /**
     * Returns the maximum.  Null values are excluded.
     *
     * @param values values.
     * @return maximum of non-null values.
     */
    static public <T> T max(final T... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        T max = null;
        int maxIndex = 0;

        for (int i = 0; i < values.length; i++) {
            final T t = values[i];

            if (!(isDeephavenNull(t) || (t instanceof Double && Double.isNaN((Double) t)) || (t instanceof Float && Float.isNaN((Float) t)))) {
                try {
                    if (max == null) {
                        max = t;
                        maxIndex = i;
                    } else {
                        if (Comparators.compare(t, max) > 0) {
                            max = t;
                            maxIndex = i;
                        }
                    }
                } catch (final IllegalArgumentException cce) {
                    throw new IllegalArgumentException("Can not compare element at rowSet: " + i + ", type: " + t.getClass() + " with element at rowSet:" + maxIndex + ", type: " + max.getClass() + "\n"
                            + cce.getMessage());
                }
            }
        }
        return max;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    static public <T extends Comparable<? super T>> ObjectVector<T> sort(final ObjectVector<T> values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new ObjectVectorDirect<>();
        }

        T[] vs = (T[]) Array.newInstance(values.getComponentType(), LongSizedDataStructure.intSize("nullToValue", values.size()));

        for(int i=0; i<vs.length; i++){
            vs[i] = values.get(i);
        }

        Arrays.sort(vs, Comparators.AscendingOrderComparator.getInstance());
        return new ObjectVectorDirect<>(vs);
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    static public <NUM extends Number> NUM[] sort(final NUM... values) {
        if (values == null) {
            return null;
        }

        final NUM[] copy = Arrays.copyOf(values, values.length);
        if (copy.length == 0) {
            return copy;
        }

        Arrays.sort(copy, Comparators.AscendingOrderComparatorNumber.getInstance());
        return copy;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    static public <T> T[] sort(final T... values) {
        if (values == null) {
            return null;
        }

        final T[] copy = Arrays.copyOf(values, values.length);
        if (copy.length == 0) {
            return copy;
        }

        Arrays.sort(copy, Comparators.AscendingOrderComparator.getInstance());
        return copy;
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    static public <T extends Comparable<? super T>> ObjectVector<T> sortDescending(final ObjectVector<T> values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new ObjectVectorDirect<>();
        }

        T[] vs = (T[]) Array.newInstance(values.getComponentType(), LongSizedDataStructure.intSize("nullToValue", values.size()));

        for(int i=0; i<vs.length; i++){
            vs[i] = values.get(i);
        }

        Arrays.sort(vs, Comparators.AscendingOrderComparator.getReversedInstance());
        return new ObjectVectorDirect<>(vs);
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    static public <NUM extends Number> NUM[] sortDescending(final NUM... values) {
        if (values == null) {
            return null;
        }

        final NUM[] copy = Arrays.copyOf(values, values.length);
        if (copy.length == 0) {
            return copy;
        }

        Arrays.sort(copy, Comparators.AscendingOrderComparatorNumber.getReversedInstance());
        return copy;
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    static public <T> T[] sortDescending(final T... values) {
        if (values == null) {
            return null;
        }

        final T[] copy = Arrays.copyOf(values, values.length);
        if (copy.length == 0) {
            return copy;
        }

        Arrays.sort(copy, Comparators.AscendingOrderComparator.getReversedInstance());
        return copy;
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @return number of distinct non-null values.
     */
    public static <T extends Comparable<? super T>> long countDistinct(final ObjectVector<T> values) {
        return countDistinct(values, false);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static <T extends Comparable<? super T>> long countDistinct(final ObjectVector<T> values, boolean countNull) {
        if(values == null) {
            return QueryConstants.NULL_LONG;
        }

        if(values.size() == 0){
            return 0;
        }

        if(values.size() == 1) {
            return !countNull && isDBNull(values.get(0)) ? 0 : 1;
        }

        final THashSet<T> keys = new THashSet<>();
        for(int ii = 0; ii < values.size(); ii++) {
            keys.add(values.get(ii));
        }

        if(!countNull) {
            keys.remove(null);
            keys.remove(NULL_BOOLEAN);
            keys.remove(NULL_CHAR);
            keys.remove(NULL_BYTE);
            keys.remove(NULL_SHORT);
            keys.remove(NULL_INT);
            keys.remove(NULL_LONG);
            keys.remove(NULL_FLOAT);
            keys.remove(NULL_DOUBLE);
        }

        return keys.size();
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static <T extends Comparable<? super T>> ObjectVector<T> distinct(final ObjectVector<T> values) {
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
    public static <T extends Comparable<? super T>> ObjectVector<T> distinct(final ObjectVector<T> values, boolean includeNull, boolean sort) {
        if(values == null) {
            return null;
        }

        if(values.size() == 0) {
            return new ObjectVectorDirect<>();
        }

        if(values.size() == 1) {
            return !includeNull && isDBNull(values.get(0)) ? new ObjectVectorDirect<>() : values;
        }

        final List<T> orderedList = new ArrayList<>();
        final THashSet<T> counts = new THashSet<>();
        for(int ii = 0; ii < values.size(); ii++) {
            T val = values.get(ii);
            if((includeNull || !isDBNull(val)) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        if(sort) {
            orderedList.sort(Comparable::compareTo);
        }

        return new ObjectVectorDirect(orderedList.toArray());
    }
}