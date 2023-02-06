/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.*;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;
import gnu.trove.list.array.*;
import gnu.trove.set.*;
import gnu.trove.set.hash.*;
import org.apache.commons.lang3.ArrayUtils;
import java.lang.reflect.Array;

import java.util.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * Basic functions that can be applied to primitive types.
 */
public class Basic {

    //////////////////////////// Object ////////////////////////////

    /**
     * Returns the null value in the Deephaven convention that corresponds to type T.
     * @param clazz The type.
     * @return The corresponding null value in the Deephaven convention if T is one of the
     *   Deephaven types with a distinguished null value. Otherwise, null,
     */
    @SuppressWarnings("unchecked")
    static public <T> T nullValueFor(Class<T> clazz) {
        if (clazz == Byte.class) {
            return (T)NULL_BYTE_BOXED;
        }

        if (clazz == Character.class) {
            return (T)NULL_CHAR_BOXED;
        }

        if (clazz == Short.class) {
            return (T)NULL_SHORT_BOXED;
        }

        if (clazz == Integer.class) {
            return (T)NULL_INT_BOXED;
        }

        if (clazz == Float.class) {
            return (T)NULL_FLOAT_BOXED;
        }

        if (clazz == Long.class) {
            return (T)NULL_LONG_BOXED;
        }

        if (clazz == Double.class) {
            return (T)NULL_DOUBLE_BOXED;
        }

        return null;
    }

    /**
     * Determines if a value is considered by the Deephaven convention to be null. In the Deephaven convention, every
     * simple type T has a special distinguished value NULL_T which is used to represent the null value for that type.
     * These values are enumerated in the {@link QueryConstants} class.
     *
     * @param value value.
     * @return true if the value is null according to the Deephaven convention, and false otherwise.
     */
    static public <T> boolean isNull(T value) {
        return value == null ||
                (value instanceof Byte && (Byte)value == NULL_BYTE) ||
                (value instanceof Character && (Character)value == NULL_CHAR) ||
                (value instanceof Short && (Short)value == NULL_SHORT) ||
                (value instanceof Integer && (Integer)value == NULL_INT) ||
                (value instanceof Float && (Float)value == NULL_FLOAT) ||
                (value instanceof Long && (Long)value == NULL_LONG) ||
                (value instanceof Double && (Double)value == NULL_DOUBLE);
    }

    /**
     * Replaces values that are null according to Deephaven convention with a specified value.
     *
     * @param value value.
     * @param replacement replacement to use when value is null according to Deephaven convention.
     * @return value, if value is not null according to Deephaven convention, replacement otherwise.
     */
    static public <T> T replaceIfNull(T value, T replacement) {
        if (isNull(value)) {
            return replacement;
        } else {
            return value;
        }
    }

    /**
     * Replaces values that are null according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is null according to Deephaven convention.
     * @return array containing value, if value is not null according to Deephaven convention, replacement otherwise.
     */
    static public <T> T[] replaceIfNull(T[] values, T replacement) {
        T[] result = values.clone();

        for (int i = 0; i < values.length; i++) {
            result[i] = replaceIfNull(values[i], replacement);
        }

        return result;
    }

    /**
     * Replaces values that are null according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is null according to Deephaven convention.
     * @return array containing value, if value is not null according to Deephaven convention, replacement otherwise.
     */
    static public <T> T[] replaceIfNull(ObjectVector<T> values, T replacement) {
        final int n = values.intSize("replaceIfNull");
        T[] result = values.toArray();

        for (int i = 0; i < n; i++) {
            result[i] = replaceIfNull(values.get(i), replacement);
        }

        return result;
    }

    /**
     * Returns the length of the input.
     *
     * @param values values.
     * @return length of the input or the Deephaven null constant for null inputs.
     */
    static public <T> long len(T[] values) {
        if (values == null) {
            return NULL_LONG;
        }

        return values.length;
    }

    /**
     * Returns the length of the input.
     *
     * @param values values.
     * @return length of the input or the Deephaven null constant for null inputs.
     */
    @SuppressWarnings("rawtypes")
    static public long len(LongSizedDataStructure values) {
        if (values == null) {
            return NULL_LONG;
        }

        return values.size();
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    @SafeVarargs
    static public <T> long countObj(T... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return countObj(new ObjectVectorDirect<>(values));
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public <T> long countObj(ObjectVector<T> values) {
        if (values == null) {
            return NULL_LONG;
        }

        final long n = values.size();
        long count = 0;

        for (long i = 0; i < n; i++) {
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
    @SafeVarargs
    static public <T> T lastObj(T... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        return values[values.length - 1];
    }

    /**
     * Returns the last value from an array.
     *
     * @param values values.
     * @return last value from the array.
     */
    static public <T> T lastObj(ObjectVector<T> values) {
        if (values == null || values.size() == 0) {
            return null;
        }

        return values.get(values.size() - 1);
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    @SafeVarargs
    static public <T> T firstObj(T... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        return values[0];
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public <T> T firstObj(ObjectVector<T> values) {
        if (values == null || values.size() == 0) {
            return null;
        }

        return values.get(0);
    }

    /**
     * Returns the nth value from an array.
     *
     * @param index index of the value to return.
     * @param values values.
     * @return nth value from the array or null, if the index is outside of the array's index range.
     */
    @SafeVarargs
    static public <T> T nthObj(long index, T... values) {
        if (values == null || index < 0 || index >= values.length) {
            return null;
        }

        return values[(int)index];
    }

    /**
     * Returns the nth value from an array.
     *
     * @param index index of the value to return.
     * @param values values.
     * @return nth value from the array or null, if the index is outside of the array's index range.
     */
    static public <T> T nthObj(long index, ObjectVector<T> values) {
        if (values == null || index < 0 || index >= values.size()) {
            return null;
        }

        return values.get(index);
    }

    /**
     * Converts a Deephaven vector to an array.
     *
     * @param values Deephaven vector
     * @return primitive array.
     */
    public static <T> T[] arrayObj(ObjectVector<T> values) {
        if (values == null) {
            return null;
        }

        return values.toArray();
    }

    /**
     * Converts an array to a Deephaven vector.
     *
     * @param values primitive array
     * @return Deephaven vector.
     */
    @SafeVarargs
    public static <T> ObjectVector<T> vecObj(T... values) {
        if (values == null) {
            return null;
        }

        return new ObjectVectorDirect<>(values);
    }

    /**
     * Checks if a value is within a range.
     *
     * @param testedValue tested value.
     * @param lowInclusiveValue lower inclusive bound of the range.
     * @param highInclusiveValue upper inclusive bound of the range.
     * @return true if the tested value is within the range, and false if the tested value is not in the range or is null.
     */
    static public <T extends Comparable<? super T>> boolean inRange(T testedValue, T lowInclusiveValue, T highInclusiveValue) {
        if (isNull(testedValue)) {
            return false;
        }

        return testedValue.compareTo(lowInclusiveValue) >= 0 && testedValue.compareTo(highInclusiveValue) <= 0;
    }

    /**
     * Checks if a value is within a discrete set of possible values.
     *
     * @param testedValue tested value.
     * @param possibleValues possible values.
     * @return true if the tested value is contained in the possible values, and false otherwise.
     */
    @SafeVarargs
    static public <T> boolean inObj(T testedValue, T... possibleValues) {
        return inObj(testedValue, new ObjectVectorDirect<>(possibleValues));
    }

    /**
     * Checks if a value is within a discrete set of possible values.
     *
     * @param testedValue tested value.
     * @param possibleValues possible values.
     * @return true if the tested value is contained in the possible values, and false otherwise.
     */
    static public <T> boolean inObj(T testedValue, ObjectVector<T> possibleValues) {
        final boolean testedIsNull = isNull(testedValue);
        final long size = possibleValues.size();

        for (long i = 0; i < size; i++) {
            final T possibleValue = possibleValues.get(i);
            final boolean possibleIsNull = isNull(possibleValue);

            if (testedIsNull == possibleIsNull && (testedIsNull || testedValue.equals(possibleValue))) {
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
    public static <T extends Comparable<? super T>> long countDistinctObj(final ObjectVector<T> values) {
        if (values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinctObj(values, false);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @return number of distinct non-null values.
     */
    @SafeVarargs
    public static <T extends Comparable<? super T>> long countDistinctObj(T... values) {
        if (values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinctObj(new ObjectVectorDirect<>(values), false);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static <T extends Comparable<? super T>> long countDistinctObj(final T[] values, boolean countNull) {
        if (values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinctObj(new ObjectVectorDirect<>(values), countNull);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public static <T extends Comparable<? super T>> long countDistinctObj(final ObjectVector<T> values, boolean countNull) {
        if (values == null) {
            return QueryConstants.NULL_LONG;
        }

        final long n = values.size();

        if (n == 0) {
            return 0;
        }

        if (n == 1) {
            return !countNull && isNull(values.get(0)) ? 0 : 1;
        }

        final THashSet<T> keys = new THashSet<>();

        for (long ii = 0; ii < n; ii++) {
            keys.add(values.get(ii));
        }

        if (!countNull) {
            keys.remove(null);
            keys.remove(NULL_BOOLEAN);
            keys.remove(NULL_CHAR_BOXED);
            keys.remove(NULL_BYTE_BOXED);
            keys.remove(NULL_SHORT_BOXED);
            keys.remove(NULL_INT_BOXED);
            keys.remove(NULL_LONG_BOXED);
            keys.remove(NULL_FLOAT_BOXED);
            keys.remove(NULL_DOUBLE_BOXED);
        }

        return keys.size();
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    @SafeVarargs
    public static <T extends Comparable<? super T>> T[] distinctObj(T... values) {
        if (values == null) {
            return null;
        }

        return distinctObj(new ObjectVectorDirect<>(values), false);
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static <T extends Comparable<? super T>> T[] distinctObj(final ObjectVector<T> values) {
        if (values == null) {
            return null;
        }

        return distinctObj(values, false);
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @param includeNull true to include null values, and false to exclude null values.
     * @return array containing only distinct items from arr.
     */
    public static <T extends Comparable<? super T>> T[] distinctObj(final T[] values, boolean includeNull) {
        if (values == null) {
            return null;
        }

        return distinctObj(new ObjectVectorDirect<>(values), includeNull);
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @param includeNull true to include null values, and false to exclude null values.
     * @return array containing only distinct items from arr.
     */
    @SuppressWarnings({"unchecked"})
    public static <T extends Comparable<? super T>> T[] distinctObj(final ObjectVector<T> values, boolean includeNull) {
        if (values == null) {
            return null;
        }

        final long n = values.size();
        final T[] empty = (T[])Array.newInstance(values.getComponentType(), 0);

        if (n == 0) {
            return empty;
        }

        if (n == 1) {
            return !includeNull && isNull(values.get(0)) ? empty : values.toArray();
        }

        final List<T> orderedList = new ArrayList<>();
        final THashSet<T> counts = new THashSet<>();

        for (long ii = 0; ii < n; ii++) {
            T val = values.get(ii);
            if ((includeNull || !isNull(val)) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        return orderedList.toArray(empty);
    }

    /**
     * Returns an array with a value repeated.
     *
     * @param value value.
     * @param size number of times to repeat the value.
     * @return array of repeated values.  If {@code size} is less than or equal to zero, an empty array is returned.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> T[] repeat(T value, int size) {
        if ( size < 0 ) {
            throw new IllegalArgumentException("Negative size: size=" + size);
        }

        final T[] array = (T[])Array.newInstance(value.getClass(), size);

        for (int i = 0; i < size; i++) {
            array[i] = value;
        }

        return array;
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    @SafeVarargs
    @SuppressWarnings({"unchecked"})
    public static <T> T[] concat(T[]... values) {

        int n = 0;

        for (T[] v : values) {
            if (v != null) {
                n += v.length;
            }
        }

        final T[] result = (T[])Array.newInstance(values.getClass().getComponentType().getComponentType(), n);
        int idx = 0;

        for (T[] v : values) {
            if (v != null) {
                for (T t : v) {
                    result[idx] = t;
                    idx++;
                }
            }
        }

        return result;
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    @SafeVarargs
    @SuppressWarnings({"unchecked"})
    public static <T> T[] concat(ObjectVector<T>... values) {

        if (values.length == 0) {
            return (T[])Array.newInstance(Object.class, 0);
        }

        int n = 0;
        ObjectVector<T> nonNullValues = null;

        for (ObjectVector<T> v : values) {
            if (v != null) {
                n += v.size();
                nonNullValues = v;
            }
        }

        final T[] result = (T[])Array.newInstance(nonNullValues == null ? Object.class : nonNullValues.getComponentType(), n);
        int idx = 0;

        for (ObjectVector<T> v : values) {
            if (v != null) {
                final long nn = v.size();
                for (long i = 0; i < nn; i++) {
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
    @SafeVarargs
    public static <T> T[] reverseObj(T... values) {
        if (values == null) {
            return null;
        }

        return reverseObj(new ObjectVectorDirect<>(values));
    }

    /**
     * Returns an array with the values reversed.
     *
     * @param values values.
     * @return array with the values reversed.
     */
    public static <T> T[] reverseObj(ObjectVector<T> values) {
        if (values == null) {
            return null;
        }

        final int n = values.intSize("reverseObj");
        @SuppressWarnings("unchecked") final T[] result = (T[])Array.newInstance(values.getComponentType(), n);

        for (int i=0; i<n; i++) {
            result[i] = values.get(i);
        }

        ArrayUtils.reverse(result);
        return result;
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    @SafeVarargs
    public static <T> long firstIndexOfObj(T val, T... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return firstIndexOfObj(val, new ObjectVectorDirect<>(values));
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    public static <T> long firstIndexOfObj(T val, ObjectVector<T> values) {
        if (values == null) {
            return NULL_LONG;
        }

        final boolean isNullVal = isNull(val);
        final long L = values.size();

        for (long i = 0; i < L; ++i) {
            T c = values.get(i);
            final boolean isnullc = isNull(c);

            if ((isnullc && isNullVal) || (!isnullc && c.equals(val)) ) {
                return i;
            }
        }

        return NULL_LONG;
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return trueCase value if condition is true, falseCase value if condition is false, or null if condition is null.
     */
    public static <T> T ifelseObj(Boolean condition, T trueCase, T falseCase) {
        if (condition == null) {
            return null;
        }

        return condition ? trueCase : falseCase;
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return An array of T whose values are determined by the corresponding elements of condition, trueCase, and falseCase.
     *         The result element will be the trueCase element if the condition element is true;
     *         the falseCase element if the condition element is false; or null if the condition element is null.
     *         Returns null if any of the inputs is null.
     */
    public static <T> T[] ifelseObj(BooleanVector condition, ObjectVector<T> trueCase, ObjectVector<T> falseCase) {
        if (condition == null || trueCase == null || falseCase == null) {
            return null;
        }

        final int n_c = condition.intSize("condition");
        final int n_t = trueCase.intSize("trueCase");
        final int n_f = falseCase.intSize("falseCase");

        if (n_c != n_t || n_c != n_f) {
            throw new IllegalArgumentException("Inconsistent input sizes: condition=" + n_c + " trueCase=" + n_t + " falseCase=" + n_f);
        }

        if (!trueCase.getComponentType().equals(falseCase.getComponentType())) {
            throw new IllegalArgumentException("Input vectors have different element types. trueCase=" + trueCase.getComponentType() + " falseCase=" + falseCase.getComponentType());
        }

        @SuppressWarnings("unchecked") final T[] result = (T[])Array.newInstance(trueCase.getComponentType(), n_c);

        for (int i=0; i < n_c; i++) {
            result[i] = condition.get(i) == null ? null : (condition.get(i) ? trueCase.get(i) : falseCase.get(i));
        }

        return result;
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return An array of T whose values are determined by the corresponding elements of condition, trueCase, and falseCase.
     *         The result element will be the trueCase element if the condition element is true;
     *         the falseCase element if the condition element is false; or null if the condition element is null.
     *         Returns null if any of the inputs is null.
     */
    public static <T> T[] ifelseObj(Boolean[] condition, T[] trueCase, T[] falseCase) {
        if (condition == null || trueCase == null || falseCase == null) {
            return null;
        }

        return ifelseObj(new BooleanVectorDirect(condition), new ObjectVectorDirect<T>(trueCase), new ObjectVectorDirect<T>(falseCase));
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return An array of T whose values are determined by the corresponding elements of condition, trueCase, and falseCase.
     *         The result element will be trueCase if the condition element is true;
     *         falseCase if the condition element is false; or null if the condition element is null.
     *         Returns null if condition is null.
     */
    public static <T> T[] ifelseObj(BooleanVector condition, T trueCase, T falseCase) {
        if (condition == null) {
            return null;
        }

        final int n_c = condition.intSize("condition");
        final T typeToUse = trueCase != null ? trueCase : falseCase;

        if (typeToUse == null) {
            throw new IllegalArgumentException("trueCase and falseCase are null.  Can not resolve a return type.");
        }

        if (trueCase != null && falseCase != null && trueCase.getClass() != falseCase.getClass()) {
            throw new IllegalArgumentException("Inputs have different types. trueCase=" + trueCase.getClass() + " falseCase=" + falseCase.getClass());
        }

        @SuppressWarnings("unchecked") final T[] result = (T[])Array.newInstance(typeToUse.getClass(), n_c);

        for (int i=0; i < n_c; i++) {
            final Boolean c = condition.get(i);
            result[i] = c == null ? null : (c ? trueCase : falseCase);
        }

        return result;
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return An array of T whose values are determined by the corresponding elements of condition, trueCase, and falseCase.
     *         The result element will be trueCase if the condition element is true;
     *         falseCase if the condition element is false; or null if the condition element is null.
     *         Returns null if condition is null.
     */
    public static <T> T[] ifelseObj(Boolean[] condition, T trueCase, T falseCase) {
        if (condition == null) {
            return null;
        }

        return ifelseObj(new BooleanVectorDirect(condition), trueCase, falseCase);
    }

    /**
     * Copies the specified array, replacing elements that represent null in the Deephaven convention by the most
     * recently encountered non-null value if one exists. Otherwise (if no such value exists), replaces those elements
     * with null.
     *
     * @param values values.
     * @return A copy of the specified array, with Deephaven null elements replaced as described above. If the specified
     *         array is null, returns null.
     */
    @SafeVarargs
    public static <T> T[] forwardFillObj(final T... values) {
        if (values == null) {
            return null;
        }

        final T[] result = Arrays.copyOf(values, values.length);

        T lastGood = null;
        for (int ii = 0; ii < values.length; ii++) {
            if (!isNull(values[ii])) {
                lastGood = values[ii];
            }

            result[ii] = lastGood;
        }
        return result;
    }

    /**
     * Copies the specified array, replacing elements that represent null in the Deephaven convention by the most
     * recently encountered non-null value if one exists. Otherwise (if no such value exists), replaces those elements
     * with null.
     *
     * @param values values.
     * @return A copy of the specified array, with Deephaven null elements replaced as described above. If the specified
     *         array is null, returns null.
     */
    public static <T> T[] forwardFillObj(ObjectVector<T> values) {
        if (values == null) {
            return null;
        }

        final int n = values.intSize("forwardFill");
        final T[] result = values.toArray();

        T lastGood = null;
        for (int ii = 0; ii < n; ii++) {
            if (!isNull(values.get(ii))) {
                lastGood = values.get(ii);
            }

            result[ii] = lastGood;
        }
        return result;
    }

    /**
     * Copies the specified array, replacing elements that represent null in the Deephaven convention by the most
     * recently encountered non-null value if one exists. Otherwise (if no such value exists), replaces those elements
     * with null.
     *
     * @param values values.
     * @return A copy of the specified array, with Deephaven null elements replaced as described above. If the specified
     *         array is null, returns null.
     */
    public static Boolean[] forwardFillObj(BooleanVector values) {
        if (values == null) {
            return null;
        }

        final int n = values.intSize("forwardFill");
        final Boolean[] result = values.toArray();

        Boolean lastGood = null;
        for (int ii = 0; ii < n; ii++) {
            if (!isNull(values.get(ii))) {
                lastGood = values.get(ii);
            }

            result[ii] = lastGood;
        }
        return result;
    }

    <#list primitiveTypes as pt>
    <#if !pt.valueType.isBoolean >


    //////////////////////////// ${pt.primitive} ////////////////////////////

    /**
     * Determines if a value is considered by the Deephaven convention to be null. In the Deephaven convention, every
     * simple type T has a special distinguished value NULL_T which is used to represent the null value for that type.
     * These values are enumerated in the {@link QueryConstants} class.
     *
     * @param value value.
     * @return true if the value is null according to the Deephaven convention, and false otherwise.
     */
    static public boolean isNull(${pt.primitive} value) {
        return value == QueryConstants.${pt.null};
    }


    <#if !pt.valueType.isBoolean >

    /**
     * Unboxes an array of values.
     *
     * @param values values.
     * @return unboxed array of values.
     */
    public static ${pt.primitive}[] unbox(${pt.boxed}... values) {
        if (values == null) {
            return null;
        }

        ${pt.primitive}[] result = new ${pt.primitive}[values.length];

        for (int i=0; i<values.length; i++) {
            ${pt.boxed} v = values[i];

            if (v == null || isNull(v.${pt.primitive}Value())) {
                result[i] = QueryConstants.${pt.null};
            } else {
                result[i] = v;
            }
        }

        return result;
    }

    </#if>

    /**
     * Replaces values that are null according to Deephaven convention with a specified value.
     *
     * @param value value.
     * @param replacement replacement to use when value is null according to Deephaven convention.
     * @return value, if value is not null according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive} replaceIfNull(${pt.primitive} value, ${pt.primitive} replacement) {
        if (isNull(value)) {
            return replacement;
        } else {
            return value;
        }
    }

    /**
     * Replaces values that are null according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is null according to Deephaven convention.
     * @return array containing value, if value is not null according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNull(${pt.primitive}[] values, ${pt.primitive} replacement) {
        return replaceIfNull(new ${pt.dbArrayDirect}(values), replacement);
    }

    /**
     * Replaces values that are null according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is null according to Deephaven convention.
     * @return array containing value, if value is not null according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNull(${pt.dbArray} values, ${pt.primitive} replacement) {
        final int n = values.intSize("replaceIfNull");
        ${pt.primitive}[] result = new ${pt.primitive}[n];

        for (int i = 0; i < n; i++) {
            result[i] = replaceIfNull(values.get(i), replacement);
        }

        return result;
    }

    /**
     * Returns the length of the input.
     *
     * @param values values.
     * @return length of the input or the Deephaven null constant for null inputs.
     */
    static public long len(${pt.primitive}[] values) {
        if (values == null) {
            return NULL_LONG;
        }

        return values.length;
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public long count(${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return count(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Counts the number of non-null values.
     *
     * @param values values.
     * @return number of non-null values.
     */
    static public long count(${pt.dbArray} values) {
        if (values == null) {
            return NULL_LONG;
        }

        final long n = values.size();
        long count = 0;

        for (long i = 0; i < n; i++) {
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
    static public ${pt.primitive} last(${pt.dbArray} values) {
        if (values == null) {
            return QueryConstants.${pt.null};
        }

        final long n = values.size();

        if (n == 0) {
            return QueryConstants.${pt.null};
        }

        return values.get(n - 1);
    }

    /**
     * Returns the last value from an array.
     *
     * @param values values.
     * @return last value from the array.
     */
    static public ${pt.primitive} last(${pt.primitive}... values) {
        if (values == null) {
            return QueryConstants.${pt.null};
        }

        return last(vec(values));
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public ${pt.primitive} first(${pt.dbArray} values) {
        if (values == null || values.size() == 0) {
            return QueryConstants.${pt.null};
        }

        return values.get(0);
    }

    /**
     * Returns the first value from an array.
     *
     * @param values values.
     * @return first value from the array.
     */
    static public ${pt.primitive} first(${pt.primitive}... values) {
        if (values == null) {
            return QueryConstants.${pt.null};
        }

        return first(vec(values));
    }

    /**
     * Returns the nth value from an array.
     *
     * @param index index of the value to return.
     * @param values values.
     * @return nth value from the array or null, if the index is outside of the array's index range.
     */
    static public ${pt.primitive} nth(long index, ${pt.dbArray} values) {
        if (index < 0 || index >= values.size()) {
            return QueryConstants.${pt.null};
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
    static public ${pt.primitive} nth(long index, ${pt.primitive}... values) {
        return nth(index, vec(values));
    }

    /**
     * Converts a Deephaven vector to a primitive array.
     *
     * @param values Deephaven vector
     * @return primitive array.
     */
    public static ${pt.primitive}[] array(${pt.dbArray} values) {
        if (values == null) {
            return null;
        }

        return values.toArray();
    }

    /**
     * Converts a primitive array to a Deephaven vector.
     *
     * @param values primitive array
     * @return Deephaven vector.
     */
    public static ${pt.dbArray} vec(${pt.primitive}... values) {
        return new ${pt.dbArrayDirect}(values);
    }

    <#if pt.valueType.isBoolean == false >

    /**
     * Checks if a value is within a range.
     *
     * @param testedValue tested value.
     * @param lowInclusiveValue lower inclusive bound of the range.
     * @param highInclusiveValue upper inclusive bound of the range.
     * @return true if the tested value is within the range, and false if the tested value is not in the range or is null.
     */
    static public boolean inRange(${pt.primitive} testedValue,${pt.primitive} lowInclusiveValue,${pt.primitive} highInclusiveValue) {
        if (isNull(testedValue)) {
            return false;
        }

        return testedValue >= lowInclusiveValue && testedValue <= highInclusiveValue;
    }

    </#if>

    /**
     * Checks if a value is within a discrete set of possible values.
     *
     * @param testedValues tested value.
     * @param possibleValues possible values.
     * @return true if the tested value is contained in the possible values, and false otherwise.
     */
    static public boolean in(${pt.primitive} testedValues,${pt.primitive}... possibleValues) {
        for (${pt.primitive} possibleValue : possibleValues) {
            if (testedValues == possibleValue) {
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
    public static long countDistinct(final ${pt.primitive}... values) {
        if (values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @return number of distinct non-null values.
     */
    public static long countDistinct(final ${pt.dbArray} values) {
        return countDistinct(values, false);
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final ${pt.primitive}[] values, boolean countNull) {
        if (values == null) {
            return QueryConstants.NULL_LONG;
        }

        return countDistinct(new ${pt.dbArrayDirect}(values), countNull);
    }

    <#if pt.valueType.isBoolean == false >

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final ${pt.dbArray} values, boolean countNull) {
        if (values == null) {
            return QueryConstants.NULL_LONG;
        }

        final long n = values.size();

        if (n == 0) {
            return 0;
        }

        if (n == 1) {
            return !countNull && values.get(0) == QueryConstants.${pt.null} ? 0 : 1;
        }

        final T${pt.primitive?capitalize}Set keys = new T${pt.primitive?capitalize}HashSet();

        for (long ii = 0; ii < n; ii++) {
            keys.add(values.get(ii));
        }

        if (!countNull) {
            keys.remove(QueryConstants.${pt.null});
        }

        return keys.size();
    }

    <#else>

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param values values.
     * @param countNull true to count null values, and false to exclude null values.
     * @return number of distinct values.
     */
    public static long countDistinct(final ${pt.dbArray} values, boolean countNull) {
        if (values == null) {
            return QueryConstants.NULL_LONG;
        }

        final long n = values.size();

        if (n == 0) {
            return 0;
        }

        if (n == 1) {
            return !countNull && values.get(0) == QueryConstants.${pt.null} ? 0 : 1;
        }

        final Set<${pt.boxed}> keys = new HashSet<${pt.boxed}>();

        for (long ii = 0; ii < n; ii++) {
            keys.add(values.get(ii));
        }

        if (!countNull) {
            keys.remove(QueryConstants.${pt.null});
        }

        return keys.size();
    }

    </#if>

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static ${pt.primitive}[] distinct(final ${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return distinct(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @return unsorted array containing only distinct non-null items from arr.
     */
    public static ${pt.primitive}[] distinct(final ${pt.dbArray} values) {
        return distinct(values, false);
    }

    <#if pt.valueType.isBoolean == false >

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @param includeNull true to include null values, and false to exclude null values.
     * @return array containing only distinct items from arr.
     */
    public static ${pt.primitive}[] distinct(final ${pt.primitive}[] values, boolean includeNull) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new ${pt.primitive}[0];
        }

        if (values.length == 1) {
            return !includeNull && values[0] == QueryConstants.${pt.null} ? new ${pt.primitive}[0] : values;
        }

        final T${pt.primitive?capitalize}ArrayList orderedList = new T${pt.primitive?capitalize}ArrayList();
        final T${pt.primitive?capitalize}Set counts = new T${pt.primitive?capitalize}HashSet();

        for (${pt.primitive} val : values) {
            if ((includeNull || val != QueryConstants.${pt.null}) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        return orderedList.toArray();
    }

        /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @param includeNull true to include null values, and false to exclude null values.
     * @return array containing only distinct items from arr.
     */
    public static ${pt.primitive}[] distinct(final ${pt.dbArray} values, boolean includeNull) {
        if (values == null) {
            return null;
        }

        final long n = values.size();

        if (n == 0) {
            return new ${pt.primitive}[0];
        }

        if (n == 1) {
            return !includeNull && values.get(0) == QueryConstants.${pt.null} ? new ${pt.primitive}[0] : values.toArray();
        }

        final T${pt.primitive?capitalize}ArrayList orderedList = new T${pt.primitive?capitalize}ArrayList();
        final T${pt.primitive?capitalize}Set counts = new T${pt.primitive?capitalize}HashSet();

        for (long ii = 0; ii < n; ii++) {
            ${pt.primitive} val = values.get(ii);
            if ((includeNull || val != QueryConstants.${pt.null}) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        return orderedList.toArray();
    }

    <#else>

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @param includeNull true to include null values, and false to exclude null values.
     * @return array containing only distinct items from arr.
     */
    public static ${pt.primitive}[] distinct(final ${pt.primitive}[] values, boolean includeNull) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new ${pt.primitive}[0];
        }

        if (values.length == 1) {
            return !includeNull && values[0] == QueryConstants.${pt.null} ? new ${pt.primitive}[0] : values;
        }

        final ArrayList<${pt.boxed}> orderedList = new ArrayList<>();
        final Set<${pt.boxed}> counts = new HashSet<>();

        for (${pt.primitive} val : values) {
            if ((includeNull || val != QueryConstants.${pt.null}) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        return orderedList.toArray(new ${pt.boxed}[0]);
    }

        /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param values values.
     * @param includeNull true to include null values, and false to exclude null values.
     * @return array containing only distinct items from arr.
     */
    public static ${pt.primitive}[] distinct(final ${pt.dbArray} values, boolean includeNull) {
        if (values == null) {
            return null;
        }

        final long n = values.size();

        if (n == 0) {
            return new ${pt.primitive}[0];
        }

        if (n == 1) {
            return !includeNull && values.get(0) == QueryConstants.${pt.null} ? new ${pt.primitive}[0] : values.toArray();
        }

        final ArrayList<${pt.boxed}> orderedList = new ArrayList<>();
        final Set<${pt.boxed}> counts = new HashSet<>();

        for (long ii = 0; ii < n; ii++) {
            ${pt.primitive} val = values.get(ii);
            if ((includeNull || val != QueryConstants.${pt.null}) && counts.add(val)) {
                orderedList.add(val);
            }
        }

        return orderedList.toArray(new ${pt.boxed}[0]);
    }

    </#if>

    /**
     * Returns an array with a value repeated.
     *
     * @param value value.
     * @param size number of times to repeat the value.
     * @return array of repeated values.  If {@code size} is less than zero, an empty array is returned.
     */
    public static ${pt.primitive}[] repeat(${pt.primitive} value, int size) {
        if (size < 0) {
            return new ${pt.primitive}[0];
        }

        final ${pt.primitive}[] result = new ${pt.primitive}[size];

        for (int i=0; i<size; i++) {
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
    public static ${pt.primitive}[] enlist(${pt.primitive}... values) {
        if (values == null) {
            return new ${pt.primitive}[0];
        }

        return values;
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static ${pt.primitive}[] concat(${pt.primitive}[]... values) {
        if (values == null) {
            return new ${pt.primitive}[0];
        }

        return concat(Arrays.stream(values).map(e->e==null?null:new ${pt.dbArrayDirect}(e)).toArray(${pt.dbArray}[]::new));
    }

    /**
     * Returns the concatenation of multiple arrays into a single array.
     *
     * @param values values.
     * @return concatenation of multiple arrays into a single array.
     */
    public static ${pt.primitive}[] concat(${pt.dbArray}... values) {
        if (values == null) {
            return new ${pt.primitive}[0];
        }

        int n = 0;

        for (${pt.dbArray} v : values) {
            if (v != null) {
                n += v.size();
            }
        }

        final ${pt.primitive}[] result = new ${pt.primitive}[n];
        int idx = 0;

        for (${pt.dbArray} v : values) {
            if (v != null) {
                final long nn = v.size();
                for (int i = 0; i < nn; i++) {
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
    public static ${pt.primitive}[] reverse(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return reverse(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns an array with the values reversed.
     *
     * @param values values.
     * @return array with the values reversed.
     */
    public static ${pt.primitive}[] reverse(${pt.dbArray} values) {
        if (values == null) {
            return null;
        }

        final int n = values.intSize("reverse");
        final ${pt.primitive}[] result = new ${pt.primitive}[n];

        for (int i=0; i<n; i++) {
            result[i] = values.get(n-1-i);
        }

        return result;
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    public static long firstIndexOf(${pt.primitive} val, ${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return firstIndexOf(val, new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    public static long firstIndexOf(${pt.primitive} val, ${pt.dbArray} values) {
        if (values == null) {
            return NULL_LONG;
        }

        final long L = values.size();

        for (long i = 0; i < L; ++i) {
            ${pt.primitive} c = values.get(i);
            if (c == val) {
                return i;
            }
        }

        return NULL_LONG;
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return trueCase value if condition is true, falseCase value if condition is false, or the Deephaven null constant if condition is null.
     */
    public static ${pt.primitive} ifelse(Boolean condition, ${pt.primitive} trueCase, ${pt.primitive} falseCase) {
        if (condition == null) {
            return ${pt.null};
        }

        return condition ? trueCase : falseCase;
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return An array of ${pt.primitive} whose values are determined by the corresponding elements of condition, trueCase, and falseCase.
     *         The result element will be the trueCase element if the condition element is true;
     *         the falseCase element if the condition element is false; or the Deephaven null constant if the condition element is null.
     *         Returns null if any of the inputs is null.
     */
    public static ${pt.primitive}[] ifelse(BooleanVector condition, ${pt.dbArray} trueCase, ${pt.dbArray} falseCase) {
        if (condition == null || trueCase == null || falseCase == null) {
            return null;
        }

        final int n_c = condition.intSize("condition");
        final int n_t = trueCase.intSize("trueCase");
        final int n_f = falseCase.intSize("falseCase");

        if (n_c != n_t || n_c != n_f) {
            throw new IllegalArgumentException("Inconsistent input sizes: condition=" + n_c + " trueCase=" + n_t + " falseCase=" + n_f);
        }

        final ${pt.primitive}[] result = new ${pt.primitive}[n_c];

        for (int i=0; i < n_c; i++) {
            result[i] = condition.get(i) == null ? ${pt.null} : (condition.get(i) ? trueCase.get(i) : falseCase.get(i));
        }

        return result;
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return An array of ${pt.primitive} whose values are determined by the corresponding elements of condition, trueCase, and falseCase.
     *         The result element will be the trueCase element if the condition element is true;
     *         the falseCase element if the condition element is false; or the Deephaven null constant if the condition element is null.
     *         Returns null if any of the inputs is null.
     */
    public static ${pt.primitive}[] ifelse(Boolean[] condition, ${pt.primitive}[] trueCase, ${pt.primitive}[] falseCase) {
        if (condition == null || trueCase == null || falseCase == null) {
            return null;
        }

        return ifelse(new BooleanVectorDirect(condition), new ${pt.dbArrayDirect}(trueCase), new ${pt.dbArrayDirect}(falseCase));
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return An array of ${pt.primitive} whose values are determined by the corresponding elements of condition, trueCase, and falseCase.
     *         The result element will be trueCase if the condition element is true;
     *         falseCase if the condition element is false; or the Deephaven null constant if the condition element is null.
     *         Returns null if condition is null.
     */
    public static ${pt.primitive}[] ifelse(BooleanVector condition, ${pt.primitive} trueCase, ${pt.primitive} falseCase) {
        if (condition == null) {
            return null;
        }

        final int n_c = condition.intSize("condition");

        final ${pt.primitive}[] result = new ${pt.primitive}[n_c];

        for (int i=0; i < n_c; i++) {
            final Boolean c = condition.get(i);
            result[i] = c == null ? ${pt.null} : (c ? trueCase : falseCase);
        }

        return result;
    }

    /**
     * Returns elements from either trueCase or falseCase, depending on condition.
     *
     * @param condition a boolean value used to select output values.
     * @param trueCase value returned when condition is true.
     * @param falseCase value returned when condition is false.
     * @return An array of ${pt.primitive} whose values are determined by the corresponding elements of condition, trueCase, and falseCase.
     *         The result element will be trueCase if the condition element is true;
     *         falseCase if the condition element is false; or the Deephaven null constant if the condition element is null.
     *         Returns null if condition is null.
     */
    public static ${pt.primitive}[] ifelse(Boolean[] condition, ${pt.primitive} trueCase, ${pt.primitive} falseCase) {
        if (condition == null) {
            return null;
        }

        return ifelse(new BooleanVectorDirect(condition), trueCase, falseCase);
    }

    /**
     * Copies the specified array, replacing elements that represent null in the Deephaven convention by the most
     * recently encountered non-null value if one exists. Otherwise (if no such value exists), replaces those elements
     * with Deephaven null.
     *
     * @param values values.
     * @return A copy of the specified array, with Deephaven null elements replaced as described above. If the
     *         specified array is null, returns null.
     */
    public static ${pt.primitive}[] forwardFill(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return forwardFill(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Copies the specified array, replacing elements that represent null in the Deephaven convention by the most
     * recently encountered non-null value if one exists. Otherwise (if no such value exists), replaces those elements
     * with Deephaven null.
     *
     * @param values values.
     * @return A copy of the specified array, with Deephaven null elements replaced as described above. If the
     *         specified array is null, returns null.
     */
    public static ${pt.primitive}[] forwardFill(${pt.dbArray} values) {
        if (values == null) {
            return null;
        }

        final int n = values.intSize("forwardFill");
        final ${pt.primitive}[] result = new ${pt.primitive}[n];

        ${pt.primitive} lastGood = QueryConstants.${pt.null};
        for (int ii = 0; ii < n; ii++) {
            if (!isNull(values.get(ii))) {
                lastGood = values.get(ii);
            }

            result[ii] = lastGood;
        }
        return result;
    }

    </#if>
    </#list>
}
