/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.util.datastructures.LongSizedDataStructure;

/**
 * Interface for positional column access to a Deephaven table column.
 */
@SuppressWarnings("unused")
public interface DataColumn<TYPE> extends LongSizedDataStructure {

    String getName();

    /**
     * @return the type of object contained within this column.
     */
    Class getType();

    /**
     * Get the array component type, or the type itself. For basic types, this is just the type. For example, if you
     * have a column of java.lang.String, this also returns java.lang.String. For array types (java Arrays), or Vector
     * (which would be returned by getType), you get the type that is contained within the array. For example, if a
     * single row in this column contains a DoubleVector, getComponentType returns double.
     *
     * @return if type is an array, the type of object within the array. Otherwise type itself.
     */
    Class getComponentType();

    /**
     * Get the contents of this data column in array form. Note that this will return an array of the appropriate
     * primitive type for columns of non-Boolean primitive types.
     * 
     * @return An appropriately-typed array containing the full contents of this data column
     */
    default Object getDirect() {
        return getDirect(0, size());
    }

    /**
     * Get the contents of a range of this data column in array form. See {@link #getDirect()} for an explanation of
     * return types. Note that it's required that {@code endIndexExclusive - startIndexInclusive < Integer.MAX_VALUE}.
     * 
     * @param startIndexInclusive The first position in the data column to include, inclusive
     * @param endIndexExclusive One more than the last position in the data column to include
     * @return An appropriately-typed array containing the contents of the specified range of this data column
     */
    default Object getDirect(final long startIndexInclusive, final long endIndexExclusive) {
        // noinspection unchecked
        final Class<TYPE> type = getType();
        if (type == byte.class || type == Byte.class) {
            return getBytes(startIndexInclusive, endIndexExclusive);
        }
        if (type == char.class || type == Character.class) {
            return getChars(startIndexInclusive, endIndexExclusive);
        }
        if (type == double.class || type == Double.class) {
            return getDoubles(startIndexInclusive, endIndexExclusive);
        }
        if (type == float.class || type == Float.class) {
            return getFloats(startIndexInclusive, endIndexExclusive);
        }
        if (type == int.class || type == Integer.class) {
            return getInts(startIndexInclusive, endIndexExclusive);
        }
        if (type == long.class || type == Long.class) {
            return getLongs(startIndexInclusive, endIndexExclusive);
        }
        if (type == short.class || type == Short.class) {
            return getShorts(startIndexInclusive, endIndexExclusive);
        }
        return get(startIndexInclusive, endIndexExclusive);
    }

    default Object getDirect(final long... indexes) {
        // noinspection unchecked
        final Class<TYPE> type = getType();
        if (type == byte.class || type == Byte.class) {
            return getBytes(indexes);
        }
        if (type == char.class || type == Character.class) {
            return getChars(indexes);
        }
        if (type == double.class || type == Double.class) {
            return getDoubles(indexes);
        }
        if (type == float.class || type == Float.class) {
            return getFloats(indexes);
        }
        if (type == int.class || type == Integer.class) {
            return getInts(indexes);
        }
        if (type == long.class || type == Long.class) {
            return getLongs(indexes);
        }
        if (type == short.class || type == Short.class) {
            return getShorts(indexes);
        }
        return get(indexes);
    }

    default Object getDirect(final int... indexes) {
        // noinspection unchecked
        final Class<TYPE> type = getType();
        if (type == byte.class || type == Byte.class) {
            return getBytes(indexes);
        }
        if (type == char.class || type == Character.class) {
            return getChars(indexes);
        }
        if (type == double.class || type == Double.class) {
            return getDoubles(indexes);
        }
        if (type == float.class || type == Float.class) {
            return getFloats(indexes);
        }
        if (type == int.class || type == Integer.class) {
            return getInts(indexes);
        }
        if (type == long.class || type == Long.class) {
            return getLongs(indexes);
        }
        if (type == short.class || type == Short.class) {
            return getShorts(indexes);
        }
        return get(indexes);
    }

    /**
     * Returns the value in the column at the row designated by the row position
     * 
     * @param index - the row position for which the data is being retrieved
     * @return the value in the column at the row designated by the row position
     */
    TYPE get(long index);

    /**
     * Return the column's values for the specified row range. Note that this will be a boxed array, for data columns of
     * primitive types.
     * 
     * @param startIndexInclusive The first position in the data column to include, inclusive
     * @param endIndexExclusive One more than the last position in the data column to include
     * @return Return the column's values for the specified row range
     */
    TYPE[] get(long startIndexInclusive, long endIndexExclusive);

    /**
     * Return the column's values for the specified rows. Note that this will be a boxed array, for data columns of
     * primitive types.
     * 
     * @param indexes The row indexes to fetch
     * @return Return the column's values for the specified rows
     */
    TYPE[] get(long... indexes);

    /**
     * Return the column's values for the specified rows. Note that this will be a boxed array, for data columns of
     * primitive types.
     * 
     * @param indexes The row indexes to fetch
     * @return Return the column's values for the specified rows
     */
    TYPE[] get(int... indexes);

    Boolean getBoolean(long index);

    Boolean[] getBooleans(long startIndexInclusive, long endIndexExclusive);

    Boolean[] getBooleans(long... indexes);

    Boolean[] getBooleans(int... indexes);

    byte getByte(long index);

    byte[] getBytes(long startIndexInclusive, long endIndexExclusive);

    byte[] getBytes(long... indexes);

    byte[] getBytes(int... indexes);

    char getChar(long index);

    char[] getChars(long startIndexInclusive, long endIndexExclusive);

    char[] getChars(long... indexes);

    char[] getChars(int... indexes);

    double getDouble(long index);

    double[] getDoubles(long startIndexInclusive, long endIndexExclusive);

    double[] getDoubles(long... indexes);

    double[] getDoubles(int... indexes);

    float getFloat(long index);

    float[] getFloats(long startIndexInclusive, long endIndexExclusive);

    float[] getFloats(long... indexes);

    float[] getFloats(int... indexes);

    int getInt(long index);

    int[] getInts(long startIndexInclusive, long endIndexExclusive);

    int[] getInts(long... indexes);

    int[] getInts(int... indexes);

    long getLong(long index);

    long[] getLongs(long startIndexInclusive, long endIndexExclusive);

    long[] getLongs(long... indexes);

    long[] getLongs(int... indexes);

    short getShort(long index);

    short[] getShorts(long startIndexInclusive, long endIndexExclusive);

    short[] getShorts(long... indexes);

    short[] getShorts(int... indexes);
}
