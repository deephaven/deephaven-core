package io.deephaven.util;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * A utility interface that allows strongly typed access to an array.
 * 
 * @param <T>
 * @see PrimitiveArrayType
 */
public interface ArrayType<T> {

    Class<T> getArrayType();

    default boolean isAssignableFrom(Class<?> clazz) {
        return getArrayType().isAssignableFrom(clazz);
    }

    default boolean isAssignableFromObject(Object o) {
        return getArrayType().isAssignableFrom(o.getClass());
    }

    /**
     * A strongly-typed equivalent to {@link Array#getLength(Object)}.
     *
     * @param array the array
     * @return the length of the array
     */
    int getLength(T array);

    /**
     * A strongly-typed equivalent to {@link Array#newInstance(Class, int)}.
     *
     * @param len the length of the new array
     * @return the new array
     */
    T newInstance(int len);

    /**
     * A strongly-typed equivalent to {@link System#arraycopy(Object, int, Object, int, int)}.
     *
     * @param src the source array.
     * @param srcPos starting position in the source array.
     * @param dest the destination array.
     * @param destPos starting position in the destination data.
     * @param length the number of array elements to be copied.
     */
    void arrayCopy(T src, int srcPos, T dest, int destPos, int length);

    /**
     * A strongly-typed equivalent to {@link Arrays#copyOfRange(Object[], int, int)}.
     *
     * @param src the array from which a range is to be copied
     * @param from the initial index of the range to be copied, inclusive
     * @param to the final index of the range to be copied, exclusive
     * @return a new array containing the specified range from the original array
     */
    default T copyOfRange(T src, int from, int to) {
        final int len = to - from;
        final T newInstance = newInstance(len);
        arrayCopy(src, from, newInstance, 0, len);
        return newInstance;
    }
}
