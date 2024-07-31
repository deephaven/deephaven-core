//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.array;

import java.lang.reflect.Array;

/**
 * Holds a reflective call that the JS API cannot make. Instead, the JS API will return an {@code Object[]} instance,
 * which will not bother JS clients, but wouldn't make sense for the JVM-based engine.
 */
class ArrayReflectUtil {
    /**
     * Delegates to {@link Array#newInstance(Class, int)} in the JVM.
     *
     * @param componentType the component type in the array to be created
     * @param length the length of the array to create
     * @return the created array
     */
    static Object newInstance(Class<?> componentType, int length) {
        return Array.newInstance(componentType, length);
    }
}
