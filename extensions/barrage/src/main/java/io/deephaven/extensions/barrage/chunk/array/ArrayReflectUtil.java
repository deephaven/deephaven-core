//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.array;

import java.lang.reflect.Array;

/**
 * This utility class exists to isolate wrappers around reflective calls that may need to have their implementations
 * replaced when emulated in the JS API.
 */
class ArrayReflectUtil {
    /**
     * Allocate an array to hold {@code length} elements of type {@code componentType}. The standard (Java)
     * implementation delegates to {@link Array#newInstance(Class, int)}.
     *
     * @param componentType the component type in the array to be created
     * @param length the length of the array to create
     * @return the created array
     */
    static Object newInstance(Class<?> componentType, int length) {
        return Array.newInstance(componentType, length);
    }
}
