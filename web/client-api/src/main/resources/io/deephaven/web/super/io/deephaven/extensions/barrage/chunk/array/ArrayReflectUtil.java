//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.array;

import java.lang.reflect.Array;

/**
 * Browser impl, that only returns a plain Object[].
 */
class ArrayReflectUtil {
    /**
     * Always returns a plain Object[].
     */
    static Object newInstance(Class<?> componentType, int length) {
        return new Object[length];
    }
}
