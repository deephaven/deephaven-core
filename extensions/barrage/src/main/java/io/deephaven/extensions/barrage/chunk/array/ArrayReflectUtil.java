//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.array;

import java.lang.reflect.Array;

public class ArrayReflectUtil {
    public static Object newInstance(Class<?> componentType, int length) {
        return Array.newInstance(componentType, length);
    }
}
