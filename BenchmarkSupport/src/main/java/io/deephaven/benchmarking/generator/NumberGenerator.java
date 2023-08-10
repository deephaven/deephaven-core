/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

public interface NumberGenerator {
    void init(@NotNull ExtendedRandom random);

    byte getByte();

    short getShort();

    int getInt();

    long getLong();

    float getFloat();

    double getDouble();

    char getChar();

    static <T extends Number> double getUpperBoundForType(@NotNull final Class<T> type) {
        if (type == byte.class || type == Byte.class) {
            return Byte.MAX_VALUE;
        } else if (type == short.class || type == Short.class) {
            return Short.MAX_VALUE;
        } else if (type == int.class || type == Integer.class) {
            return Integer.MAX_VALUE;
        } else if (type == long.class || type == Long.class) {
            return Long.MAX_VALUE;
        } else if (type == float.class || type == Float.class) {
            return Float.MAX_VALUE;
        } else if (type == double.class || type == Double.class) {
            return Double.MAX_VALUE;
        }
        throw new IllegalStateException("Unsupported Number type: " + type.toGenericString());
    }

    static <T extends Number> double getLowerBoundForType(@NotNull final Class<T> type) {
        if (type == byte.class || type == Byte.class) {
            return Byte.MIN_VALUE;
        } else if (type == short.class || type == Short.class) {
            return Short.MIN_VALUE;
        } else if (type == int.class || type == Integer.class) {
            return Integer.MIN_VALUE;
        } else if (type == long.class || type == Long.class) {
            return Long.MIN_VALUE;
        } else if (type == float.class || type == Float.class) {
            return Float.MIN_VALUE;
        } else if (type == double.class || type == Double.class) {
            return Double.MIN_VALUE;
        }

        throw new IllegalStateException("Unsupported Number type: " + type.toGenericString());
    }
}
