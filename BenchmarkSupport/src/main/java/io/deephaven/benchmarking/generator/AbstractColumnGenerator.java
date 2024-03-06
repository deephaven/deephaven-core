//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmarking.generator;

import io.deephaven.engine.table.ColumnDefinition;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ColumnGenerator} that can be used to generate a typed column
 * 
 * @param <T> The type.
 */
public abstract class AbstractColumnGenerator<T> implements ColumnGenerator<T> {
    private final ColumnDefinition<T> def;

    AbstractColumnGenerator(@NotNull final Class<T> type, @NotNull final String name) {
        this.def = ColumnDefinition.fromGenericType(name, type);
    }

    @NotNull
    @Override
    public String getName() {
        return def.getName();
    }

    @NotNull
    @Override
    public ColumnDefinition<T> getDefinition() {
        return def;
    }

    @NotNull
    @Override
    public String getUpdateString(@NotNull final String varName) {
        return def.getName() + "=(" + def.getDataType().getSimpleName() + ")" + varName + chooseGetter(def);
    }

    public T get() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public byte getByte() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public char getChar() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public short getShort() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public int getInt() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public long getLong() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public float getFloat() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public double getDouble() {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * @return the string to use in update() to get the correct type.
     */
    private static String chooseGetter(@NotNull final ColumnDefinition<?> def) {
        final Class<?> type = def.getDataType();
        if (type == byte.class || type == Byte.class) {
            return ".getByte()";
        } else if (type == short.class || type == Short.class) {
            return ".getShort()";
        } else if (type == int.class || type == Integer.class) {
            return ".getInt()";
        } else if (type == long.class || type == Long.class) {
            return ".getLong()";
        } else if (type == float.class || type == Float.class) {
            return ".getFloat()";
        } else if (type == double.class || type == Double.class) {
            return ".getDouble()";
        }

        return ".get()";
    }
}
