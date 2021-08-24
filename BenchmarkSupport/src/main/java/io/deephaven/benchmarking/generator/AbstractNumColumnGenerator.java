package io.deephaven.benchmarking.generator;

import io.deephaven.db.tables.ColumnDefinition;

/**
 * A {@link ColumnGenerator} that can be used to generate a typed number column
 * 
 * @param <T> The type.
 */
public abstract class AbstractNumColumnGenerator<T extends Number> implements ColumnGenerator<T> {
    private final ColumnDefinition<T> def;

    AbstractNumColumnGenerator(Class<T> type, String name) {
        def = ColumnDefinition.fromGenericType(name, type);
    }

    @Override
    public ColumnDefinition<T> getDefinition() {
        return def;
    }

    @Override
    public String getUpdateString(String varName) {
        return def.getName() + "=(" + def.getDataType().getSimpleName() + ")" + varName
            + chooseGetter();
    }

    public abstract byte getByte();

    public abstract short getShort();

    public abstract int getInt();

    public abstract long getLong();

    public abstract float getFloat();

    public abstract double getDouble();

    /**
     * @return the string to use in update() to get the correct type.
     */
    private String chooseGetter() {
        final Class<T> type = def.getDataType();
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

        throw new IllegalStateException("Unsupported numeric type: " + type.toGenericString());
    }

    @Override
    public String getName() {
        return def.getName();
    }
}
