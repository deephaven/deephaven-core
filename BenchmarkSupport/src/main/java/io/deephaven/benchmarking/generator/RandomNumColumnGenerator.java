package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;

/**
 * A {@link ColumnGenerator} That generates a typed number value randomly.
 * 
 * @param <T>
 */
public class RandomNumColumnGenerator<T extends Number> extends AbstractNumColumnGenerator<T> {
    protected ExtendedRandom generator = null;
    private final double min, max;

    public RandomNumColumnGenerator(Class<T> type, String name) {
        this(type, name, getLowerBoundForType(type), getUpperBoundForType(type));
    }

    public RandomNumColumnGenerator(Class<T> type, String name, double min, double max) {
        super(type, name);
        this.min = min;
        this.max = max;
    }

    @Override
    public void init(ExtendedRandom random) {
        generator = random;
    }

    @Override
    public byte getByte() {
        return (byte) getInt();
    }

    @Override
    public short getShort() {
        return (short) getInt();
    }

    @Override
    public int getInt() {
        return generator.nextInt((int) min, (int) max);
    }

    @Override
    public long getLong() {
        return generator.nextLong((long) min, (long) max);
    }

    @Override
    public float getFloat() {
        return (float) getDouble();
    }

    @Override
    public double getDouble() {
        return generator.nextDouble(min, max);
    }

    private static <T extends Number> double getUpperBoundForType(Class<T> type) {
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

    private static <T extends Number> double getLowerBoundForType(Class<T> type) {
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
