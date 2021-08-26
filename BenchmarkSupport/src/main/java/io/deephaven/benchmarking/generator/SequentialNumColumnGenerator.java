package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;

/**
 * A {@link ColumnGenerator} that generates numbers sequentially with specialized rollover behavior.
 * 
 * @param <T>
 */
public class SequentialNumColumnGenerator<T extends Number> extends AbstractNumColumnGenerator<T> {
    public enum Mode {
        /** No limits, count up forever */
        NoLimit,

        /** When the max is reached, reset to the starting value */
        RollAtLimit,

        /** When the limit is reached reverse the counting direction */
        ReverseAtLimit
    }

    private final double start;
    private final double step;
    private final double max;
    private final Mode mode;
    private double current;
    private int direction = 1;

    public SequentialNumColumnGenerator(Class<T> type, String name, double start, double step) {
        this(type, name, start, step, 0, Mode.NoLimit);
    }

    public SequentialNumColumnGenerator(Class<T> type, String name, double start, double step,
        double max, Mode mode) {
        super(type, name);
        this.start = start;
        this.current = start;
        this.step = step;
        this.max = max;
        this.mode = mode;
    }

    @Override
    public void init(ExtendedRandom random) {
        current = start;
        direction = 1;
    }

    @Override
    public byte getByte() {
        return (byte) getDouble();
    }

    @Override
    public short getShort() {
        return (short) getDouble();
    }

    @Override
    public int getInt() {
        return (int) getDouble();
    }

    @Override
    public long getLong() {
        return (long) getDouble();
    }

    @Override
    public float getFloat() {
        return (float) getDouble();
    }

    @Override
    public double getDouble() {
        final double localCurrent = current;
        current += (direction * step);

        switch (mode) {
            case NoLimit:
                break;
            case RollAtLimit:
                if (current >= max) {
                    current = start;
                }
                break;
            case ReverseAtLimit:
                if ((current >= max && direction > 0) || (current <= step && direction < 0)) {
                    direction *= -1;
                }
                break;
        }

        return localCurrent;
    }
}
