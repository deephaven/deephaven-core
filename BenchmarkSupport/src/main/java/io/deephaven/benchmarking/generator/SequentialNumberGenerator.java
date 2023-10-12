/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ColumnGenerator} that generates numbers sequentially with specialized rollover behavior.
 */
public class SequentialNumberGenerator implements NumberGenerator {
    public enum Mode {
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

    public SequentialNumberGenerator(
            final double start,
            final double step,
            final double max,
            @NotNull final Mode mode) {
        this.start = start;
        this.current = start;
        this.step = step;
        this.max = max;
        this.mode = mode;
    }

    @Override
    public void init(@NotNull final ExtendedRandom random) {
        current = start;
        direction = 1;
    }

    @Override
    public byte getByte() {
        return (byte) getDouble();
    }

    @Override
    public char getChar() {
        return (char) getShort();
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
