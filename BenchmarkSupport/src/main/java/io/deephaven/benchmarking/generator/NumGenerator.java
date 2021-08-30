package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import io.deephaven.benchmarking.generator.random.NormalExtendedRandom;
import io.deephaven.benchmarking.generator.random.ThreadLocalExtendedRandom;

import java.util.Random;

public class NumGenerator implements DataGenerator<Number> {
    private final ExtendedRandom random;
    private final double lower, upper;

    /**
     * Get a {@link NumGenerator} using a {@link ThreadLocalExtendedRandom} as it's random source.
     */
    public NumGenerator() {
        this(-Double.MAX_VALUE, Double.MAX_VALUE);
    }

    public NumGenerator(long seed) {
        this(-Double.MAX_VALUE, Double.MAX_VALUE, seed);
    }

    public NumGenerator(double lower, double upper) {
        this(lower, upper, ThreadLocalExtendedRandom.getInstance());
    }

    public NumGenerator(double lower, double upper, long seed) {
        this(lower, upper, new NormalExtendedRandom(new Random(seed)));
    }

    public NumGenerator(double lower, double upper, ExtendedRandom random) {
        this.lower = lower;
        this.upper = upper;
        this.random = random;
    }

    public Number get() {
        return getDouble();
    }

    public byte getByte() {
        return (byte) getInt();
    }

    public double getDouble() {
        return random.nextDouble(lower, upper);
    }

    public float getFloat() {
        return (float) getDouble();
    }

    public int getInt() {
        return random.nextInt((int) lower, (int) upper);
    }

    public long getLong() {
        return random.nextLong((long) lower, (long) upper);
    }

    public short getShort() {
        return (short) getInt();
    }

    public char getChar() {
        return (char) getShort();
    }
}
