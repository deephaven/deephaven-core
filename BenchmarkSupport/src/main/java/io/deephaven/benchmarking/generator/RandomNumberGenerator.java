/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

public class RandomNumberGenerator implements NumberGenerator {
    private final double lower, upper;
    private ExtendedRandom random;

    public RandomNumberGenerator(double lower, double upper) {
        this.lower = lower;
        this.upper = upper;
    }

    @Override
    public void init(@NotNull final ExtendedRandom random) {
        this.random = random;
    }

    @Override
    public byte getByte() {
        return (byte) getInt();
    }

    @Override
    public double getDouble() {
        return random.nextDouble(lower, upper);
    }

    @Override
    public float getFloat() {
        return (float) getDouble();
    }

    @Override
    public int getInt() {
        return random.nextInt((int) lower, (int) upper);
    }

    @Override
    public long getLong() {
        return random.nextLong((long) lower, (long) upper);
    }

    @Override
    public short getShort() {
        return (short) getInt();
    }

    @Override
    public char getChar() {
        return (char) getShort();
    }
}
