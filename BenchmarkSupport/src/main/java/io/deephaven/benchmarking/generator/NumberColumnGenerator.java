//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ColumnGenerator} That generates a typed number value randomly.
 * 
 * @param <T>
 */
public class NumberColumnGenerator<T> extends AbstractColumnGenerator<T> {
    private final NumberGenerator generator;

    public NumberColumnGenerator(
            @NotNull final Class<T> type,
            @NotNull final String name,
            @NotNull final NumberGenerator generator) {
        super(type, name);
        this.generator = generator;
    }

    @Override
    public void init(@NotNull final ExtendedRandom random) {
        generator.init(random);
    }

    @Override
    public byte getByte() {
        return generator.getByte();
    }

    @Override
    public short getShort() {
        return generator.getShort();
    }

    @Override
    public int getInt() {
        return generator.getInt();
    }

    @Override
    public long getLong() {
        return generator.getLong();
    }

    @Override
    public float getFloat() {
        return generator.getFloat();
    }

    @Override
    public double getDouble() {
        return generator.getDouble();
    }

    @Override
    public char getChar() {
        return generator.getChar();
    }
}
