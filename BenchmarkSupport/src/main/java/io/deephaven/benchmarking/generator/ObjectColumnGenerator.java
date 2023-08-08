/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ColumnGenerator} That generates a typed number value randomly.
 * 
 * @param <T>
 */
public class ObjectColumnGenerator<T> extends AbstractColumnGenerator<T> {
    private final ObjectGenerator<T> generator;

    public ObjectColumnGenerator(@NotNull final Class<T> type,
            @NotNull final String name,
            @NotNull final ObjectGenerator<T> generator) {
        super(type, name);
        this.generator = generator;
    }

    @Override
    public void init(@NotNull final ExtendedRandom random) {
        generator.init(random);
    }

    @Override
    public T get() {
        return generator.get();
    }
}
