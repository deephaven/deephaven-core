/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

public class FuzzyNumberGenerator extends SequentialNumberGenerator {
    private final double fuzz;
    private ExtendedRandom random;

    public FuzzyNumberGenerator(
            final double start,
            final double step,
            final double max,
            final double fuzz,
            final @NotNull Mode mode) {
        super(start, step, max, mode);
        this.fuzz = fuzz;
    }

    @Override
    public void init(@NotNull final ExtendedRandom random) {
        super.init(random);
        this.random = random;
    }

    @Override
    public double getDouble() {
        return super.getDouble() * random.nextDouble(1.0 - fuzz, 1.0 + fuzz);
    }
}
