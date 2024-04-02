//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmarking.generator;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

public class InstantGenerator implements ObjectGenerator<Instant> {

    private final NumberGenerator generator;

    public InstantGenerator(@NotNull final NumberGenerator generator) {
        this.generator = generator;
    }

    @Override
    public void init(@NotNull final ExtendedRandom random) {
        generator.init(random);
    }

    public Instant get() {
        return DateTimeUtils.epochNanosToInstant(generator.getLong());
    }
}
