/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.generator;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;

import java.time.Instant;

public class InstantColumnGenerator implements ColumnGenerator<Instant> {

    private NumGenerator gen;
    private final ColumnDefinition<Instant> def;
    private final long min;
    private final long max;

    public InstantColumnGenerator(String name) {
        this(name, 0, Long.MAX_VALUE);
    }

    public InstantColumnGenerator(String name, Instant min, Instant max) {
        this(name, DateTimeUtils.epochNanos(min), DateTimeUtils.epochNanos(max));
    }

    private InstantColumnGenerator(String name, long min, long max) {
        def = ColumnDefinition.ofTime(name);
        this.min = min;
        this.max = max;
    }

    @Override
    public ColumnDefinition<Instant> getDefinition() {
        return def;
    }

    @Override
    public String getUpdateString(String varName) {
        return def.getName() + "=(DateTime)" + varName + ".get()";
    }

    @Override
    public String getName() {
        return def.getName();
    }

    @Override
    public void init(ExtendedRandom random) {
        gen = new NumGenerator(min, max, random);
    }

    public Instant get() {
        return DateTimeUtils.epochNanosToInstant(gen.getLong());
    }
}
