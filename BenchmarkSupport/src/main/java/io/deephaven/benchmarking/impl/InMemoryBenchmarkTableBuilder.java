package io.deephaven.benchmarking.impl;

import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;

/**
 * The basic implementation of {@link BenchmarkTableBuilder}. It allows users to specify table type and add columns,
 * while specifying their RNG properties.
 */
public class InMemoryBenchmarkTableBuilder extends AbstractBenchmarkTableBuilder {

    public InMemoryBenchmarkTableBuilder(String name, int size) {
        super(name, size);

    }

    @Override
    public BenchmarkTable build() {
        return new InMemoryBenchmarkTable(name, size, rngSeed, getColumnGenerators());

    }
}
