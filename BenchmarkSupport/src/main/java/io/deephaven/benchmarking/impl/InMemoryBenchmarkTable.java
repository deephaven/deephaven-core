package io.deephaven.benchmarking.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;

import java.io.File;
import java.util.List;
import java.util.Random;

/**
 * An In memory only implementation of {@link BenchmarkTable}. This class uses a single internal {@link ExtendedRandom}
 * wrapping a unique {@link Random} as the RNG source for all underlying data generation. This simplifies overall data
 * generation and makes it easier to guarantee that it produces identical tables given identical seeds.
 */
public class InMemoryBenchmarkTable extends AbstractGeneratedTable {
    /**
     * Create a {@link BenchmarkTable} that produces in memory only tables based on the inputset of
     * {@link ColumnGenerator}s
     *
     * @param name The name of the table (to be used in tagging DbInternal data)
     * @param nRows The number of rows to generate
     * @param seed The RNG seed to use.
     * @param columnGenerators The set of {@link ColumnGenerator}s used to create the internal {@link TableDefinition}
     *        and data.
     */
    InMemoryBenchmarkTable(String name, long nRows, long seed, List<ColumnGenerator<?>> columnGenerators) {
        super(name, nRows, seed, columnGenerators);

        reset();
    }

    @Override
    protected Table populate() {
        return generateTable();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public File getLocation() {
        throw new UnsupportedOperationException();
    }
}
