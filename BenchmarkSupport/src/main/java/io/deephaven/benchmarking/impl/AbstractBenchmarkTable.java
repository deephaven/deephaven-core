package io.deephaven.benchmarking.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;
import io.deephaven.benchmarking.generator.random.NormalExtendedRandom;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * The base implementation of {@link BenchmarkTable}. This includes all of the common things that the other
 * specializations require.
 */
public abstract class AbstractBenchmarkTable implements BenchmarkTable {
    private final String name;
    private final long rngSeed;
    private Map<String, ColumnGenerator<?>> generatorMap = Collections.emptyMap();
    private ExtendedRandom rand;

    AbstractBenchmarkTable(@NotNull String name, long rngSeed, @NotNull List<ColumnGenerator<?>> generators) {
        this.name = name;
        this.rngSeed = rngSeed;
        this.rand = new NormalExtendedRandom(new Random(rngSeed));

        populateAndAddGenerators(generators);
    }

    /**
     * Populate the map of column to generator and add it to the {@link QueryScope}
     *
     * @param generators The {@link ColumnGenerator}s to map.
     */
    private void populateAndAddGenerators(@NotNull List<ColumnGenerator<?>> generators) {
        this.generatorMap = generators.isEmpty() ? Collections.emptyMap() : new LinkedHashMap<>();

        // Build a mapping, as well as a tableDef to use.
        for (final ColumnGenerator<?> gen : generators) {
            final String varName = "_gen_" + getName() + "_" + gen.getName();
            generatorMap.put(varName, gen);
            QueryScope.addParam(varName, gen);
        }
    }

    /**
     * @return the map of column names to {@link ColumnGenerator}s
     */
    Map<String, ColumnGenerator<?>> getGeneratorMap() {
        return generatorMap;
    }

    @Override
    public void reset() {
        rand = new NormalExtendedRandom(new Random(rngSeed));
        generatorMap.values().forEach(gen -> gen.init(rand));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Table getTable() {
        return getTable(true);
    }

    @Override
    public Table getTable(boolean resetRNG) {
        if (resetRNG) {
            reset();
        }
        return populate();
    }

    abstract protected Table populate();
}
