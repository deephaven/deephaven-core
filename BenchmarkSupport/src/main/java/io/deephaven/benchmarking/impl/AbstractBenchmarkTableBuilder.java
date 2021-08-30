package io.deephaven.benchmarking.impl;

import io.deephaven.hash.KeyedObjectHash;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * The basic implementation of {@link BenchmarkTableBuilder}. It allows users to specify table type and add columns,
 * while specifying their RNG properties.
 */
public abstract class AbstractBenchmarkTableBuilder<SELF extends BenchmarkTableBuilder>
        implements BenchmarkTableBuilder {
    protected final String name;
    protected final KeyedObjectHash<String, ColumnGenerator<?>> columns =
            new KeyedObjectHash<>(new ColumnGeneratorKey());
    protected long rngSeed = 0;
    final long size;

    public AbstractBenchmarkTableBuilder(String name, int size) {
        if (name == null || name.isEmpty()) {
            throw new IllegalStateException("This TableBuilder must have a name.");
        }
        this.name = name;

        if (size <= 0) {
            throw new IllegalStateException("Table size must be > 0");
        }

        this.size = size;
    }

    @Override
    public SELF setSeed(long seed) {
        rngSeed = seed;
        // noinspection unchecked
        return (SELF) this;
    }

    @Override
    public SELF addColumn(ColumnGenerator<?> generator) {
        if (!columns.add(generator)) {
            throw new IllegalArgumentException("Column " + generator.getName() + " already exists");
        }

        // noinspection unchecked
        return (SELF) this;
    }


    private static final class ColumnGeneratorKey extends KeyedObjectKey.Basic<String, ColumnGenerator<?>> {
        @Override
        public String getKey(ColumnGenerator<?> columnGenerator) {
            return columnGenerator.getName();
        }
    }

    @NotNull
    protected List<ColumnGenerator<?>> getColumnGenerators() {
        final List<ColumnGenerator<?>> generators = new ArrayList<>(columns.size());
        columns.forEach(generators::add);
        return generators;
    }

}
