package io.deephaven.benchmarking.impl;

import io.deephaven.db.tables.Table;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * The basic implementation of {@link BenchmarkTableBuilder}. It allows users to specify table type
 * and add columns, while specifying their RNG properties.
 */
public class TableBackedBenchmarkTableBuilder extends AbstractBenchmarkTableBuilder {


    private Table sourceTable;

    public TableBackedBenchmarkTableBuilder(String name, @NotNull Table fromTable) {
        super(name, (int) fromTable.size());

        if (fromTable.isLive()) {
            throw new IllegalArgumentException("Live source tables are not supported right now.");
        }

        this.sourceTable = fromTable;
    }

    @Override
    public BenchmarkTable build() {
        return new TableBackedBenchmarkTable(name, sourceTable, rngSeed, getColumnGenerators());
    }

}
