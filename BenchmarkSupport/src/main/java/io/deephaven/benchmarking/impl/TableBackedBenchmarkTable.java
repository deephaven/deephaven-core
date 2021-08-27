package io.deephaven.benchmarking.impl;

import io.deephaven.db.tables.Table;
import io.deephaven.benchmarking.generator.ColumnGenerator;

import java.io.File;
import java.util.List;

/**
 * A {@link io.deephaven.benchmarking.BenchmarkTable} implementation that is based of an existing table, adding the
 * {@link ColumnGenerator}s provided as new columns via {@link Table#update(String...)}
 */
public class TableBackedBenchmarkTable extends AbstractBenchmarkTable {
    private final Table sourceTable;

    TableBackedBenchmarkTable(String name, Table sourceTable, long rngSeed, List<ColumnGenerator<?>> columnsToAdd) {
        super(name, rngSeed, columnsToAdd);
        this.sourceTable = sourceTable;
    }

    @Override
    protected Table populate() {
        return sourceTable.update(getGeneratorMap().entrySet().stream()
                .map(ent -> ent.getValue().getUpdateString(ent.getKey())).toArray(String[]::new));
    }

    @Override
    public void cleanup() {

    }



    @Override
    public long getSize() {
        return sourceTable.size();
    }

    @Override
    public File getLocation() {
        throw new UnsupportedOperationException();
    }
}
