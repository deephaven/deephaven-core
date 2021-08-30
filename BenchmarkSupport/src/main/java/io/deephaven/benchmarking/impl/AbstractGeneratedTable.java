package io.deephaven.benchmarking.impl;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.SparseSelect;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractGeneratedTable extends AbstractBenchmarkTable {
    private final long nRows;
    private final TableDefinition definition;

    public AbstractGeneratedTable(@NotNull String name, long nRows, long rngSeed,
            @NotNull List<ColumnGenerator<?>> generators) {
        super(name, rngSeed, generators);
        this.nRows = nRows;
        List<ColumnDefinition<?>> definitions = getGeneratorMap()
                .values()
                .stream()
                .map(ColumnGenerator::getDefinition)
                .map(ColumnDefinition::withNormal)
                .collect(Collectors.toList());
        definition = new TableDefinition(definitions);
    }

    protected Table generateTable() {
        return SparseSelect.sparseSelect(TableTools.emptyTable(nRows).updateView(
                getGeneratorMap().entrySet().stream().map(ent -> ent.getValue().getUpdateString(ent.getKey()))
                        .toArray(String[]::new)));
    }

    @Override
    public long getSize() {
        return nRows;
    }

    protected TableDefinition getDefinition() {
        return definition;
    }

    protected abstract Table populate();
}
