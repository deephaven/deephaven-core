//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmarking.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.util.TableTools;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.util.SafeCloseable;
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
        definition = TableDefinition.of(definitions);
    }

    protected Table generateTable() {
        // the generators share one random number generator and keep unsynchronized state, so their columns must be
        // evaluated serially, one column at a time, for a seed to reproduce the same table
        try (final SafeCloseable ignored = ExecutionContext.getContext()
                .withOperationInitializer(OperationInitializer.NON_PARALLELIZABLE).open()) {
            return TableTools.emptyTable(nRows).updateView(
                    getGeneratorMap().entrySet().stream().map(ent -> ent.getValue().getUpdateString(ent.getKey()))
                            .toArray(String[]::new))
                    .select();
        }
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
