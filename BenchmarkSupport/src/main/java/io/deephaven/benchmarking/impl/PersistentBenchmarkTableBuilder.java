/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.impl;

import io.deephaven.benchmarking.BenchmarkTable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class PersistentBenchmarkTableBuilder extends AbstractBenchmarkTableBuilder<PersistentBenchmarkTableBuilder> {

    private final LinkedHashSet<String> groupingColumns = new LinkedHashSet<>();
    private String partitioningFormula;
    private String sortingFormula;
    private int partitionCount;

    public PersistentBenchmarkTableBuilder(String name, int size) {
        super(name, size);

    }

    public PersistentBenchmarkTableBuilder addGroupingColumns(String... columns) {
        if (columns == null || columns.length == 0) {
            throw new IllegalArgumentException("Columns must not be null or empty");
        }

        groupingColumns.addAll(Arrays.asList(columns));

        return this;
    }

    public PersistentBenchmarkTableBuilder setPartitioningFormula(String formula) {
        partitioningFormula = formula;
        return this;
    }

    public PersistentBenchmarkTableBuilder setSortingFormula(String formula) {
        sortingFormula = formula;
        return this;
    }

    public PersistentBenchmarkTableBuilder setPartitionCount(int nPartitions) {
        partitionCount = nPartitions;
        return this;
    }

    @Override
    public BenchmarkTable build() {

        final Set<String> missingGroupingColumns = new HashSet<>(groupingColumns);
        columns.keySet().forEach(missingGroupingColumns::remove);

        if (!missingGroupingColumns.isEmpty()) {
            throw new IllegalStateException("Grouping requested on the following nonexistant columns "
                    + String.join(", ", missingGroupingColumns));
        }

        // TODO (deephaven/deephaven-core/issues/147): Replace this with a Parquet-backed table, or delete this entirely
        // and use in-memory always
        return new InMemoryBenchmarkTable(name, size, rngSeed, getColumnGenerators());
    }
}
