/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.util.tables;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Holds a handle on a one click table that may get swapped out for another table.
 */
public abstract class SwappableTableOneClickAbstract extends SwappableTable implements SwappablePartitionedTable {
    private static final long serialVersionUID = 2L;

    protected final long updateInterval;
    protected final boolean requireAllFiltersToDisplay;
    protected final String[] byColumns;
    protected final PartitionedTableHandle partitionedTableHandle;
    private final Comparable seriesName;

    protected SwappableTableOneClickAbstract(@NotNull final Comparable seriesName,
            final long updateInterval,
            @NotNull final PartitionedTableHandle partitionedTableHandle,
            final boolean requireAllFiltersToDisplay,
            @NotNull final String[] byColumns) {
        super(partitionedTableHandle);
        Require.neqNull(byColumns, "byColumns");
        this.partitionedTableHandle = partitionedTableHandle;
        this.seriesName = seriesName;
        this.updateInterval = updateInterval;
        this.requireAllFiltersToDisplay = requireAllFiltersToDisplay;
        this.byColumns = byColumns;
    }

    @Override
    public void addColumn(final String column) {
        this.partitionedTableHandle.addColumn(column);
    }

    public Comparable getSeriesName() {
        return seriesName;
    }

    public abstract List<String> getByColumns();

    @Override
    public PartitionedTableHandle getPartitionedTableHandle() {
        return partitionedTableHandle;
    }

    public boolean isRequireAllFiltersToDisplay() {
        return requireAllFiltersToDisplay;
    }
}
