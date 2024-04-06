//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util.tables;

import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.plot.errors.PlotExceptionCause;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Holds a {@link PartitionedTable}.
 */
public abstract class PartitionedTableHandle implements Serializable, PlotExceptionCause {
    private static final AtomicInteger nextId = new AtomicInteger();

    /** A reference to the source {@link PartitionedTable} for this handle. */
    private transient PartitionedTable partitionedTable;

    /** The key columns that define the partitioned table, in order */
    private String[] orderedKeyColumns;

    /** The set of all columns that will be used by clients of this handle. */
    private final Set<String> requiredColumns;

    /** A Set of ordered key columns for the map */
    private final Set<String> keyColumns;

    /** A unique ID to identify this handle */
    private final int id;

    /** If this handle is supplying a OneClick */
    private boolean oneClickMap = false;

    private final PlotInfo plotInfo;

    protected PartitionedTableHandle(
            final Collection<String> columns,
            final String[] keyColumns,
            final PlotInfo plotInfo) {
        this.id = nextId.incrementAndGet();
        this.requiredColumns = new LinkedHashSet<>(columns);
        Collections.addAll(this.requiredColumns, keyColumns);
        this.keyColumns = new LinkedHashSet<>(Arrays.asList(keyColumns));
        this.plotInfo = plotInfo;
    }

    public abstract TableDefinition getTableDefinition();

    public PartitionedTable getPartitionedTable() {
        return partitionedTable;
    }

    // Please call setKeyColumnsOrdered if necessary after using this method
    public void setPartitionedTable(final PartitionedTable partitionedTable) {
        this.partitionedTable = partitionedTable;
    }

    // Setting the PartitionedTable may result in a different order for columns
    public void setKeyColumnsOrdered(final String[] orderedKeyColumns) {
        this.orderedKeyColumns = orderedKeyColumns;
    }

    public int id() {
        return id;
    }

    public void addColumn(final String column) {
        requiredColumns.add(column);
    }

    public Set<String> getColumns() {
        return requiredColumns;
    }

    /**
     * Get the set of columns to .view() when the table is fetched. Typically this is identical to {@link #getColumns()}
     * however, there are situations where the transformations applied to a PartitionedTable result in columns that are
     * not present in the base PartitionedTable. (for example catHistPlot).
     *
     * @return The columms to apply via view on fetch
     */
    public Set<String> getFetchViewColumns() {
        return getColumns();
    }

    public Set<String> getKeyColumns() {
        return keyColumns;
    }

    public String[] getKeyColumnsOrdered() {
        return orderedKeyColumns;
    }

    @Override
    public PlotInfo getPlotInfo() {
        return plotInfo;
    }

    public void setOneClickMap(boolean isOneClick) {
        this.oneClickMap = isOneClick;
    }

    public boolean isOneClickMap() {
        return oneClickMap;
    }

    public void applyFunction(final Function<Table, Table> function) {
        // do nothing
    }
}
