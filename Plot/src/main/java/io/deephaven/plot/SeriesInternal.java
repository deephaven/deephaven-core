//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot;

import io.deephaven.plot.util.tables.PartitionedTableHandle;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.plot.util.tables.TableHandle;

import java.util.Set;

/**
 * Base series that all data series inherit from.
 */
public interface SeriesInternal extends Series {

    /**
     * Gets the axes on which this data will be plotted.
     *
     * @return axes on which this data will be plotted
     */
    AxesImpl axes();

    /**
     * Gets the id for the data series.
     */
    int id();

    /**
     * Gets the name of this data series.
     *
     * @return name of this data series
     */
    Comparable name();

    /**
     * Create a copy of the series on a different set of axes.
     *
     * @param axes new axes.
     * @return copy of the series on a different set of axes.
     */
    SeriesInternal copy(final AxesImpl axes);

    /**
     * Add a table that drives this series.
     *
     * @param h table handle.
     */
    void addTableHandle(TableHandle h);

    /**
     * Removes a table that drives this series.
     *
     * @param h table handle.
     */
    void removeTableHandle(TableHandle h);

    /**
     * Gets all of the tables driving this series.
     *
     * @return all of the tables driving this series.
     */
    Set<TableHandle> getTableHandles();

    /**
     * Gets all of the partitioned tables driving this series.
     *
     * @return all of the partitioned tables driving this series.
     */
    Set<PartitionedTableHandle> getPartitionedTableHandles();

    /**
     * Add a partitioned table that drives this series.
     *
     * @param map partitioned table.
     */
    void addPartitionedTableHandle(PartitionedTableHandle map);

    /**
     * Adds a swappable table that drives this series.
     *
     * @param st swappable table
     */
    void addSwappableTable(SwappableTable st);

    /**
     * Gets the swappable tables that drive this series.
     *
     * @return swappable tables that drive this series.
     */
    Set<SwappableTable> getSwappableTables();
}
