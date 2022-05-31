/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.sources.DeferredGroupingColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;

/**
 * Manager for ColumnSources in a Table.
 */
public interface ColumnSourceManager {

    /**
     * Get a map of name to {@link DeferredGroupingColumnSource} for the column sources maintained by this manager.
     *
     * @return An unmodifiable view of the column source map maintained by this manager.
     */
    Map<String, ? extends DeferredGroupingColumnSource<?>> getColumnSources();

    /**
     * Turn off column grouping, and clear the groupings on all GROUPING column sources. Note that this does *not*
     * affect PARTITIONING columns.
     */
    void disableGrouping();

    /**
     * Add a table location to the list to be checked in run().
     * 
     * @param tableLocation The table location to be added
     */
    void addLocation(@NotNull TableLocation tableLocation);

    /**
     * Observe size changes in the previously added table locations, and update the managed column sources accordingly.
     * 
     * @return The RowSet of added keys
     */
    WritableRowSet refresh();

    /**
     * Get the added locations, first the ones that have been "included" (found to exist with non-zero size) in order of
     * inclusion, then the remainder in order of discovery.
     * 
     * @return All known locations, ordered as described
     */
    Collection<TableLocation> allLocations();

    /**
     * Get the added locations that have been found to exist and have non-zero size.
     * 
     * @return The added locations that have been found to exist and have non-zero size
     */
    @SuppressWarnings("unused")
    Collection<TableLocation> includedLocations();

    /**
     * Report whether this ColumnSourceManager has no locations that have been "included" (i.e. found to exist with
     * non-zero size).
     * 
     * @return True if there are no included locations
     */
    boolean isEmpty();
}
