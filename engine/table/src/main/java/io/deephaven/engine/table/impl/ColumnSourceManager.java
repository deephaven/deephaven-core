//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * Manager for ColumnSources in a Table.
 */
public interface ColumnSourceManager extends LivenessNode {

    /**
     * Get a map of name to {@link ColumnSource} for the column sources maintained by this manager.
     *
     * @return An unmodifiable view of the column source map maintained by this manager.
     */
    Map<String, ? extends ColumnSource<?>> getColumnSources();

    /**
     * Add a table location to the list to be checked in run().
     * 
     * @param tableLocation The table location to be added
     */
    void addLocation(@NotNull TableLocation tableLocation);

    /**
     * Observe initial sizes for the previously added table locations, and update the managed column sources
     * accordingly. Create any {@link DataIndex data indexes} that may be derived from the locations.
     *
     * @return The initial set of initially-available row keys, to be owned by the caller. This row set will have a
     *         {@link io.deephaven.engine.table.impl.indexer.DataIndexer data indexer} populated with any data indexes
     *         that were created.
     */
    TrackingWritableRowSet initialize();

    /**
     * Observe size changes in the previously added table locations, and update the managed column sources accordingly.
     *
     * @return The set of added row keys, to be owned by the caller
     */
    TableUpdate refresh();

    /**
     * Advise this ColumnSourceManager that an error has occurred, and that it will no longer be {@link #refresh()
     * refreshed}. This method should ensure that the error is delivered to downstream {@link TableListener listeners}
     * if appropriate.
     *
     * @param error The error that occurred
     * @param entry The failing node's entry, if known
     */
    void deliverError(@NotNull Throwable error, @Nullable TableListener.Entry entry);

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
     * Get the added locations that have been found to exist and have non-zero size as a table containing the
     * {@link io.deephaven.engine.rowset.RowSet row sets} for each location. May only be called after
     * {@link #initialize()}. The returned table will also have columns corresponding to the partitions found in the
     * locations, for the convenience of many downstream operations.
     *
     * @return The added locations that have been found to exist and have non-zero size
     */
    Table locationTable();

    /**
     * Get the name of the column that contains the {@link TableLocation} values from {@link #locationTable()}.
     *
     * @return The name of the location column
     */
    String locationColumnName();

    /**
     * Get the name of the column that contains the {@link RowSet} values from {@link #locationTable()}.
     *
     * @return The name of the row set column
     */
    String rowSetColumnName();

    /**
     * Report whether this ColumnSourceManager has no locations that have been "included" (i.e. found to exist with
     * non-zero size).
     * 
     * @return True if there are no included locations
     */
    boolean isEmpty();

    /**
     * Remove a table location key from the sources.
     *
     * @param tableLocationKey the location key being removed
     */
    void removeLocationKey(@NotNull ImmutableTableLocationKey tableLocationKey);

    /**
     * Get a map of Table attributes that can be applied to the output source table, given the update modes of the
     * underlying table location provider.
     *
     * @param tableUpdateMode The update mode of the table location set
     * @param tableLocationUpdateMode The update mode of the table location rows
     */
    Map<String, Object> getTableAttributes(
            @NotNull TableUpdateMode tableUpdateMode,
            @NotNull TableUpdateMode tableLocationUpdateMode);
}
