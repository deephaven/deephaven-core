/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;

/**
 * Manager for ColumnSources in a Table.
 */
public interface ColumnSourceManager {

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
     * Observe size changes in the previously added table locations, and update the managed column sources accordingly.
     *
     * @param initializing Whether we are initializing the column manager
     *
     * @return The RowSet of added keys
     */
    WritableRowSet refresh(final boolean initializing);

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
     * {@link io.deephaven.engine.rowset.RowSet row sets} for each location.
     *
     * @return The added locations that have been found to exist and have non-zero size
     */
    @SuppressWarnings("unused")
    Table locationTable();

    /**
     * Get the name of the column that contains the {@link TableLocation} values from {@link #locationTable()}.
     *
     * @return The name of the location column
     */
    @SuppressWarnings("unused")
    String locationColumnName();

    /**
     * Get the name of the column that contains the offset values from {@link #locationTable()}.
     *
     * @return The name of the location column
     */
    @SuppressWarnings("unused")
    String offsetColumnName();

    /**
     * Get the name of the column that contains the {@link RowSet} values from {@link #locationTable()}.
     *
     * @return The name of the row set column
     */
    @SuppressWarnings("unused")
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
     * @return true if the location key was actually removed
     * @param tableLocationKey the location key being removed
     */
    boolean removeLocationKey(@NotNull ImmutableTableLocationKey tableLocationKey);
}
