package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for the mutable fields of a table location. Supports multi-value copy methods, so that applications needing
 * a consistent view of all fields can work with a local copy while only locking this object for a short while.
 */
public interface TableLocationState {

    long NULL_SIZE = Long.MIN_VALUE;
    long NULL_TIME = Long.MIN_VALUE;

    /**
     * @return The Object that accessors should synchronize on if they want to invoke multiple getters with consistent
     *         results.
     */
    @NotNull
    Object getStateLock();

    /**
     * @return The (possibly-empty) {@link RowSet} of a table location, or {@code null} if RowSet information is unknown
     *         or does not exist for this table location.
     * @implNote This RowSet must not have any key larger than
     *           {@link RegionedColumnSource#ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK the region mask}.
     * @apiNote The returned RowSet will be a "copy", meaning the caller must {@link RowSet#close()} it when finished.
     */
    RowSet getRowSet();

    /**
     * @return The size of a table location: <br>
     *         {@link #NULL_SIZE NULL_SIZE}: Size information is unknown or does not exist for this location <br>
     *         {@code >= 0}: The table location exists and has (possibly empty) data
     */
    long getSize();

    /**
     * @return The last modified time for a table location, in milliseconds from the epoch: <br>
     *         {@link #NULL_TIME NULL_TIME}: Modification time information is unknown or does not exist for this
     *         location <br>
     *         {@code >= 0}: The time this table was last modified, in milliseconds from the UTC epoch
     */
    long getLastModifiedTimeMillis();

    /**
     * Copy all state values from this to the supplied holder.
     *
     * @param destinationHolder The destination for output
     * @return Whether any of destinationHolder's values changed
     */
    @FinalDefault
    default boolean copyStateValuesTo(@NotNull TableLocationStateHolder destinationHolder) {
        synchronized (getStateLock()) {
            return destinationHolder.setValues(getRowSet(), getLastModifiedTimeMillis());
        }
    }
}
