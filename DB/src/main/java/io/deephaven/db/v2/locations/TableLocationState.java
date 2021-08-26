package io.deephaven.db.v2.locations;

import io.deephaven.db.v2.sources.regioned.RegionedColumnSource;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for the mutable fields of a table location. Supports multi-value copy methods, so that
 * applications needing a consistent view of all fields can work with a local copy while only
 * locking this object for a short while.
 */
public interface TableLocationState {

    long NULL_SIZE = Long.MIN_VALUE;
    long NULL_TIME = Long.MIN_VALUE;

    /**
     * @return The Object that accessors should synchronize on if they want to invoke multiple
     *         getters with consistent results.
     */
    @NotNull
    Object getStateLock();

    /**
     * @return The (possibly-empty) {@link ReadOnlyIndex index} of a table location, or {@code null}
     *         if index information is unknown or does not exist for this table location.
     * @implNote This index must not have any key larger than
     *           {@link RegionedColumnSource#ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK the
     *           region mask}.
     * @apiNote The returned index will be a "clone", meaning the caller must
     *          {@link ReadOnlyIndex#close()} it when finished.
     */
    ReadOnlyIndex getIndex();

    /**
     * @return The size of a table location: <br>
     *         {@link #NULL_SIZE NULL_SIZE}: Size information is unknown or does not exist for this
     *         location <br>
     *         {@code >= 0}: The table location exists and has (possibly empty) data
     */
    long getSize();

    /**
     * @return The last modified time for a table location, in milliseconds from the epoch: <br>
     *         {@link #NULL_TIME NULL_TIME}: Modification time information is unknown or does not
     *         exist for this location <br>
     *         {@code >= 0}: The time this table was last modified, in milliseconds from the UTC
     *         epoch
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
            return destinationHolder.setValues(getIndex(), getLastModifiedTimeMillis());
        }
    }
}
