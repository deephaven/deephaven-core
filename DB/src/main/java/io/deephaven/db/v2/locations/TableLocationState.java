package io.deephaven.db.v2.locations;

import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Interface for the mutable fields of a table location.  Supports multi-value copy methods, so that applications
 * needing a consistent view of all fields can work with a local copy while only locking this object for a short while.
 */
public interface TableLocationState {

    long NULL_SIZE = Long.MIN_VALUE;
    long NULL_TIME = Long.MIN_VALUE;

    /**
     * @return The Object that accessors should synchronize on if they want to invoke multiple getters with consistent results.
     */
    @NotNull Object getStateLock();

    /**
     * @return The size of a table location:
     *         <br>NULL_SIZE : Size information is unknown or does not exist for this table location
     *         <br>     >= 0 : The table location exists and has (possibly empty) data
     */
    long getSize();

    /**
     * @return The last modified time for a table location, in milliseconds from the epoch:
     *         <br>NULL_TIME : Modification time information is unknown or does not exist for this table location
     *         <br>    >= 0L : The time this table was last modified, in milliseconds from the UTC epoch
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
            return destinationHolder.setValues(getSize(), getLastModifiedTimeMillis());
        }
    }

    /**
     * Write all state values from this to the supplied output.
     *
     * @param output The output for output
     */
    @FinalDefault
    default void writeStateValuesTo(@NotNull DataOutput output) throws IOException {
        synchronized (getStateLock()) {
            output.writeLong(getSize());
            output.writeLong(getLastModifiedTimeMillis());
        }
    }
}
