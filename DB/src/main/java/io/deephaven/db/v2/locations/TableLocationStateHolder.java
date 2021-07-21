package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.IOException;

/**
 * Tool for generic multi-field "atomic" get/set of state values for a table location.
 * NB: Possibly-concurrent usages should be externally synchronized.
 */
public class TableLocationStateHolder implements TableLocationState {

    private volatile long size = NULL_SIZE;
    private volatile long lastModifiedTimeMillis = NULL_TIME;

    private TableLocationStateHolder(final long size, final long lastModifiedTimeMillis) {
        this.size = size;
        this.lastModifiedTimeMillis = lastModifiedTimeMillis;
    }

    public TableLocationStateHolder() {
        this(NULL_SIZE, NULL_TIME);
    }

//------------------------------------------------------------------------------------------------------------------
    // TableLocationState implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public @NotNull final Object getStateLock() {
        return this;
    }

    @Override
    public final long getSize() {
        return size;
    }

    @Override
    public final long getLastModifiedTimeMillis() {
        return lastModifiedTimeMillis;
    }

    //------------------------------------------------------------------------------------------------------------------
    // Setters
    //------------------------------------------------------------------------------------------------------------------

    /**
     * Clear this holder, by reinitializing all fields to their "null" equivalents.
     */
    protected final void clearValues() {
        setValues(NULL_SIZE, NULL_TIME);
    }

    /**
     * Set all state values.
     *
     * @param size The new size
     * @param lastModifiedTimeMillis The new modification time
     * @return Whether any of the values changed
     */
    public final synchronized boolean setValues(final long size, final long lastModifiedTimeMillis) {
        boolean changed = false;
        if (size != this.size) {
            changed = true;
            this.size = size;
        }
        if (lastModifiedTimeMillis != this.lastModifiedTimeMillis) {
            changed = true;
            this.lastModifiedTimeMillis = lastModifiedTimeMillis;
        }
        return changed;
    }

    /**
     * Read all values from the supplied input into this state holder.
     *
     * @param input A input to read from
     * @return Whether any of the values changed
     */
    @SuppressWarnings("UnusedReturnValue")
    public final boolean readValuesFrom(@NotNull final DataInput input) throws IOException {
        return setValues(input.readLong(), input.readLong());
    }
}
