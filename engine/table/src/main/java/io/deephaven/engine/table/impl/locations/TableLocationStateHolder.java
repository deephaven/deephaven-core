package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tool for generic multi-field "atomic" get/set of state values for a table location. NB: Possibly-concurrent usages
 * should be externally synchronized.
 */
public class TableLocationStateHolder implements TableLocationState {

    private RowSet rowSet;
    private volatile long lastModifiedTimeMillis;

    private TableLocationStateHolder(@Nullable final RowSet rowSet, final long lastModifiedTimeMillis) {
        this.rowSet = rowSet;
        this.lastModifiedTimeMillis = lastModifiedTimeMillis;
    }

    public TableLocationStateHolder() {
        this(null, NULL_TIME);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocationState implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final Object getStateLock() {
        return this;
    }

    @Override
    public final synchronized RowSet getRowSet() {
        return rowSet.copy();
    }

    @Override
    public final synchronized long getSize() {
        return rowSet == null ? NULL_SIZE : rowSet.size();
    }

    @Override
    public final long getLastModifiedTimeMillis() {
        return lastModifiedTimeMillis;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Setters
    // ------------------------------------------------------------------------------------------------------------------

    /**
     * Clear this holder, by reinitializing all fields to their "null" equivalents.
     */
    protected final void clearValues() {
        setValues(null, NULL_TIME);
    }

    /**
     * Set all state values.
     *
     * @param rowSet The new RowSet. Ownership passes to this holder; callers should {@link RowSet#copy() copy} it if
     *        necessary.
     * @param lastModifiedTimeMillis The new modification time
     * @return Whether any of the values changed
     */
    public final synchronized boolean setValues(@Nullable final RowSet rowSet,
            final long lastModifiedTimeMillis) {
        boolean changed = false;

        if (rowSet != this.rowSet) {
            // Currently, locations *must* be add-only. Consequently, we assume that a size check is sufficient.
            changed = (rowSet == null || this.rowSet == null || rowSet.size() != this.rowSet.size());
            if (this.rowSet != null) {
                this.rowSet.close();
            }
            this.rowSet = rowSet;
        }
        if (lastModifiedTimeMillis != this.lastModifiedTimeMillis) {
            changed = true;
            this.lastModifiedTimeMillis = lastModifiedTimeMillis;
        }
        return changed;
    }
}
