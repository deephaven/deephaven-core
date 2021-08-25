package io.deephaven.db.v2.locations;

import io.deephaven.db.v2.utils.ReadOnlyIndex;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tool for generic multi-field "atomic" get/set of state values for a table location. NB: Possibly-concurrent usages
 * should be externally synchronized.
 */
public class TableLocationStateHolder implements TableLocationState {

    private ReadOnlyIndex index;
    private volatile long lastModifiedTimeMillis;

    private TableLocationStateHolder(@Nullable final ReadOnlyIndex index, final long lastModifiedTimeMillis) {
        this.index = index;
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
    public final synchronized ReadOnlyIndex getIndex() {
        return index.clone();
    }

    @Override
    public final synchronized long getSize() {
        return index == null ? NULL_SIZE : index.size();
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
     * @param index The new index. Ownership passes to this holder; callers should {@link ReadOnlyIndex#clone() clone}
     *        it if necessary.
     * @param lastModifiedTimeMillis The new modification time
     * @return Whether any of the values changed
     */
    public final synchronized boolean setValues(@Nullable final ReadOnlyIndex index,
            final long lastModifiedTimeMillis) {
        boolean changed = false;

        if (index != this.index) {
            // Currently, locations *must* be add-only. Consequently, we assume that a size check is sufficient.
            changed = (index == null || this.index == null || index.size() != this.index.size());
            if (this.index != null) {
                this.index.close();
            }
            this.index = index;
        }
        if (lastModifiedTimeMillis != this.lastModifiedTimeMillis) {
            changed = true;
            this.lastModifiedTimeMillis = lastModifiedTimeMillis;
        }
        return changed;
    }
}
