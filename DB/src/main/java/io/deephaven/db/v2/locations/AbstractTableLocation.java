/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.string.StringUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Partial TableLocation implementation for use by TableDataService implementations.
 */
public abstract class AbstractTableLocation<TKT extends TableKey, CLT extends ColumnLocation>
        extends SubscriptionAggregator<TableLocation.Listener>
        implements TableLocation<CLT> {

    private @NotNull final TKT tableKey;
    private @NotNull final TableLocationLookupKey<String> tableLocationKey;

    private final TableLocationStateHolder state = new TableLocationStateHolder();
    private final KeyedObjectHashMap<CharSequence, CLT> columnLocations = new KeyedObjectHashMap<>(StringUtils.charSequenceKey());

    /**
     * @param tableKey Table key for the table this location belongs to
     * @param tableLocationKey A key whose field values will be deep-copied to this location
     * @param supportsSubscriptions Whether subscriptions are to be supported
     */
    protected AbstractTableLocation(@NotNull final TKT tableKey,
                                    @NotNull final TableLocationKey tableLocationKey,
                                    final boolean supportsSubscriptions) {
        super(supportsSubscriptions);
        this.tableKey = Require.neqNull(tableKey, "tableKey");
        this.tableLocationKey = new TableLocationLookupKey.Immutable(Require.neqNull(tableLocationKey, "tableLocationKey"));
    }

    @Override
    public final String toString() {
        return toStringHelper();
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableLocationKey implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public @NotNull final String getInternalPartition() {
        return tableLocationKey.getInternalPartition();
    }

    @Override
    public @NotNull final String getColumnPartition() {
        return tableLocationKey.getColumnPartition();
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableLocationState implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public @NotNull final Object getStateLock() {
        return state.getStateLock();
    }

    @Override
    public final long getSize() {
        return state.getSize();
    }

    @Override
    public final long getLastModifiedTimeMillis() {
        return state.getLastModifiedTimeMillis();
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableLocation implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public @NotNull TKT getTableKey() {
        return tableKey;
    }

    @Override
    protected final void deliverInitialSnapshot(@NotNull final Listener listener) {
        listener.handleUpdate();
    }

    /**
     * See TableLocationState for documentation of values.
     *
     * @param size The new size
     * @param lastModifiedTimeMillis The new lastModificationTimeMillis
     */
    public final void handleUpdate(final long size, final long lastModifiedTimeMillis) {
        if (state.setValues(size, lastModifiedTimeMillis) && supportsSubscriptions()) {
            deliverUpdateNotification();
        }
    }

    /**
     * Update all state fields from source's values, as in {@link #handleUpdate(long, long)}.
     * See {@link TableLocationState} for documentation of values.
     * @param source The source to copy state values from
     */
    public void handleUpdate(@NotNull final TableLocationState source) {
        if (source.copyStateValuesTo(state) && supportsSubscriptions()) {
            deliverUpdateNotification();
        }
    }

    private void deliverUpdateNotification() {
        synchronized (subscriptions) {
            if (subscriptions.deliverNotification(Listener::handleUpdate, true)) {
                onEmpty();
            }
        }
    }

    @Override
    public @NotNull CLT getColumnLocation(@NotNull CharSequence name) {
        return columnLocations.putIfAbsent(name, n -> makeColumnLocation(n.toString()));
    }

    protected abstract @NotNull CLT makeColumnLocation(@NotNull final String name);

    /**
     * Clear all column locations (usually because a truncated location was observed).
     */
    protected final void clearColumnLocations() {
        columnLocations.clear();
    }
}
