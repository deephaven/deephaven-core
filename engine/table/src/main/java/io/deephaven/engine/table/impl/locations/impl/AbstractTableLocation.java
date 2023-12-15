/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.hash.KeyedObjectHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Partial TableLocation implementation for use by TableDataService implementations.
 */
public abstract class AbstractTableLocation
        extends SubscriptionAggregator<TableLocation.Listener>
        implements TableLocation {
    protected static final SoftReference<BasicDataIndex> NO_INDEX_SENTINEL = new SoftReference<>(null);

    private final ImmutableTableKey tableKey;
    private final ImmutableTableLocationKey tableLocationKey;

    private final TableLocationStateHolder state = new TableLocationStateHolder();
    private final KeyedObjectHashMap<CharSequence, ColumnLocation> columnLocations =
            new KeyedObjectHashMap<>(StringUtils.charSequenceKey());

    /** A map of data index columns to materialized index tables for this location. */
    protected volatile Map<List<String>, SoftReference<BasicDataIndex>> cachedIndexes;

    /**
     * @param tableKey Table key for the table this location belongs to
     * @param tableLocationKey Table location key that identifies this location
     * @param supportsSubscriptions Whether subscriptions are to be supported
     */
    protected AbstractTableLocation(@NotNull final TableKey tableKey,
            @NotNull final TableLocationKey tableLocationKey,
            final boolean supportsSubscriptions) {
        super(supportsSubscriptions);
        this.tableKey = Require.neqNull(tableKey, "tableKey").makeImmutable();
        this.tableLocationKey = Require.neqNull(tableLocationKey, "tableLocationKey").makeImmutable();
        cachedIndexes = new HashMap<>();
    }

    @Override
    public final String toString() {
        return toStringHelper();
    }


    // ------------------------------------------------------------------------------------------------------------------
    // TableLocationState implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final Object getStateLock() {
        return state.getStateLock();
    }

    @Override
    public final RowSet getRowSet() {
        return state.getRowSet();
    }

    @Override
    public final long getSize() {
        return state.getSize();
    }

    @Override
    public final long getLastModifiedTimeMillis() {
        return state.getLastModifiedTimeMillis();
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocation implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final ImmutableTableKey getTableKey() {
        return tableKey;
    }

    @Override
    @NotNull
    public final ImmutableTableLocationKey getKey() {
        return tableLocationKey;
    }

    @Override
    protected final void deliverInitialSnapshot(@NotNull final Listener listener) {
        listener.handleUpdate();
    }

    /**
     * See TableLocationState for documentation of values.
     *
     * @param rowSet The new RowSet. Ownership passes to this location; callers should {@link RowSet#copy() copy} it if
     *        necessary.
     * @param lastModifiedTimeMillis The new lastModificationTimeMillis
     */
    public final void handleUpdate(final RowSet rowSet, final long lastModifiedTimeMillis) {
        if (state.setValues(rowSet, lastModifiedTimeMillis) && supportsSubscriptions()) {
            deliverUpdateNotification();
        }
    }

    /**
     * Update all state fields from source's values, as in {@link #handleUpdate(RowSet, long)}. See
     * {@link TableLocationState} for documentation of values.
     *
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
    @NotNull
    public final ColumnLocation getColumnLocation(@NotNull final CharSequence name) {
        return columnLocations.putIfAbsent(name, n -> makeColumnLocation(n.toString()));
    }

    @NotNull
    protected abstract ColumnLocation makeColumnLocation(@NotNull final String name);

    /**
     * Clear all column locations (usually because a truncated location was observed).
     */
    @SuppressWarnings("unused")
    protected final void clearColumnLocations() {
        columnLocations.clear();
    }

    @Nullable
    @Override
    public final BasicDataIndex getDataIndex(@NotNull final String... columns) {
        final List<String> colNames = Arrays.asList(columns);
        BasicDataIndex index = null;
        if (cachedIndexes != null) {
            final SoftReference<BasicDataIndex> cachedIndex = cachedIndexes.get(colNames);
            if (cachedIndex == NO_INDEX_SENTINEL) {
                return null;
            }

            if (cachedIndex != null) {
                index = cachedIndex.get();
                if (index != null) {
                    return index;
                }
            }
        }

        synchronized (this) {
            if (cachedIndexes == null) {
                cachedIndexes = new HashMap<>();
            }

            final SoftReference<BasicDataIndex> cachedIndex = cachedIndexes.get(colNames);
            if (cachedIndex == NO_INDEX_SENTINEL) {
                return null;
            }

            if (cachedIndex != null) {
                index = cachedIndex.get();
            }

            if (index == null) {
                index = loadDataIndex(columns);

                if (index == null) {
                    cachedIndexes.put(colNames, NO_INDEX_SENTINEL);
                } else {
                    cachedIndexes.put(colNames, new SoftReference<>(index));
                }
            }

            return index;
        }
    }

    /**
     * Load the data index from the location implementation. Implementations of this method should not perform any
     * result caching.
     *
     * @param columns the columns to load an index for.
     * @return the data index table, or an empty table or null if none existed.
     */
    @Nullable
    protected abstract BasicDataIndex loadDataIndex(@NotNull final String... columns);
}
