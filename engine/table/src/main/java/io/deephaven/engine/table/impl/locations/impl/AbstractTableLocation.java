//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.liveness.*;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.impl.util.FieldUtils;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Partial TableLocation implementation for use by TableDataService implementations.
 */
public abstract class AbstractTableLocation
        extends SubscriptionAggregator<TableLocation.Listener>
        implements TableLocation, DelegatingLivenessReferent {

    private final ImmutableTableKey tableKey;
    private final ImmutableTableLocationKey tableLocationKey;

    private final TableLocationStateHolder state = new TableLocationStateHolder();
    private final KeyedObjectHashMap<CharSequence, ColumnLocation> columnLocations =
            new KeyedObjectHashMap<>(StringUtils.charSequenceKey());

    private final ReferenceCountedLivenessReferent livenessReferent;

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<AbstractTableLocation, KeyedObjectHashMap> CACHED_DATA_INDEXES_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    AbstractTableLocation.class, KeyedObjectHashMap.class, "cachedDataIndexes");
    private static final SoftReference<BasicDataIndex> NO_INDEX_SENTINEL = new SoftReference<>(null);
    private static final KeyedObjectKey<List<String>, CachedDataIndex> CACHED_DATA_INDEX_KEY =
            new KeyedObjectKey.BasicAdapter<>(CachedDataIndex::getColumns);

    /** A map of data index columns to cache nodes for materialized data indexes for this location. */
    @SuppressWarnings("unused")
    private volatile KeyedObjectHashMap<List<String>, CachedDataIndex> cachedDataIndexes;

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

        livenessReferent = new ReferenceCountedLivenessReferent() {
            @OverridingMethodsMustInvokeSuper
            @Override
            protected void destroy() {
                super.destroy();
                AbstractTableLocation.this.destroy();
            }
        };
    }

    @Override
    public final String toString() {
        return toStringHelper();
    }

    @Override
    public LivenessReferent asLivenessReferent() {
        return livenessReferent;
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
    public final void clearColumnLocations() {
        columnLocations.clear();
    }

    /**
     * Caching structure for loaded data indexes.
     */
    private class CachedDataIndex {

        private final List<String> columns;

        private volatile SoftReference<BasicDataIndex> indexReference;

        private CachedDataIndex(@NotNull final List<String> columns) {
            this.columns = columns;
        }

        private List<String> getColumns() {
            return columns;
        }

        private BasicDataIndex getDataIndex() {
            SoftReference<BasicDataIndex> localReference = indexReference;
            BasicDataIndex localIndex;
            if (localReference == NO_INDEX_SENTINEL) {
                return null;
            }
            if (localReference != null && (localIndex = localReference.get()) != null) {
                return localIndex;
            }
            synchronized (this) {
                localReference = indexReference;
                if (localReference == NO_INDEX_SENTINEL) {
                    return null;
                }
                if (localReference != null && (localIndex = localReference.get()) != null) {
                    return localIndex;
                }
                localIndex = loadDataIndex(columns.toArray(String[]::new));
                indexReference = localIndex == null ? NO_INDEX_SENTINEL : new SoftReference<>(localIndex);
                return localIndex;
            }
        }
    }

    @Override
    @Nullable
    public final BasicDataIndex getDataIndex(@NotNull final String... columns) {
        final List<String> columnNames = new ArrayList<>(columns.length);
        Collections.addAll(columnNames, columns);
        columnNames.sort(String::compareTo);

        // noinspection unchecked
        final KeyedObjectHashMap<List<String>, CachedDataIndex> localCachedDataIndexes =
                FieldUtils.ensureField(this, CACHED_DATA_INDEXES_UPDATER, null,
                        () -> new KeyedObjectHashMap<>(CACHED_DATA_INDEX_KEY));
        return localCachedDataIndexes.putIfAbsent(columnNames, CachedDataIndex::new).getDataIndex();
    }

    /**
     * Load the data index from the location implementation. Implementations of this method should not perform any
     * result caching.
     *
     * @param columns The columns to load an index for
     * @return The data index, or {@code null} if none exists
     * @apiNote This method is {@code public} for use in delegating implementations, and should not be called directly
     *          otherwise.
     */
    @InternalUseOnly
    @Nullable
    public abstract BasicDataIndex loadDataIndex(@NotNull String... columns);

    // ------------------------------------------------------------------------------------------------------------------
    // Reference counting implementation
    // ------------------------------------------------------------------------------------------------------------------

    /**
     * The reference count has reached zero, we can clear this location and release any resources.
     */
    protected void destroy() {
        handleUpdate(null, System.currentTimeMillis());
        clearColumnLocations();

        // The key may be holding resources that can be cleared.
        tableLocationKey.clear();
    }
}
