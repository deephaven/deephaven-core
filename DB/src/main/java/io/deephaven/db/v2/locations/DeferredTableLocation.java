/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

public abstract class DeferredTableLocation<CLT extends ColumnLocation> implements TableLocation {

    private final ImmutableTableKey tableKey;

    @FunctionalInterface
    public interface TableLocationCreator<CLT extends ColumnLocation> {
        TableLocation makeTableLocation(@NotNull TableKey tableKey, @NotNull TableLocationKey tableLocationKey);
    }

    private volatile TableLocation tableLocation;
    private TableLocationCreator<CLT> tableLocationCreator;

    private DeferredTableLocation(@NotNull final TableKey tableKey,
                                  @NotNull final TableLocationCreator<CLT> tableLocationCreator) {
        this.tableKey = tableKey.makeImmutable();
        this.tableLocationCreator = Require.neqNull(tableLocationCreator, "tableLocationCreator");
    }

    abstract TableLocationKey getTableLocationKey();

    @NotNull
    private TableLocation getTableLocation() {
        if (tableLocation == null) {
            synchronized (this) {
                if (tableLocation == null) {
                    tableLocation = tableLocationCreator.makeTableLocation(getTableKey(), getTableLocationKey());
                    tableLocationCreator = null;
                }
            }
        }

        return tableLocation;
    }

    @Override
    public final String toString() {
        return toStringHelper();
    }

    @Override
    public final String getImplementationName() {
        final TableLocation localTableLocation = tableLocation;
        if (localTableLocation == null) {
            return "DeferredTableLocation";
        }
        return "DeferredTableLocation(" + localTableLocation.getImplementationName() + ')';
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableLocationKey implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final CharSequence getInternalPartition() {
        return getTableLocationKey().getInternalPartition();
    }

    @Override
    @NotNull
    public final CharSequence getColumnPartition() {
        return getTableLocationKey().getColumnPartition();
    }

    //------------------------------------------------------------------------------------------------------------------
    // Shared partial TableLocation implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    @NotNull
    public final TableKey getTableKey() {
        return tableKey;
    }

    @Override
    @NotNull
    public final CLT getColumnLocation(@NotNull final CharSequence name) {
        return getTableLocation().getColumnLocation(name);
    }

    /**
     * DeferredTableLocation that needs to initialize its inner location for all non-key methods.
     */
    public static final class DataDriven<CLT extends ColumnLocation> extends DeferredTableLocation<CLT> {

        private final TableLocationKey tableLocationKey;

        public DataDriven(@NotNull final TableKey tableKey,
                          @NotNull final TableLocationKey tableLocationKey,
                          @NotNull final TableLocationCreator<CLT> tableLocationCreator) {
            super(tableKey, tableLocationCreator);
            this.tableLocationKey = TableLocationLookupKey.getImmutableKey(Require.neqNull(tableLocationKey, "tableLocationKey"));
        }

        @Override
        final TableLocationKey getTableLocationKey() {
            return tableLocationKey;
        }

        //--------------------------------------------------------------------------------------------------------------
        // TableLocationState implementation
        //--------------------------------------------------------------------------------------------------------------

        @Override
        @NotNull
        public final Object getStateLock() {
            return super.getTableLocation().getStateLock();
        }

        @Override
        public final long getSize() {
            return super.getTableLocation().getSize();
        }

        @Override
        public final long getLastModifiedTimeMillis() {
            return super.getTableLocation().getLastModifiedTimeMillis();
        }

        //--------------------------------------------------------------------------------------------------------------
        // Remaining TableLocation implementation
        //--------------------------------------------------------------------------------------------------------------

        @Override
        public final boolean supportsSubscriptions() {
            return super.getTableLocation().supportsSubscriptions();
        }

        @Override
        public final void subscribe(@NotNull final Listener listener) {
            super.getTableLocation().subscribe(listener);
        }

        @Override
        public final void unsubscribe(@NotNull final Listener listener) {
            super.getTableLocation().unsubscribe(listener);
        }

        @Override
        public final void refresh() {
            super.getTableLocation().refresh();
        }
    }

    /**
     * DeferredTableLocation that implements metadata getters by delegating to a snapshot. Snapshotted TableLocations
     * never support subscriptions, and don't allow any sort of metadata refresh.
     */
    public static final class SnapshotDriven<CLT extends ColumnLocation> extends DeferredTableLocation<CLT> {

        private final TableLocationMetadataIndex.TableLocationSnapshot tableLocationSnapshot;

        public SnapshotDriven(@NotNull final TableKey tableKey,
                              @NotNull final TableLocationMetadataIndex.TableLocationSnapshot tableLocationSnapshot,
                              @NotNull final TableLocationCreator<CLT> tableLocationCreator) {
            super(tableKey, tableLocationCreator);
            this.tableLocationSnapshot = tableLocationSnapshot;
        }

        @Override
        final TableLocationKey getTableLocationKey() {
            return tableLocationSnapshot;
        }

        //--------------------------------------------------------------------------------------------------------------
        // TableLocationState implementation
        //--------------------------------------------------------------------------------------------------------------

        @Override
        @NotNull
        public final Object getStateLock() {
            return this;
        }

        @Override
        public final long getSize() {
            return tableLocationSnapshot.getSize();
        }

        @Override
        public final long getLastModifiedTimeMillis() {
            return tableLocationSnapshot.getLastModifiedTimeMillis();
        }

        //--------------------------------------------------------------------------------------------------------------
        // Remaining TableLocation implementation
        //--------------------------------------------------------------------------------------------------------------

        @Override
        public final boolean supportsSubscriptions() {
            return false;
        }

        @Override
        public final void subscribe(@NotNull final Listener listener) {
            if (tableLocationSnapshot != null) {
                throw new UnsupportedOperationException(this + " doesn't support subscriptions");
            }
        }

        @Override
        public final void unsubscribe(@NotNull final Listener listener) {
            if (tableLocationSnapshot != null) {
                throw new UnsupportedOperationException(this + " doesn't support subscriptions");
            }
        }

        @Override
        public final void refresh() {
        }
    }
}
