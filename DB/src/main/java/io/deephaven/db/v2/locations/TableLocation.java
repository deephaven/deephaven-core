/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;

/**
 * <br> =====================================================================================================================
 * <br>
 * <br> Building block for Deephaven file-based tables, with helper methods for discovering locations and their sizes.
 * <br>
 * <br> ================================================= INTERFACE =========================================================
 * <br>
 * <br> A location specifies the column and size data location for a splayed table.  The location may be either:
 * <br> (1) the sole location of a stand-alone splayed table, or
 * <br> (2) a single location of a nested partitioned table, in which case it belongs to an internal partition and a column
 * partition.
 * <br>
 * <br> ================================================== COMMENTS =========================================================
 * <br>
 * <br> Future work may allow more fields from TableLocationKey or TableLocationState to be accessed as columns.
 * <br>
 * <br> =====================================================================================================================
 */
public interface TableLocation extends NamedImplementation, LogOutputAppendable, TableLocationState {

    /**
     * Listener interface for anything that wants to know about changes to a location.
     */
    interface Listener extends BasicTableDataListener {

        /**
         * Notify the listener that the table location has been updated.  This may be called "spuriously," i.e. in cases
         * where there has been no substantive update since the last handleUpdate() invocation.  Implementations should
         * use appropriate measures to avoid reacting to spurious updates.
         */
        void handleUpdate();
    }

    /**
     * @return An {@link ImmutableTableKey} instance for the enclosing table
     */
    @NotNull
    ImmutableTableKey getTableKey();

    /**
     * @return An {@link ImmutableTableLocationKey} instance for this location
     */
    @NotNull
    ImmutableTableLocationKey getKey();

    /**
     * Enumeration of possible table location formats.
     */
    enum Format {
        /**
         * The Apache Parquet columnar format.
         */
        PARQUET
    }

    /**
     * Does this location support subscriptions? That is, can this location ever have ticking data?
     *
     * @return True if this location supports subscriptions
     */
    boolean supportsSubscriptions();

    /**
     * <p>Subscribe to pushed location updates. Subscribing more than once with the same listener without an
     * intervening unsubscribe is an error, and may result in undefined behavior.
     * <p>This is a possibly asynchronous operation - listener will receive 1 or more handleUpdate callbacks,
     * followed by 0 or 1 handleException callbacks during invocation and continuing after completion, on a thread
     * determined by the implementation.  Don't hold a lock that prevents notification delivery while subscribing!
     * <p>This method only guarantees eventually consistent state.  To force a state update, use refresh() after
     * subscription completes.
     *
     * @param listener A listener
     */
    void subscribe(@NotNull Listener listener);

    /**
     * Unsubscribe from pushed location updates.
     *
     * @param listener The listener to forget about
     */
    void unsubscribe(@NotNull Listener listener);

    /**
     * Initialize or refresh state information.
     */
    void refresh();

    /**
     * @param name The column name
     * @return The ColumnLocation for the defined column under this table location
     */
    @NotNull
    ColumnLocation getColumnLocation(@NotNull CharSequence name);

    //------------------------------------------------------------------------------------------------------------------
    // LogOutputAppendable implementation / toString() override helper
    //------------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getTableKey())
                .append(':').append(getImplementationName())
                .append('[').append((getInternalPartition() == NULL_PARTITION ? "" : getInternalPartition()))
                .append('/').append(getColumnPartition() == NULL_PARTITION ? "" : getColumnPartition())
                .append(']');
    }

    @Override
    @FinalDefault
    default String toStringHelper() {
        return getTableKey().toString()
                + ':' + getImplementationName()
                + '[' + (getInternalPartition() == NULL_PARTITION ? "" : getInternalPartition())
                + '/' + (getColumnPartition() == NULL_PARTITION ? "" : getColumnPartition())
                + ']';
    }

    /**
     * Format the table key without implementation specific bits.
     *
     * @return a formatted string
     */
    @FinalDefault
    default String toGenericString() {
        return "TableLocation"
                + '[' + getTableKey().getNamespace()
                + '/' + getTableKey().getTableName()
                + '/' + getTableKey().getTableType().getDescriptor()
                + "]:[" + (getInternalPartition() == NULL_PARTITION ? "" : getInternalPartition())
                + '/' + (getColumnPartition() == NULL_PARTITION ? "" : getColumnPartition())
                + ']';
    }

    /**
     * Optional toString path with more implementation detail.
     *
     * @return detailed conversion to string
     */
    @FinalDefault
    default String toStringDetailed() {
        return toStringHelper();
    }
}
