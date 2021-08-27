/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * A dynamically changing table.
 *
 * The DynamicTable interface provides support for listening for table changes and errors.
 */
public interface DynamicTable extends Table, NotificationQueue.Dependency, DynamicNode, SystemicObject {
    /**
     * <p>
     * Wait for updates to this DynamicTable.
     * <p>
     * In some implementations, this call may also terminate in case of interrupt or spurious wakeup (see
     * java.util.concurrent.locks.Condition#await()).
     *
     * @throws InterruptedException In the event this thread is interrupted
     */
    void awaitUpdate() throws InterruptedException;

    /**
     * <p>
     * Wait for updates to this DynamicTable.
     * <p>
     * In some implementations, this call may also terminate in case of interrupt or spurious wakeup (see
     * java.util.concurrent.locks.Condition#await()).
     *
     * @param timeout The maximum time to wait in milliseconds.
     *
     * @return false if the timeout elapses without notification, true otherwise.
     * @throws InterruptedException In the event this thread is interrupted
     */
    boolean awaitUpdate(long timeout) throws InterruptedException;

    /**
     * Subscribe for updates to this table. Listener will be invoked via the LiveTableMonitor notification queue
     * associated with this DynamicTable.
     *
     * @param listener listener for updates
     */
    default void listenForUpdates(Listener listener) {
        listenForUpdates(listener, false);
    }

    /**
     * Subscribe for updates to this table. After the optional initial image, listener will be invoked via the
     * LiveTableMonitor notification queue associated with this DynamicTable.
     *
     * @param listener listener for updates
     * @param replayInitialImage true to process updates for all initial rows in the table plus all new row changes;
     *        false to only process new row changes
     */
    void listenForUpdates(Listener listener, boolean replayInitialImage);

    /**
     * Subscribe for updates to this table. Listener will be invoked via the LiveTableMonitor notification queue
     * associated with this DynamicTable.
     *
     * @param listener listener for updates
     */
    void listenForUpdates(ShiftAwareListener listener);

    /**
     * Subscribe for updates to this table. Direct listeners are invoked immediately when changes are published, rather
     * than via a LiveTableMonitor notification queue.
     *
     * @param listener listener for updates
     */
    void listenForDirectUpdates(Listener listener);

    /**
     * Unsubscribe the supplied listener.
     *
     * @param listener listener for updates
     */
    void removeUpdateListener(Listener listener);

    /**
     * Unsubscribe the supplied listener.
     *
     * @param listener listener for updates
     */
    void removeUpdateListener(ShiftAwareListener listener);

    /**
     * Unsubscribe the supplied listener.
     *
     * @param listener listener for updates
     */
    void removeDirectUpdateListener(final Listener listener);

    /**
     * Initiate update delivery to this table's listeners. Will notify direct listeners before completing, and enqueue
     * notifications for all other listeners.
     *
     * @param added index values added to the table
     * @param removed index values removed from the table
     * @param modified index values modified in the table.
     */
    default void notifyListeners(Index added, Index removed, Index modified) {
        notifyListeners(new ShiftAwareListener.Update(added, removed, modified, IndexShiftData.EMPTY,
                modified.isEmpty() ? ModifiedColumnSet.EMPTY : ModifiedColumnSet.ALL));
    }

    /**
     * Initiate update delivery to this table's listeners. Will notify direct listeners before completing, and enqueue
     * notifications for all other listeners.
     *
     * @param update the set of table changes to propagate The caller gives this update object away; the invocation of
     *        {@code notifyListeners} takes ownership, and will call {@code release} on it once it is not used anymore;
     *        callers should pass a {@code clone} for updates they intend to further use.
     */
    void notifyListeners(ShiftAwareListener.Update update);

    /**
     * Initiate failure delivery to this table's listeners. Will notify direct listeners before completing, and enqueue
     * notifications for all other listeners.
     *
     * @param e error
     * @param sourceEntry performance tracking
     */
    void notifyListenersOnError(Throwable e, @Nullable UpdatePerformanceTracker.Entry sourceEntry);

    /**
     * @return true if this table is in a failure state.
     */
    default boolean isFailed() {
        return false;
    }

    /**
     * Retrieve the {@link ModifiedColumnSet} that will be used when propagating updates from this table.
     *
     * @param columnNames the columns that should belong to the resulting set.
     * @return the resulting ModifiedColumnSet for the given columnNames
     */
    default ModifiedColumnSet newModifiedColumnSet(String... columnNames) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the table used to construct columnSets. It is an error if {@code columnNames} and {@code columnSets}
     * are not the same length. The transformer will mark {@code columnSets[i]} as dirty if the column represented by
     * {@code columnNames[i]} is dirty.
     *
     * @param columnNames the source columns
     * @param columnSets the destination columns in the convenient ModifiedColumnSet form
     * @return a transformer that knows the dirty details
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(String[] columnNames,
            ModifiedColumnSet[] columnSets) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the provided resultTable.
     *
     * @param resultTable the destination table
     * @param columnNames the columns that map one-to-one with the result table
     * @return a transformer that passes dirty details via an identity mapping
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(DynamicTable resultTable,
            String... columnNames) {
        final ModifiedColumnSet[] columnSets = new ModifiedColumnSet[columnNames.length];
        for (int i = 0; i < columnNames.length; ++i) {
            columnSets[i] = resultTable.newModifiedColumnSet(columnNames[i]);
        }
        return newModifiedColumnSetTransformer(columnNames, columnSets);
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the provided resultTable.
     *
     * @param resultTable the destination table
     * @param matchPairs the columns that map one-to-one with the result table
     * @return a transformer that passes dirty details via an identity mapping
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(DynamicTable resultTable,
            MatchPair... matchPairs) {
        final ModifiedColumnSet[] columnSets = new ModifiedColumnSet[matchPairs.length];
        for (int ii = 0; ii < matchPairs.length; ++ii) {
            columnSets[ii] = resultTable.newModifiedColumnSet(matchPairs[ii].left());
        }
        return newModifiedColumnSetTransformer(MatchPair.getRightColumns(matchPairs), columnSets);
    }

    /**
     * Create a transformer that uses an identity mapping from one ColumnSourceMap to another. The two CSMs must have
     * equivalent column names and column ordering.
     *
     * @param newColumns the column source map for result table
     * @return a simple Transformer that makes a cheap, but CSM compatible copy
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetIdentityTransformer(
            final Map<String, ColumnSource<?>> newColumns) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a transformer that uses an identity mapping from one DynamicTable to another. The two tables must have
     * equivalent column names and column ordering.
     *
     * @param other the result table
     * @return a simple Transformer that makes a cheap, but CSM compatible copy
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetIdentityTransformer(DynamicTable other) {
        throw new UnsupportedOperationException();
    }
}
