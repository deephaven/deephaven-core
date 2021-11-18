package io.deephaven.engine.rowset;

import io.deephaven.engine.updategraph.LogicalClock;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * <p>
 * {@link RowSet} that internally tracks changes and maintains a consistent snapshot of its previous state, valid during
 * the {@link LogicalClock.State#Updating updating} phase of its associated {@link LogicalClock LogicalClock}.
 * <p>
 * Also adds support for hosting opaque index information.
 */
public interface TrackingRowSet extends RowSet {

    long sizePrev();

    WritableRowSet getPrevRowSet();

    long getPrev(long pos);

    long firstRowKeyPrev();

    long lastRowKeyPrev();

    /**
     * Returns the position in {@code [0..(size-1)]} where the row key is found in the previous rowSet. If not found,
     * then return {@code (-(position it would be) - 1)}, as in Array.binarySearch.
     *
     * @param rowKey The row key to search for
     * @return A position from {@code [0..(size-1)]} if the row key was found. If the row key was not found, then
     *         {@code (-position - 1)} as in Array.binarySearch
     */
    long findPrev(long rowKey);

    /**
     * Minimal interface for optional, opaque indexer objects hosted by TrackingRowSet instances.
     */
    interface Indexer {

        /**
         * Callback for the host TrackingRowSet to report a modification that may invalidate cached indexing
         * information.
         */
        void rowSetChanged();
    }

    /**
     * Get an opaque {@link Indexer} object previously associated with this TrackingRowSet, or set and get one created
     * with {@code indexerFactory} if this is the first invocation.
     *
     * @param indexerFactory The indexer factory to be used if no indexer has been set previously
     * @return An opaque indexer object associated with this TrackingRowSet
     */
    <INDEXER_TYPE extends Indexer> INDEXER_TYPE indexer(@NotNull Function<TrackingRowSet, INDEXER_TYPE> indexerFactory);

    @Override
    default TrackingWritableRowSet writableCast() {
        return (TrackingWritableRowSet) this;
    }
}
