package io.deephaven.engine.rowset;

import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.util.datastructures.LongSizedDataStructure;
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

    /**
     * Get the size of this TrackingRowSet as of the end of the previous update graph cycle.
     *
     * @return The previous size
     */
    long sizePrev();

    /**
     * Get the size of this TrackingRowSet as of the end of the previous update graph cycle, constrained to be between
     * {@code 0} and {@value Integer#MAX_VALUE}.
     *
     * @return The previous size, as an {@code int}
     */
    default int intSizePrev() {
        return LongSizedDataStructure.intSize("TrackingRowSet.intSizePrev()", sizePrev());
    }

    /**
     * Get a copy of the value of this TrackingRowSet as of the end of the previous update graph cycle. As in other
     * operations that return a {@link WritableRowSet}, the result must be {@link #close() closed} by the caller when it
     * is no longer needed. The result will never be a {@link TrackingRowSet}; use {@link WritableRowSet#toTracking()}
     * on the result as needed.
     *
     * @return A copy of the previous value
     */
    WritableRowSet copyPrev();

    /**
     * Same as {@code get(rowPosition)}, as of the end of the previous update graph cycle.
     *
     * @param rowPosition A row position in this RowSet between {@code 0} and {@code sizePrev() - 1}.
     * @return The row key previously at the supplied row position
     */
    long getPrev(long rowPosition);

    /**
     * Same as {@code firstRowKey()}, as of the end of the previous update graph cycle.
     *
     * @return The previous first row key
     */
    long firstRowKeyPrev();

    /**
     * Same as {@code lastRowKey()}, as of the end of the previous update graph cycle.
     *
     * @return The previous last row key
     */
    long lastRowKeyPrev();

    /**
     * Returns the position in {@code [0..(size-1)]} where the row key is found in the previous value of this. If not
     * found, then return {@code (-(position it would be) - 1)}, as in Array.binarySearch.
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
