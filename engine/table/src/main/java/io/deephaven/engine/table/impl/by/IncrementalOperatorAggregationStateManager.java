//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.SafeCloseable;

/**
 * Interface for ChunkedOperatorAggregationHelper to process incremental updates.
 */
public interface IncrementalOperatorAggregationStateManager extends OperatorAggregationStateManager {
    SafeCloseable makeProbeContext(ColumnSource<?>[] probeSources, long maxSize);

    /**
     * Allow our managers to do a little bit of work at the very start of the update cycle. We have this method so that
     * even if nothing is to be done, we rehash a little bit on each cycle to avoid always rehashing when there is other
     * work to be done. Called before any change to the hash table in the cycle.
     *
     * @param step the clock step of the update cycle
     */
    void beginUpdateCycle(long step);

    /**
     * The step of the last update cycle that {@link #beginUpdateCycle(long) began} changing the hash table, set before
     * its first change. A cycle that has rehash work left, adds rows, modifies keys, {@link #tombstoneStates removes
     * states} or {@link #shiftOutputPositions moves them} may change the hash table, so it records its step; a cycle
     * that only removes rows without removing a state, or modifies other columns, leaves the hash table unchanged, and
     * records nothing. A reader on another thread reads the clock's step before its reads, and this after them behind a
     * {@link java.lang.invoke.VarHandle#acquireFence() fence}. Compared to the reader's step, this is:
     * <ul>
     * <li>less: no cycle at or after the reader's step changed the hash table during the reads, so they saw its state
     * at that step, which is both its previous and current state;</li>
     * <li>equal: the reader's step's cycle has begun changing the hash table, so the reads may not have seen its
     * previous state; they saw its current state if the reader waited for that cycle to finish;</li>
     * <li>greater: a later cycle has begun changing the hash table, so the reads may not have seen either state at the
     * reader's step.</li>
     * </ul>
     *
     * @return the step of the last update cycle that began changing the hash table, or
     *         {@link io.deephaven.util.QueryConstants#NULL_LONG} if none has
     */
    long lastStateChangeStep();

    void startTrackingPrevValues();

    void remove(SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions);

    void findModifications(SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions);

    /**
     * Remove the hash table entries for states that are empty at the end of an update cycle. Their output positions are
     * not assigned to new states.
     *
     * @param removed the output positions of the empty states
     */
    void tombstoneStates(RowSet removed);

    /**
     * Release the storage for the blocks of the output position to hash slot mapping that lie entirely within a range
     * of output positions whose states have all been removed by {@link #tombstoneStates(RowSet)}.
     *
     * @param firstOutputPosition the first output position
     * @param lastOutputPosition the last output position, inclusive
     */
    void releaseOutputPositionBlocks(long firstOutputPosition, long lastOutputPosition);

    /**
     * Move states to new output positions, updating the hash table to match. Every shift must move states toward lower
     * positions. Called before the result's key columns move, so that the step this records for
     * {@link #lastStateChangeStep()} precedes their change as well.
     *
     * @param shiftData the shifts to apply to the states' output positions
     */
    void shiftOutputPositions(RowSetShiftData shiftData);
}
