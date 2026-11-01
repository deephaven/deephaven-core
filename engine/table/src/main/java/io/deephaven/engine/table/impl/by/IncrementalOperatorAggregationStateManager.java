//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
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
     * The step of the last update cycle that {@link #beginUpdateCycle(long) began}, for a reader of the hash table on
     * another thread. A cycle marks its step before it changes the hash table. A reader reads the clock's step before
     * its reads, when every earlier cycle has finished, and this after them, behind a
     * {@link java.lang.invoke.VarHandle#acquireFence() fence}. While this is earlier than the reader's step, the reads
     * saw the hash table's state before that step's cycle; while it is no later, they saw its state once that step's
     * cycle finished, if the reader waited for that.
     *
     * @return the step of the last update cycle that began, or {@link io.deephaven.util.QueryConstants#NULL_LONG}
     */
    long updateCycleStep();

    void startTrackingPrevValues();

    void remove(SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions);

    void findModifications(SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions);
}
