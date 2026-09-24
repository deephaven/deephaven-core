//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;

/**
 * Interface for ChunkedOperatorAggregationHelper to process incremental updates.
 */
public interface IncrementalOperatorAggregationStateManager extends OperatorAggregationStateManager {
    SafeCloseable makeProbeContext(ColumnSource<?>[] probeSources, long maxSize);

    /**
     * Allow our managers to do a little bit of work at the very start of the update cycle. We have this method so that
     * even if nothing is to be done, we rehash a little bit on each cycle to avoid always rehashing when there is other
     * work to be done.
     */
    void beginUpdateCycle();

    void startTrackingPrevValues();

    void remove(SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions);

    void findModifications(SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions);

    /**
     * Can this state manager actually reclaim states?
     * 
     * @return true if this state manager is capable of reclaiming states, false otherwise
     */
    boolean canReclaim();

    /**
     * Reclaim any rows that are free in the result table (depending on thresholds)
     *
     * @param resultRowset
     * @param downstream the downstream update, which may need to be changed to reflect the reclaimed rows
     * @param outputPosition
     * @param maxShiftedStates the maximum number of rows that can be shifted as part of reclamation
     * @param operators
     */
    void reclaimFreedRows(TrackingWritableRowSet resultRowset, TableUpdateImpl downstream, MutableInt outputPosition,
            long maxShiftedStates, IterativeChunkedAggregationOperator[] operators);

    void removeStates(RowSet removed);
}
