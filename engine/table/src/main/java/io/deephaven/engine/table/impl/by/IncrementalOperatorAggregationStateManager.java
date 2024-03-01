/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
     * Allow our managers to do a little bit of work at the very start of the update cycle.  We have this method so that
     * even if nothing is to be done, we rehash a little bit on each cycle to avoid always rehashing when there is other
     * work to be done.
     */
    void beginUpdateCycle();

    void startTrackingPrevValues();

    void remove(SafeCloseable pc, RowSequence rowSequence, ColumnSource<?> [] sources,
            WritableIntChunk<RowKeys> outputPositions);

    void findModifications(SafeCloseable pc, RowSequence rowSequence, ColumnSource<?> [] sources,
            WritableIntChunk<RowKeys> outputPositions);
}
