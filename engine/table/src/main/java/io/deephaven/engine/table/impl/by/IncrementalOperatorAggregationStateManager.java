package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Interface for ChunkedOperatorAggregationHelper to process incremental updates.
 */
public interface IncrementalOperatorAggregationStateManager extends OperatorAggregationStateManager {
    SafeCloseable makeProbeContext(ColumnSource<?>[] probeSources, long maxSize);

    void startTrackingPrevValues();

    void setRowSize(int outputPosition, long size);

    void addForUpdate(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources, MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions, WritableIntChunk<RowKeys> reincarnatedPositions);

    void remove(final SafeCloseable pc, RowSequence indexToRemove, ColumnSource<?> [] sources, WritableIntChunk<RowKeys> outputPositions, WritableIntChunk<RowKeys> emptiedPositions);

    void findModifications(final SafeCloseable pc, RowSequence modifiedIndex, ColumnSource<?> [] sources, WritableIntChunk<RowKeys> outputPositions);
}
