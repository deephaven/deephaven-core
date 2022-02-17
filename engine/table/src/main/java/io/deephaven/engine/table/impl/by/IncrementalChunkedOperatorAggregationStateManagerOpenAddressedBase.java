/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

public abstract class IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase
        extends OperatorAggregationStateManagerOpenAddressedPivotBase
        implements IncrementalOperatorAggregationStateManager {
    // our state value used when nothing is there
    protected static final int EMPTY_OUTPUT_POSITION = QueryConstants.NULL_INT;

    // the state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation)
    protected final IntegerArraySource mainOutputPosition = new IntegerArraySource();

    // used as a row redirection for the output key sources
    protected final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    // how many values are in each state, addressed by output row key
    protected final LongArraySource rowCountSource = new LongArraySource();

    // state variables that exist as part of the update
    protected MutableInt nextOutputPosition;
    protected WritableIntChunk<RowKeys> outputPositions;

    protected IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase(ColumnSource<?>[] tableKeySources,
                                                                                 int tableSize,
                                                                                 double maximumLoadFactor,
                                                                                 double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        mainOutputPosition.ensureCapacity(tableSize);
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return makeBuildContext(buildSources, maxSize);
    }

    @Override
    public void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.nextOutputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        buildTable((BuildContext) bc, rowSequence, sources, this::build);
    }

    @Override
    public void onNextChunk(int size) {
        outputPositionToHashSlot.ensureCapacity(nextOutputPosition.intValue() + size);
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final RowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[mainKeySources.length];
        for (int kci = 0; kci < mainKeySources.length; ++kci) {
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultIndexToHashSlot, mainKeySources[kci]);
        }
        return keyHashTableSources;
    }

    @Override
    public void addForUpdate(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
                             MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions,
                             final WritableIntChunk<RowKeys> reincarnatedPositions) {
        outputPositions.setSize(rowSequence.intSize());
        reincarnatedPositions.setSize(0);
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        buildTable((BuildContext) bc, rowSequence, sources, ((chunkOk, sourceKeyChunks) -> buildForUpdate(chunkOk, sourceKeyChunks, reincarnatedPositions)));
    }

    protected abstract void buildForUpdate(RowSequence chunkOk, Chunk[] sourceKeyChunks, WritableIntChunk<RowKeys> reincarnatedPositions);

    @Override
    public void remove(final SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
                       WritableIntChunk<RowKeys> outputPositions, final WritableIntChunk<RowKeys> emptiedPositions) {
        outputPositions.setSize(rowSequence.intSize());
        emptiedPositions.setSize(0);
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        probeTable((ProbeContext) pc, rowSequence, true, sources, (chunkOk, sourceKeyChunks) -> IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.this.doRemoveProbe(chunkOk, sourceKeyChunks, emptiedPositions));
    }

    protected abstract void doRemoveProbe(RowSequence chunkOk, Chunk[] sourceKeyChunks, WritableIntChunk<RowKeys> emptiedPositions);

    @Override
    public void findModifications(final SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
                                  WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        probeTable((ProbeContext) pc, rowSequence, false, sources, IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.this::doModifyProbe);
        throw new IllegalStateException();
    }

    protected abstract void doModifyProbe(RowSequence chunkOk, Chunk[] sourceKeyChunks);

    @Override
    public void startTrackingPrevValues() {
    }

    @Override
    public void setRowSize(int outputPosition, long size) {
        rowCountSource.set(outputPosition, size);
    }
}
