/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

public abstract class StaticChunkedOperatorAggregationStateManagerOpenAddressedBase
        extends OperatorAggregationStateManagerOpenAddressedBase implements HashHandler {
    // our state value used when nothing is there
    protected static final int EMPTY_OUTPUT_POSITION = QueryConstants.NULL_INT;

    // the state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation)
    protected final ImmutableIntArraySource mainOutputPosition = new ImmutableIntArraySource();

    // used as a row redirection for the output key sources
    private final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    // state variables that exist as part of the update
    private MutableInt outputPosition;
    private WritableIntChunk<RowKeys> outputPositions;

    protected StaticChunkedOperatorAggregationStateManagerOpenAddressedBase(ColumnSource<?>[] tableKeySources,
            int tableSize,
            double maximumLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
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
        this.outputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        buildTable(this, (BuildContext) bc, rowSequence, sources);
    }

    @Override
    public void doMainInsert(int tableLocation, int chunkPosition) {
        final int nextOutputPosition = outputPosition.getAndIncrement();
        outputPositions.set(chunkPosition, nextOutputPosition);
        mainOutputPosition.set(tableLocation, nextOutputPosition);
        outputPositionToHashSlot.set(nextOutputPosition, tableLocation);
    }

    @Override
    public void doMoveMain(int oldTableLocation, int newTableLocation) {
        outputPositionToHashSlot.set(mainOutputPosition.getUnsafe(newTableLocation), newTableLocation);
    }

    @Override
    public void doPromoteOverflow(int overflowLocation, int mainInsertLocation) {
        throw new IllegalStateException();
    }

    @Override
    public void onNextChunk(int size) {
        outputPositionToHashSlot.ensureCapacity(outputPosition.intValue() + size);
    }

    @Override
    public void doMainFound(int tableLocation, int chunkPosition) {
        outputPositions.set(chunkPosition, mainOutputPosition.getUnsafe(tableLocation));
    }

    @Override
    public void doOverflowFound(int overflowLocation, int chunkPosition) {
        throw new IllegalStateException();
    }

    @Override
    public void doOverflowInsert(int overflowLocation, int chunkPosition) {
        throw new IllegalStateException();
    }

    @Override
    public void doMissing(int chunkPosition) {
        // we never probe
        throw new IllegalStateException();
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
}
