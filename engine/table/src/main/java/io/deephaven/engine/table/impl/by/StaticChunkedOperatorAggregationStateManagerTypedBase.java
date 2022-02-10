/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

public abstract class StaticChunkedOperatorAggregationStateManagerTypedBase
        extends OperatorAggregationStateManagerTypedBase {
    private final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    // state variables that exist as part of the update
    private MutableInt outputPosition;
    private WritableIntChunk<RowKeys> outputPositions;

    protected StaticChunkedOperatorAggregationStateManagerTypedBase(ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return makeBuildContext(buildSources, maxSize);
    }

    @Override
    public void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        outputPositions.setSize(rowSequence.intSize());
        buildTable((BuildContext) bc, rowSequence, sources);
    }

    @Override
    public void doMainInsert(int tableLocation, int chunkPosition) {
        int position = outputPosition.getAndIncrement();
        outputPositions.set(chunkPosition, position);
        stateSource.set(tableLocation, position);
        outputPositionToHashSlot.set(position, tableLocation);
    }

    @Override
    public void moveMain(int oldTableLocation, int newTableLocation) {
        final int position = stateSource.getUnsafe(newTableLocation);
        outputPositionToHashSlot.set(position, newTableLocation);
    }

    @Override
    public void promoteOverflow(int overflowLocation, int mainInsertLocation) {
        outputPositionToHashSlot.set(stateSource.getUnsafe(mainInsertLocation), mainInsertLocation);
    }

    @Override
    public void nextChunk(int size) {
        outputPositionToHashSlot.ensureCapacity(outputPosition.intValue() + size);
    }

    @Override
    public void doMainFound(int tableLocation, int chunkPosition) {
        outputPositions.set(chunkPosition, stateSource.getUnsafe(tableLocation));
    }

    @Override
    public void doOverflowFound(int overflowLocation, int chunkPosition) {
        final int position = overflowStateSource.getUnsafe(overflowLocation);
        outputPositions.set(chunkPosition, position);
    }

    @Override
    public void doOverflowInsert(int overflowLocation, int chunkPosition) {
        final int position = outputPosition.getAndIncrement();
        overflowStateSource.set(overflowLocation, position);
        outputPositions.set(chunkPosition, position);
        outputPositionToHashSlot.set(position, HashTableColumnSource.overflowLocationToHashLocation(overflowLocation));
    }

    @Override
    public void doMissing(int chunkPosition) {
        // we never probe
        throw new IllegalStateException();
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final WritableRowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[keySources.length];
        for (int kci = 0; kci < keySources.length; ++kci) {
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultIndexToHashSlot,
                    new HashTableColumnSource(keySources[kci], overflowKeySources[kci]));
        }
        return keyHashTableSources;
    }
}
