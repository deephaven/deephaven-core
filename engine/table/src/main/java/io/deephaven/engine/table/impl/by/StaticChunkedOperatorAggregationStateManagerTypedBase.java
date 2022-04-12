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
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

public abstract class StaticChunkedOperatorAggregationStateManagerTypedBase
        extends OperatorAggregationStateManagerTypedBase implements HashHandler {
    // our state value used when nothing is there
    protected static final int EMPTY_OUTPUT_POSITION = QueryConstants.NULL_INT;

    // the state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation)
    protected final IntegerArraySource mainOutputPosition = new IntegerArraySource();

    // the state value for an overflow entry, parallel with overflowKeySources (the state is an output row key for the
    // aggregation)
    protected final IntegerArraySource overflowOutputPosition = new IntegerArraySource();

    // used as a row redirection for the output key sources
    private final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    // state variables that exist as part of the update
    private MutableInt outputPosition;
    private WritableIntChunk<RowKeys> outputPositions;

    protected StaticChunkedOperatorAggregationStateManagerTypedBase(ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        ensureCapacity(tableSize);
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
        outputPositionToHashSlot.set(mainOutputPosition.getUnsafe(mainInsertLocation), mainInsertLocation);
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
        outputPositions.set(chunkPosition, overflowOutputPosition.getUnsafe(overflowLocation));
    }

    @Override
    public void doOverflowInsert(int overflowLocation, int chunkPosition) {
        final int nextOutputPosition = outputPosition.getAndIncrement();
        outputPositions.set(chunkPosition, nextOutputPosition);
        overflowOutputPosition.set(overflowLocation, nextOutputPosition);
        outputPositionToHashSlot.set(nextOutputPosition,
                HashTableColumnSource.overflowLocationToHashLocation(overflowLocation));
    }

    @Override
    public void doMissing(int chunkPosition) {
        // we never probe
        throw new IllegalStateException();
    }

    @Override
    protected void ensureMainState(int tableSize) {
        mainOutputPosition.ensureCapacity(tableSize);
    }

    @Override
    protected void ensureOverflowState(int newCapacity) {
        overflowOutputPosition.ensureCapacity(newCapacity);
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final RowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[mainKeySources.length];
        for (int kci = 0; kci < mainKeySources.length; ++kci) {
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultIndexToHashSlot,
                    new HashTableColumnSource(mainKeySources[kci], overflowKeySources[kci]));
        }
        return keyHashTableSources;
    }
}
