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
        extends OperatorAggregationStateManagerOpenAddressedBase {
    // our state value used when nothing is there
    protected static final int EMPTY_OUTPUT_POSITION = QueryConstants.NULL_INT;

    // the state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation)
    protected final ImmutableIntArraySource mainOutputPosition = new ImmutableIntArraySource();

    // used as a row redirection for the output key sources
    protected final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    // state variables that exist as part of the update
    protected MutableInt nextOutputPosition;
    protected WritableIntChunk<RowKeys> outputPositions;

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
        this.nextOutputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        buildTable((BuildContext) bc, rowSequence, sources);
    }

    @Override
    public void onNextChunk(int size) {
        outputPositionToHashSlot.ensureCapacity(nextOutputPosition.intValue() + size, false);
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
