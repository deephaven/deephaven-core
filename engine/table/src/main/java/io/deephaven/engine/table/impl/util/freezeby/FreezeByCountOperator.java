//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.freezeby;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;

import java.util.Collections;
import java.util.Map;

public class FreezeByCountOperator implements IterativeChunkedAggregationOperator {
    private final ByteArraySource rowCount;
    /**
     * Whether the operator only receives additions. A destination then receives at most one row, and any addition that
     * reaches the freeze operators is to a destination that was empty when the cycle began, so no previous counts are
     * kept.
     */
    private final boolean addOnly;
    private boolean inInitialization;

    public FreezeByCountOperator(final boolean addOnly) {
        rowCount = new ByteArraySource();
        this.addOnly = addOnly;
        inInitialization = true;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int position = startPositions.get(ii);
            final int destination = destinations.get(position);
            if (length.get(ii) != 1) {
                throw new IllegalStateException("FreezeBy only allows one row per state!");
            }
            setFilled(destination);
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int position = startPositions.get(ii);
            final int destination = destinations.get(position);
            setEmpty(destination);
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        if (chunkSize != 1) {
            throw new IllegalStateException("FreezeBy only allows one row per state!");
        }
        setFilled(destination);
        return false;
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        setEmpty(destination);
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        rowCount.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.emptyMap();
    }

    @Override
    public void startTrackingPrevValues() {
        if (!addOnly) {
            rowCount.startTrackingPrevValues();
        }
        inInitialization = false;
    }

    private void setFilled(long destination) {
        final byte oldCount = rowCount.getAndSetUnsafe(destination, (byte) 1);
        if (oldCount != 0 && oldCount != QueryConstants.NULL_BYTE) {
            throw new IllegalStateException("FreezeBy only allows one row per state!");
        }
    }

    private void setEmpty(long destination) {
        if (addOnly) {
            throw new UnsupportedOperationException("Add-only freeze operators do not support removals");
        }
        final byte count = rowCount.getAndSetUnsafe(destination, (byte) 0);
        if (count != 1) {
            throw new IllegalStateException("FreezeBy only allows one row per state, old count: " + count);
        }
    }

    boolean wasDestinationEmpty(long destination) {
        if (inInitialization || addOnly) {
            return true;
        }
        final byte prevByte = rowCount.getPrevByte(destination);
        return prevByte == 0 || prevByte == QueryConstants.NULL_BYTE;
    }
}
