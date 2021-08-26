/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils.freezeby;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.sources.ByteArraySource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;

import java.util.Collections;
import java.util.Map;

public class FreezeByCountOperator implements IterativeChunkedAggregationOperator {
    private final ByteArraySource rowCount;
    private boolean inInitialization;

    public FreezeByCountOperator() {
        rowCount = new ByteArraySource();
        inInitialization = true;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Attributes.Values> values,
            LongChunk<? extends Attributes.KeyIndices> inputIndices, IntChunk<Attributes.KeyIndices> destinations,
            IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.ChunkLengths> length,
            WritableBooleanChunk<Attributes.Values> stateModified) {
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
    public void removeChunk(BucketedContext context, Chunk<? extends Attributes.Values> values,
            LongChunk<? extends Attributes.KeyIndices> inputIndices, IntChunk<Attributes.KeyIndices> destinations,
            IntChunk<Attributes.ChunkPositions> startPositions, IntChunk<Attributes.ChunkLengths> length,
            WritableBooleanChunk<Attributes.Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int position = startPositions.get(ii);
            final int destination = destinations.get(position);
            setEmpty(destination);
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Attributes.Values> values,
            LongChunk<? extends Attributes.KeyIndices> inputIndices, long destination) {
        if (chunkSize != 1) {
            throw new IllegalStateException("FreezeBy only allows one row per state!");
        }
        setFilled(destination);
        return false;
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Attributes.Values> values,
            LongChunk<? extends Attributes.KeyIndices> inputIndices, long destination) {
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
        rowCount.startTrackingPrevValues();
        inInitialization = false;
    }

    private void setFilled(long destination) {
        final byte oldCount = rowCount.getAndSetUnsafe(destination, (byte) 1);
        if (oldCount != 0 && oldCount != QueryConstants.NULL_BYTE) {
            throw new IllegalStateException("FreezeBy only allows one row per state!");
        }
    }

    private void setEmpty(long destination) {
        final byte count = rowCount.getAndSetUnsafe(destination, (byte) 0);
        if (count != 1) {
            throw new IllegalStateException("FreezeBy only allows one row per state, old count: " + count);
        }
    }

    boolean wasDestinationEmpty(long destination) {
        if (inInitialization) {
            return true;
        }
        final byte prevByte = rowCount.getPrevByte(destination);
        return prevByte == 0 || prevByte == QueryConstants.NULL_BYTE;
    }
}
