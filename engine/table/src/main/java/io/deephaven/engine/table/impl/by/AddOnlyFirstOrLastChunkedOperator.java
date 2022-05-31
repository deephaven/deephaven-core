package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;

public class AddOnlyFirstOrLastChunkedOperator extends BaseAddOnlyFirstOrLastChunkedOperator {
    AddOnlyFirstOrLastChunkedOperator(boolean isFirst, MatchPair[] resultPairs, Table originalTable,
            String exposeRedirectionAs) {
        super(isFirst, resultPairs, originalTable, exposeRedirectionAs);
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, addChunk(inputRowKeys, startPosition, runLength, destination));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(inputRowKeys, 0, inputRowKeys.size(), destination);
    }

    private boolean addChunk(LongChunk<? extends RowKeys> indices, int start, int length, long destination) {
        if (length == 0) {
            return false;
        }
        final long candidate = isFirst ? indices.get(start) : indices.get(start + length - 1);
        return updateRedirections(destination, candidate);
    }

    @Override
    public boolean addRowSet(SingletonContext context, RowSet rowSet, long destination) {
        if (rowSet.isEmpty()) {
            return false;
        }
        final long candidate = isFirst ? rowSet.firstRowKey() : rowSet.lastRowKey();
        return updateRedirections(destination, candidate);
    }

    private boolean updateRedirections(long destination, long candidate) {
        final long oldValue = redirections.getUnsafe(destination);

        if (oldValue == QueryConstants.NULL_LONG) {
            redirections.set(destination, candidate);
            return true;
        }

        final boolean modified = isFirst ? candidate < oldValue : candidate > oldValue;
        if (modified) {
            redirections.set(destination, candidate);
        }
        return modified;
    }

    @Override
    public boolean unchunkedRowSet() {
        return true;
    }
}
