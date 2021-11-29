package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * A lastBy aggregation operator for stream tables.
 *
 * @see Table#STREAM_TABLE_ATTRIBUTE
 */
public class StreamLastChunkedOperator extends CopyingPermutedStreamFirstOrLastChunkedOperator {

    StreamLastChunkedOperator(@NotNull final MatchPair[] resultPairs, @NotNull final Table streamTable) {
        super(resultPairs, streamTable);
    }

    @Override
    public final boolean unchunkedRowSet() {
        return true;
    }

    @Override
    public void addChunk(final BucketedContext context, // Unused
            final Chunk<? extends Values> values, // Unused
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length,
            @NotNull final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            redirections.set(destination, inputRowKeys.get(startPosition + runLength - 1));
            stateModified.set(ii, true);
        }
    }

    @Override
    public boolean addChunk(final SingletonContext context, // Unused
            final int chunkSize,
            final Chunk<? extends Values> values, // Unused
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            final long destination) {
        if (chunkSize == 0) {
            return false;
        }
        redirections.set(destination, inputRowKeys.get(chunkSize - 1));
        return true;
    }

    @Override
    public boolean addRowSet(final SingletonContext context,
            @NotNull final RowSet rowSet,
            final long destination) {
        if (rowSet.isEmpty()) {
            return false;
        }
        redirections.set(destination, rowSet.lastRowKey());
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        copyStreamToResult(resultTable.getRowSet());
        redirections = null;
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream,
            @NotNull final RowSet newDestinations) {
        Assert.assertion(downstream.removed().isEmpty() && downstream.shifted().empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        try (final RowSequence changedDestinations = downstream.modified().union(downstream.added())) {
            copyStreamToResult(changedDestinations);
        }
        redirections = null;
    }
}
