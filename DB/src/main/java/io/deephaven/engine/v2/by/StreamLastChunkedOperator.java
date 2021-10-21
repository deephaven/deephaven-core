package io.deephaven.engine.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.select.MatchPair;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.ShiftAwareListener;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.engine.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.utils.Index;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.utils.ReadOnlyIndex;
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
    public final boolean unchunkedIndex() {
        return true;
    }

    @Override
    public void addChunk(final BucketedContext context, // Unused
            final Chunk<? extends Values> values, // Unused
            @NotNull final LongChunk<? extends RowKeys> inputIndices,
            @NotNull final IntChunk<Attributes.RowKeys> destinations,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length,
            @NotNull final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            redirections.set(destination, inputIndices.get(startPosition + runLength - 1));
            stateModified.set(ii, true);
        }
    }

    @Override
    public boolean addChunk(final SingletonContext context, // Unused
            final int chunkSize,
            final Chunk<? extends Values> values, // Unused
            @NotNull final LongChunk<? extends RowKeys> inputIndices,
            final long destination) {
        if (chunkSize == 0) {
            return false;
        }
        redirections.set(destination, inputIndices.get(chunkSize - 1));
        return true;
    }

    @Override
    public boolean addIndex(final SingletonContext context,
            @NotNull final Index index,
            final long destination) {
        if (index.isEmpty()) {
            return false;
        }
        redirections.set(destination, index.lastRowKey());
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        copyStreamToResult(resultTable.getIndex());
        redirections = null;
    }

    @Override
    public void propagateUpdates(@NotNull final ShiftAwareListener.Update downstream,
            @NotNull final ReadOnlyIndex newDestinations) {
        Assert.assertion(downstream.removed.empty() && downstream.shifted.empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        try (final RowSequence changedDestinations = downstream.modified.union(downstream.added)) {
            copyStreamToResult(changedDestinations);
        }
        redirections = null;
    }
}
