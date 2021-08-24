package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sort.permute.PermuteKernel;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableBooleanChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import org.jetbrains.annotations.NotNull;

/**
 * A lastBy aggregation operator for stream tables.
 *
 * @see Table#STREAM_TABLE_ATTRIBUTE
 */
public class StreamLastChunkedOperator extends CopyingPermutedStreamFirstOrLastChunkedOperator {

    StreamLastChunkedOperator(@NotNull final MatchPair[] resultPairs,
        @NotNull final Table streamTable) {
        super(resultPairs, streamTable);
    }

    @Override
    public final boolean unchunkedIndex() {
        return true;
    }

    @Override
    public void addChunk(final BucketedContext context, // Unused
        final Chunk<? extends Values> values, // Unused
        @NotNull final LongChunk<? extends KeyIndices> inputIndices,
        @NotNull final IntChunk<KeyIndices> destinations,
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
        @NotNull final LongChunk<? extends KeyIndices> inputIndices,
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
        redirections.set(destination, index.lastKey());
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
        try (final OrderedKeys changedDestinations = downstream.modified.union(downstream.added)) {
            copyStreamToResult(changedDestinations);
        }
        redirections = null;
    }
}
