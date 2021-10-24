package io.deephaven.engine.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.select.MatchPair;
import io.deephaven.engine.util.DhCharComparisons;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.ShiftAwareListener;
import io.deephaven.engine.v2.sources.CharacterArraySource;
import io.deephaven.engine.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.engine.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.utils.RowSetBuilder;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.utils.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * Chunked aggregation operator for sorted first/last-by using a char sort-column on stream tables.
 */
public class CharStreamSortedFirstOrLastChunkedOperator extends CopyingPermutedStreamFirstOrLastChunkedOperator {

    private final boolean isFirst;
    private final boolean isCombo;
    private final CharacterArraySource sortColumnValues;

    /**
     * <p>The next destination slot that we expect to be used.
     * <p>Any destination at or after this one has an undefined value in {@link #sortColumnValues}.
     */
    private long nextDestination;
    private RowSetBuilder changedDestinationsBuilder;

    CharStreamSortedFirstOrLastChunkedOperator(
            final boolean isFirst,
            final boolean isCombo,
            @NotNull final MatchPair[] resultPairs,
            @NotNull final Table originalTable) {
        super(resultPairs, originalTable);
        this.isFirst = isFirst;
        this.isCombo = isCombo;
        // region sortColumnValues initialization
        sortColumnValues = new CharacterArraySource();
        // endregion sortColumnValues initialization
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        super.ensureCapacity(tableSize);
        sortColumnValues.ensureCapacity(tableSize, false);
    }

    @Override
    public void resetForStep(@NotNull final ShiftAwareListener.Update upstream) {
        super.resetForStep(upstream);
        if (isCombo) {
            changedDestinationsBuilder = TrackingMutableRowSet.CURRENT_FACTORY.getRandomBuilder();
        }
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, // Unused
                         @NotNull final Chunk<? extends Values> values,
                         @NotNull final LongChunk<? extends RowKeys> inputIndices,
                         @NotNull final IntChunk<RowKeys> destinations,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         @NotNull final IntChunk<ChunkLengths> length,
                         @NotNull final WritableBooleanChunk<Values> stateModified) {
        final CharChunk<? extends Values> typedValues = values.asCharChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(typedValues, inputIndices, startPosition, runLength, destination));
        }
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, // Unused
                            final int chunkSize,
                            @NotNull final Chunk<? extends Values> values,
                            @NotNull final LongChunk<? extends RowKeys> inputIndices,
                            final long destination) {
        return addChunk(values.asCharChunk(), inputIndices, 0, inputIndices.size(), destination);
    }

    private boolean addChunk(@NotNull final CharChunk<? extends Values> values,
                             @NotNull final LongChunk<? extends RowKeys> indices,
                             final int start,
                             final int length,
                             final long destination) {
        if (length == 0) {
            return false;
        }
        final boolean newDestination = destination >= nextDestination;

        int bestChunkPos;
        char bestValue;
        if (newDestination) {
            ++nextDestination;
            bestChunkPos = start;
            bestValue = values.get(start);
        } else {
            bestChunkPos = -1;
            bestValue = sortColumnValues.getUnsafe(destination);
        }

        for (int ii = newDestination ? 1 : 0; ii < length; ++ii) {
            final int chunkPos = start + ii;
            final char value = values.get(chunkPos);
            final int comparison = DhCharComparisons.compare(value, bestValue);
            // @formatter:off
            // No need to compare relative indices. A stream's logical rowSet is always monotonically increasing.
            final boolean better =
                    ( isFirst && comparison <  0) ||
                    (!isFirst && comparison >= 0)  ;
            // @formatter:on
            if (better) {
                bestChunkPos = chunkPos;
                bestValue = value;
            }
        }
        if (bestChunkPos == -1) {
            return false;
        }
        if (changedDestinationsBuilder != null) {
            changedDestinationsBuilder.addKey(destination);
        }
        redirections.set(destination, indices.get(bestChunkPos));
        sortColumnValues.set(destination, bestValue);
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        copyStreamToResult(resultTable.getIndex());
        redirections = null;
    }

    @Override
    public void propagateUpdates(@NotNull ShiftAwareListener.Update downstream, @NotNull RowSet newDestinations) {
        Assert.assertion(downstream.removed.empty() && downstream.shifted.empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        // In a combo-agg, we may get modifications from other other operators that we didn't record as modifications in
        // our redirections, so we separately track updated destinations.
        try (final RowSequence changedDestinations = isCombo ? changedDestinationsBuilder.build() : downstream.modified.union(downstream.added)) {
            copyStreamToResult(changedDestinations);
        }
        redirections = null;
        changedDestinationsBuilder = null;
    }
}
