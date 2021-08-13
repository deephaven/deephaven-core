/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharStreamSortedFirstOrLastChunkedOperator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.util.DhDoubleComparisons;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.DoubleArraySource;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import org.jetbrains.annotations.NotNull;

/**
 * Chunked aggregation operator for sorted first/last-by using a double sort-column on stream tables.
 */
public class DoubleStreamSortedFirstOrLastChunkedOperator extends CopyingPermutedStreamFirstOrLastChunkedOperator {

    private final boolean isFirst;
    private final boolean isCombo;
    private final DoubleArraySource sortColumnValues;

    /**
     * <p>The next destination slot that we expect to be used.
     * <p>Any destination at or after this one has an undefined value in {@link #sortColumnValues}.
     */
    private long nextDestination;
    private Index.RandomBuilder changedDestinationsBuilder;

    DoubleStreamSortedFirstOrLastChunkedOperator(
            final boolean isFirst,
            final boolean isCombo,
            @NotNull final MatchPair[] resultPairs,
            @NotNull final Table originalTable) {
        super(resultPairs, originalTable);
        this.isFirst = isFirst;
        this.isCombo = isCombo;
        // region sortColumnValues initialization
        sortColumnValues = new DoubleArraySource();
        // endregion sortColumnValues initialization
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        super.ensureCapacity(tableSize);
        sortColumnValues.ensureCapacity(tableSize);
    }

    @Override
    public void resetForStep(@NotNull final ShiftAwareListener.Update upstream) {
        super.resetForStep(upstream);
        if (isCombo) {
            changedDestinationsBuilder = Index.CURRENT_FACTORY.getRandomBuilder();
        }
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, // Unused
                         @NotNull final Chunk<? extends Values> values,
                         @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                         @NotNull final IntChunk<KeyIndices> destinations,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         @NotNull final IntChunk<ChunkLengths> length,
                         @NotNull final WritableBooleanChunk<Values> stateModified) {
        final DoubleChunk<? extends Values> doubleValues = values.asDoubleChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(doubleValues, inputIndices, startPosition, runLength, destination));
        }
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, // Unused
                            final int chunkSize,
                            @NotNull final Chunk<? extends Values> values,
                            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                            final long destination) {
        return addChunk(values.asDoubleChunk(), inputIndices, 0, inputIndices.size(), destination);
    }

    private boolean addChunk(@NotNull final DoubleChunk<? extends Values> values,
                             @NotNull final LongChunk<? extends KeyIndices> indices,
                             final int start,
                             final int length,
                             final long destination) {
        if (length == 0) {
            return false;
        }
        final boolean newDestination = destination >= nextDestination;

        int bestChunkPos;
        double bestValue;
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
            final double value = values.get(chunkPos);
            final int comparison = DhDoubleComparisons.compare(value, bestValue);
            // @formatter:off
            // No need to compare relative indices. A stream's logical index is always monotonically increasing.
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
    public void propagateUpdates(@NotNull ShiftAwareListener.Update downstream, @NotNull ReadOnlyIndex newDestinations) {
        Assert.assertion(downstream.removed.empty() && downstream.shifted.empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        // In a combo-agg, we may get modifications from other other operators that we didn't record as modifications in
        // our redirections, so we separately track updated destinations.
        try (final OrderedKeys changedDestinations = isCombo ? changedDestinationsBuilder.getIndex() : downstream.modified.union(downstream.added)) {
            copyStreamToResult(changedDestinations);
        }
        redirections = null;
        changedDestinationsBuilder = null;
    }
}
