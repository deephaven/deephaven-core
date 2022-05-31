/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharStreamSortedFirstOrLastChunkedOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * Chunked aggregation operator for sorted first/last-by using a long sort-column on stream tables.
 */
public class LongStreamSortedFirstOrLastChunkedOperator extends CopyingPermutedStreamFirstOrLastChunkedOperator {

    private final boolean isFirst;
    private final boolean isCombo;
    private final LongArraySource sortColumnValues;

    private RowSetBuilderRandom changedDestinationsBuilder;

    LongStreamSortedFirstOrLastChunkedOperator(
            final boolean isFirst,
            final boolean isCombo,
            @NotNull final MatchPair[] resultPairs,
            @NotNull final Table originalTable) {
        super(resultPairs, originalTable);
        this.isFirst = isFirst;
        this.isCombo = isCombo;
        // region sortColumnValues initialization
        sortColumnValues = new LongArraySource();
        // endregion sortColumnValues initialization
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        super.ensureCapacity(tableSize);
        sortColumnValues.ensureCapacity(tableSize, false);
    }

    @Override
    public void resetForStep(@NotNull final TableUpdate upstream) {
        super.resetForStep(upstream);
        if (isCombo) {
            changedDestinationsBuilder = RowSetFactory.builderRandom();
        }
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, // Unused
                         @NotNull final Chunk<? extends Values> values,
                         @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
                         @NotNull final IntChunk<RowKeys> destinations,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         @NotNull final IntChunk<ChunkLengths> length,
                         @NotNull final WritableBooleanChunk<Values> stateModified) {
        final LongChunk<? extends Values> typedValues = values.asLongChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(typedValues, inputRowKeys, startPosition, runLength, destination));
        }
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, // Unused
                            final int chunkSize,
                            @NotNull final Chunk<? extends Values> values,
                            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
                            final long destination) {
        return addChunk(values.asLongChunk(), inputRowKeys, 0, inputRowKeys.size(), destination);
    }

    private boolean addChunk(@NotNull final LongChunk<? extends Values> values,
                             @NotNull final LongChunk<? extends RowKeys> indices,
                             final int start,
                             final int length,
                             final long destination) {
        if (length == 0) {
            return false;
        }
        final boolean newDestination = redirections.getUnsafe(destination) == QueryConstants.NULL_LONG;

        int bestChunkPos;
        long bestValue;
        if (newDestination) {
            bestChunkPos = start;
            bestValue = values.get(start);
        } else {
            bestChunkPos = -1;
            bestValue = sortColumnValues.getUnsafe(destination);
        }

        for (int ii = newDestination ? 1 : 0; ii < length; ++ii) {
            final int chunkPos = start + ii;
            final long value = values.get(chunkPos);
            final int comparison = LongComparisons.compare(value, bestValue);
            // @formatter:off
            // No need to compare relative row keys. A stream's logical row set is always monotonically increasing.
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
        copyStreamToResult(resultTable.getRowSet());
        redirections = null;
    }

    @Override
    public void propagateUpdates(@NotNull TableUpdate downstream, @NotNull RowSet newDestinations) {
        Assert.assertion(downstream.removed().isEmpty() && downstream.shifted().empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        // In a combo-agg, we may get modifications from other operators that we didn't record as modifications in
        // our redirections, so we separately track updated destinations.
        try (final RowSequence changedDestinations = isCombo ? changedDestinationsBuilder.build() : downstream.modified().union(downstream.added())) {
            copyStreamToResult(changedDestinations);
        }
        redirections = null;
        changedDestinationsBuilder = null;
    }
}
