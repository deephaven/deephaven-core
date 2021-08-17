/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAddOnlySortedFirstOrLastChunkedOperator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.by;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.util.DhLongComparisons;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Chunked aggregation operator for sorted first/last-by using a long sort-column on add-only tables.
 */
public class LongAddOnlySortedFirstOrLastChunkedOperator extends BaseAddOnlyFirstOrLastChunkedOperator {

    private final LongArraySource sortColumnValues;

    LongAddOnlySortedFirstOrLastChunkedOperator(
            final boolean isFirst,
            @NotNull final MatchPair[] resultPairs,
            @NotNull final Table originalTable,
            final String exposeRedirectionAs) {
        super(isFirst, resultPairs, originalTable, exposeRedirectionAs);
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
    public void addChunk(final BucketedContext bucketedContext, // Unused
                         @NotNull final Chunk<? extends Values> values,
                         @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                         @NotNull final IntChunk<KeyIndices> destinations,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         @NotNull final IntChunk<ChunkLengths> length,
                         @NotNull final WritableBooleanChunk<Values> stateModified) {
        final LongChunk<? extends Values> typedValues = values.asLongChunk();
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
                            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                            final long destination) {
        return addChunk(values.asLongChunk(), inputIndices, 0, inputIndices.size(), destination);
    }

    private boolean addChunk(@NotNull final LongChunk<? extends Values> values,
                             @NotNull final LongChunk<? extends KeyIndices> indices,
                             final int start,
                             final int length,
                             final long destination) {
        if (length == 0) {
            return false;
        }
        final long initialIndex = redirections.getUnsafe(destination);
        final boolean newDestination = initialIndex == NULL_LONG;

        long bestIndex;
        long bestValue;
        if (newDestination) {
            bestIndex = indices.get(start);
            bestValue = values.get(start);
        } else {
            bestIndex = initialIndex;
            bestValue = sortColumnValues.getUnsafe(destination);
        }
        for (int ii = newDestination ? 1 : 0; ii < length; ++ii) {
            final long index = indices.get(start + ii);
            final long value = values.get(start + ii);
            final int comparison = DhLongComparisons.compare(value, bestValue);
            // @formatter:off
            final boolean better =
                    ( isFirst && (comparison < 0 || (comparison == 0 && index < bestIndex))) ||
                    (!isFirst && (comparison > 0 || (comparison == 0 && index > bestIndex)))  ;
            // @formatter:on
            if (better) {
                bestIndex = index;
                bestValue = value;
            }
        }
        if (bestIndex == initialIndex) {
            return false;
        }
        redirections.set(destination, bestIndex);
        sortColumnValues.set(destination, bestValue);
        return true;
    }
}
