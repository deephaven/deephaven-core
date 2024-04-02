//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharAddOnlySortedFirstOrLastChunkedOperator and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Chunked aggregation operator for sorted first/last-by using a short sort-column on add-only tables.
 */
public class ShortAddOnlySortedFirstOrLastChunkedOperator extends BaseAddOnlyFirstOrLastChunkedOperator {

    private final ShortArraySource sortColumnValues;

    ShortAddOnlySortedFirstOrLastChunkedOperator(
            final boolean isFirst,
            @NotNull final MatchPair[] resultPairs,
            @NotNull final Table originalTable,
            final String exposeRedirectionAs) {
        super(isFirst, resultPairs, originalTable, exposeRedirectionAs);
        // region sortColumnValues initialization
        sortColumnValues = new ShortArraySource();
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
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length,
            @NotNull final WritableBooleanChunk<Values> stateModified) {
        final ShortChunk<? extends Values> typedValues = values.asShortChunk();
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
        return addChunk(values.asShortChunk(), inputRowKeys, 0, inputRowKeys.size(), destination);
    }

    private boolean addChunk(@NotNull final ShortChunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> indices,
            final int start,
            final int length,
            final long destination) {
        if (length == 0) {
            return false;
        }
        final long initialIndex = redirections.getUnsafe(destination);
        final boolean newDestination = initialIndex == NULL_LONG;

        long bestIndex;
        short bestValue;
        if (newDestination) {
            bestIndex = indices.get(start);
            bestValue = values.get(start);
        } else {
            bestIndex = initialIndex;
            bestValue = sortColumnValues.getUnsafe(destination);
        }
        for (int ii = newDestination ? 1 : 0; ii < length; ++ii) {
            final long index = indices.get(start + ii);
            final short value = values.get(start + ii);
            final int comparison = ShortComparisons.compare(value, bestValue);
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
