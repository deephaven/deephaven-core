//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.InstantArraySource;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.table.impl.ssms.LongSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;

public class InstantPercentileTypeHelper implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final InstantArraySource resultColumn;

    InstantPercentileTypeHelper(double percentile, WritableColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (InstantArraySource) resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, QueryConstants.NULL_LONG);
        } else {
            final long targetLo = Math.round((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            return setResult(destination, ((LongSegmentedSortedMultiset) ssmLo).getMaxLong());
        }
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, QueryConstants.NULL_LONG);
    }

    private boolean setResult(long destination, long newResult) {
        final long oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy,
            IntChunk<ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers) {
        final LongChunk<? extends Values> asLongChunk = valueCopy.asLongChunk();
        final LongSegmentedSortedMultiset ssmLo = (LongSegmentedSortedMultiset) segmentedSortedMultiSet;
        final long hiValue = ssmLo.getMaxLong();

        final int result = upperBound(asLongChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && asLongChunk.get(result - 1) == hiValue && counts.get(result - 1) > hiCount) {
            leftOvers.set((int) (counts.get(result - 1) - hiCount));
        } else {
            leftOvers.set(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy,
            IntChunk<ChunkLengths> counts, int startPosition, int runLength) {
        final LongChunk<? extends Values> asLongChunk = valueCopy.asLongChunk();
        final LongSegmentedSortedMultiset ssmLo = (LongSegmentedSortedMultiset) segmentedSortedMultiSet;
        final long hiValue = ssmLo.getMaxLong();

        final int result = upperBound(asLongChunk, startPosition, startPosition + runLength, hiValue);

        return result - startPosition;
    }

    /**
     * Return the highest index in valuesToSearch leq searchValue.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first index to search for
     * @param hi one past the last index to search in
     * @param searchValue the value to find
     * @return the highest index that is less than or equal to valuesToSearch
     */
    private static int upperBound(LongChunk<? extends Values> valuesToSearch, int lo, int hi,
            long searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final long testValue = valuesToSearch.get(mid);
            final boolean moveHi = LongComparisons.gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

}
