/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPercentileTypeHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.engine.table.impl.ssms.ShortSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import org.apache.commons.lang3.mutable.MutableInt;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortPercentileTypeHelper implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final ShortArraySource resultColumn;

    ShortPercentileTypeHelper(double percentile, ArrayBackedColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (ShortArraySource) resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, NULL_SHORT);
        } else {
            final long targetLo = Math.round((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            return setResult(destination, ((ShortSegmentedSortedMultiset)ssmLo).getMaxShort());
        }
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_SHORT);
    }

    private boolean setResult(long destination, short newResult) {
        final short oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers) {
        final ShortChunk<? extends Values> asShortChunk = valueCopy.asShortChunk();
        final ShortSegmentedSortedMultiset ssmLo = (ShortSegmentedSortedMultiset)segmentedSortedMultiSet;
        final short hiValue = ssmLo.getMaxShort();

        final int result = upperBound(asShortChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && ShortComparisons.eq(asShortChunk.get(result - 1), hiValue) && counts.get(result - 1) > hiCount) {
            leftOvers.setValue((int)(counts.get(result - 1) - hiCount));
        } else {
            leftOvers.setValue(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition, int runLength) {
        final ShortChunk<? extends Values> asShortChunk = valueCopy.asShortChunk();
        final ShortSegmentedSortedMultiset ssmLo = (ShortSegmentedSortedMultiset)segmentedSortedMultiSet;
        final short hiValue = ssmLo.getMaxShort();

        final int result = upperBound(asShortChunk, startPosition, startPosition + runLength, hiValue);

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
    private static int upperBound(ShortChunk<? extends Values> valuesToSearch, int lo, int hi, short searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final short testValue = valuesToSearch.get(mid);
            final boolean moveHi = gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    private static int doComparison(short lhs, short rhs) {
        return ShortComparisons.compare(lhs, rhs);
    }

    private static boolean gt(short lhs, short rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}