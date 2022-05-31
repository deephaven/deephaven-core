package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.engine.table.impl.ssms.CharSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import org.apache.commons.lang3.mutable.MutableInt;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class CharPercentileTypeHelper implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final CharacterArraySource resultColumn;

    CharPercentileTypeHelper(double percentile, ArrayBackedColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (CharacterArraySource) resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, NULL_CHAR);
        } else {
            final long targetLo = Math.round((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            return setResult(destination, ((CharSegmentedSortedMultiset)ssmLo).getMaxChar());
        }
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_CHAR);
    }

    private boolean setResult(long destination, char newResult) {
        final char oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers) {
        final CharChunk<? extends Values> asCharChunk = valueCopy.asCharChunk();
        final CharSegmentedSortedMultiset ssmLo = (CharSegmentedSortedMultiset)segmentedSortedMultiSet;
        final char hiValue = ssmLo.getMaxChar();

        final int result = upperBound(asCharChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && CharComparisons.eq(asCharChunk.get(result - 1), hiValue) && counts.get(result - 1) > hiCount) {
            leftOvers.setValue((int)(counts.get(result - 1) - hiCount));
        } else {
            leftOvers.setValue(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition, int runLength) {
        final CharChunk<? extends Values> asCharChunk = valueCopy.asCharChunk();
        final CharSegmentedSortedMultiset ssmLo = (CharSegmentedSortedMultiset)segmentedSortedMultiSet;
        final char hiValue = ssmLo.getMaxChar();

        final int result = upperBound(asCharChunk, startPosition, startPosition + runLength, hiValue);

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
    private static int upperBound(CharChunk<? extends Values> valuesToSearch, int lo, int hi, char searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final char testValue = valuesToSearch.get(mid);
            final boolean moveHi = gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    private static int doComparison(char lhs, char rhs) {
        return CharComparisons.compare(lhs, rhs);
    }

    private static boolean gt(char lhs, char rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}