/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPercentileTypeHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.engine.table.impl.ssms.DoubleSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import org.apache.commons.lang3.mutable.MutableInt;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoublePercentileTypeHelper implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final DoubleArraySource resultColumn;

    DoublePercentileTypeHelper(double percentile, ArrayBackedColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (DoubleArraySource) resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, NULL_DOUBLE);
        } else {
            final long targetLo = Math.round((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            return setResult(destination, ((DoubleSegmentedSortedMultiset)ssmLo).getMaxDouble());
        }
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_DOUBLE);
    }

    private boolean setResult(long destination, double newResult) {
        final double oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers) {
        final DoubleChunk<? extends Values> asDoubleChunk = valueCopy.asDoubleChunk();
        final DoubleSegmentedSortedMultiset ssmLo = (DoubleSegmentedSortedMultiset)segmentedSortedMultiSet;
        final double hiValue = ssmLo.getMaxDouble();

        final int result = upperBound(asDoubleChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && DoubleComparisons.eq(asDoubleChunk.get(result - 1), hiValue) && counts.get(result - 1) > hiCount) {
            leftOvers.setValue((int)(counts.get(result - 1) - hiCount));
        } else {
            leftOvers.setValue(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition, int runLength) {
        final DoubleChunk<? extends Values> asDoubleChunk = valueCopy.asDoubleChunk();
        final DoubleSegmentedSortedMultiset ssmLo = (DoubleSegmentedSortedMultiset)segmentedSortedMultiSet;
        final double hiValue = ssmLo.getMaxDouble();

        final int result = upperBound(asDoubleChunk, startPosition, startPosition + runLength, hiValue);

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
    private static int upperBound(DoubleChunk<? extends Values> valuesToSearch, int lo, int hi, double searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final double testValue = valuesToSearch.get(mid);
            final boolean moveHi = gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    private static int doComparison(double lhs, double rhs) {
        return DoubleComparisons.compare(lhs, rhs);
    }

    private static boolean gt(double lhs, double rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}