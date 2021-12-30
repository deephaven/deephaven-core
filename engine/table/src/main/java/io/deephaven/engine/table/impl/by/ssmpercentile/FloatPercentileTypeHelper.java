/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPercentileTypeHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.engine.table.impl.ssms.FloatSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import org.apache.commons.lang3.mutable.MutableInt;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatPercentileTypeHelper implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final FloatArraySource resultColumn;

    FloatPercentileTypeHelper(double percentile, ArrayBackedColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (FloatArraySource) resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, NULL_FLOAT);
        } else {
            final long targetLo = Math.round((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            return setResult(destination, ((FloatSegmentedSortedMultiset)ssmLo).getMaxFloat());
        }
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_FLOAT);
    }

    private boolean setResult(long destination, float newResult) {
        final float oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers) {
        final FloatChunk<? extends Values> asFloatChunk = valueCopy.asFloatChunk();
        final FloatSegmentedSortedMultiset ssmLo = (FloatSegmentedSortedMultiset)segmentedSortedMultiSet;
        final float hiValue = ssmLo.getMaxFloat();

        final int result = upperBound(asFloatChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && FloatComparisons.eq(asFloatChunk.get(result - 1), hiValue) && counts.get(result - 1) > hiCount) {
            leftOvers.setValue((int)(counts.get(result - 1) - hiCount));
        } else {
            leftOvers.setValue(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy, IntChunk<ChunkLengths> counts, int startPosition, int runLength) {
        final FloatChunk<? extends Values> asFloatChunk = valueCopy.asFloatChunk();
        final FloatSegmentedSortedMultiset ssmLo = (FloatSegmentedSortedMultiset)segmentedSortedMultiSet;
        final float hiValue = ssmLo.getMaxFloat();

        final int result = upperBound(asFloatChunk, startPosition, startPosition + runLength, hiValue);

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
    private static int upperBound(FloatChunk<? extends Values> valuesToSearch, int lo, int hi, float searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final float testValue = valuesToSearch.get(mid);
            final boolean moveHi = gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    private static int doComparison(float lhs, float rhs) {
        return FloatComparisons.compare(lhs, rhs);
    }

    private static boolean gt(float lhs, float rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}