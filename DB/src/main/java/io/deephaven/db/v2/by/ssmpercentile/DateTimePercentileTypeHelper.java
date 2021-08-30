package io.deephaven.db.v2.by.ssmpercentile;

import io.deephaven.db.util.DhLongComparisons;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.DateTimeArraySource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.ssms.LongSegmentedSortedMultiset;
import io.deephaven.db.v2.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableInt;


public class DateTimePercentileTypeHelper
    implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final DateTimeArraySource resultColumn;

    DateTimePercentileTypeHelper(double percentile, ArrayBackedColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (DateTimeArraySource) resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi,
        long destination) {
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
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet,
        Chunk<? extends Attributes.Values> valueCopy, IntChunk<Attributes.ChunkLengths> counts,
        int startPosition, int runLength, MutableInt leftOvers) {
        final LongChunk<? extends Attributes.Values> asLongChunk = valueCopy.asLongChunk();
        final LongSegmentedSortedMultiset ssmLo =
            (LongSegmentedSortedMultiset) segmentedSortedMultiSet;
        final long hiValue = ssmLo.getMaxLong();

        final int result =
            upperBound(asLongChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && asLongChunk.get(result - 1) == hiValue
            && counts.get(result - 1) > hiCount) {
            leftOvers.setValue((int) (counts.get(result - 1) - hiCount));
        } else {
            leftOvers.setValue(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet,
        Chunk<? extends Attributes.Values> valueCopy, IntChunk<Attributes.ChunkLengths> counts,
        int startPosition, int runLength) {
        final LongChunk<? extends Attributes.Values> asLongChunk = valueCopy.asLongChunk();
        final LongSegmentedSortedMultiset ssmLo =
            (LongSegmentedSortedMultiset) segmentedSortedMultiSet;
        final long hiValue = ssmLo.getMaxLong();

        final int result =
            upperBound(asLongChunk, startPosition, startPosition + runLength, hiValue);

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
    private static int upperBound(LongChunk<? extends Attributes.Values> valuesToSearch, int lo,
        int hi, long searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final long testValue = valuesToSearch.get(mid);
            final boolean moveHi = gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    private static int doComparison(long lhs, long rhs) {
        return DhLongComparisons.compare(lhs, rhs);
    }

    private static boolean gt(long lhs, long rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}
