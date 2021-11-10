package io.deephaven.engine.v2.by.ssmpercentile;

import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.util.DhObjectComparisons;
import io.deephaven.engine.v2.sources.ArrayBackedColumnSource;
import io.deephaven.engine.v2.sources.BooleanArraySource;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.IntChunk;
import io.deephaven.engine.v2.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.engine.v2.ssms.SegmentedSortedMultiSet;
import org.apache.commons.lang3.mutable.MutableInt;


public class BooleanPercentileTypeHelper implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final BooleanArraySource resultColumn;

    BooleanPercentileTypeHelper(double percentile, ArrayBackedColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (BooleanArraySource) resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, null);
        } else {
            final long targetLo = Math.round((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            return setResult(destination, (Boolean)((ObjectSegmentedSortedMultiset)ssmLo).getMaxObject());
        }
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, null);
    }

    private boolean setResult(long destination, Boolean newResult) {
        final byte newResultAsByte = BooleanUtils.booleanAsByte(newResult);
        final byte oldResult = resultColumn.getAndSetUnsafe(destination, newResultAsByte);
        return oldResult != newResultAsByte;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Attributes.Values> valueCopy, IntChunk<Attributes.ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers) {
        final ObjectChunk<Object, ? extends Attributes.Values> asObjectChunk = valueCopy.asObjectChunk();
        final ObjectSegmentedSortedMultiset ssmLo = (ObjectSegmentedSortedMultiset)segmentedSortedMultiSet;
        final Object hiValue = ssmLo.getMaxObject();

        final int result = upperBound(asObjectChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && asObjectChunk.get(result - 1) == hiValue && counts.get(result - 1) > hiCount) {
            leftOvers.setValue((int)(counts.get(result - 1) - hiCount));
        } else {
            leftOvers.setValue(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Attributes.Values> valueCopy, IntChunk<Attributes.ChunkLengths> counts, int startPosition, int runLength) {
        final ObjectChunk<Object, ? extends Attributes.Values> asObjectChunk = valueCopy.asObjectChunk();
        final ObjectSegmentedSortedMultiset ssmLo = (ObjectSegmentedSortedMultiset)segmentedSortedMultiSet;
        final Object hiValue = ssmLo.getMaxObject();

        final int result = upperBound(asObjectChunk, startPosition, startPosition + runLength, hiValue);

        return result - startPosition;
    }

    /**
     * Return the highest rowSet in valuesToSearch leq searchValue.
     *
     * @param valuesToSearch the values to search for searchValue in
     * @param lo the first rowSet to search for
     * @param hi one past the last rowSet to search in
     * @param searchValue the value to find
     * @return the highest rowSet that is less than or equal to valuesToSearch
     */
    private static int upperBound(ObjectChunk<Object, ? extends Attributes.Values> valuesToSearch, int lo, int hi, Object searchValue) {
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final Object testValue = valuesToSearch.get(mid);
            final boolean moveHi = gt(testValue, searchValue);
            if (moveHi) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        return hi;
    }

    private static int doComparison(Object lhs, Object rhs) {
        return DhObjectComparisons.compare(lhs, rhs);
    }

    private static boolean gt(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}
