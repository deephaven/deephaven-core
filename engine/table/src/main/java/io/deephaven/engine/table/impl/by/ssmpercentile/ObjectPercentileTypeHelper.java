//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPercentileTypeHelper and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmpercentile;

import java.util.Objects;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import org.apache.commons.lang3.mutable.MutableInt;


public class ObjectPercentileTypeHelper implements SsmChunkedPercentileOperator.PercentileTypeHelper {
    private final double percentile;
    private final ObjectArraySource resultColumn;

    ObjectPercentileTypeHelper(double percentile, WritableColumnSource resultColumn) {
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (ObjectArraySource) resultColumn;
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

            return setResult(destination, ((ObjectSegmentedSortedMultiset) ssmLo).getMaxObject());
        }
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, null);
    }

    private boolean setResult(long destination, Object newResult) {
        final Object oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy,
            IntChunk<ChunkLengths> counts, int startPosition, int runLength, MutableInt leftOvers) {
        final ObjectChunk<Object, ? extends Values> asObjectChunk = valueCopy.asObjectChunk();
        final ObjectSegmentedSortedMultiset ssmLo = (ObjectSegmentedSortedMultiset) segmentedSortedMultiSet;
        final Object hiValue = ssmLo.getMaxObject();

        final int result = upperBound(asObjectChunk, startPosition, startPosition + runLength, hiValue);

        final long hiCount = ssmLo.getMaxCount();
        if (result > startPosition && ObjectComparisons.eq(asObjectChunk.get(result - 1), hiValue)
                && counts.get(result - 1) > hiCount) {
            leftOvers.setValue((int) (counts.get(result - 1) - hiCount));
        } else {
            leftOvers.setValue(0);
        }

        return result - startPosition;
    }

    @Override
    public int pivot(SegmentedSortedMultiSet segmentedSortedMultiSet, Chunk<? extends Values> valueCopy,
            IntChunk<ChunkLengths> counts, int startPosition, int runLength) {
        final ObjectChunk<Object, ? extends Values> asObjectChunk = valueCopy.asObjectChunk();
        final ObjectSegmentedSortedMultiset ssmLo = (ObjectSegmentedSortedMultiset) segmentedSortedMultiSet;
        final Object hiValue = ssmLo.getMaxObject();

        final int result = upperBound(asObjectChunk, startPosition, startPosition + runLength, hiValue);

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
    private static int upperBound(ObjectChunk<Object, ? extends Values> valuesToSearch, int lo, int hi, Object searchValue) {
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
        return ObjectComparisons.compare(lhs, rhs);
    }

    private static boolean gt(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}
