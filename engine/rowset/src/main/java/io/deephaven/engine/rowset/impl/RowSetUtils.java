/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.datastructures.LongRangeConsumer;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.WritableLongChunk;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;

public class RowSetUtils {

    static String toString(RowSet rowSet, int maxRanges) {
        int count = 0;
        final StringBuilder result = new StringBuilder("{");
        boolean isFirst = true;
        try (final RowSet.RangeIterator it = rowSet.rangeIterator()) {
            while (it.hasNext()) {
                it.next();
                result.append(isFirst ? "" : ",").append(it.currentRangeStart())
                        .append(it.currentRangeEnd() != it.currentRangeStart() ? "-" + it.currentRangeEnd() : "");
                isFirst = false;
                if (count++ > maxRanges) {
                    result.append("...");
                    break;
                }
            }
        }
        result.append("}");
        return result.toString();
    }

    public static void fillKeyIndicesChunk(final RowSet index,
            final WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
        chunkToFill.setSize(0); // so that we can actually add from the beginning.
        index.forEachRowKey((final long v) -> {
            chunkToFill.add(v);
            return true;
        });
    }

    public static void fillKeyRangesChunk(final RowSet index,
            final WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        chunkToFill.setSize(0);
        index.forAllRowKeyRanges((final long start, final long end) -> {
            chunkToFill.add(start);
            chunkToFill.add(end);
        });
    }

    static TLongArrayList[] findMissing(final RowSet base, final RowSet keys) {
        final TLongArrayList indices = new TLongArrayList();
        final TLongArrayList counts = new TLongArrayList();

        int countIndex = -1;
        long currentCount = 0;
        long lastPos = -2;

        for (final RowSet.RangeIterator iterator = keys.rangeIterator(); iterator.hasNext();) {
            iterator.next();
            final long currentRangeStart = iterator.currentRangeStart();

            final long key = base.find(currentRangeStart);
            Assert.ltZero(key, "key");
            if (-key - 1 == lastPos) {
                currentCount += iterator.currentRangeEnd() - currentRangeStart + 1;
                counts.set(countIndex, currentCount);
            } else {
                lastPos = -key - 1;
                indices.add(lastPos);
                currentCount = iterator.currentRangeEnd() - currentRangeStart + 1;
                counts.add(currentCount);
                countIndex++;
            }
        }
        return new TLongArrayList[] {indices, counts};
    }

    public static LogOutput append(final LogOutput logOutput, final RowSet.RangeIterator it) {
        int count = 0;
        logOutput.append("{");
        boolean isFirst = true;
        while (it.hasNext()) {
            it.next();

            if (!isFirst) {
                logOutput.append(",");
            }

            logOutput.append(it.currentRangeStart());

            if (it.currentRangeEnd() != it.currentRangeStart()) {
                logOutput.append("-").append(it.currentRangeEnd());
            }

            isFirst = false;
            if (count++ > 200) {
                logOutput.append("...");
                break;
            }
        }
        logOutput.append("}");
        return logOutput;
    }

    static boolean equalsDeepImpl(final RowSet index, final RowSet other) {
        final RowSet.RangeIterator it1 = other.rangeIterator();
        final RowSet.RangeIterator it2 = index.rangeIterator();
        while (it1.hasNext() && it2.hasNext()) {
            it1.next();
            it2.next();
            if (it1.currentRangeStart() != it2.currentRangeStart() || it1.currentRangeEnd() != it2.currentRangeEnd()) {
                return false;
            }
        }
        return !(it1.hasNext() || it2.hasNext());
    }

    static boolean equals(final RowSet index, final Object other) {
        if (!(other instanceof RowSet)) {
            return false;
        }
        final RowSet otherRowSet = (RowSet) other;
        return index.size() == otherRowSet.size() && RowSetUtils.equalsDeepImpl(index, otherRowSet);
    }

    public interface Comparator {
        /**
         * Compare the underlying target to the provided value. Return -1, 0, or 1 if target is less than, equal, or
         * greater than the provided value, respectively.
         *
         * @param value
         * @return -1 if target &lt; value; 0 if value == target; +1 if value &lt; target.
         */
        int directionToTargetFrom(final long value);
    }

    /**
     * Look for the biggest value of i that satisfies begin &lt;= i &lt;= end and comp.directionToTargetFrom(i) &gt; 0,
     * or some value that satisfies comp.directionToTargetFrom(i) == 0.
     *
     * @param begin The beginning of the range (inclusive)
     * @param end The end of the range (inclusive)
     * @param comp a Comparator.
     * @return the last position i inside the provided range that satisfies comp.directionToTargetFrom(i) &gt; 0, or
     *         some position that satisfies comp.directionToTargetFrom(i) == 0.
     */
    public static long rangeSearch(final long begin, final long end, final Comparator comp) {
        long i = begin;
        if (comp.directionToTargetFrom(i) <= 0) {
            return i;
        }
        long j = end;
        if (comp.directionToTargetFrom(j) >= 0) {
            return j;
        }
        while (true) {
            final long mid = (i + j) / 2;
            final int c = comp.directionToTargetFrom(mid);
            if (c < 0) {
                if (j == mid || j - i <= 1) {
                    return i;
                }
                j = mid;
                continue;
            }
            if (c > 0) {
                if (i == mid || j - i <= 1) {
                    return mid;
                }
                i = mid;
                continue;
            }
            // c == 0.
            return mid;
        }
    }

    /**
     * This is equivalent to `sourceRowSet.invert(destRowSet).forAllRowKeyRanges(lrc)`, but requires O(1) space. Note
     * that coalescing adjacent position-space runs enables callers to make minimal System.arraycopy calls.
     *
     * @param sourceRowSet RowSet to find the destRowSet keys in - ranges in the callback will be on this RowSet
     * @param destRowSet RowSet values to look for within sourceRowSet
     * @param lrc consumer to handle each inverted range that is encountered
     */
    public static void forAllInvertedLongRanges(final RowSet sourceRowSet, final RowSet destRowSet,
            final LongRangeConsumer lrc) {
        final MutableBoolean hasPending = new MutableBoolean();
        final MutableLong pendingStart = new MutableLong(RowSequence.NULL_ROW_KEY);
        final MutableLong pendingEnd = new MutableLong(RowSequence.NULL_ROW_KEY);
        final RowSequence.Iterator sourceProbe = sourceRowSet.getRowSequenceIterator();
        final MutableLong sourceOffset = new MutableLong();
        destRowSet.forAllRowKeyRanges((start, end) -> {
            final long sourceStart = sourceOffset.getValue() + sourceProbe.advanceAndGetPositionDistance(start);
            final long sourceEnd = sourceStart + sourceProbe.advanceAndGetPositionDistance(end);
            if (!hasPending.booleanValue()) {
                pendingStart.setValue(sourceStart);
                pendingEnd.setValue(sourceEnd);
                hasPending.setValue(true);
            } else if (pendingEnd.longValue() + 1 == sourceStart) {
                pendingEnd.setValue(sourceEnd);
            } else {
                lrc.accept(pendingStart.longValue(), pendingEnd.longValue());
                pendingStart.setValue(sourceStart);
                pendingEnd.setValue(sourceEnd);
            }
            sourceOffset.setValue(sourceEnd);
        });
        if (hasPending.booleanValue()) {
            lrc.accept(pendingStart.longValue(), pendingEnd.longValue());
        }
    }
}
