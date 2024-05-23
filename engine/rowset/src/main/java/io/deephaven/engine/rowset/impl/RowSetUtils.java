//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.datastructures.LongRangeConsumer;
import io.deephaven.util.mutable.MutableLong;
import gnu.trove.list.array.TLongArrayList;
import org.apache.commons.lang3.mutable.MutableBoolean;

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
            final long sourceStart = sourceOffset.longValue() + sourceProbe.advanceAndGetPositionDistance(start);
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

    public static class CombinedRangeIterator implements RowSet.RangeIterator {
        public enum RangeMembership {
            FIRST_ONLY(1), SECOND_ONLY(2), BOTH(3);

            private final int membershipBits;

            RangeMembership(final int membershipBits) {
                this.membershipBits = membershipBits;
            }

            public int membershipBits() {
                return membershipBits;
            }

            public boolean hasFirst() {
                return (membershipBits & 1) != 0;
            }

            public boolean hasSecond() {
                return (membershipBits & 2) != 0;
            }
        }

        private final RowSet.RangeIterator it1;
        private final RowSet.RangeIterator it2;

        // -1 we have used up all the current range for it1; this means we need to try to fetch
        // another range from it1 if we haven't tried that already.
        private long it1CurrStart;

        // -2 means we have checked it1.hasNext() already and
        // realized there are no more ranges available.
        private long it1CurrEnd;

        // -1 we have used up all the current range for it2; this means we need to try to fetch
        // another range from it2 if we haven't tried that already.
        private long it2CurrStart;

        // -2 means we have checked it1.hasNext() already and
        // realized there are no more ranges available.
        private long it2CurrEnd;

        private long currStart = -1, currEnd = -1;
        private RangeMembership currMembership = null;

        /***
         *
         * Provide the means to iterate over the combined ranges of two provided iterators. The resulting ranges in this
         * object are tagged with first, second, or both, to indicate if the current range was from the first iterator,
         * second iterator, or both respectively.
         *
         * @param it1 First iterator
         * @param it2 Second iterator
         */
        public CombinedRangeIterator(final RowSet.RangeIterator it1, final RowSet.RangeIterator it2) {
            this.it1 = it1;
            this.it2 = it2;
            it1CurrStart = -1;
            it1CurrEnd = -1;
            it2CurrStart = -1;
            it2CurrEnd = -1;
        }

        @Override
        public boolean hasNext() {
            if (it1CurrStart == -1 && it1CurrEnd != -2) {
                primeIter1();
            }
            if (it2CurrStart == -1 && it2CurrEnd != -2) {
                primeIter2();
            }
            return it1CurrEnd != -2 || it2CurrEnd != -2;
        }

        private void primeIter1() {
            if (it1.hasNext()) {
                it1.next();
                it1CurrStart = it1.currentRangeStart();
                it1CurrEnd = it1.currentRangeEnd();
            } else {
                it1CurrEnd = -2;
            }
        }

        private void primeIter2() {
            if (it2.hasNext()) {
                it2.next();
                it2CurrStart = it2.currentRangeStart();
                it2CurrEnd = it2.currentRangeEnd();
            } else {
                it2CurrEnd = -2;
            }
        }

        @Override
        public long next() {
            if (it1CurrStart != -1) {
                if (it2CurrStart != -1) {
                    if (it1CurrEnd < it2CurrStart) { // it1's range is completely to the left of it2's range.
                        currMembership = RangeMembership.FIRST_ONLY;
                        currStart = it1CurrStart;
                        currEnd = it1CurrEnd;
                        it1CurrStart = -1; // we consumed the it1 range completely.
                    } else if (it2CurrEnd < it1CurrStart) { // it1's range is completely to the right of it2's range.
                        currMembership = RangeMembership.SECOND_ONLY;
                        currStart = it2CurrStart;
                        currEnd = it2CurrEnd;
                        it2CurrStart = -1; // we consumed the it2 range completely.
                    } else { // it1's range has a non-empty overlap with it2's range.
                        final boolean it1WasStart, it2WasStart, it1WasEnd, it2WasEnd;
                        if (it1CurrStart < it2CurrStart) {
                            currMembership = RangeMembership.FIRST_ONLY;
                            currStart = it1CurrStart;
                            currEnd = it2CurrStart - 1;
                            it1WasStart = true;
                            it2WasStart = false;
                            it1WasEnd = it2WasEnd = false;
                        } else if (it2CurrStart < it1CurrStart) {
                            currMembership = RangeMembership.SECOND_ONLY;
                            currStart = it2CurrStart;
                            currEnd = it1CurrStart - 1;
                            it1WasStart = false;
                            it2WasStart = true;
                            it1WasEnd = it2WasEnd = false;
                        } else { // it1CurrStart == it2CurrStart
                            currMembership = RangeMembership.BOTH;
                            currStart = it1CurrStart;
                            it1WasStart = it2WasStart = true;
                            if (it1CurrEnd < it2CurrEnd) {
                                currEnd = it1CurrEnd;
                                it1WasEnd = true;
                                it2WasEnd = false;
                            } else if (it2CurrEnd < it1CurrEnd) {
                                currEnd = it2CurrEnd;
                                it1WasEnd = false;
                                it2WasEnd = true;
                            } else { // it1CurrEnd == it2CurrEnd
                                currEnd = it2CurrEnd;
                                it1WasEnd = true;
                                it2WasEnd = true;
                            }
                        }
                        // Consume from each iterator range appropriately.
                        if (it1WasStart) {
                            if (it1WasEnd) {
                                it1CurrStart = -1;
                            } else {
                                it1CurrStart = currEnd + 1;
                            }
                        }
                        if (it2WasStart) {
                            if (it2WasEnd) {
                                it2CurrStart = -1;
                            } else {
                                it2CurrStart = currEnd + 1;
                            }
                        }
                    }
                } else { // it2currStart == -1, which at this point means no more it2 ranges.
                    currMembership = RangeMembership.FIRST_ONLY;
                    currStart = it1CurrStart;
                    currEnd = it1CurrEnd;
                    it1CurrStart = -1; // we consumed the it1 range completely.
                }
            } else { // it1CurrStart == -1, which at this point means no more it1 ranges.
                if (it2CurrStart == -1) {
                    throw new IllegalStateException("Internal invariant violated");
                }
                currMembership = RangeMembership.SECOND_ONLY;
                currStart = it2CurrStart;
                currEnd = it2CurrEnd;
                it2CurrStart = -1; // we consumed the it2 range completely.
            }
            return currStart;
        }

        @Override
        public void close() {
            it1.close();
            it2.close();
        }

        @Override
        public long currentRangeStart() {
            return currStart;
        }

        @Override
        public long currentRangeEnd() {
            return currEnd;
        }

        public RangeMembership currentRangeMembership() {
            return currMembership;
        }

        @Override
        public boolean advance(long notUsed) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void postpone(long notUsed) {
            throw new UnsupportedOperationException();
        }
    }
}
