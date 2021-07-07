/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseablePair;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * A set of sorted shifts. To apply shifts without losing data, use {@link IndexShiftData#apply(Callback)}. The callback
 * will be invoked with shifts in an order that will preserve data when applied immediately using memmove semantics.
 * Internally the shifts are ordered by rangeStart. The {@link IndexShiftData.Builder} will verify that no two ranges
 * overlap before or after shifting and assert that the constructed {@code IndexShiftData} will be valid.
 */
public final class IndexShiftData implements Serializable, LogOutputAppendable {

    private static final int BEGIN_RANGE_ATTR  = 0;
    private static final int END_RANGE_ATTR    = 1;
    private static final int SHIFT_DELTA_ATTR  = 2;
    private static final int NUM_ATTR          = 3;

    /**
     * {@code payload} is organized into triplets of (rangeStart, rangeEnd, shiftDelta). Triplets are ordered by
     * rangeStart. This is not the order that will apply shifts without losing data.
     */
    private final TLongList payload;

    /**
     * {@code polaritySwapIndices} are indices into {@code payload} where the previous and current range's
     * {@code shiftDelta} swap between positive and negative shifts.
     */
    private final TIntList polaritySwapIndices;

    private IndexShiftData() {
        this.payload = new TLongArrayList();
        this.polaritySwapIndices = new TIntArrayList();
    }

    /**
     * The number of shifts embedded in the payload.
     * @return the number of shifts
     */
    public final int size() {
        return payload.size() / 3;
    }

    private volatile long cachedEffectiveSize = -1;

    /**
     * The number of keys affected by shift commands.
     * @return number of keys affected by shifts
     */
    public final long getEffectiveSize() {
        if (cachedEffectiveSize < 0) {
            long cc = 0;
            for (int idx = 0; idx < size(); ++idx) {
                cc += getEndRange(idx) - getBeginRange(idx) + 1;
            }
            cachedEffectiveSize = cc;
        }
        return cachedEffectiveSize;
    }

    /**
     * The number of keys affected by shift commands.
     * @param clamp the maximum size to return
     * @return number of keys affected by shifts
     */
    public final long getEffectiveSizeClamped(long clamp) {
        if (cachedEffectiveSize < 0) {
            long cc = 0;
            for (int idx = 0; idx < size(); ++idx) {
                cc += getEndRange(idx) - getBeginRange(idx) + 1;
                if (cc >= clamp) {
                    return clamp;
                }
            }
            cachedEffectiveSize = cc;
        }
        return Math.min(clamp, cachedEffectiveSize);
    }

    /**
     * Get the inclusive begin offset of the {@code idx}th shift.
     * @param idx which pair to get offset for
     * @return the offset
     */
    public final long getBeginRange(int idx) {
        return payload.get(idx * NUM_ATTR + BEGIN_RANGE_ATTR);
    }

    /**
     * Get the inclusive end offset of the {@code idx}th shift.
     * @param idx which pair to get offset for
     * @return the offset
     */
    public final long getEndRange(int idx) {
        return payload.get(idx * NUM_ATTR + END_RANGE_ATTR);
    }

    /**
     * Get the absolute shift of the {@code idx}th shift.
     * @param idx which pair to get shift for
     * @return the shift
     */
    public final long getShiftDelta(int idx) {
        return payload.get(idx * NUM_ATTR + SHIFT_DELTA_ATTR);
    }

    /**
     * Verify invariants of internal data structures hold.
     */
    public final void validate() {
        int polarOffset = 0;
        for (int idx = 0; idx < size(); ++idx) {
            Assert.leq(getBeginRange(idx), "getBeginRange(idx)", getEndRange(idx), "getEndRange(idx)");
            Assert.neqZero(getShiftDelta(idx), "getShiftDelta(idx)");

            if (idx == 0) {
                continue;
            }

            // Check no overlap in original key space.
            Assert.lt(getEndRange(idx - 1), "getEndRange(idx - 1)",
                    getBeginRange(idx), "getBeginRange(idx)");

            // Check no overlap in new key space.
            final long newPrevEnd = getEndRange(idx - 1) + getShiftDelta(idx - 1);
            final long newCurrBegin = getBeginRange(idx) + getShiftDelta(idx);
            Assert.lt(newPrevEnd, "newPrevEnd", newCurrBegin, "newCurrBeing");

            // Verify if shift delta changes that it is considered a different run.
            final int prevShiftSign = getShiftDelta(idx - 1) < 0 ? -1 : 1;
            final int currShiftSign = getShiftDelta(idx) < 0 ? -1 : 1;
            if (prevShiftSign != currShiftSign) {
                Assert.gt(polaritySwapIndices.size(), "polaritySwapIndices.size()", polarOffset, "polarOffset");
                Assert.eq(polaritySwapIndices.get(polarOffset), "polaritySwapIndices.get(polarOffset)", idx, "idx");
                ++polarOffset;
            }
        }
    }

    /**
     * Queries whether this IndexShiftData is empty (i.e. has no shifts).
     * @return true if the size() of this is zero, false if the size is greater than zero
     */
    public final boolean empty() {
        return size() == 0;
    }

    /**
     * Queries whether this IndexShiftData is non-empty (i.e. has at least one shift).
     * @return true if the size() of this Index greater than zero, false if the size is zero
     */
    public final boolean nonempty() { return !empty(); }

    @Override
    public String toString() {
        return append(new LogOutputStringImpl()).toString();
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return append(logOutput, 200);
    }

    public LogOutput append(final LogOutput logOutput, final int maxShifts) {
        int count = 0;
        logOutput.append("{");
        boolean isFirst = true;
        for (int idx = 0; idx < size(); ++idx) {
            final long shift = getShiftDelta(idx);
            logOutput.append(isFirst ? "" : ",")
                    .append("[").append(getBeginRange(idx))
                    .append(",").append(getEndRange(idx))
                    .append(shift < 0 ? "]" : "]+").append(shift);
            isFirst = false;
            if (++count >= maxShifts) {
                logOutput.append(",...");
                break;
            }
        }
        logOutput.append("}");
        return logOutput;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof IndexShiftData)) {
            return false;
        }
        final IndexShiftData shiftData = (IndexShiftData) obj;
        // Note that comparing payload is sufficient. The polarity indices are precomputed from the payload.
        return shiftData.payload.equals(payload);
    }

    /**
     * Immutable, re-usable {@link IndexShiftData} for an empty set of shifts.
     */
    public static final IndexShiftData EMPTY = new IndexShiftData();

    @FunctionalInterface
    public interface Callback {
        /**
         * Process the shift.
         * @param beginRange start of range (inclusive)
         * @param endRange end of range (inclusive)
         * @param shiftDelta amount range has moved by
         */
        void shift(long beginRange, long endRange, long shiftDelta);
    }

    /**
     * Apply all shifts in a memmove-semantics-safe ordering through the provided {@code shiftCallback}.
     *
     * Use this to move from pre-shift keyspace to post-shift keyspace.
     *
     * @param shiftCallback the callback that will process all shifts
     */
    public void apply(final Callback shiftCallback) {
        for (int idx = 0; idx < polaritySwapIndices.size(); ++idx) {
            int start = (idx == 0) ? 0 : polaritySwapIndices.get(idx - 1);
            int end = polaritySwapIndices.get(idx) - 1;
            final long dir = getShiftDelta(start) > 0 ? -1 : 1;
            if (dir < 0) {
                final int tmp = start;
                start = end;
                end = tmp;
            }
            for (int jdx = start; jdx != end + dir; jdx += dir) {
                shiftCallback.shift(getBeginRange(jdx), getEndRange(jdx), getShiftDelta(jdx));
            }
        }
    }

    /**
     * Apply all shifts in reverse in a memmove-semantics-safe ordering through the provided {@code shiftCallback}.
     *
     * Use this to move from post-shift keyspace to pre-shift keyspace.
     *
     * @param shiftCallback the callback that will process all reverse shifts
     */
    public void unapply(final Callback shiftCallback) {
        for (int idx = 0; idx < polaritySwapIndices.size(); ++idx) {
            int start = (idx == 0) ? 0 : polaritySwapIndices.get(idx - 1);
            int end = polaritySwapIndices.get(idx) - 1;
            final long dir = getShiftDelta(start) > 0 ? 1 : -1;
            if (dir < 0) {
                final int tmp = start;
                start = end;
                end = tmp;
            }
            for (int jdx = start; jdx != end + dir; jdx += dir) {
                final long delta = getShiftDelta(jdx);
                shiftCallback.shift(getBeginRange(jdx) + delta, getEndRange(jdx) + delta, -delta);
            }
        }
    }

    /**
     * Apply all shifts to the provided index. Moves index from pre-shift keyspace to post-shift keyspace.
     * @param index the index to shift
     */
    public void apply(final Index index) {
        final Index.SequentialBuilder toRemove = Index.FACTORY.getSequentialBuilder();
        final Index.SequentialBuilder toInsert = Index.FACTORY.getSequentialBuilder();
        try (final OrderedKeys.Iterator okit = index.getOrderedKeysIterator()) {
            for (int idx = 0; idx < size(); ++idx) {
                final long beginRange = getBeginRange(idx);
                final long endRange = getEndRange(idx);
                final long shiftDelta = getShiftDelta(idx);

                if (!okit.advance(beginRange)) {
                    break;
                }

                toRemove.appendRange(beginRange, endRange);
                okit.getNextOrderedKeysThrough(endRange).forAllLongRanges((s, e) -> {
                    toInsert.appendRange(s + shiftDelta, e + shiftDelta);
                });
            }
        }

        try (final Index remove = toRemove.getIndex();
             final Index insert = toInsert.getIndex()) {
            index.remove(remove);
            index.insert(insert);
        }
    }

    /**
     * Apply a shift to the provided index. Moves index from pre-shift keyspace to post-shift keyspace.
     *
     * @param index      The index to apply the shift to
     * @param beginRange start of range (inclusive)
     * @param endRange   end of range (inclusive)
     * @param shiftDelta amount range has moved by
     * @return Whether there was any overlap found to shift
     */
    public static boolean applyShift(@NotNull final Index index, final long beginRange, final long endRange, final long shiftDelta) {
        try (final Index toShift = index.subindexByKey(beginRange, endRange)) {
            if (toShift.empty()) {
                return false;
            }
            index.removeRange(beginRange, endRange);
            toShift.shiftInPlace(shiftDelta);
            index.insert(toShift);
            return true;
        }
    }

    /**
     * Unapply all shifts to the provided index. Moves index from post-shift keyspace to pre-shift keyspace.
     * @param index the index to shift
     */
    public void unapply(final Index index) {
        final Index.SequentialBuilder toRemove = Index.FACTORY.getSequentialBuilder();
        final Index.SequentialBuilder toInsert = Index.FACTORY.getSequentialBuilder();
        try (final OrderedKeys.Iterator okit = index.getOrderedKeysIterator()) {
            for (int idx = 0; idx < size(); ++idx) {
                final long beginRange = getBeginRange(idx);
                final long endRange = getEndRange(idx);
                final long shiftDelta = getShiftDelta(idx);

                if (!okit.advance(beginRange + shiftDelta)) {
                    break;
                }

                toRemove.appendRange(beginRange + shiftDelta, endRange + shiftDelta);
                okit.getNextOrderedKeysThrough(endRange + shiftDelta).forAllLongRanges((s, e) -> {
                    toInsert.appendRange(s - shiftDelta, e - shiftDelta);
                });
            }
        }

        try (final Index remove = toRemove.getIndex();
             final Index insert = toInsert.getIndex()) {
            index.remove(remove);
            index.insert(insert);
        }
    }

    /**
     * Unapply all shifts to the provided index. Moves index from post-shift keyspace to pre-shift keyspace.
     * @param index the index to shift
     * @param offset an additional offset to apply to all shifts (such as when applying to a wrapped table)
     */
    public void unapply(final Index index, final long offset) {
        // NB: This is an unapply callback, and beginRange, endRange, and shiftDelta have been adjusted so that this is a reversed shift,
        //     hence we use the applyShift helper.
        unapply((beginRange, endRange, shiftDelta) -> applyShift(index, beginRange + offset, endRange + offset, shiftDelta));
    }

    /**
     * Unapply a shift to the provided index. Moves index from post-shift keyspace to pre-shift keyspace.
     *
     * @param index      The index to apply the shift to
     * @param beginRange start of range (inclusive)
     * @param endRange   end of range (inclusive)
     * @param shiftDelta amount range has moved by
     * @return Whether there was any overlap found to shift
     */
    public static boolean unapplyShift(@NotNull final Index index, final long beginRange, final long endRange, final long shiftDelta) {
        try (final Index toShift = index.subindexByKey(beginRange + shiftDelta, endRange + shiftDelta)) {
            if (toShift.empty()) {
                return false;
            }
            index.removeRange(beginRange + shiftDelta, endRange + shiftDelta);
            toShift.shiftInPlace(-shiftDelta);
            index.insert(toShift);
            return true;
        }
    }

    @FunctionalInterface
    public interface SingleElementShiftCallback {
        /**
         * Process the shift.
         * @param key the key to shift
         * @param shiftDelta amount key has moved by
         */
        void shift(long key, long shiftDelta);
    }

    public void forAllInIndex(final ReadOnlyIndex filterIndex, final SingleElementShiftCallback callback) {
        boolean hasReverseShift = false;
        ReadOnlyIndex.SearchIterator it = filterIndex.reverseIterator();
        FORWARD_SHIFT: for (int ii = size() - 1; ii >= 0; --ii) {
            final long delta = getShiftDelta(ii);
            if (delta < 0) {
                hasReverseShift = true;
                continue;
            }
            final long start = getBeginRange(ii);
            final long end = getEndRange(ii);
            if (!it.advance(end)) {
                break;
            }
            while (it.currentValue() >= start) {
                callback.shift(it.currentValue(), delta);
                if (!it.hasNext()) {
                    break FORWARD_SHIFT;
                }
                it.nextLong();
            }
        }

        if (!hasReverseShift) {
            return;
        }

        it = filterIndex.searchIterator();
        REVERSE_SHIFT: for (int ii = 0; ii < size(); ++ii) {
            final long delta = getShiftDelta(ii);
            if (delta > 0) {
                continue;
            }
            final long start = getBeginRange(ii);
            final long end = getEndRange(ii);
            if (!it.advance(start)) {
                break;
            }
            while (it.currentValue() <= end) {
                callback.shift(it.currentValue(), delta);
                if (!it.hasNext()) {
                    break REVERSE_SHIFT;
                }
                it.nextLong();
            }
        }
    }

    public interface Iterator {
        boolean hasNext();
        void next();

        long beginRange();
        long endRange();
        long shiftDelta();

        boolean polarityReversed();

        Iterator EMPTY = new Iterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public void next() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long beginRange() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long endRange() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long shiftDelta() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean polarityReversed() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private class ApplyIterator implements Iterator {
        int psi = -1;

        int start = -1;
        int end = -1;
        int dir = -1;

        @Override
        public boolean hasNext() {
            final boolean remainingSwaps = psi < polaritySwapIndices.size() - 1;
            final boolean remainingValuesInSwap = start != end;
            return remainingSwaps || remainingValuesInSwap;
        }

        @Override
        public void next() {
            if (start == end) {
                psi++;
                if (psi == 0) {
                    start = 0;
                } else {
                    start = polaritySwapIndices.get(psi - 1);
                }
                end = polaritySwapIndices.get(psi) - 1;

                dir = getShiftDelta(start) > 0 ? - 1 : 1;
                if (dir < 0) {
                    final int tmp = start;
                    start = end;
                    end = tmp;
                }
            } else {
                start += dir;
            }
        }

        @Override
        public boolean polarityReversed() {
            return dir < 0;
        }

        @Override
        public long beginRange() {
            return getBeginRange(start);
        }

        @Override
        public long endRange() {
            return getEndRange(start);
        }

        @Override
        public long shiftDelta() {
            return getShiftDelta(start);
        }
    }

    public Iterator applyIterator() {
        if (empty()) {
            return Iterator.EMPTY;
        }
        return new ApplyIterator();
    }

    /**
     * Intersects this IndexShiftData against the provided Index.
     * @param index the index to test for intersections (pre-shift keyspace)
     * @return an IndexShiftData containing only non-empty shifts
     */
    public IndexShiftData intersect(final Index index) {
        final Builder builder = new Builder();

        for (int idx = 0; idx < size(); ++idx) {
            if (index.overlapsRange(getBeginRange(idx), getEndRange(idx))) {
                builder.shiftRange(getBeginRange(idx), getEndRange(idx), getShiftDelta(idx));
            }
        }

        return builder.build();
    }

    /**
     * Helper utility to build instances of {@link IndexShiftData} with internally consistent data.
     * No other ranges should be added to this builder after {@link Builder#build} is invoked.
     */
    public static class Builder {
        private IndexShiftData shiftData;

        public Builder() {
            this.shiftData = new IndexShiftData();
        }

        /**
         * @return true iff there is at least one shift appended to this builder
         */
        public boolean nonempty() {
            return shiftData.payload.size() > 0;
        }

        /**
         * Gets the last index assigned to a shift.
         * @return The greatest index assigned to a shift or -1 if no shifts exist yet.
         */
        public long lastShiftEnd() {
            return shiftData.size() > 0 ? shiftData.getEndRange(shiftData.size() - 1) : -1;
        }

        /**
         * Shift a range of keys. The shift must be consistent with previously added shifts.
         * @param beginRange first key to shift (inclusive)
         * @param endRange last key to shift (inclusive)
         * @param shiftDelta offset to shift by; may be negative
         */
        public void shiftRange(final long beginRange, final long endRange, final long shiftDelta) {
            if (shiftDelta == 0 || endRange < beginRange) {
                return;
            }

            final int prevIdx = shiftData.size() - 1;

            // Coalesce when possible.
            if (prevIdx >= 0 && shiftData.getShiftDelta(prevIdx) == shiftDelta
                    && shiftData.getEndRange(prevIdx) + 1 == beginRange) {
                shiftData.payload.set(prevIdx * NUM_ATTR + END_RANGE_ATTR, endRange);
                return;
            }

            shiftData.payload.add(beginRange);
            shiftData.payload.add(endRange);
            shiftData.payload.add(shiftDelta);

            if (prevIdx < 0) {
                return;
            }

            // If previous shift has different sign than shiftDelta, we must add current index to split run into chunks
            if ((shiftData.getShiftDelta(prevIdx) < 0 ? -1 : 1) * shiftDelta < 0) {
                shiftData.polaritySwapIndices.add(shiftData.size() - 1); // note the -1 excludes the new range
            }

            if (beginRange <= shiftData.getEndRange(prevIdx)) {
                throw new IllegalArgumentException("new range [" + beginRange + "," + endRange
                        + "]->" + shiftDelta + " overlaps previous [" + shiftData.getBeginRange(prevIdx) + ","
                        + shiftData.getEndRange(prevIdx) + "]->" + shiftData.getShiftDelta(prevIdx));
            }
            if (beginRange + shiftDelta <= shiftData.getEndRange(prevIdx) + shiftData.getShiftDelta(prevIdx)) {
                throw new IllegalArgumentException("new resulting range [" + beginRange + "," + endRange
                        + "]->" + shiftDelta + " overlaps previous [" + shiftData.getBeginRange(prevIdx) + ","
                        + shiftData.getEndRange(prevIdx) + "]->" + shiftData.getShiftDelta(prevIdx));
            }
        }

        public long getMinimumValidBeginForNextDelta(long nextShiftDelta) {
            if (shiftData.empty()) {
                return nextShiftDelta < 0 ? -nextShiftDelta : 0;
            }
            final int idx = shiftData.size() - 1;
            return Math.max(shiftData.getEndRange(idx) + 1, shiftData.getEndRange(idx) + shiftData.getShiftDelta(idx) - nextShiftDelta + 1);
        }

        /**
         * Make final modifications to the {@link IndexShiftData} and return it.
         * @return the built IndexShiftData
         */
        public IndexShiftData build() {
            final IndexShiftData retVal = shiftData;
            shiftData = null;

            if (retVal.empty()) {
                return IndexShiftData.EMPTY;
            }

            // Complete the current run.
            retVal.polaritySwapIndices.add(retVal.size());

            return retVal;
        }

        /**
         * Use this method to append shifts that propagate from a parent table to a subset of a dependent table. The
         * canonical use-case is merge, where tables are shifted in key-space so that they do not overlap each other.
         * If one of these merged tables has a shift, then it must propagate these shifts to the merged table in the
         * appropriately shifted key space.
         *
         * This method also supports shifting the entire range in addition to propagating upstream shifts. For example,
         * if a table needs more keyspace, then any tables slotted to the right (in a greater keyspace) will need to
         * shift out of the way to free up the keyspace for the table.
         *
         * This method assumes that 1) the upstream shift data is valid and 2) shifts can be truncated when they extend
         * beyond the table's known range.
         *
         * @param innerShiftData the upstream shifts oriented in upstream keyspace [0, innerRange)
         * @param prevOffset the previous offset where this sub-table began
         * @param prevCardinality the cardinality of the keyspace previously allocated to this table
         * @param currOffset the new offset where this sub-table begins (may be equal to prevOffset)
         * @param currCardinality the cardinality of the keyspace currently allocated to this table
         */
        public void appendShiftData(final IndexShiftData innerShiftData, final long prevOffset, final long prevCardinality, final long currOffset, final long currCardinality) {
            long watermarkKey = 0; // id space of source table

            // These bounds seem weird. We are going to insert a shift for the keyspace prior to the shift with
            // index sidx. Thus, the first and last sidx are to cover shifting via `indexSpaceInserted` on the
            // outside of shifts. Note that we use the knowledge/contract that shift data is ordered by key.
            for (int sidx = 0; sidx < innerShiftData.size() + 1; ++sidx) {
                final long nextShiftEnd;
                final long nextShiftStart;
                final long nextShiftDelta;
                if (sidx < innerShiftData.size()) {
                    nextShiftDelta = innerShiftData.getShiftDelta(sidx);
                    // Shifts to indices less than zero are meaningless and might cause our builder to complain.
                    nextShiftStart = Math.max(innerShiftData.getBeginRange(sidx), nextShiftDelta < 0 ? -nextShiftDelta : 0);
                    // Shifts beyond the cardinality are meaningless (assumptions) but might destroy neighboring table data.
                    nextShiftEnd = Math.min(Math.min(prevCardinality - 1, currCardinality - 1 - nextShiftDelta), innerShiftData.getEndRange(sidx));
                } else {
                    nextShiftEnd = nextShiftStart = prevCardinality;
                    nextShiftDelta = 0;
                }

                // insert range prior to here; note shift ends are inclusive so we need the -1 for endRange
                final long innerEnd = Math.min(prevCardinality - 1, nextShiftStart - 1)
                        + (nextShiftDelta < 0 ? nextShiftDelta : 0);

                shiftRange(watermarkKey + prevOffset, innerEnd + prevOffset, currOffset - prevOffset);

                if (sidx >= innerShiftData.size() || nextShiftStart > prevCardinality) {
                    break;
                }

                // insert this range
                shiftRange(nextShiftStart + prevOffset, nextShiftEnd + prevOffset, currOffset - prevOffset + nextShiftDelta);
                watermarkKey = nextShiftEnd + 1 + (nextShiftDelta > 0 ? nextShiftDelta : 0);
            }
        }

        /**
         * This method adjusts the previous shift so that the upcoming shift will not be considered overlapping. This is
         * useful if the previous shift included empty space for efficiency, but would intersect with our new shift.
         * @param nextShiftBegin The first real-key that needs to shift in the upcoming shift.
         * @param nextShiftDelta The delta that applies to the upcoming shift.
         */
        public void limitPreviousShiftFor(long nextShiftBegin, long nextShiftDelta) {
            while (shiftData.nonempty()) {
                final int prevIdx = shiftData.size() - 1;
                if (nextShiftBegin <= shiftData.getEndRange(prevIdx)) {
                    shiftData.payload.set(prevIdx * NUM_ATTR + END_RANGE_ATTR, nextShiftBegin - 1);
                }
                if (nextShiftBegin + nextShiftDelta <= shiftData.getEndRange(prevIdx) + shiftData.getShiftDelta(prevIdx)) {
                    shiftData.payload.set(prevIdx * NUM_ATTR + END_RANGE_ATTR, nextShiftBegin + nextShiftDelta - shiftData.getShiftDelta(prevIdx) - 1);
                }

                if (shiftData.getEndRange(prevIdx) < shiftData.getBeginRange(prevIdx)) {
                    // remove shift completely:
                    shiftData.payload.remove(shiftData.payload.size() - 3,  3);
                    final int numSwaps = shiftData.polaritySwapIndices.size();
                    if (numSwaps > 0 && shiftData.polaritySwapIndices.get(numSwaps - 1) >= shiftData.size()) {
                        shiftData.polaritySwapIndices.removeAt(numSwaps - 1);
                    }
                } else {
                    return;
                }
            }
        }
    }

    /**
     * Helper utility to build instances of {@link IndexShiftData} with internally consistent data.
     * No other ranges should be added to this builder after {@link Builder#build} is invoked.
     * <p>
     * Differs from {@link Builder} in that it coalesces ranges with the same delta if they have no intervening keys
     * in the pre-shift keys of the input, e.g. a {@link io.deephaven.db.tables.Table}.</p>
     * <p>
     * The data should be presented to the builder in shift iterator order, meaning the first contiguous run with
     * a given polarity is presented to the builder, then the next run is presented with the opposite polarity.
     * When the polarity is reversed (i.e., the delta is positive); the ranges must be presented in reverse (descending)
     * order within the run.  When the polarity is not reversed (i.e., the delta is negative); the ranges must be
     * presented in ascending order.</p>
     */
    public static final class SmartCoalescingBuilder implements SafeCloseable {
        /**
         * The pre shift keys of the table we are generating shift data for.
         */
        private ReadOnlyIndex preShiftKeys;
        /**
         * A forward iterator, which is used for all shifts that do not have reversed polarity (i.e. negative delta).
         * We create this on the first negative delta shift and reuse it until we are closed.
         */
        private ReadOnlyIndex.SearchIterator preShiftKeysIteratorForward;
        /**
         * For each run of shifts that have reversed polarity (positive delta), we create a new reverse iterator.  We
         * reuse this until we find a negative delta shift and then close it.
         */
        private ReadOnlyIndex.SearchIterator preShiftKeysIteratorReverse;
        /**
         * The resultant shift data.
         */
        private IndexShiftData shiftData;

        /**
         * The index of the first range that needs to be reversed.  -1 if there is no range to reverse at the moment.
         */
        private int rangeToReverseStart = -1;

        /**
         * True if our last shift was reverse polarity, used to identify when the polarity inverts.
         */
        private boolean lastPolarityReversed = false;

        /**
         * The next key that we have iterated forward (used to skip irrelevant ranges).
         */
        private long nextForwardKey;
        /**
         * The next key that we have reverse iterated (used to skip irrelevant ranges).
         */
        private long nextReverseKey;
        /**
         * The next key after our last shift range.  We record this value so that if two subsequent shifts have the same
         * delta, but do not include the intervening key we do not permit coalescing.  If there is no intervening key,
         * we permit coalescing.  ReadOnlyIndex.NULL_KEY indicates there is no intervening key of interest.
         */
        private long interveningKey = ReadOnlyIndex.NULL_KEY;

        /**
         * The last point at which we started the reverse iterator.
         */
        private long lastReverseIteratorStart = ReadOnlyIndex.NULL_KEY;

        /**
         * Make a builder that tries to coalesce non-adjacent ranges with the same delta if there are no intervening
         * keys in the pre-shift ordered keys.
         *
         * @param preShiftKeys The pre-shift ordered keys for the space being shifted.
         */
        public SmartCoalescingBuilder(@NotNull final ReadOnlyIndex preShiftKeys) {
            this.preShiftKeys = preShiftKeys;
            shiftData = new IndexShiftData();
        }

        /**
         * @return true iff there is at least one shift appended to this builder
         */
        public boolean nonempty() {
            return shiftData.payload.size() > 0;
        }

        /**
         * Shift a range of keys. The shift must be consistent with previously added shifts.
         *
         * @param beginRange first key to shift (inclusive)
         * @param endRange last key to shift (inclusive)
         * @param shiftDelta offset to shift by; may be negative
         */
        public void shiftRange(final long beginRange, final long endRange, final long shiftDelta) {
            if (shiftDelta == 0 || endRange < beginRange) {
                return;
            }

            final boolean polarityReversed = shiftDelta > 0;
            final boolean polarityChanged = lastPolarityReversed != polarityReversed;
            final boolean reinitializeReverseIterator = polarityReversed && (polarityChanged || beginRange > lastReverseIteratorStart);
            if (polarityChanged || reinitializeReverseIterator) {
                interveningKey = ReadOnlyIndex.NULL_KEY;
                if (lastPolarityReversed) {
                    maybeReverseLastRun();
                    if (preShiftKeysIteratorReverse != null) {
                        preShiftKeysIteratorReverse.close();
                        preShiftKeysIteratorReverse = null;
                    }
                    // we take care of creating the iterator below for the case where the polarity is not reversed
                    // (but only once, as the iterator is usable for this entire builder)
                }
            }

            if (reinitializeReverseIterator) {
                maybeReverseLastRun();
                Assert.eqNull(preShiftKeysIteratorReverse, "preShiftKeysIteratorReverse");
                preShiftKeysIteratorReverse = preShiftKeys.reverseIterator();
                lastReverseIteratorStart = endRange;
                if (!preShiftKeysIteratorReverse.advance(endRange)) {
                    nextReverseKey = ReadOnlyIndex.NULL_KEY;
                } else {
                    nextReverseKey = preShiftKeysIteratorReverse.currentValue();
                }
                rangeToReverseStart = shiftData.size();
            }
            lastPolarityReversed = polarityReversed;

            if (!polarityReversed && preShiftKeysIteratorForward == null) {
                preShiftKeysIteratorForward = preShiftKeys.searchIterator();
                if (preShiftKeysIteratorForward.hasNext()) {
                    nextForwardKey = preShiftKeysIteratorForward.nextLong();
                } else {
                    nextForwardKey = ReadOnlyIndex.NULL_KEY;
                }
            }

            final long nextInterveningKey;
            if (polarityReversed) {
                if (nextReverseKey == ReadOnlyIndex.NULL_KEY || nextReverseKey < beginRange) {
                    return;
                }
                if (beginRange == 0 || !preShiftKeysIteratorReverse.advance(beginRange - 1)) {
                    nextInterveningKey = nextReverseKey = ReadOnlyIndex.NULL_KEY;
                } else {
                    nextInterveningKey = nextReverseKey = preShiftKeysIteratorReverse.currentValue();
                }
            } else {
                if (nextForwardKey == ReadOnlyIndex.NULL_KEY || nextForwardKey > endRange) {
                    return;
                }
                if (endRange == Long.MAX_VALUE || !preShiftKeysIteratorForward.advance(endRange + 1)) {
                    nextInterveningKey = nextForwardKey = ReadOnlyIndex.NULL_KEY;
                } else {
                    nextInterveningKey = nextForwardKey = preShiftKeysIteratorForward.currentValue();
                }
            }

            final int currentRangeIndex = shiftData.size() - 1;
            // Coalesce when possible.
            if (currentRangeIndex >= 0 && shiftData.getShiftDelta(currentRangeIndex) == shiftDelta) {
                // if we had an intervening key between the last end (or begin) and the current begin (or end); then
                // these two ranges can not be coalesced
                if (polarityReversed) {
                    if (interveningKey == ReadOnlyIndex.NULL_KEY || interveningKey <= endRange) {
                        // we must merge these ranges; this is not as simple as the forward case, because if we had the
                        // same reverse iterator as last time (i.e. the polarity was applied "correctly"), we should
                        // simply be able to update the beginning of the range.  However, if the existing range is
                        // before this range; it means we are in a new segment of shifts; and must merge ourselves
                        // to the existing shift by extending the end
                        final long existingBegin = shiftData.getBeginRange(currentRangeIndex);
                        final long existingEnd = shiftData.getEndRange(currentRangeIndex);
                        if (existingBegin < beginRange) {
                            // if there was an intervening key between our beginRange and the existing end, we can not merge
                            if (nextInterveningKey <= existingEnd) {
                                shiftData.payload.set(currentRangeIndex * 3 + 1, endRange);
                                interveningKey = nextInterveningKey;
                                return;
                            }
                        } else {
                            shiftData.payload.set(currentRangeIndex * 3, beginRange);
                            interveningKey = nextInterveningKey;
                            return;
                        }
                    }
                } else {
                    if (interveningKey == ReadOnlyIndex.NULL_KEY || interveningKey >= beginRange) {
                        shiftData.payload.set(currentRangeIndex * 3 + 1, endRange);
                        interveningKey = nextInterveningKey;
                        return;
                    }
                }
            }

            // we could not coalesce
            interveningKey = nextInterveningKey;
            shiftData.payload.add(beginRange);
            shiftData.payload.add(endRange);
            shiftData.payload.add(shiftDelta);

            if (currentRangeIndex < 0) {
                return;
            }

            // If previous shift has different sign than shiftDelta, we must add current index to split run into chunks.
            final boolean polaritySwap = (shiftData.getShiftDelta(currentRangeIndex) < 0 ? -1 : 1) * shiftDelta < 0;
            if (polaritySwap) {
                shiftData.polaritySwapIndices.add(shiftData.size() - 1); // NB: The -1 excludes the new range.
            }

            if (!polarityReversed) {
                if (beginRange <= shiftData.getEndRange(currentRangeIndex)) {
                    throw new IllegalArgumentException("new range [" + beginRange + "," + endRange
                            + "]->" + shiftDelta + " overlaps previous [" + shiftData.getBeginRange(currentRangeIndex) + ","
                            + shiftData.getEndRange(currentRangeIndex) + "]->" + shiftData.getShiftDelta(currentRangeIndex));
                }
                if (beginRange + shiftDelta <= shiftData.getEndRange(currentRangeIndex) + shiftData.getShiftDelta(currentRangeIndex)) {
                    throw new IllegalArgumentException("new resulting range [" + beginRange + "," + endRange
                            + "]->" + shiftDelta + " overlaps previous [" + shiftData.getBeginRange(currentRangeIndex) + ","
                            + shiftData.getEndRange(currentRangeIndex) + "]->" + shiftData.getShiftDelta(currentRangeIndex));
                }
            } else if (!reinitializeReverseIterator) {
                // we are in the midst of a sequence of reversed polarity things, so we should be less than the previous shift
                if (beginRange >= shiftData.getEndRange(currentRangeIndex)) {
                    throw new IllegalArgumentException("new range [" + beginRange + "," + endRange
                            + "]->" + shiftDelta + " overlaps previous [" + shiftData.getBeginRange(currentRangeIndex) + ","
                            + shiftData.getEndRange(currentRangeIndex) + "]->" + shiftData.getShiftDelta(currentRangeIndex));
                }
                if (beginRange + shiftDelta >= shiftData.getEndRange(currentRangeIndex) + shiftData.getShiftDelta(currentRangeIndex)) {
                    throw new IllegalArgumentException("new resulting range [" + beginRange + "," + endRange
                            + "]->" + shiftDelta + " overlaps previous [" + shiftData.getBeginRange(currentRangeIndex) + ","
                            + shiftData.getEndRange(currentRangeIndex) + "]->" + shiftData.getShiftDelta(currentRangeIndex));
                }
            }
        }

        /**
         * When the polarity is reversed, we build the run backwards; and we flip it around when transitioning to the
         * next run (or when the final build is called).
         */
        private void maybeReverseLastRun() {
            if (rangeToReverseStart >= 0) {
                final int runLength = shiftData.size() - rangeToReverseStart;
                for (int ii = 0; ii < runLength / 2; ++ii) {
                    final int firstIdx = (rangeToReverseStart + ii) * 3;
                    final int lastIdx = (rangeToReverseStart + runLength - ii - 1) * 3;

                    final long tmpStart = shiftData.payload.get(firstIdx);
                    final long tmpEnd = shiftData.payload.get(firstIdx + 1);
                    final long tmpDelta = shiftData.payload.get(firstIdx + 2);

                    shiftData.payload.set(firstIdx, shiftData.payload.get(lastIdx));
                    shiftData.payload.set(firstIdx + 1, shiftData.payload.get(lastIdx + 1));
                    shiftData.payload.set(firstIdx + 2, shiftData.payload.get(lastIdx + 2));


                    shiftData.payload.set(lastIdx, tmpStart);
                    shiftData.payload.set(lastIdx + 1, tmpEnd);
                    shiftData.payload.set(lastIdx + 2, tmpDelta);
                }
                rangeToReverseStart = -1;
            }
        }

        /**
         * Make final modifications to the {@link IndexShiftData} and return it. Invoke {@link #close()} to minimize
         * the lifetime of the pre-shift {@link OrderedKeys.Iterator}.
         *
         * @return The built IndexShiftData
         */
        public IndexShiftData build() {
            maybeReverseLastRun();

            final IndexShiftData result = shiftData;
            close();

            if (result.empty()) {
                return IndexShiftData.EMPTY;
            }

            // Complete the current run.
            result.polaritySwapIndices.add(result.size());

            return result;
        }

        @Override
        public void close() {
            preShiftKeys.close();
            if (preShiftKeysIteratorForward != null) {
                preShiftKeysIteratorForward.close();
            }
            if (preShiftKeysIteratorReverse != null) {
                preShiftKeysIteratorReverse.close();
            }
            preShiftKeys = null;
            preShiftKeysIteratorForward = null;
            preShiftKeysIteratorReverse = null;
            shiftData = null;
        }
    }

    /**
     * This method creates two parallel Index structures that contain postShiftIndex keys affected by shifts. The two
     * Indexes have the same size. An element at position k in the first index is the pre-shift key for the same row
     * whose post-shift key is at position k in the second index.
     *
     * @param postShiftIndex The index of keys that were shifted in post-shift keyspace. It should not contain rows
     *                       that did not exist prior to the shift.
     * @return A SafeCloseablePair of preShiftedKeys and postShiftedKeys that intersect this IndexShiftData with postShiftIndex.
     */
    public SafeCloseablePair<Index, Index> extractParallelShiftedRowsFromPostShiftIndex(final ReadOnlyIndex postShiftIndex) {
        if (empty()) {
            return SafeCloseablePair.of(Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex());
        }

        final Index.SequentialBuilder preShiftBuilder = Index.FACTORY.getSequentialBuilder();
        final Index.SequentialBuilder postShiftBuilder = Index.FACTORY.getSequentialBuilder();

        try (final OrderedKeys.Iterator okit = postShiftIndex.getOrderedKeysIterator()) {
            for (int idx = 0; idx < size(); ++idx) {
                final long beginRange = getBeginRange(idx);
                final long endRange = getEndRange(idx);
                final long shiftDelta = getShiftDelta(idx);

                if (!okit.advance(beginRange + shiftDelta)) {
                    break;
                }

                okit.getNextOrderedKeysThrough(endRange + shiftDelta).forAllLongRanges((s, e) -> {
                    preShiftBuilder.appendRange(s - shiftDelta, e - shiftDelta);
                    postShiftBuilder.appendRange(s, e);
                });
            }
        }

        final SafeCloseablePair<Index, Index> retVal =
                SafeCloseablePair.of(preShiftBuilder.getIndex(), postShiftBuilder.getIndex());
        Assert.eq(retVal.first.size(), "retVal.first.size()", retVal.second.size(), "retVal.second.size()");
        return retVal;
    }
}
