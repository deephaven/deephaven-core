//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRangesInt;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRangesShort;

/**
 * Which of two {@link OrderedLongSet}s should receive an insert of the other, for
 * {@link io.deephaven.engine.rowset.WritableRowSet#absorb absorb}, which unlike an insert may edit either side.
 * <p>
 * Both directions produce the same keys, so this only chooses which set is edited in place and which is read. It is a
 * cost estimate and nothing more: getting it wrong loses some of the saving, never the answer. Everything it reads is
 * O(1), because the whole point is to spend less than the difference it is choosing between.
 */
final class InsertCostEstimation {

    private InsertCostEstimation() {}

    /**
     * Relative costs, per entry, of the ways a set's entries reach the result of an insert, in units of the cheapest of
     * them: the receiver's own entries, moved once to grow its storage or to open room among them.
     * <p>
     * An entry appended onto the end costs more than that move and much less than a placement, and how much more
     * depends on what is appended: an {@link RspBitmap}'s spans go over one reference at a time with each one's
     * container marked shared, where a {@link SortedRanges}' packed entries are an array copy. An entry placed among
     * the receiver's existing entries pays a search and the per-entry bookkeeping its representation keeps, and for a
     * span a container merge. An entry carried into a set built for the answer, which is what happens when the receiver
     * cannot be written to as it stands, falls between the two and is paid for both sides rather than one.
     * <p>
     * These ratios are measured rather than assumed: {@code RowSetAbsorbBench} times both directions of each shape it
     * covers, and no other set of weights agrees with all of them.
     */
    private static final int MOVED_ENTRY_COST = 1;
    private static final int APPENDED_RANGE_COST = 1;
    private static final int APPENDED_SPAN_COST = 2;
    private static final int PLACED_ENTRY_COST = 8;
    private static final int REBUILT_ENTRY_COST = 4;

    /**
     * How much of another representation a set can take without being rebuilt in that representation: an
     * {@link RspBitmap} takes ranges from any of them, a {@link SortedRanges} holds only what fits in its packed array,
     * and a {@link SingleRange} holds one range and can only answer with a copy of the other side.
     */
    private static final int SPAN_RANK = 2;
    private static final int PACKED_RANK = 1;
    private static final int SINGLE_RANK = 0;

    private static int representationRank(final OrderedLongSet set) {
        if (set instanceof RspBitmap) {
            return SPAN_RANK;
        }
        return set instanceof SortedRanges ? PACKED_RANK : SINGLE_RANK;
    }

    /**
     * An estimate, in {@link #MOVED_ENTRY_COST} units, of inserting an argument of {@code argumentEntries} entries into
     * a receiver of {@code receiverEntries}.
     *
     * @param argumentRank The argument's {@link #representationRank}, which sets what appending its entries costs
     * @param append Whether every key of the argument follows the receiver's last key
     * @param inPlace Whether the receiver can be written to as it stands
     */
    private static long insertCost(final long receiverEntries, final long argumentEntries, final int argumentRank,
            final boolean append, final boolean inPlace) {
        if (!inPlace) {
            // The receiver is shared, or cannot hold a result that includes the argument's keys. Either way both
            // sides' entries are carried into a new set.
            return REBUILT_ENTRY_COST * (receiverEntries + argumentEntries);
        }
        final long perArgumentEntry;
        if (!append) {
            perArgumentEntry = PLACED_ENTRY_COST;
        } else {
            perArgumentEntry = argumentRank == SPAN_RANK ? APPENDED_SPAN_COST : APPENDED_RANGE_COST;
        }
        return MOVED_ENTRY_COST * receiverEntries + perArgumentEntry * argumentEntries;
    }

    /**
     * Whether {@code receiver}, a {@link SortedRanges}, can hold a result of {@code receiverEntries} plus
     * {@code argumentEntries} entries spanning {@code keySpan}, rather than giving up and rebuilding as an
     * {@link RspBitmap}.
     * <p>
     * Its packed array stores offsets from a base, and the width it stores them at bounds both how many entries fit and
     * how far from that base they reach. A short-packed array is the one width that will not repack on the way up; an
     * int or long packed array repacks into whichever width fits, so it reaches the widest entry count, and a
     * long-packed one reaches across the whole key space besides.
     * <p>
     * Within a width the dense bound is smaller than the sparse one taken here, so a dense set can still be rebuilt
     * where this says it fits. The reach is likewise measured from the receiver's first key rather than from the base
     * its offsets are actually stored against, which removing leading keys can leave below that first key. Neither is
     * worth chasing: both err the same way, scoring a rebuild as an append and losing part of the saving, and neither
     * can pick a direction that produces the wrong keys. What this rules out is the case the width alone already
     * settles.
     */
    private static boolean packedHoldsResult(final OrderedLongSet receiver, final long receiverEntries,
            final long argumentEntries, final long keySpan) {
        final long entryCapacity;
        final long reach;
        if (receiver instanceof SortedRangesShort) {
            entryCapacity = SortedRanges.SHORT_MAX_CAPACITY;
            reach = Short.MAX_VALUE;
        } else if (receiver instanceof SortedRangesInt) {
            entryCapacity = SortedRanges.INT_SPARSE_MAX_CAPACITY;
            reach = Integer.MAX_VALUE;
        } else {
            // A long-packed array repacks into whichever narrower width fits as it grows, so it reaches the wider
            // entry bound while the result still fits int offsets, and only its own once it does not. Its offsets
            // span the whole key space either way.
            entryCapacity = keySpan <= Integer.MAX_VALUE
                    ? SortedRanges.INT_SPARSE_MAX_CAPACITY
                    : SortedRanges.LONG_SPARSE_MAX_CAPACITY;
            reach = Long.MAX_VALUE;
        }
        return receiverEntries + argumentEntries <= entryCapacity && keySpan <= reach;
    }

    /**
     * Whether {@code receiver} can take the argument's entries without a set being built for the answer: nobody else is
     * reading it, it can hold the argument's representation, and, when it is a {@link SortedRanges}, the result still
     * fits its packed array.
     */
    private static boolean writesInPlace(final OrderedLongSet receiver, final int receiverRank,
            final long receiverEntries, final int argumentRank, final long argumentEntries, final long keySpan) {
        if (receiverRank < argumentRank || receiver.ixRefCount() > 1) {
            return false;
        }
        return receiverRank != PACKED_RANK
                || packedHoldsResult(receiver, receiverEntries, argumentEntries, keySpan);
    }

    /**
     * Whether an absorb of {@code theirs} into {@code mine} should be run the other way around, with {@code theirs}
     * receiving {@code mine}.
     * <p>
     * An append is not on its own a reason to choose a direction. Appending is cheap per entry, but it is paid for
     * every entry of the argument, where the other direction pays only to move the receiver's own; a large set appended
     * onto a small one loses to placing the small one's entries into the large one.
     */
    static boolean shouldInsertReversed(final OrderedLongSet mine, final OrderedLongSet theirs) {
        final long mineEntries = mine.ixEntryCount();
        final long theirsEntries = theirs.ixEntryCount();
        if (mineEntries == 0 || theirsEntries == 0) {
            // One side holds no keys: the forward insert answers with whichever set holds them, without copying it.
            return false;
        }
        final long mineFirst = mine.ixFirstKey();
        final long mineLast = mine.ixLastKey();
        final long theirsFirst = theirs.ixFirstKey();
        final long theirsLast = theirs.ixLastKey();
        // A SingleRange holds one contiguous range, so one covering the other side's first and last key covers
        // every key it holds. Inserting into it then answers with the set itself and touches nothing, which no
        // estimate is going to beat.
        if (mine instanceof SingleRange && mineFirst <= theirsFirst && theirsLast <= mineLast) {
            return false;
        }
        if (theirs instanceof SingleRange && theirsFirst <= mineFirst && mineLast <= theirsLast) {
            return true;
        }
        final int mineRank = representationRank(mine);
        final int theirsRank = representationRank(theirs);
        // What the result has to span either way, which is what decides how widely a packed array would have to pack.
        final long keySpan = Math.max(mineLast, theirsLast) - Math.min(mineFirst, theirsFirst);
        final long forwardCost = insertCost(mineEntries, theirsEntries, theirsRank, mineLast < theirsFirst,
                writesInPlace(mine, mineRank, mineEntries, theirsRank, theirsEntries, keySpan));
        final long reversedCost = insertCost(theirsEntries, mineEntries, mineRank, theirsLast < mineFirst,
                writesInPlace(theirs, theirsRank, theirsEntries, mineRank, mineEntries, keySpan));
        if (reversedCost != forwardCost) {
            return reversedCost < forwardCost;
        }
        // Equal estimates mean neither side can be written to as it stands, so both directions build a set for the
        // answer out of the same entries. What is left to choose on is which side goes in one entry at a time, and
        // that should be the smaller one.
        return mineEntries < theirsEntries;
    }
}
