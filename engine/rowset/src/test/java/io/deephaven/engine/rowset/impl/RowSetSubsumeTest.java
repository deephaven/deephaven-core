//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRangesLong;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRangesShort;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.LogicalClockImpl;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link WritableRowSet#subsume} against {@link WritableRowSet#insert(RowSet)}, which it has to agree with on keys
 * while being free to disagree on which side it edits.
 * <p>
 * The interesting part is the bookkeeping rather than the merge: whichever direction is chosen, the receiver ends up
 * holding the union, the argument ends up empty and still usable, and neither side leaks a reference to an inner set or
 * writes through one that somebody else is reading. The matrix here pairs every representation with every other one at
 * sizes and key positions that make both directions attractive, and runs each pair with each side's inner set shared
 * and unshared, which is what decides whether an edit can be made in place.
 */
public class RowSetSubsumeTest {

    @Rule
    public final EngineCleanup engineCleanup = new EngineCleanup();

    /** How an operand's keys are held, which is what decides the cost of receiving the other side. */
    private enum Rep {
        EMPTY, SINGLE_RANGE, SORTED_RANGES, RSP
    }

    /**
     * A named key set, as a flat array of inclusive {@code [start, end]} pairs, and the representation to hold it in.
     */
    private static final class Operand {
        private final String name;
        private final Rep rep;
        private final long[] ranges;

        private Operand(final String name, final Rep rep, final long[] ranges) {
            this.name = name;
            this.rep = rep;
            this.ranges = ranges;
        }

        /** A fresh row set holding our keys, which the caller owns and must close. */
        private WritableRowSetImpl build() {
            final WritableRowSetImpl rowSet = new WritableRowSetImpl(innerSet());
            // The representation is the point of the operand; a build that quietly produced another one would leave
            // the pair it belongs to measuring something else.
            assertEquals(name, rep, repOf(rowSet.getInnerSet()));
            return rowSet;
        }

        /** Our inner set, registered with {@code owned} so that the test gives its reference back. */
        private OrderedLongSet innerSetOwned(final OwnedSets owned) {
            return owned.take(innerSet());
        }

        private OrderedLongSet innerSet() {
            switch (rep) {
                case EMPTY:
                    return OrderedLongSet.EMPTY;
                case SINGLE_RANGE:
                    assertEquals(name, 2, ranges.length);
                    return SingleRange.make(ranges[0], ranges[1]);
                case SORTED_RANGES: {
                    SortedRanges sr = SortedRanges.makeEmpty();
                    for (int ri = 0; ri < ranges.length; ri += 2) {
                        sr = sr.appendRange(ranges[ri], ranges[ri + 1]);
                        assertNotNull(name + " does not fit in a SortedRanges", sr);
                    }
                    return sr;
                }
                case RSP: {
                    RspBitmap rb = RspBitmap.makeEmpty();
                    for (int ri = 0; ri < ranges.length; ri += 2) {
                        rb = rb.addRangeUnsafe(ranges[ri], ranges[ri + 1]);
                    }
                    rb.finishMutations();
                    return rb;
                }
                default:
                    throw new IllegalStateException("unhandled representation " + rep);
            }
        }
    }

    private static Rep repOf(final OrderedLongSet innerSet) {
        if (innerSet.ixIsEmpty()) {
            return Rep.EMPTY;
        }
        if (innerSet instanceof SingleRange) {
            return Rep.SINGLE_RANGE;
        }
        if (innerSet instanceof SortedRanges) {
            return Rep.SORTED_RANGES;
        }
        return Rep.RSP;
    }

    /** {@code count} single keys {@code stride} apart, as a flat array of inclusive {@code [start, end]} pairs. */
    private static long[] singletons(final long start, final long stride, final int count) {
        final long[] ranges = new long[2 * count];
        for (int ri = 0; ri < count; ++ri) {
            ranges[2 * ri] = ranges[2 * ri + 1] = start + ri * stride;
        }
        return ranges;
    }

    /** {@code count} runs of {@code len} keys, {@code stride} apart. */
    private static long[] runs(final long start, final long stride, final long len, final int count) {
        final long[] ranges = new long[2 * count];
        for (int ri = 0; ri < count; ++ri) {
            ranges[2 * ri] = start + ri * stride;
            ranges[2 * ri + 1] = ranges[2 * ri] + len - 1;
        }
        return ranges;
    }

    /**
     * The operands the matrix pairs. Between them they cover every representation, both orders of "the argument is the
     * bigger side", key sets that lie wholly below or wholly above one another, and key sets that interleave.
     */
    private static final Operand[] OPERANDS = {
            new Operand("empty", Rep.EMPTY, new long[0]),
            new Operand("singleLow", Rep.SINGLE_RANGE, new long[] {0, 99}),
            new Operand("singleHigh", Rep.SINGLE_RANGE, new long[] {10_000_000, 10_000_099}),
            new Operand("singleWide", Rep.SINGLE_RANGE, new long[] {0, 20_000_000}),
            new Operand("srLow", Rep.SORTED_RANGES, singletons(0, 4, 16)),
            new Operand("srHigh", Rep.SORTED_RANGES, singletons(10_000_000, 4, 16)),
            new Operand("srMany", Rep.SORTED_RANGES, singletons(2, 4, 1000)),
            new Operand("srRuns", Rep.SORTED_RANGES, runs(500_000, 1000, 100, 16)),
            new Operand("rspLow", Rep.RSP, singletons(0, BLOCK_SIZE, 16)),
            new Operand("rspHigh", Rep.RSP, singletons(1000L * BLOCK_SIZE, BLOCK_SIZE, 16)),
            new Operand("rspMany", Rep.RSP, singletons(1, BLOCK_SIZE, 2000)),
            new Operand("rspDense", Rep.RSP, runs(0, 2L * BLOCK_SIZE, BLOCK_SIZE, 32)),
    };

    /** The keys of both operands, built without going through any of the code under test. */
    private static WritableRowSet expectedUnion(final Operand left, final Operand right) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        for (final Operand operand : new Operand[] {left, right}) {
            for (int ri = 0; ri < operand.ranges.length; ri += 2) {
                builder.addRange(operand.ranges[ri], operand.ranges[ri + 1]);
            }
        }
        return builder.build();
    }

    /**
     * The inner sets a test builds directly, released together when it is done. {@link Operand#innerSet()} and the
     * {@code ix*} operations hand back owned references, so a test working at that layer has to give each one back the
     * way the row set wrappers do.
     */
    private static final class OwnedSets implements SafeCloseable {

        private final List<OrderedLongSet> owned = new ArrayList<>();

        private OrderedLongSet take(final OrderedLongSet set) {
            owned.add(set);
            return set;
        }

        @Override
        public void close() {
            owned.forEach(OrderedLongSet::ixRelease);
        }
    }

    /**
     * {@code receiver.ixInsert(argument)}, registering the result with {@code owned} only when it is a reference the
     * test does not already hold. An insert the receiver takes in place answers with the receiver itself and acquires
     * nothing; anything else -- a new set, or the argument with a reference of its own taken -- is another reference to
     * give back.
     */
    private static OrderedLongSet insertOwned(final OwnedSets owned, final OrderedLongSet receiver,
            final OrderedLongSet argument) {
        final OrderedLongSet result = receiver.ixInsert(argument);
        if (result != receiver) {
            owned.take(result);
        }
        return result;
    }

    @Test
    public void testRepresentationMatrix() {
        // Both directions have to be exercised; a heuristic that stopped choosing one of them would otherwise leave
        // half of this test measuring the other one twice.
        final boolean[] directionsSeen = new boolean[2];
        for (final Operand left : OPERANDS) {
            for (final Operand right : OPERANDS) {
                for (int variant = 0; variant < 4; ++variant) {
                    checkSubsume(left, right, (variant & 1) != 0, (variant & 2) != 0, directionsSeen);
                }
            }
        }
        assertTrue("some pair subsumes forward", directionsSeen[0]);
        assertTrue("some pair subsumes in reverse", directionsSeen[1]);
    }

    /**
     * Subsume {@code right} into {@code left}, optionally holding a second reference to either side's inner set first,
     * and check the keys, the emptied argument and the references.
     */
    private static void checkSubsume(
            final Operand left,
            final Operand right,
            final boolean shareLeft,
            final boolean shareRight,
            final boolean[] directionsSeen) {
        final String what = left.name + " subsumes " + right.name
                + (shareLeft ? " sharedLeft" : "") + (shareRight ? " sharedRight" : "");
        // The shares outlive the two row sets, so that what closing those gives back can be checked. Nothing else
        // may hold a reference to either inner set while the subsume runs: an extra one would make that side shared,
        // which is another variant's case and not this one's.
        final WritableRowSetImpl leftShare;
        final WritableRowSetImpl rightShare;
        try (final WritableRowSet expected = expectedUnion(left, right);
                final WritableRowSetImpl target = left.build();
                final WritableRowSetImpl source = right.build()) {
            leftShare = shareLeft ? (WritableRowSetImpl) target.copy() : null;
            rightShare = shareRight ? (WritableRowSetImpl) source.copy() : null;
            directionsSeen[InsertCostEstimation.shouldInsertReversed(target.getInnerSet(), source.getInnerSet()) ? 1
                    : 0] =
                            true;

            target.subsume(source);

            target.validate(what);
            source.validate(what);
            assertEquals(what, expected, target);
            assertTrue(what, source.isEmpty());
            if (leftShare != null) {
                // A set somebody else is reading must not have been written through.
                try (final WritableRowSet leftOnly = left.build()) {
                    assertEquals(what, leftOnly, leftShare);
                }
            }
            if (rightShare != null) {
                try (final WritableRowSet rightOnly = right.build()) {
                    assertEquals(what, rightOnly, rightShare);
                }
            }

            // The argument is emptied, not closed: its owner still has to close it, and can still use it.
            source.insert(Long.MAX_VALUE / 2);
            assertEquals(what, 1, source.size());
            source.clear();
        }
        // Closing both sides has to give back every reference each of them took, including the one the insert took
        // to whichever inner set it did not consume, leaving each surviving share the only holder of its own.
        for (final WritableRowSetImpl share : new WritableRowSetImpl[] {leftShare, rightShare}) {
            if (share == null) {
                continue;
            }
            // The shared empty set answers one whatever its holders do, so it says nothing either way.
            if (!share.getInnerSet().ixIsEmpty()) {
                assertEquals(what, 1, share.refCount());
            }
            share.close();
        }
    }

    @Test
    public void testUnsharedReceiverKeepsSoleReference() {
        for (final Operand left : OPERANDS) {
            for (final Operand right : OPERANDS) {
                try (final WritableRowSetImpl target = left.build();
                        final WritableRowSetImpl source = right.build()) {
                    target.subsume(source);
                    if (!target.getInnerSet().ixIsEmpty()) {
                        assertEquals(left.name + " subsumes " + right.name, 1, target.refCount());
                    }
                }
            }
        }
    }

    @Test
    public void testSubsumeSelfThrows() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(10, 20)) {
            try {
                rowSet.subsume(rowSet);
                fail("subsuming a row set into itself");
            } catch (IllegalArgumentException expected) {
                // expected
            }
            // The rejected call left the keys alone.
            assertEquals(11, rowSet.size());
            assertEquals(10, rowSet.firstRowKey());
            assertEquals(20, rowSet.lastRowKey());
        }
    }

    @Test
    public void testSubsumeSharedInnerSet() {
        // Two row sets over one inner set: the union is the keys they both already hold, and the argument still has
        // to end up empty without taking the receiver's keys with it. Only the reference-counted representations can
        // get into that state -- a SingleRange answers ixCowRef() with a copy, so two wrappers over one never share
        // -- so this goes through each of them rather than through RowSetFactory.
        for (final Operand operand : new Operand[] {
                new Operand("sharedRsp", Rep.RSP, singletons(0, BLOCK_SIZE, 16)),
                new Operand("sharedSortedRanges", Rep.SORTED_RANGES, singletons(0, 4, 16))}) {
            try (final WritableRowSetImpl target = operand.build();
                    final WritableRowSetImpl source = (WritableRowSetImpl) target.copy();
                    final WritableRowSet expected = operand.build()) {
                assertSame(operand.name, target.getInnerSet(), source.getInnerSet());
                assertEquals(operand.name, 2, target.refCount());

                target.subsume(source);

                target.validate(operand.name);
                assertEquals(operand.name, expected, target);
                assertTrue(operand.name, source.isEmpty());
                // The shared set was neither copied nor mutated; the receiver simply kept it and the argument gave
                // its reference back.
                assertEquals(operand.name, 1, target.refCount());
            }
        }
    }

    @Test
    public void testDirectionChoicePricesAPackedOverflowAsARebuild() {
        try (final OwnedSets owned = new OwnedSets()) {
            // A SortedRanges that cannot hold the result converts itself to an RspBitmap and inserts into that, so an
            // append whose result outgrows the packed array is a rebuild and not the bulk move it looks like. The keys
            // are a block apart so that the packing is the wide one, whose capacity the sizes here are chosen against.
            final int large = SortedRanges.INT_SPARSE_MAX_CAPACITY - 16;
            final Operand small = new Operand("small", Rep.SORTED_RANGES, singletons(0, BLOCK_SIZE, 64));
            final Operand largeAbove = new Operand("largeAbove", Rep.SORTED_RANGES,
                    singletons(1024L * BLOCK_SIZE, BLOCK_SIZE, large));
            assertTrue(small.innerSetOwned(owned).ixEntryCount()
                    + largeAbove.innerSetOwned(owned).ixEntryCount() > SortedRanges.INT_SPARSE_MAX_CAPACITY);

            // The insert's own answer first: the estimate reconstructs the packing limits, so asserting only the
            // direction would let the two drift apart unnoticed.
            assertTrue(insertOwned(owned, small.innerSetOwned(owned),
                    largeAbove.innerSetOwned(owned)) instanceof RspBitmap);
            // Both directions therefore rebuild, and the small side is the one that should go in entry by entry.
            assertTrue(InsertCostEstimation.shouldInsertReversed(
                    small.innerSetOwned(owned), largeAbove.innerSetOwned(owned)));

            // The same shape with room to spare stays an append, which is what says the capacity is what decided it.
            final Operand fits = new Operand("fits", Rep.SORTED_RANGES,
                    singletons(1024L * BLOCK_SIZE, BLOCK_SIZE, large / 2));
            assertTrue(small.innerSetOwned(owned).ixEntryCount()
                    + fits.innerSetOwned(owned).ixEntryCount() <= SortedRanges.INT_SPARSE_MAX_CAPACITY);
            assertTrue(
                    insertOwned(owned, small.innerSetOwned(owned), fits.innerSetOwned(owned)) instanceof SortedRanges);
            assertFalse(InsertCostEstimation.shouldInsertReversed(
                    small.innerSetOwned(owned), fits.innerSetOwned(owned)));
        }
    }

    @Test
    public void testDirectionChoiceLetsACoveringSingleRangeReceive() {
        try (final OwnedSets owned = new OwnedSets()) {
            // A SingleRange that already covers the other side answers an insert with itself, so it should receive
            // however the sizes compare. Both of these hold one entry, so nothing but the coverage can decide it.
            final OrderedLongSet covering = owned.take(SingleRange.make(0, 1000));
            final OrderedLongSet inside = owned.take(SingleRange.make(100, 100));
            assertTrue(InsertCostEstimation.shouldInsertReversed(inside, covering));
            assertFalse(InsertCostEstimation.shouldInsertReversed(covering, inside));

            // And it holds when the covered side is the one with all the entries, where size alone would say otherwise.
            final OrderedLongSet many =
                    new Operand("many", Rep.RSP, singletons(1, BLOCK_SIZE, 4000)).innerSetOwned(owned);
            final OrderedLongSet coveringMany = owned.take(SingleRange.make(0, 4000L * BLOCK_SIZE));
            assertTrue(InsertCostEstimation.shouldInsertReversed(many, coveringMany));
            assertFalse(InsertCostEstimation.shouldInsertReversed(coveringMany, many));

            // Overlapping without covering is not the same thing, and goes back to the estimate.
            final OrderedLongSet overlapping = owned.take(SingleRange.make(100, 2L * BLOCK_SIZE));
            assertFalse(InsertCostEstimation.shouldInsertReversed(many, overlapping));
        }
    }

    @Test
    public void testDirectionChoiceUsesTheReceiversOwnPackedWidth() {
        try (final OwnedSets owned = new OwnedSets()) {
            // A short-packed array is the one width that does not repack on the way up, and its offsets only reach
            // Short.MAX_VALUE from their base. Past either bound the insert gives up and answers with an RspBitmap,
            // well inside what an int-packed array would have held, and the estimate has to know that. Every probe
            // builds its operands afresh: an insert the receiver can take edits it in place, which would leave the
            // next probe a different question.
            final Operand narrow = new Operand("narrow", Rep.SORTED_RANGES, singletons(256, 2, 1000));
            assertTrue("expected a short-packed receiver", narrow.innerSetOwned(owned) instanceof SortedRangesShort);

            // Inside both bounds the packed array holds the result, so the append is worth taking.
            final Operand fits = new Operand("fits", Rep.SORTED_RANGES, singletons(256 + 2L * 4096, 2, 1400));
            assertTrue(
                    insertOwned(owned, narrow.innerSetOwned(owned), fits.innerSetOwned(owned)) instanceof SortedRanges);
            assertFalse(
                    InsertCostEstimation.shouldInsertReversed(narrow.innerSetOwned(owned), fits.innerSetOwned(owned)));

            // The same entry counts, moved past what short offsets reach: the insert rebuilds, and the direction with
            // it. Only the keys differ between this and the case above, so reach is what decided it.
            final Operand pastReach = new Operand("pastReach", Rep.SORTED_RANGES,
                    singletons(256 + 4L * Short.MAX_VALUE, 2, 1400));
            assertTrue(insertOwned(owned, narrow.innerSetOwned(owned),
                    pastReach.innerSetOwned(owned)) instanceof RspBitmap);
            assertTrue(InsertCostEstimation.shouldInsertReversed(narrow.innerSetOwned(owned),
                    pastReach.innerSetOwned(owned)));

            // And too many entries for the width rebuilds just the same, with the keys left in reach.
            final Operand pastCapacity =
                    new Operand("pastCapacity", Rep.SORTED_RANGES, singletons(256 + 2L * 4096, 2, 3200));
            assertTrue(insertOwned(owned, narrow.innerSetOwned(owned),
                    pastCapacity.innerSetOwned(owned)) instanceof RspBitmap);
            assertTrue(
                    InsertCostEstimation.shouldInsertReversed(narrow.innerSetOwned(owned),
                            pastCapacity.innerSetOwned(owned)));
        }
    }

    @Test
    public void testDirectionChoicePricesAWideLongPackedOverflowAsARebuild() {
        try (final OwnedSets owned = new OwnedSets()) {
            // A long-packed array can repack into a narrower width as it grows, but only while the result still fits
            // the narrower offsets. Over a span past what an int reaches it stays long-packed, and its own capacity is
            // the lower of the two, so a result between the two bounds rebuilds where a narrower span would not have.
            final long wideStride = 1L << 21;
            final int half = SortedRanges.LONG_SPARSE_MAX_CAPACITY - 1096;
            final Operand low = new Operand("low", Rep.SORTED_RANGES, singletons(0, wideStride, half));
            final Operand high =
                    new Operand("high", Rep.SORTED_RANGES, singletons((long) half * wideStride, wideStride, half));
            assertTrue(low.innerSetOwned(owned) instanceof SortedRangesLong);
            assertTrue(2L * half > SortedRanges.LONG_SPARSE_MAX_CAPACITY);
            assertTrue(2L * half < SortedRanges.INT_SPARSE_MAX_CAPACITY);
            assertTrue(
                    high.innerSetOwned(owned).ixLastKey() - low.innerSetOwned(owned).ixFirstKey() > Integer.MAX_VALUE);

            // The insert gives up on the packed array, so appending onto the low side is not the bulk move it looks
            // like; with both directions rebuilding, the smaller side is the one that should go in entry by entry.
            assertTrue(insertOwned(owned, low.innerSetOwned(owned), high.innerSetOwned(owned)) instanceof RspBitmap);
            assertFalse(InsertCostEstimation.shouldInsertReversed(low.innerSetOwned(owned), high.innerSetOwned(owned)));
            assertFalse(InsertCostEstimation.shouldInsertReversed(high.innerSetOwned(owned), low.innerSetOwned(owned)));
        }
    }

    @Test
    public void testSubsumeRejectsUnmodifiableViews() {
        try (final TrackingWritableRowSet tracking = RowSetFactory.fromRange(0, 9).toTracking();
                final WritableRowSet other = RowSetFactory.fromRange(20, 29)) {
            // prev() hands back an unmodifiable view, which rejects every mutator through its hooks; subsume mutates
            // both sides, so it has to be rejected from either position.
            final WritableRowSet prev = (WritableRowSet) tracking.prev();
            try {
                prev.subsume(other);
                fail("subsuming into an unmodifiable view");
            } catch (UnsupportedOperationException expected) {
                // expected
            }
            try {
                other.subsume(prev);
                fail("subsuming an unmodifiable view");
            } catch (UnsupportedOperationException expected) {
                // expected
            }
            // The rejected calls must not have moved any keys.
            assertEquals(10, tracking.size());
            assertEquals(10, other.size());
        }
    }

    @Test
    public void testSubsumeIntoTracking() {
        final LogicalClockImpl clock = (LogicalClockImpl) ExecutionContext.getContext().getUpdateGraph().clock();
        try (final TrackingWritableRowSet tracking = RowSetFactory.fromRange(0, 9).toTracking()) {
            clock.startUpdateCycle();
            try (final WritableRowSet added = RowSetFactory.fromRange(20, 29)) {
                tracking.subsume(added);
                assertTrue(added.isEmpty());
            }
            assertEquals(20, tracking.size());
            assertEquals(10, tracking.sizePrev());
            assertEquals(9, tracking.lastRowKeyPrev());
            clock.completeUpdateCycle();
        }
    }

    @Test
    public void testSubsumeFromTracking() {
        final LogicalClockImpl clock = (LogicalClockImpl) ExecutionContext.getContext().getUpdateGraph().clock();
        try (final TrackingWritableRowSet tracking = RowSetFactory.fromRange(0, 9).toTracking();
                final WritableRowSet target = RowSetFactory.fromRange(20, 29)) {
            clock.startUpdateCycle();
            target.subsume(tracking);
            assertTrue(tracking.isEmpty());
            // The emptied set keeps the previous value it snapshotted on the way in, and the keys went to the target
            // rather than being shared with it.
            assertEquals(10, tracking.sizePrev());
            assertEquals(20, target.size());
            clock.completeUpdateCycle();
        }
    }

    @Test
    public void testSubsumeEmptiesArgumentOfEveryRepresentation() {
        for (final Operand operand : OPERANDS) {
            try (final WritableRowSet target = RowSetFactory.empty();
                    final WritableRowSetImpl source = operand.build()) {
                target.subsume(source);
                assertTrue(operand.name, source.isEmpty());
                assertSame(operand.name, OrderedLongSet.EMPTY, source.getInnerSet());
                try (final WritableRowSet expected = expectedUnion(operand, operand)) {
                    assertEquals(operand.name, expected, target);
                }
            }
        }
    }

    @Test
    public void testAgreesWithInsertOnRandomSets() {
        final Random random = new Random(0x5EED);
        for (int trial = 0; trial < 500; ++trial) {
            try (final WritableRowSet target = randomRowSet(random);
                    final WritableRowSet source = randomRowSet(random);
                    final WritableRowSet inserted = target.copy()) {
                inserted.insert(source);
                target.subsume(source);
                target.validate("trial " + trial);
                assertEquals("trial " + trial, inserted, target);
                assertTrue("trial " + trial, source.isEmpty());
            }
        }
    }

    private static WritableRowSet randomRowSet(final Random random) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        // A span of key space wide enough that some pairs land in the same RSP block and others do not, and a range
        // count that reaches past what a SortedRanges holds often enough to cover both representations.
        final long origin = random.nextInt(4) * 4L * BLOCK_SIZE;
        final int ranges = random.nextInt(3000);
        for (int ri = 0; ri < ranges; ++ri) {
            final long start = origin + (long) random.nextInt(8 * BLOCK_SIZE);
            builder.addRange(start, start + random.nextInt(16));
        }
        return builder.build();
    }

    @Test
    public void testDirectionChoicePrefersTheCheaperSide() {
        try (final OwnedSets owned = new OwnedSets()) {
            // A small set going into a large one of the same representation: the large one receives, in place.
            final OrderedLongSet small =
                    new Operand("small", Rep.RSP, singletons(1, BLOCK_SIZE, 4)).innerSetOwned(owned);
            final OrderedLongSet large =
                    new Operand("large", Rep.RSP, singletons(3, BLOCK_SIZE, 4000)).innerSetOwned(owned);
            assertFalse(InsertCostEstimation.shouldInsertReversed(large, small));
            assertTrue(InsertCostEstimation.shouldInsertReversed(small, large));

            // A single range never receives when the other side holds more than one: it can only answer with a copy
            // of the other side, where the other side can take one range in place.
            final OrderedLongSet single = owned.take(SingleRange.make(BLOCK_SIZE / 2, BLOCK_SIZE / 2 + 1));
            assertTrue(InsertCostEstimation.shouldInsertReversed(single, large));
            assertFalse(InsertCostEstimation.shouldInsertReversed(large, single));

            // An empty side is never worth reversing for: the forward insert answers with the other side as it is.
            assertFalse(InsertCostEstimation.shouldInsertReversed(OrderedLongSet.EMPTY, large));
            assertFalse(InsertCostEstimation.shouldInsertReversed(large, OrderedLongSet.EMPTY));

            // A large set that lies entirely above a small one does not get to be an append just because the keys
            // line up: appending is paid per entry of the argument, so putting 4000 spans onto 4 loses to placing
            // those 4 among the 4000. Turned around, the same pair prepends, and that is a move of the receiver
            // plus four placements, which is cheaper still -- so neither order reverses.
            final OrderedLongSet largeAbove = new Operand("largeAbove", Rep.RSP,
                    singletons(5000L * BLOCK_SIZE, BLOCK_SIZE, 4000)).innerSetOwned(owned);
            assertTrue(InsertCostEstimation.shouldInsertReversed(small, largeAbove));
            assertFalse(InsertCostEstimation.shouldInsertReversed(largeAbove, small));

            // A shared receiver has to copy itself before it can take an append, and that still beats making the
            // small side receive the large one.
            final OrderedLongSet smallAbove = new Operand("smallAbove", Rep.RSP,
                    singletons(5000L * BLOCK_SIZE, BLOCK_SIZE, 4)).innerSetOwned(owned);
            final OrderedLongSet largeShared = owned.take(large.ixCowRef());
            assertFalse(InsertCostEstimation.shouldInsertReversed(largeShared, smallAbove));
        }
    }
}
