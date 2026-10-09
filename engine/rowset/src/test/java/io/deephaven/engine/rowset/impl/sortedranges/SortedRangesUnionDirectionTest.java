//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.util.SafeCloseable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Which side {@link SortedRanges#ixUnionOnNew} builds its answer from.
 * <p>
 * A union allocates whatever it returns, so the only thing to choose is what it allocates and how much of the work is
 * per entry. Both directions answer with the same keys, which is why the choice itself is asserted through
 * {@link SortedRanges#unionBuildsFromOther} rather than inferred from the result: a union that always built from one
 * side would satisfy every assertion about the keys.
 */
public class SortedRangesUnionDirectionTest {

    /** The sets a test owns directly, released together when it is done. */
    private static final class OwnedSets implements SafeCloseable {

        private final List<OrderedLongSet> owned = new ArrayList<>();

        private <T extends OrderedLongSet> T take(final T set) {
            owned.add(set);
            return set;
        }

        @Override
        public void close() {
            owned.forEach(OrderedLongSet::ixRelease);
        }
    }

    /** {@code count} single keys {@code stride} apart, which must fit a packed array. */
    private static SortedRanges singles(final OwnedSets owned, final long start, final long stride, final int count) {
        SortedRanges sr = SortedRanges.makeEmpty();
        for (int ri = 0; ri < count && sr != null; ++ri) {
            final long key = start + ri * stride;
            sr = sr.appendRange(key, key);
        }
        assertNotNull("set of " + count + " keys does not fit a packed array", sr);
        return owned.take(sr);
    }

    private static RspBitmap spans(final OwnedSets owned, final long start, final long stride, final int count) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int ri = 0; ri < count; ++ri) {
            final long key = start + ri * stride;
            rb = rb.addRangeUnsafe(key, key);
        }
        rb.finishMutations();
        return owned.take(rb);
    }

    @Test
    public void testACoveringSingleRangeIsTheUnion() {
        try (final OwnedSets owned = new OwnedSets()) {
            final SortedRanges sr = singles(owned, BLOCK_SIZE, BLOCK_SIZE, 64);
            final OrderedLongSet covering = owned.take(SingleRange.make(0, 65L * BLOCK_SIZE));

            final OrderedLongSet union = owned.take(sr.ixUnionOnNew(covering));
            // The covering range is the answer, reached without copying the packed array to add the range to it.
            assertTrue(union instanceof SingleRange);
            assertEquals(covering.ixFirstKey(), union.ixFirstKey());
            assertEquals(covering.ixLastKey(), union.ixLastKey());
            assertEquals(covering.ixCardinality(), union.ixCardinality());
            // Neither side is changed by asking.
            assertEquals(64, sr.ixCardinality());
        }
    }

    @Test
    public void testASingleRangeThatCoversOnlyOneEndStillMerges() {
        // The two ways the covering test can fail, one per side of the condition.
        try (final OwnedSets owned = new OwnedSets()) {
            final SortedRanges sr = singles(owned, 10L * BLOCK_SIZE, BLOCK_SIZE, 64);

            // Starts above our first key: covers our last, not our first.
            final OrderedLongSet high = owned.take(SingleRange.make(20L * BLOCK_SIZE, 100L * BLOCK_SIZE));
            final OrderedLongSet withHigh = owned.take(sr.ixUnionOnNew(high));
            assertFalse(withHigh instanceof SingleRange);
            assertEquals(sr.ixFirstKey(), withHigh.ixFirstKey());
            assertEquals(high.ixLastKey(), withHigh.ixLastKey());

            // Ends below our last key: covers our first, not our last.
            final OrderedLongSet low = owned.take(SingleRange.make(0, 20L * BLOCK_SIZE));
            final OrderedLongSet withLow = owned.take(sr.ixUnionOnNew(low));
            assertFalse(withLow instanceof SingleRange);
            assertEquals(0, withLow.ixFirstKey());
            assertEquals(sr.ixLastKey(), withLow.ixLastKey());
        }
    }

    @Test
    public void testTheLargerPackedSideIsBuiltFrom() {
        // Two packed sets whose union outgrows the packed array. The direction is asserted directly, since both
        // orders answer with the same keys.
        try (final OwnedSets owned = new OwnedSets()) {
            final SortedRanges few = singles(owned, 0, 2L * BLOCK_SIZE, 64);
            final SortedRanges many =
                    singles(owned, 128L * BLOCK_SIZE, 2L * BLOCK_SIZE, SortedRanges.INT_SPARSE_MAX_CAPACITY - 1);
            assertTrue(few.count() + many.count() > SortedRanges.INT_SPARSE_MAX_CAPACITY);

            assertTrue("the smaller side should build from the larger", few.unionBuildsFromOther(many));
            assertFalse("the larger side should build from itself", many.unionBuildsFromOther(few));

            final OrderedLongSet fewFirst = owned.take(few.ixUnionOnNew(many));
            final OrderedLongSet manyFirst = owned.take(many.ixUnionOnNew(few));
            assertTrue(fewFirst instanceof RspBitmap);
            assertTrue(manyFirst instanceof RspBitmap);
            assertEquals(few.ixCardinality() + many.ixCardinality(), fewFirst.ixCardinality());
            assertEquals(fewFirst.ixCardinality(), manyFirst.ixCardinality());
            assertEquals(fewFirst.ixFirstKey(), manyFirst.ixFirstKey());
            assertEquals(fewFirst.ixLastKey(), manyFirst.ixLastKey());
        }
    }

    @Test
    public void testTheLargerSideIsBuiltFromAgainstABitmap() {
        // The same choice where the other side is already spans, which is the shape that reaches unionRspOnNew.
        try (final OwnedSets owned = new OwnedSets()) {
            final SortedRanges few = singles(owned, 0, 2L * BLOCK_SIZE, 16);
            final RspBitmap manySpans = spans(owned, 128L * BLOCK_SIZE, 2L * BLOCK_SIZE, 4096);
            assertTrue(manySpans.ixEntryCount() > few.count());
            assertTrue("a bitmap with more spans should be built from", few.unionBuildsFromOther(manySpans));

            final OrderedLongSet union = owned.take(few.ixUnionOnNew(manySpans));
            assertTrue(union instanceof RspBitmap);
            assertEquals(few.ixCardinality() + manySpans.ixCardinality(), union.ixCardinality());
            assertEquals(few.ixFirstKey(), union.ixFirstKey());
            assertEquals(manySpans.ixLastKey(), union.ixLastKey());

            // And the other way, where we are the side with more entries and the bitmap is the small one.
            final RspBitmap fewSpans = spans(owned, 1_000_000L * BLOCK_SIZE, 2L * BLOCK_SIZE, 4);
            final SortedRanges manyRanges = singles(owned, 0, 2L * BLOCK_SIZE, 1024);
            assertFalse("a bitmap with fewer spans should not be built from",
                    manyRanges.unionBuildsFromOther(fewSpans));

            final OrderedLongSet other = owned.take(manyRanges.ixUnionOnNew(fewSpans));
            assertTrue(other instanceof RspBitmap);
            assertEquals(manyRanges.ixCardinality() + fewSpans.ixCardinality(), other.ixCardinality());
            assertEquals(manyRanges.ixFirstKey(), other.ixFirstKey());
            assertEquals(fewSpans.ixLastKey(), other.ixLastKey());
        }
    }
}
