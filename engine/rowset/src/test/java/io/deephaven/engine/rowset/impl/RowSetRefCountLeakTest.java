//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.LogicalClockImpl;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A reference held on a rowset and never given back leaves it permanently shared, so every later mutation copies it,
 * and on a live table the count climbs without bound. Repeating each operation and watching the count is what
 * distinguishes a reference that is returned from one that is not.
 *
 * <p>
 * SingleRange cannot show any of this: it hands out copies rather than references and reports a constant count of one.
 * Every watched set below is therefore an RspBitmap or a SortedRanges.
 */
public class RowSetRefCountLeakTest {
    @Rule
    final public EngineCleanup engineCleanup = new EngineCleanup();

    private static final int REPETITIONS = 20;

    /**
     * Enough spans that an operation stopping early leaves some unread. An iterator releases the reference it holds
     * once its cursor runs off the end of the spans, so a walk that happens to finish cannot show a leak.
     */
    private static RspBitmap manyBlockRsp(final long firstStart) {
        final RspBitmap rsp = RspBitmap.makeSingleRange(firstStart, firstStart + 4);
        for (int i = 2; i < 10; ++i) {
            rsp.addRangeUnsafeNoWriteCheck(i * BLOCK_SIZE, i * BLOCK_SIZE + 5);
        }
        rsp.finishMutations();
        return rsp;
    }

    private static SortedRanges manyRangeSortedRanges(final long gapStart) {
        SortedRanges sr = SortedRanges.makeSingleRange(0, 100);
        for (int i = 3; i < 12; ++i) {
            sr = sr.addRange(i * BLOCK_SIZE + gapStart, i * BLOCK_SIZE + gapStart + 50);
        }
        return sr;
    }

    private static void assertHoldsSteady(final String what, final OrderedLongSet watched, final Runnable op) {
        final int steadyState = watched.ixRefCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            op.run();
        }
        assertEquals(what + " after " + REPETITIONS + " operations", steadyState, watched.ixRefCount());
    }

    private static WritableRowSetImpl rowSetOf(final OrderedLongSet innerSet) {
        return new WritableRowSetImpl(innerSet);
    }

    // 3.1 -- adopting an inner set we already hold must not acquire a reference that is then never released.

    @Test
    public void testRepeatedResetToTheSameSourceHoldsSteady() {
        for (final OrderedLongSet inner : new OrderedLongSet[] {manyBlockRsp(5), manyRangeSortedRanges(0)}) {
            try (final WritableRowSetImpl source = rowSetOf(inner);
                    final WritableRowSetImpl target = rowSetOf(OrderedLongSet.EMPTY)) {
                target.resetTo(source);
                assertHoldsSteady(inner.getClass().getSimpleName() + " reset to", inner,
                        () -> target.resetTo(source));
            }
        }
    }

    @Test
    public void testUpdateThatRemovesOurselvesHoldsSteady() {
        final RspBitmap inner = manyBlockRsp(5);
        try (final WritableRowSetImpl added = rowSetOf(inner);
                final WritableRowSetImpl target = rowSetOf(OrderedLongSet.EMPTY)) {
            // Sharing an inner set with added is what makes the update below adopt one we already hold.
            target.resetTo(added);
            assertHoldsSteady("update removing ourselves", inner, () -> target.update(added, target));
        }
    }

    @Test
    public void testIdleUpdateCyclesHoldSteady() {
        final RspBitmap inner = manyBlockRsp(5);
        final TrackingWritableRowSet rowSet = rowSetOf(inner).toTracking();
        final LogicalClockImpl clock = (LogicalClockImpl) ExecutionContext.getContext().getUpdateGraph().clock();
        // One full cycle first, so that prev has caught up to current and the count has settled.
        clock.startUpdateCycle();
        rowSet.sizePrev();
        clock.completeUpdateCycle();
        rowSet.sizePrev();
        assertHoldsSteady("idle update cycles", inner, () -> {
            clock.startUpdateCycle();
            rowSet.sizePrev();
            clock.completeUpdateCycle();
            rowSet.sizePrev();
        });
    }

    // 3.2 -- comparing rowsets walks both with iterators that are abandoned at the first difference.

    @Test
    public void testComparingRowSetsThatDifferDoesNotRetainThem() {
        final RspBitmap left = manyBlockRsp(5);
        final RspBitmap right = manyBlockRsp(6);
        try (final WritableRowSetImpl a = rowSetOf(left);
                final WritableRowSetImpl b = rowSetOf(right)) {
            assertEquals("equal cardinalities, so the walk gets past that check", a.size(), b.size());
            assertHoldsSteady("unequal comparison, left", left, () -> assertFalse(a.equals(b)));
            assertHoldsSteady("unequal comparison, right", right, () -> assertFalse(a.equals(b)));
            // A walk that reaches the end releases on its own; keep that path pinned too.
            try (final WritableRowSet sameAsA = a.copy()) {
                assertHoldsSteady("equal comparison", left, () -> assertTrue(a.equals(sameAsA)));
            }
        }
    }

    @Test
    public void testComparingBitmapsThatDifferDoesNotRetainThem() {
        // RspBitmap.equals is a separate walk from the one RowSet.equals uses.
        final RspBitmap left = manyBlockRsp(5);
        final RspBitmap right = manyBlockRsp(6);
        assertEquals("equal cardinalities", left.getCardinality(), right.getCardinality());
        assertHoldsSteady("unequal bitmap comparison", right, () -> assertFalse(left.equals(right)));
    }

    // 3.3 -- shifting by zero hands back a reference to the set being shifted rather than a copy of it.

    @Test
    public void testUnshiftedInsertDoesNotRetainTheInsertedSet() {
        final RspBitmap bitmap = manyBlockRsp(1005);
        assertHoldsSteady("unshifted insert of a bitmap", bitmap,
                () -> SortedRanges.makeSingleRange(5, 7).ixInsertWithShift(0, bitmap));
        final SortedRanges sortedRanges = manyRangeSortedRanges(0);
        assertHoldsSteady("unshifted insert of sorted ranges", sortedRanges,
                () -> SortedRanges.makeSingleRange(5, 7).ixInsertWithShift(0, sortedRanges));
        // A real shift copies, so the reference asked for is the one used; keep that path pinned.
        assertHoldsSteady("shifted insert", bitmap,
                () -> SortedRanges.makeSingleRange(5, 7).ixInsertWithShift(11, bitmap));
    }

    /**
     * Taking a subrange that covers the whole set hands back a reference to the set itself rather than a copy; if the
     * result is then compacted onto a different implementation, that reference has to be given back.
     */
    @Test
    public void testWholeSetSubrangeThatCompactsDoesNotRetainTheSource() {
        // One span holding one contiguous range, so compacting turns it into a SingleRange.
        final RspBitmap inner = RspBitmap.makeSingleRange(5, 9);
        try (final WritableRowSetImpl rs = rowSetOf(inner)) {
            assertHoldsSteady("whole-set key range subset", inner, () -> {
                try (final RowSet sub = rs.subSetByKeyRange(rs.firstRowKey(), rs.lastRowKey())) {
                    assertEquals("the subset is the whole set", rs.size(), sub.size());
                }
            });
            assertHoldsSteady("whole-set position range subset", inner, () -> {
                try (final RowSet sub = rs.subSetByPositionRange(0, rs.size())) {
                    assertEquals("the subset is the whole set", rs.size(), sub.size());
                }
            });
        }
        // A bitmap that cannot compact keeps the reference it handed out, which is correct; pin that too.
        final RspBitmap wide = manyBlockRsp(5);
        try (final WritableRowSetImpl rs = rowSetOf(wide)) {
            assertHoldsSteady("whole-set subset of a bitmap that cannot compact", wide, () -> {
                try (final RowSet sub = rs.subSetByKeyRange(rs.firstRowKey(), rs.lastRowKey())) {
                    assertEquals("the subset is the whole set", rs.size(), sub.size());
                }
            });
        }
    }

    // 3.4 -- logging stops after a couple hundred ranges, abandoning the iterator there.

    @Test
    public void testTruncatedLoggingDoesNotRetainTheRowSet() {
        final RspBitmap inner = RspBitmap.makeSingleRange(0, 0);
        for (int i = 1; i < 500; ++i) {
            inner.addRangeUnsafeNoWriteCheck(4L * i, 4L * i + 1);
        }
        inner.finishMutations();
        try (final WritableRowSetImpl rowSet = rowSetOf(inner)) {
            assertHoldsSteady("truncated logging", inner, () -> {
                final LogOutputStringImpl logOutput = new LogOutputStringImpl();
                rowSet.append(logOutput);
                assertTrue("logging was truncated", logOutput.toString().contains("..."));
            });
        }
    }

    // 3.5 -- query paths that answer before running out of ranges.

    @Test
    public void testOverlapDoesNotRetainEitherSide() {
        // The iterator comes off the receiver in one direction and off the argument in the other.
        final RspBitmap bitmap = manyBlockRsp(5);
        final SortedRanges sortedRanges = manyRangeSortedRanges(0);
        assertHoldsSteady("bitmap overlaps sorted ranges, watching the receiver", bitmap,
                () -> assertTrue(bitmap.ixOverlaps(sortedRanges)));
        assertHoldsSteady("sorted ranges overlaps bitmap, watching the argument", bitmap,
                () -> assertTrue(sortedRanges.ixOverlaps(bitmap)));
    }

    @Test
    public void testDecidingNotASubsetDoesNotRetainTheOtherSet() {
        // Within other's first and last keys, but with a key sitting in one of its early gaps.
        final SortedRanges other = manyRangeSortedRanges(0);
        final RspBitmap subject = RspBitmap.makeSingleRange(5, 9);
        subject.addRangeUnsafeNoWriteCheck(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 1);
        subject.addRangeUnsafeNoWriteCheck(11 * BLOCK_SIZE, 11 * BLOCK_SIZE + 1);
        subject.finishMutations();
        assertHoldsSteady("subset test", other, () -> assertFalse(subject.ixSubsetOf(other)));
    }

    @Test
    public void testInvertTruncatedByMaxPositionDoesNotRetainTheKeys() {
        final RspBitmap keys = manyBlockRsp(5);
        assertHoldsSteady("single range invert", keys,
                () -> SingleRange.make(0, 12 * BLOCK_SIZE).ixInvertOnNew(keys, 2));
        final SortedRanges sortedRanges = manyRangeSortedRanges(0).addRange(5, 9);
        assertHoldsSteady("sorted ranges invert", keys, () -> sortedRanges.ixInvertOnNew(keys, 2));
    }
}
