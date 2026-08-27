//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.LogicalClockImpl;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Adopting an inner set we already hold must not acquire a reference we then never release; the reference count of a
 * rowset that keeps being reset to the same source has to hold steady rather than climb with every call.
 */
public class RowSetAssignRefCountTest {
    @Rule
    final public EngineCleanup engineCleanup = new EngineCleanup();

    private static final int REPETITIONS = 20;

    // SingleRange hands out copies rather than references and reports a constant count of one, so only the two
    // reference-counted implementations can show this.
    private static WritableRowSetImpl rsp(final long start, final long end) {
        return new WritableRowSetImpl(RspBitmap.makeSingleRange(start, end));
    }

    private static WritableRowSetImpl sortedRanges(final long start, final long end) {
        return new WritableRowSetImpl(SortedRanges.makeSingleRange(start, end));
    }

    @Test
    public void testRepeatedResetToTheSameSourceHoldsSteady() {
        for (final WritableRowSetImpl source : new WritableRowSetImpl[] {rsp(5, 9), sortedRanges(5, 9)}) {
            try (final WritableRowSetImpl closeSource = source;
                    final WritableRowSetImpl target = new WritableRowSetImpl(OrderedLongSet.EMPTY)) {
                target.resetTo(source);
                final int steadyState = source.refCount();
                for (int i = 0; i < REPETITIONS; ++i) {
                    target.resetTo(source);
                }
                assertEquals("reference count after " + REPETITIONS + " more resets", steadyState,
                        source.refCount());
            }
        }
    }

    @Test
    public void testUpdateThatRemovesOurselvesHoldsSteady() {
        try (final WritableRowSetImpl target = new WritableRowSetImpl(OrderedLongSet.EMPTY);
                final WritableRowSetImpl added = rsp(5, 9)) {
            // Sharing an inner set with added is what makes the update below adopt one we already hold.
            target.resetTo(added);
            final int steadyState = added.refCount();
            for (int i = 0; i < REPETITIONS; ++i) {
                target.update(added, target);
            }
            assertEquals("reference count after " + REPETITIONS + " updates", steadyState, added.refCount());
        }
    }

    @Test
    public void testIdleUpdateCyclesHoldSteady() {
        final TrackingWritableRowSet rowSet = rsp(1, 1).toTracking();
        final LogicalClockImpl clock = (LogicalClockImpl) ExecutionContext.getContext().getUpdateGraph().clock();
        // One full cycle first, so that prev has caught up to current and the count has settled.
        clock.startUpdateCycle();
        rowSet.sizePrev();
        clock.completeUpdateCycle();
        rowSet.sizePrev();
        final int steadyState = ((WritableRowSetImpl) rowSet).refCount();
        for (int i = 0; i < REPETITIONS; ++i) {
            clock.startUpdateCycle();
            rowSet.sizePrev();
            clock.completeUpdateCycle();
            rowSet.sizePrev();
        }
        assertEquals("reference count after " + REPETITIONS + " idle cycles", steadyState,
                ((WritableRowSetImpl) rowSet).refCount());
    }
}
