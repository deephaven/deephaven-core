//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link SearchRangeIterator#advance} positions the iterator at the key it is given, so the range it then reports must
 * begin no earlier than that key. An implementation that returns early when the current range already covers the key,
 * without moving its start, reports keys the caller has already advanced past.
 */
public class TestRangeIteratorAdvanceClamp {

    private static Container[] containersHoldingZeroTo(final int endExclusive) {
        final ArrayContainer array = new ArrayContainer(endExclusive);
        for (int v = 0; v < endExclusive; ++v) {
            array.iset((short) v);
        }
        return new Container[] {
                array,
                new RunContainer(0, endExclusive),
                new BitmapContainer().iadd(0, endExclusive),
                Container.singleRange(0, endExclusive),
        };
    }

    @Test
    public void testAdvanceWithinTheCurrentRangeClampsStart() {
        for (final Container c : containersHoldingZeroTo(175)) {
            final String name = c.getClass().getSimpleName();
            final SearchRangeIterator it = c.getShortRangeIterator(0);
            assertTrue(name + ": has a first range", it.hasNext());
            it.next();
            assertEquals(name + ": the first range starts at zero", 0, it.start());

            assertTrue(name + ": advance lands inside the current range", it.advance(174));
            assertEquals(name + ": start is clamped to the key advanced to", 174, it.start());
            assertTrue(name + ": the range still covers that key", it.end() > 174);
        }
    }

    /** Advancing to a key the iterator has already passed must not move it backwards. */
    @Test
    public void testAdvanceBackwardsDoesNotRewind() {
        for (final Container c : containersHoldingZeroTo(175)) {
            final String name = c.getClass().getSimpleName();
            final SearchRangeIterator it = c.getShortRangeIterator(0);
            it.next();
            assertTrue(name, it.advance(100));
            assertEquals(name + ": clamped to 100", 100, it.start());
            assertTrue(name, it.advance(50));
            assertEquals(name + ": stays at 100", 100, it.start());
        }
    }
}
