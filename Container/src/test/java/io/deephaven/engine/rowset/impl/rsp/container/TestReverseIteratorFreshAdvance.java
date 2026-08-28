//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A reverse iterator that has not been stepped yet sits one past the container's last value. Advancing such an iterator
 * has to step onto the largest value at or below the key asked for, as every implementation but one does; answering
 * with the untouched starting position reports a value the container does not hold.
 */
public class TestReverseIteratorFreshAdvance {

    private static Container[] containersHolding(final int begin, final int endExclusive) {
        final ArrayContainer array = new ArrayContainer(endExclusive - begin);
        for (int v = begin; v < endExclusive; ++v) {
            array.iset((short) v);
        }
        return new Container[] {
                array,
                new RunContainer(begin, endExclusive),
                new BitmapContainer().iadd(begin, endExclusive),
                Container.singleRange(begin, endExclusive),
        };
    }

    @Test
    public void testFreshAdvanceAboveTheContainerLandsOnItsLastValue() {
        for (final Container c : containersHolding(32766, 32769)) {
            final String name = c.getClass().getSimpleName();
            final ShortAdvanceIterator it = c.getReverseShortIterator();
            assertTrue(name + ": advance from a fresh iterator", it.advance(65535));
            assertEquals(name + ": lands on the last value", 32768, it.currAsInt());
            assertTrue(name + ": the value is in the container", c.contains((short) it.currAsInt()));
        }
    }

    @Test
    public void testFreshAdvanceInsideTheContainer() {
        for (final Container c : containersHolding(10, 21)) {
            final String name = c.getClass().getSimpleName();
            final ShortAdvanceIterator it = c.getReverseShortIterator();
            assertTrue(name + ": advance to a key inside", it.advance(15));
            assertEquals(name + ": lands on that key", 15, it.currAsInt());
            assertTrue(name + ": the value is in the container", c.contains((short) it.currAsInt()));
        }
        // Exactly at the last value.
        for (final Container c : containersHolding(10, 21)) {
            final String name = c.getClass().getSimpleName();
            final ShortAdvanceIterator it = c.getReverseShortIterator();
            assertTrue(name + ": advance to the last value", it.advance(20));
            assertEquals(name + ": lands on it", 20, it.currAsInt());
        }
    }

    /** Stepping first and then advancing already worked; keep it working. */
    @Test
    public void testAdvanceAfterStepping() {
        for (final Container c : containersHolding(10, 21)) {
            final String name = c.getClass().getSimpleName();
            final ShortAdvanceIterator it = c.getReverseShortIterator();
            assertEquals(name + ": first value going down", 20, it.nextAsInt());
            assertTrue(name, it.advance(15));
            assertEquals(name + ": advanced to 15", 15, it.currAsInt());
        }
    }
}
