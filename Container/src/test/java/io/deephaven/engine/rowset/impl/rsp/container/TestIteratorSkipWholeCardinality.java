//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Asking an iterator to skip exactly as many values as the container holds leaves nothing to iterate. That is a request
 * every implementation should answer with an exhausted iterator rather than rejecting.
 */
public class TestIteratorSkipWholeCardinality {

    private static Container[] containersHoldingOneValue() {
        final ArrayContainer array = new ArrayContainer(1);
        array.iset((short) 5);
        return new Container[] {
                Container.singleton((short) 5),
                array,
                new RunContainer(5, 6),
                new BitmapContainer().iset((short) 5),
                Container.singleRange(5, 6),
        };
    }

    @Test
    public void testSkippingEveryValueGivesAnExhaustedBatchIterator() {
        for (final Container c : containersHoldingOneValue()) {
            final String name = c.getClass().getSimpleName();
            assertEquals(name + ": holds one value", 1, c.getCardinality());
            // hasNext is the documented way to tell; calling next on an exhausted iterator is a caller error and
            // implementations differ on what they do with it, so only the contract is asserted here.
            final ContainerShortBatchIterator it = c.getShortBatchIterator(1);
            assertFalse(name + ": exhausted", it.hasNext());
        }
    }

    @Test
    public void testSkippingEveryValueGivesAnExhaustedRangeIterator() {
        for (final Container c : containersHoldingOneValue()) {
            final String name = c.getClass().getSimpleName();
            final SearchRangeIterator it = c.getShortRangeIterator(1);
            assertFalse(name + ": exhausted", it.hasNext());
        }
    }

    /**
     * The same with several values, and with each fixture's implementation asserted. A single-value fixture is not
     * enough: the factory answers a one-value range with a singleton, so the single-range implementation would go
     * untested.
     */
    @Test
    public void testSkippingEveryValueOfAMultiValueContainer() {
        final int card = 11;
        for (final Container c : multiValueContainers(10, 21)) {
            final String name = c.getClass().getSimpleName();
            assertEquals(name + ": holds " + card + " values", card, c.getCardinality());

            final SearchRangeIterator ranges = c.getShortRangeIterator(card);
            assertFalse(name + ": range iterator exhausted", ranges.hasNext());

            final ContainerShortBatchIterator batch = c.getShortBatchIterator(card);
            assertFalse(name + ": batch iterator exhausted", batch.hasNext());

            // And a partial skip still yields the rest.
            final SearchRangeIterator partial = c.getShortRangeIterator(card - 1);
            assertTrue(name + ": partial skip has a value", partial.hasNext());
            partial.next();
            assertEquals(name + ": partial skip lands on the last value", 20, partial.start());
        }
    }

    /** One fixture per implementation able to hold a contiguous run, each asserted to be the class we mean. */
    private static Container[] multiValueContainers(final int begin, final int endExclusive) {
        final ArrayContainer array = new ArrayContainer(endExclusive - begin);
        for (int v = begin; v < endExclusive; ++v) {
            array.iset((short) v);
        }
        final Container singleRange = new SingleRangeContainer(begin, endExclusive);
        assertEquals("the fixture is a single range container", "SingleRangeContainer",
                singleRange.getClass().getSimpleName());
        return new Container[] {
                singleRange,
                array,
                new BitmapContainer().iadd(begin, endExclusive),
                new RunContainer(begin, endExclusive),
        };
    }

    /** Skipping nothing still yields the value. */
    @Test
    public void testSkippingNothing() {
        for (final Container c : containersHoldingOneValue()) {
            final String name = c.getClass().getSimpleName();
            final ContainerShortBatchIterator batch = c.getShortBatchIterator(0);
            final short[] buf = new short[4];
            assertEquals(name + ": one value", 1, batch.next(buf, 0, 4));
            assertEquals(name + ": the value", 5, ContainerUtil.toIntUnsigned(buf[0]));

            final SearchRangeIterator ranges = c.getShortRangeIterator(0);
            ranges.next();
            assertEquals(name + ": range start", 5, ranges.start());
            assertEquals(name + ": range end", 6, ranges.end());
        }
    }
}
