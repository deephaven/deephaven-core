//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
