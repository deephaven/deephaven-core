//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins the relationship {@link Container#find} documents against {@link Container#rank}: for a value the container
 * holds, find is the zero-based position of that value and rank is the count of values at or below it, so find is one
 * less than rank.
 */
public class TestFindVersusRank {

    private static Container[] containersHolding(final int... values) {
        final ArrayContainer array = new ArrayContainer(values.length);
        final BitmapContainer bitmap = new BitmapContainer();
        RunContainer run = new RunContainer();
        for (final int v : values) {
            array.iset((short) v);
            bitmap.iset((short) v);
            run = (RunContainer) run.iset((short) v);
        }
        return new Container[] {array, bitmap, run};
    }

    @Test
    public void testFindIsOneLessThanRankForValuesPresent() {
        final int[] values = {0, 1, 10, 4000, 32767, 32768, 65535};
        for (final Container c : containersHolding(values)) {
            final String name = c.getClass().getSimpleName();
            for (int i = 0; i < values.length; ++i) {
                final short v = (short) values[i];
                assertTrue(name + ": contains " + values[i], c.contains(v));
                assertEquals(name + ": find(" + values[i] + ") is the position", i, c.find(v));
                assertEquals(name + ": rank(" + values[i] + ") counts through it", i + 1, c.rank(v));
                assertEquals(name + ": find is one less than rank", c.rank(v) - 1, c.find(v));
            }
        }
    }

    @Test
    public void testFindForValuesAbsent() {
        for (final Container c : containersHolding(10, 20, 30)) {
            final String name = c.getClass().getSimpleName();
            // Not present: -(insertion point) - 1, as Array.binarySearch reports.
            assertEquals(name + ": below everything", -1, c.find((short) 5));
            assertEquals(name + ": between the first two", -2, c.find((short) 15));
            assertEquals(name + ": above everything", -4, c.find((short) 35));
            // rank of an absent value counts what precedes it, and needs no minus one.
            assertEquals(name + ": rank below everything", 0, c.rank((short) 5));
            assertEquals(name + ": rank between the first two", 1, c.rank((short) 15));
        }
    }
}
