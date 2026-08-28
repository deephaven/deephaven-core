//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * An empty range asks for nothing, and a range whose end precedes its start is emptier still. Operations taking a range
 * must treat both as a no-op rather than deriving indices from a wrapped endpoint.
 */
public class TestContainerEmptyRangeOps {

    private static List<Integer> valuesOf(final Container c) {
        final List<Integer> out = new ArrayList<>();
        final ShortIterator it = c.getShortIterator();
        while (it.hasNext()) {
            out.add(it.nextAsInt());
        }
        return out;
    }

    /** inot has to guard the empty range the way not already does. */
    @Test
    public void testArrayContainerInotOverAnEmptyRange() {
        final List<Integer> expected = List.of(10, 20, 30);
        for (final int at : new int[] {0, 1, 10, 20, 65535}) {
            final ArrayContainer c = new ArrayContainer(new short[] {10, 20, 30}, 3);
            final Container after = c.inot(at, at);
            assertEquals("inot(" + at + ", " + at + ")", expected, valuesOf(after));
            assertEquals("inot(" + at + ", " + at + ") cardinality", 3, after.getCardinality());
        }
        // not already handles this; keep the two in step.
        final ArrayContainer c = new ArrayContainer(new short[] {10, 20, 30}, 3);
        assertEquals("not(0, 0)", expected, valuesOf(c.not(0, 0)));
    }
}
