//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Run lengths are stored as unsigned shorts. A run of more than 32768 values has a stored length whose short is
 * negative, so reading it without widening it as unsigned undercounts the cardinality by 65536 per run.
 */
public class TestRunContainerNotLongRun {

    /** Cardinality counted from the container itself, independent of whatever it recorded. */
    private static int countByIteration(final Container c) {
        int n = 0;
        final ShortIterator it = c.getShortIterator();
        while (it.hasNext()) {
            it.nextAsInt();
            ++n;
        }
        return n;
    }

    private static void assertComplement(final String what, final Container original, final Container complement,
            final int rangeStart, final int rangeEnd) {
        assertEquals(what + ": cardinality agrees with the contents", countByIteration(complement),
                complement.getCardinality());
        for (int v = 0; v < 65536; ++v) {
            final boolean inRange = v >= rangeStart && v < rangeEnd;
            final boolean expected = inRange != original.contains((short) v);
            assertEquals(what + ": value " + v, expected, complement.contains((short) v));
        }
    }

    /** A long run before the flipped range: its length is what gets misread. */
    @Test
    public void testNotWithALongLeadingRun() {
        final RunContainer c = new RunContainer(0, 40000, 50000, 50001);
        assertTrue("the leading run is longer than a signed short can hold", c.getLengthAsInt(0) > Short.MAX_VALUE);
        assertComplement("long leading run", c, c.not(45000, 45001), 45000, 45001);
    }

    /** The whole block as one run, flipped in the middle. */
    @Test
    public void testNotWithTheWholeBlockAsOneRun() {
        final RunContainer c = new RunContainer(0, 65536);
        assertComplement("full block", c, c.not(3, 9285), 3, 9285);
    }

    /** inot delegates to not when it lacks room, and xor of a single range routes through not as well. */
    @Test
    public void testInotAndXorWithALongLeadingRun() {
        assertComplement("inot", new RunContainer(0, 40000, 50000, 50001),
                new RunContainer(0, 40000, 50000, 50001).inot(45000, 45001), 45000, 45001);

        final Container full = new RunContainer(0, 65536);
        final Container xored = full.xor(Container.singleRange(11822, 65473));
        assertEquals("xor cardinality agrees with the contents", countByIteration(xored), xored.getCardinality());
        assertComplement("xor", full, xored, 11822, 65473);
    }

    /** A leading run short enough to be read correctly either way, for contrast. */
    @Test
    public void testNotWithAShortLeadingRun() {
        final RunContainer c = new RunContainer(0, 100, 50000, 50001);
        assertComplement("short leading run", c, c.not(200, 300), 200, 300);
    }
}
