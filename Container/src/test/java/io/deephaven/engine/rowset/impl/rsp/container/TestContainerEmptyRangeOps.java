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

    /**
     * A single range container derives its result from the requested bounds, so an empty or backwards range leaves it
     * describing a range whose end precedes its start, or split into runs that overlap.
     */
    @Test
    public void testSingleRangeContainerDegenerateRanges() {
        // andRange of an empty range keeps nothing.
        for (final int at : new int[] {0, 2, 65535}) {
            final Container r = Container.singleRange(0, 65536).andRange(at, at);
            r.validate();
            assertEquals("andRange(" + at + ", " + at + ") is empty", 0, r.getCardinality());
        }

        // remove and not over an empty range change nothing.
        for (final int at : new int[] {0, 2, 65535}) {
            final Container removed = Container.singleRange(0, 65536).remove(at, at);
            removed.validate();
            assertEquals("remove(" + at + ", " + at + ") keeps everything", 65536, removed.getCardinality());

            final Container notted = Container.singleRange(0, 65536).not(at, at);
            notted.validate();
            assertEquals("not(" + at + ", " + at + ") keeps everything", 65536, notted.getCardinality());
        }

        // A range whose end precedes its start asks for even less than nothing.
        final Container backwards = Container.singleRange(0, 65536).remove(10, 5);
        backwards.validate();
        assertEquals("remove(10, 5) keeps everything", 65536, backwards.getCardinality());

        // And the same requests against a container that does not start at zero.
        final Container offset = Container.singleRange(100, 200).remove(150, 150);
        offset.validate();
        assertEquals("remove(150, 150) keeps everything", 100, offset.getCardinality());
    }

    /** The other implementations already treat these as no-ops; keep every one in agreement. */
    @Test
    public void testOtherImplementationsAgreeOnDegenerateRanges() {
        for (final Container c : new Container[] {
                new ArrayContainer(new short[] {10, 20, 30}, 3),
                new RunContainer(10, 11, 20, 21).iset((short) 30),
                new BitmapContainer().iset((short) 10).iset((short) 20).iset((short) 30),
        }) {
            final String name = c.getClass().getSimpleName();
            final Container removed = c.remove(20, 20);
            removed.validate();
            assertEquals(name + " remove(20, 20)", List.of(10, 20, 30), valuesOf(removed));
            final Container anded = c.andRange(20, 20);
            anded.validate();
            assertEquals(name + " andRange(20, 20)", 0, anded.getCardinality());
        }
    }
}
