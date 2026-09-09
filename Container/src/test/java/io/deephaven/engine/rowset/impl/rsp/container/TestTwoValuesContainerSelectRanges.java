//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * {@link Container#selectRanges} is handed an iterator over position ranges, plural, and must answer for every one of
 * them; a sibling implementation that stops after the first silently drops values.
 */
public class TestTwoValuesContainerSelectRanges {

    private static List<Integer> select(final Container c, final int... positionRanges) {
        final List<Integer> out = new ArrayList<>();
        c.selectRanges((start, end) -> {
            out.add(start);
            out.add(end);
        }, new RangeIterator.ArrayBacked(positionRanges));
        return out;
    }

    @Test
    public void testSeveralPositionRanges() {
        final Container c = Container.twoValues((short) 2352, (short) 47280);
        // Positions [0,1) and [1,2) as two separate ranges: both values, in order.
        assertEquals(List.of(2352, 2353, 47280, 47281), select(c, 0, 1, 1, 2));
    }

    @Test
    public void testASinglePositionRangeStillWorks() {
        final Container c = Container.twoValues((short) 2352, (short) 47280);
        assertEquals(List.of(2352, 2353), select(c, 0, 1));
        assertEquals(List.of(47280, 47281), select(c, 1, 2));
        assertEquals(List.of(2352, 2353, 47280, 47281), select(c, 0, 2));
    }

    /** Every implementation must agree about the same positions. */
    @Test
    public void testAgreesWithOtherImplementations() {
        final int a = 2352;
        final int b = 47280;
        final List<Integer> expected = List.of(a, a + 1, b, b + 1);
        for (final Container c : new Container[] {
                Container.twoValues((short) a, (short) b),
                new ArrayContainer(new short[] {(short) a, (short) b}, 2),
                new RunContainer(a, a + 1, b, b + 1),
                new BitmapContainer().iset((short) a).iset((short) b),
        }) {
            assertEquals(c.getClass().getSimpleName(), expected, select(c, 0, 1, 1, 2));
        }
    }
}
