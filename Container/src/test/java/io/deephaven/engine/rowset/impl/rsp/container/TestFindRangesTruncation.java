//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * {@link Container#findRanges} accumulates a run of consecutive positions and flushes it when the run breaks. When it
 * stops early because {@code maxPos} has been reached, whatever it has accumulated so far still has to be flushed.
 */
public class TestFindRangesTruncation {

    /** The three sparse values 10, 20, 30 on every implementation that can hold them. */
    private static Container[] sparseContainers() {
        final ArrayContainer array = new ArrayContainer(3);
        array.iset((short) 10);
        array.iset((short) 20);
        array.iset((short) 30);
        return new Container[] {
                array,
                new BitmapContainer().iset((short) 10).iset((short) 20).iset((short) 30),
                new RunContainer(10, 11).iset((short) 20).iset((short) 30),
        };
    }

    /** Two values, which the two-value and sparse forms can both hold. */
    private static Container[] twoValueContainers() {
        final ArrayContainer array = new ArrayContainer(2);
        array.iset((short) 10);
        array.iset((short) 20);
        return new Container[] {
                Container.twoValues((short) 10, (short) 20),
                array,
                new BitmapContainer().iset((short) 10).iset((short) 20),
                new RunContainer(10, 11).iset((short) 20),
        };
    }

    /** A contiguous run, which every implementation can hold. */
    private static Container[] rangeContainers(final int begin, final int endExclusive) {
        final ArrayContainer array = new ArrayContainer(endExclusive - begin);
        for (int v = begin; v < endExclusive; ++v) {
            array.iset((short) v);
        }
        return new Container[] {
                Container.singleRange(begin, endExclusive),
                array,
                new BitmapContainer().iadd(begin, endExclusive),
                new RunContainer(begin, endExclusive),
        };
    }

    private static List<Integer> findRanges(final Container c, final int maxPos, final int... valueRanges) {
        final List<Integer> out = new ArrayList<>();
        c.findRanges((start, end) -> {
            out.add(start);
            out.add(end);
        }, new RangeIterator.ArrayBacked(valueRanges), maxPos);
        return out;
    }

    /** Truncation landing on the last position of an accumulated run must still report that run. */
    @Test
    public void testTruncationFlushesTheAccumulatedRun() {
        for (final Container c : sparseContainers()) {
            final String name = c.getClass().getSimpleName();
            // Values 10 and 20 are positions 0 and 1; maxPos 0 keeps only position 0.
            assertEquals(name + ": maxPos 0 over two values", List.of(0, 1),
                    findRanges(c, 0, 10, 11, 20, 21));
            // maxPos 1 keeps both.
            assertEquals(name + ": maxPos 1 over two values", List.of(0, 2),
                    findRanges(c, 1, 10, 11, 20, 21));
            // All three values, truncated after the second.
            assertEquals(name + ": maxPos 1 over three values", List.of(0, 2),
                    findRanges(c, 1, 10, 11, 20, 21, 30, 31));
        }
    }

    @Test
    public void testTruncationOnTwoValueContainers() {
        for (final Container c : twoValueContainers()) {
            final String name = c.getClass().getSimpleName();
            assertEquals(name + ": maxPos 0", List.of(0, 1), findRanges(c, 0, 10, 11, 20, 21));
            assertEquals(name + ": maxPos 1", List.of(0, 2), findRanges(c, 1, 10, 11, 20, 21));
        }
    }

    /** A contiguous run, where the positions themselves are consecutive. */
    @Test
    public void testTruncationOnAContiguousRun() {
        for (final Container c : rangeContainers(100, 110)) {
            final String name = c.getClass().getSimpleName();
            assertEquals(name + ": maxPos 0", List.of(0, 1), findRanges(c, 0, 100, 110));
            assertEquals(name + ": maxPos 3", List.of(0, 4), findRanges(c, 3, 100, 110));
            assertEquals(name + ": maxPos past the end", List.of(0, 10), findRanges(c, 100, 100, 110));
        }
    }

    /** A singleton, for completeness. */
    @Test
    public void testTruncationOnASingleton() {
        for (final Container c : new Container[] {Container.singleton((short) 7),
                new ArrayContainer(1).iset((short) 7), new RunContainer(7, 8)}) {
            final String name = c.getClass().getSimpleName();
            assertEquals(name + ": maxPos 0", List.of(0, 1), findRanges(c, 0, 7, 8));
        }
    }
}
