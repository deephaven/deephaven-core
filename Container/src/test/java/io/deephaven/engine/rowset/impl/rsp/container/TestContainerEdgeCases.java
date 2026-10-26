//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.rsp.container.ContainerTestCommon.valuesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Edge cases of the container API that the {@code RspBitmap} layer never asks for (empty ranges, empty rank ranges, a
 * container as its own operand), but that every implementation must still answer the same way.
 */
public class TestContainerEdgeCases {

    private static final int[] RUNS = {1, 5, 10, 20, 30, 31, 40, 50};

    /** One fresh fixture per implementation; each one is built anew so in-place operations cannot leak across. */
    private static List<Supplier<Container>> fixtures() {
        final List<Supplier<Container>> out = new ArrayList<>();
        out.add(() -> {
            final ArrayContainer c = new ArrayContainer(64);
            for (int ii = 0; ii < RUNS.length; ii += 2) {
                for (int v = RUNS[ii]; v < RUNS[ii + 1]; ++v) {
                    c.iset((short) v);
                }
            }
            return c;
        });
        out.add(() -> {
            final BitmapContainer c = new BitmapContainer();
            for (int ii = 0; ii < RUNS.length; ii += 2) {
                c.iadd(RUNS[ii], RUNS[ii + 1]);
            }
            return c;
        });
        out.add(() -> {
            Container c = new RunContainer(RUNS[0], RUNS[1], RUNS[2], RUNS[3]);
            for (int ii = 4; ii < RUNS.length; ii += 2) {
                c = c.iadd(RUNS[ii], RUNS[ii + 1]);
            }
            return c;
        });
        out.add(() -> Container.twoValues((short) 10, (short) 20));
        out.add(() -> Container.singleton((short) 10));
        out.add(() -> new SingleRangeContainer(10, 20));
        out.add(Container::empty);
        return out;
    }

    private static String name(final Container c) {
        return c.getClass().getSimpleName();
    }

    @Test
    public void testEmptyRangeIsContainedAndOverlapsNothing() {
        final int[][] ranges = {{0, 0}, {1, 1}, {10, 10}, {12, 12}, {30, 30}, {64, 64}, {70, 70}, {65535, 65535},
                {65536, 65536}, {10, 5}, {65536, 0}};
        for (final Supplier<Container> fixture : fixtures()) {
            final Container c = fixture.get();
            for (final int[] r : ranges) {
                final String m = name(c) + " [" + r[0] + ", " + r[1] + ")";
                assertTrue(m + " contains", c.contains(r[0], r[1]));
                assertFalse(m + " overlapsRange", c.overlapsRange(r[0], r[1]));
            }
        }
        // A bitmap whose first word is full used to index past the end for [0, 0).
        final BitmapContainer full = new BitmapContainer();
        full.iadd(0, 64);
        assertTrue(full.contains(0, 0));
        assertFalse(full.overlapsRange(0, 0));
    }

    @Test
    public void testEmptyRankRangeSelectsNothing() {
        for (final Supplier<Container> fixture : fixtures()) {
            final Container c = fixture.get();
            final int card = c.getCardinality();
            for (final int r : new int[] {0, card / 2, card}) {
                final Container selected = c.select(r, r);
                selected.validate();
                assertTrue(name(c) + " select(" + r + ", " + r + ")", selected.isEmpty());
                if (r > 0) {
                    final Container reversed = c.select(r, r - 1);
                    reversed.validate();
                    assertTrue(name(c) + " select(" + r + ", " + (r - 1) + ")", reversed.isEmpty());
                }
            }
        }
        final RunContainer run = new RunContainer(10, 20);
        for (final int r : new int[] {0, 5, 10}) {
            assertTrue("static select(" + r + ", " + r + ")", RunContainer.select(run, r, r).isEmpty());
        }
        assertTrue("static select(5, 4)", RunContainer.select(run, 5, 4).isEmpty());
    }

    private static List<Integer> selectRanges(final Container c, final int... positionRanges) {
        final List<Integer> out = new ArrayList<>();
        c.selectRanges((start, end) -> {
            out.add(start);
            out.add(end);
        }, new RangeIterator.ArrayBacked(positionRanges));
        return out;
    }

    private static List<Integer> findRanges(final Container c, final int... valueRanges) {
        final List<Integer> out = new ArrayList<>();
        final boolean reachedMax = c.findRanges((start, end) -> {
            out.add(start);
            out.add(end);
        }, new RangeIterator.ArrayBacked(valueRanges), Integer.MAX_VALUE);
        assertFalse(name(c) + " findRanges reached maxPos", reachedMax);
        return out;
    }

    @Test
    public void testSelectAndFindRangesOverNoValues() {
        for (final Supplier<Container> fixture : fixtures()) {
            final Container c = fixture.get();
            final String m = name(c);
            assertEquals(m + " selectRanges()", List.of(), selectRanges(c));
            assertEquals(m + " findRanges()", List.of(), findRanges(c));
            // An empty range past the last position is still empty, not out of bounds.
            final int card = c.getCardinality();
            assertEquals(m + " selectRanges([card + 1, card + 1))", List.of(), selectRanges(c, card + 1, card + 1));
            if (c.isEmpty()) {
                continue;
            }
            final int first = c.first();
            assertEquals(m + " selectRanges([0, 0))", List.of(), selectRanges(c, 0, 0));
            assertEquals(m + " findRanges([first, first))", List.of(), findRanges(c, first, first));
            // An empty range among real ones contributes nothing.
            assertEquals(m + " selectRanges([0, 0), [0, 1))", List.of(first, first + 1),
                    selectRanges(c, 0, 0, 0, 1));
            assertEquals(m + " findRanges([first, first), [first, first + 1))", List.of(0, 1),
                    findRanges(c, first, first, first, first + 1));
            final int last = c.last();
            assertEquals(m + " selectRanges([card - 1, card), [card, card))", List.of(last, last + 1),
                    selectRanges(c, card - 1, card, card, card));
            assertEquals(m + " findRanges([last, last + 1), [last + 1, last + 1))", List.of(card - 1, card),
                    findRanges(c, last, last + 1, last + 1, last + 1));
        }
        // An empty range asks nothing even of a container with no values to give.
        for (final Container c : new Container[] {new ArrayContainer(), new BitmapContainer(), new RunContainer(),
                RunContainer.makeByWrapping(new short[0], 0, 0), Container.empty()}) {
            assertEquals(name(c) + " empty, selectRanges([0, 0))", List.of(), selectRanges(c, 0, 0));
            assertEquals(name(c) + " empty, selectRanges([1, 1))", List.of(), selectRanges(c, 1, 1));
            assertEquals(name(c) + " empty, findRanges([0, 0))", List.of(), findRanges(c, 0, 0));
        }
    }

    /** A {@code false} from advance leaves the iterator exhausted, whatever it had returned before. */
    @Test
    public void testReverseAdvanceFailureExhausts() {
        for (final Supplier<Container> fixture : fixtures()) {
            final List<Integer> descending = valuesOf(fixture.get());
            Collections.reverse(descending);
            for (int consumed = 1; consumed <= descending.size(); ++consumed) {
                for (int v = 0; v <= 52; ++v) {
                    final Container c = fixture.get();
                    final ShortAdvanceIterator it = c.getReverseShortIterator();
                    for (int ii = 0; ii < consumed; ++ii) {
                        it.next();
                    }
                    final String m = name(c) + " after " + consumed + " advance(" + v + ")";
                    int expectedAt = -1;
                    for (int ii = consumed - 1; ii < descending.size(); ++ii) {
                        if (descending.get(ii) <= v) {
                            expectedAt = ii;
                            break;
                        }
                    }
                    final boolean found = it.advance(v);
                    assertEquals(m, expectedAt != -1, found);
                    if (found) {
                        assertEquals(m + " curr", (int) descending.get(expectedAt), it.currAsInt());
                        assertEquals(m + " hasNext", expectedAt + 1 < descending.size(), it.hasNext());
                    } else {
                        assertFalse(m + " hasNext", it.hasNext());
                    }
                }
            }
        }
    }

    private static void checkSelfOperand(final String op, final BinaryOperator<Container> f,
            final boolean keepsValues) {
        for (final Supplier<Container> fixture : fixtures()) {
            final Container c = fixture.get();
            final List<Integer> expected = keepsValues ? valuesOf(c) : List.of();
            final String m = name(c) + "." + op + "(itself)";
            final Container result = f.apply(c, c);
            result.validate();
            assertEquals(m, expected, valuesOf(result));
            assertEquals(m + " cardinality", expected.size(), result.getCardinality());
        }
    }

    @Test
    public void testSelfOperand() {
        checkSelfOperand("or", Container::or, true);
        checkSelfOperand("and", Container::and, true);
        checkSelfOperand("andNot", Container::andNot, false);
        checkSelfOperand("xor", Container::xor, false);
        checkSelfOperand("ior", Container::ior, true);
        checkSelfOperand("iand", Container::iand, true);
        checkSelfOperand("iandNot", Container::iandNot, false);
        checkSelfOperand("ixor", Container::ixor, false);
    }

    /** Galloping must be chosen by how many values each side holds, not by how much room its array has. */
    @Test
    public void testIntersectionWithAnEmptyButRoomyArray() {
        final ArrayContainer one = new ArrayContainer(new short[] {0});
        final ArrayContainer roomy = new ArrayContainer(4000);
        assertEquals(List.of(), valuesOf(new ArrayContainer(new short[] {0}).and(roomy)));
        assertEquals(List.of(), valuesOf(one.iand(roomy)));
        assertEquals(List.of(), valuesOf(roomy.iand(new ArrayContainer(new short[] {0}))));

        // Stale values beyond the logical length must not be matched.
        final short[] stale = new short[200];
        stale[0] = 7;
        stale[1] = 9;
        final short[] buffer = new short[2];
        assertEquals(0, ContainerUtil.unsignedIntersect2by2(new short[] {7, 9}, 2, stale, 0, buffer));
        assertEquals(0, ContainerUtil.unsignedIntersect2by2(stale, 0, new short[] {7, 9}, 2, buffer));
    }

    /** A run container may hold more than {@code Short.MAX_VALUE} runs. */
    @Test
    public void testRunContainsBitmapWithManyRuns() {
        final int nRuns = 32768;
        final short[] valueslength = new short[2 * nRuns];
        final BitmapContainer evens = new BitmapContainer();
        // As many values as the run container, so the cardinality check cannot answer early.
        final BitmapContainer lastOdd = new BitmapContainer();
        for (int ii = 0; ii < nRuns; ++ii) {
            valueslength[2 * ii] = (short) (2 * ii);
            evens.iset((short) (2 * ii));
            lastOdd.iset((short) (ii == nRuns - 1 ? 65535 : 2 * ii));
        }
        final RunContainer run = RunContainer.makeByWrapping(valueslength, nRuns, nRuns);
        assertTrue(run.contains(evens));
        assertFalse(run.contains(lastOdd));
    }

    /** Ranges are compared as unsigned values, so a run crossing 0x7FFF/0x8000 is one range. */
    @Test
    public void testArrayForEachRangeAcrossTheSignBoundary() {
        final ArrayContainer c = new ArrayContainer(new short[] {(short) 0x7FFE, (short) 0x7FFF, (short) 0x8000,
                (short) 0x8001});
        final List<Integer> ranges = new ArrayList<>();
        c.forEachRange(0, (start, endInclusive) -> {
            ranges.add(ContainerUtil.toIntUnsigned(start));
            ranges.add(ContainerUtil.toIntUnsigned(endInclusive));
            return true;
        });
        assertEquals(List.of(0x7FFE, 0x8001), ranges);
    }

    @Test
    public void testSearchOverAnEmptyWindow() {
        final short[] array = {5, 10};
        // Everything in the array is below the target, so only the window keeps position 0 out of the answer.
        final ContainerUtil.TargetComparator below20 = value -> Integer.compare(20, value);
        assertEquals(-1, ContainerUtil.search(array, 1, 1, below20));
        assertEquals(1, ContainerUtil.search(array, 1, 2, below20));
    }

    @Test
    public void testEmptyContainerFirstAndLast() {
        final Container empty = Container.empty();
        assertThrows(NoSuchElementException.class, empty::first);
        assertThrows(NoSuchElementException.class, empty::last);
    }
}
