/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl.sortedranges;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import io.deephaven.base.testing.Shuffle;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.OrderedLongSetBuilderSequential;
import io.deephaven.engine.rowset.impl.ValidationSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class SortedRangesTest {
    private static final int runs = 1;
    private static final int lastRun = 1; // to help offset the seed when doing multiple runs.
    private static final int seed0 = 27112311 + lastRun;

    @Test
    public void testSimpleAdd() {
        SortedRanges sar = new SortedRangesLong(2);
        final long start = 1;
        final long end = 127;
        final long step = 2;
        assertEquals(end - start, step * ((end - start) / step));
        for (long i = start; i <= end; i += step) {
            sar = sar.add(i);
            assertNotNull("i == " + i, sar);
        }
        for (long i = start; i <= end; i += step) {
            assertTrue("i==" + i, sar.contains(i));
            final long next = i + step;
            for (long j = i + 1; j < next; ++j) {
                assertFalse("j == " + j, sar.contains(j));
            }
        }
        assertEquals(1 + (end - start) / step, sar.getCardinality());
        for (long i = start; i < end; i += step) {
            final long next = i + step;
            for (long j = i + 1; j < next; ++j) {
                sar = sar.add(j);
                assertNotNull("j ==" + j, sar);
            }
        }
        assertEquals(end - start + 1, sar.getCardinality());
    }

    @Test
    public void testAddCases() {
        SortedRanges sar = new SortedRangesShort(2, 0);
        sar = sar.add(1);
        sar.validate();
        assertEquals(1, sar.getCardinality());
        assertTrue(sar.contains(1));
        sar = sar.add(2);
        sar.validate();
        assertEquals(2, sar.getCardinality());
        assertTrue(sar.contains(2));
        sar = sar.add(3);
        sar.validate();
        assertEquals(3, sar.getCardinality());
        assertTrue(sar.contains(3));
        sar = sar.add(2);
        sar.validate();
        assertEquals(3, sar.getCardinality());
        sar = sar.add(3);
        sar.validate();
        assertEquals(3, sar.getCardinality());

        sar = new SortedRangesShort(2, 0);
        sar = sar.addRange(9, 10);
        long card = 2;
        sar = sar.add(8);
        ++card;
        sar.validate();
        assertEquals(card, sar.getCardinality());
        sar = sar.addRange(3, 4);
        card += 2;
        sar.validate();
        assertEquals(card, sar.getCardinality());
        sar = sar.add(5);
        ++card;
        sar.validate();
        assertEquals(card, sar.getCardinality());
        sar = sar.add(6);
        ++card;
        sar.validate();
        assertEquals(card, sar.getCardinality());
        sar = sar.add(7);
        ++card;
        sar.validate();
        assertEquals(card, sar.getCardinality());

        sar = new SortedRangesShort(2, 0);
        sar = sar.addRange(9, 10);
        sar = sar.add(7);
        sar.validate();
        assertEquals(3, sar.getCardinality());
        sar = sar.add(8);
        sar.validate();
        assertEquals(4, sar.getCardinality());
        sar = sar.add(12);
        sar.validate();
        assertEquals(5, sar.getCardinality());
        sar = sar.add(11);
        sar.validate();
        assertEquals(6, sar.getCardinality());

        sar = new SortedRangesShort(2, 0);
        sar = sar.addRange(3, 3);
        sar = sar.add(7);
        sar = sar.add(4);
        sar.validate();
        assertEquals(3, sar.getCardinality());

        sar = new SortedRangesShort(2, 0);
        sar = sar.addRange(5, 5);
        sar = sar.add(0);
        sar = sar.add(4);
        sar.validate();
        assertEquals(3, sar.getCardinality());
    }

    @Test
    public void testSimpleAddRange() {
        SortedRanges sar = new SortedRangesShort(2, 0);
        final long start = 1;
        final long end = 133;
        final int rlen = 3;
        for (long i = start; i <= end - rlen + 1; i += 2 * rlen) {
            sar = sar.addRange(i, i + rlen - 1);
            assertNotNull("i == " + i, sar);
        }
        for (long i = start; i <= end - rlen + 1; i += 2 * rlen) {
            for (long j = i; j <= i + rlen - 1; ++j) {
                assertTrue("j==" + j, sar.contains(j));
            }
            final long next = i + 2 * rlen;
            for (long j = i + rlen; j < next; ++j) {
                assertFalse("j == " + j, sar.contains(j));
            }
        }
        for (long i = start; i <= end - rlen + 1; i += 2 * rlen) {
            final long next = i + 2 * rlen;
            final long rend = Math.min(next - 1, end);
            sar = sar.addRange(i + rlen, rend);
            assertNotNull("i ==" + i, sar);
            for (long j = i + rlen; j <= rend; ++j) {
                assertTrue("j == " + j, sar.contains(j));
            }
        }
        assertEquals(end - start, sar.getCardinality());
    }

    @Test
    public void testAddRangeCases() {
        SortedRanges sar = new SortedRangesShort(2, 6);
        sar = sar.add(7);
        sar = sar.addRange(8, 9);
        assertTrue(sar.containsRange(8, 9));
        assertEquals(3, sar.getCardinality());

        sar = new SortedRangesShort(2, 10);
        sar = sar.addRange(10, 20);
        sar = sar.addRange(11, 19);
        assertTrue(sar.containsRange(10, 20));
        assertEquals(11, sar.getCardinality());
        sar = sar.addRange(10, 19);
        assertTrue(sar.containsRange(10, 20));
        assertEquals(11, sar.getCardinality());
        sar = sar.addRange(11, 20);
        assertTrue(sar.containsRange(10, 20));
        assertEquals(11, sar.getCardinality());
        sar = sar.addRange(10, 20);
        assertTrue(sar.containsRange(10, 20));
        assertEquals(11, sar.getCardinality());

        sar = new SortedRangesShort(2, 7);
        sar = sar.add(9);
        sar = sar.addRange(9, 11);
        assertTrue(sar.containsRange(9, 11));
        assertEquals(3, sar.getCardinality());
        sar = sar.addRange(11, 13);
        assertTrue(sar.containsRange(9, 13));
        assertEquals(5, sar.getCardinality());
        sar = sar.addRange(9, 13);
        assertTrue(sar.containsRange(9, 13));
        assertEquals(5, sar.getCardinality());
        sar = sar.addRange(9, 14);
        assertTrue(sar.containsRange(9, 14));
        assertEquals(6, sar.getCardinality());
        sar = sar.addRange(14, 16);
        assertTrue(sar.containsRange(9, 16));
        assertEquals(8, sar.getCardinality());
        sar = sar.addRange(100, 200);
        assertTrue(sar.containsRange(9, 16));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(109, sar.getCardinality());
        sar = sar.addRange(16, 18);
        assertTrue(sar.containsRange(9, 18));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(111, sar.getCardinality());
        sar = sar.add(40);
        assertTrue(sar.containsRange(9, 18));
        assertTrue(sar.contains(40));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(112, sar.getCardinality());
        sar = sar.addRange(40, 50);
        assertTrue(sar.containsRange(9, 18));
        assertTrue(sar.containsRange(40, 49));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(122, sar.getCardinality());
        sar = sar.addRange(40, 55);
        assertTrue(sar.containsRange(9, 18));
        assertTrue(sar.containsRange(40, 55));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(127, sar.getCardinality());
        sar = sar.addRange(54, 60);
        assertTrue(sar.containsRange(9, 18));
        assertTrue(sar.containsRange(40, 60));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(132, sar.getCardinality());
        sar = sar.add(20);
        assertTrue(sar.containsRange(9, 18));
        assertTrue(sar.contains(20));
        assertTrue(sar.containsRange(40, 60));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(133, sar.getCardinality());
        sar = sar.addRange(21, 22);
        assertTrue(sar.containsRange(9, 18));
        assertTrue(sar.containsRange(20, 22));
        assertTrue(sar.containsRange(40, 60));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(135, sar.getCardinality());
        sar = sar.addRange(61, 65);
        assertTrue(sar.containsRange(9, 18));
        assertTrue(sar.containsRange(20, 22));
        assertTrue(sar.containsRange(40, 65));
        assertTrue(sar.containsRange(100, 200));
        assertEquals(140, sar.getCardinality());
        sar = sar.addRange(8, 201);
        assertTrue(sar.containsRange(8, 201));
        assertEquals(194, sar.getCardinality());
        sar = sar.addRange(7, 200);
        assertTrue(sar.containsRange(7, 201));
        assertEquals(195, sar.getCardinality());
        sar = sar.add(205);
        assertTrue(sar.containsRange(7, 201));
        assertTrue(sar.contains(205));
        assertEquals(196, sar.getCardinality());
        sar = sar.addRange(200, 204);
        assertTrue(sar.containsRange(7, 205));
        assertEquals(199, sar.getCardinality());
    }

    @Test
    public void testAddRangeCases2() {
        SortedRanges sar = new SortedRangesShort(2, 5);
        sar = sar.addRange(10, 20);
        sar = sar.add(30);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.contains(30));
        assertEquals(12, sar.getCardinality());
        sar = sar.addRange(25, 30);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.containsRange(25, 30));
        assertEquals(17, sar.getCardinality());

        sar = new SortedRangesShort(2, 5);
        sar = sar.addRange(10, 20);
        sar = sar.add(30);
        sar = sar.addRange(40, 50);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.contains(30));
        assertTrue(sar.containsRange(40, 50));
        assertEquals(23, sar.getCardinality());
        sar = sar.addRange(25, 29);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.containsRange(25, 30));
        assertTrue(sar.containsRange(40, 50));
        assertEquals(28, sar.getCardinality());

        sar = new SortedRangesShort(2, 5);
        sar = sar.addRange(10, 20);
        sar = sar.add(30);
        sar = sar.add(40);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.contains(30));
        assertTrue(sar.contains(40));
        assertEquals(13, sar.getCardinality());
        sar = sar.addRange(29, 31);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.containsRange(29, 31));
        assertTrue(sar.contains(40));
        assertEquals(15, sar.getCardinality());

        sar = new SortedRangesShort(2, 7);
        sar = sar.addRange(10, 20);
        sar = sar.add(30);
        sar = sar.add(40);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.contains(30));
        assertTrue(sar.contains(40));
        assertEquals(13, sar.getCardinality());
        sar = sar.addRange(29, 41);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.containsRange(29, 41));
        assertEquals(24, sar.getCardinality());

        sar = new SortedRangesShort(2, 4);
        sar = sar.add(5);
        sar = sar.addRange(10, 20);
        assertTrue(sar.contains(5));
        assertTrue(sar.containsRange(10, 20));
        assertEquals(12, sar.getCardinality());
        sar = sar.addRange(6, 9);
        assertTrue(sar.containsRange(5, 20));
        assertEquals(16, sar.getCardinality());

        sar = new SortedRangesShort(2, 3);
        sar = sar.add(5);
        sar = sar.add(10);
        assertTrue(sar.contains(5));
        assertTrue(sar.contains(10));
        assertEquals(2, sar.getCardinality());
        sar = sar.addRange(6, 9);
        assertTrue(sar.containsRange(5, 10));
        assertEquals(6, sar.getCardinality());

        sar = new SortedRangesShort(2, 5);
        sar = sar.add(5);
        sar = sar.add(10);
        sar = sar.add(15);
        assertTrue(sar.contains(5));
        assertTrue(sar.contains(10));
        assertTrue(sar.contains(15));
        assertEquals(3, sar.getCardinality());
        sar = sar.addRange(6, 13);
        assertTrue(sar.containsRange(5, 13));
        assertTrue(sar.contains(15));
        assertEquals(10, sar.getCardinality());

        sar = new SortedRangesShort(2, 5);
        sar = sar.addRange(10, 20);
        sar = sar.add(30);
        assertTrue(sar.containsRange(10, 20));
        assertTrue(sar.contains(30));
        assertEquals(12, sar.getCardinality());
        sar = sar.addRange(5, 29);
        assertTrue(sar.containsRange(5, 30));
        assertEquals(26, sar.getCardinality());
    }

    @Test
    public void testSimpleRemove() {
        final SortedRanges[] sars = new SortedRanges[] {
                new SortedRangesLong(2),
                new SortedRangesInt(2, 1),
                new SortedRangesShort(2, 1)};
        for (SortedRanges sar : sars) {
            TLongArrayList vs = new TLongArrayList(128);
            long rangeStart = 1;
            long rangeEnd = 2;
            long cardinality = 0;
            final long[] rangeSteps = new long[] {0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
            for (int i = 0; i < rangeSteps.length; ++i) {
                final long step = rangeSteps[i];
                final String m = "i==" + i;
                for (long v = rangeStart; v <= rangeEnd; ++v) {
                    assertFalse(m + ",v==" + v, sar.contains(v));
                }
                sar = sar.addRange(rangeStart, rangeEnd);
                cardinality += rangeEnd - rangeStart + 1;
                assertEquals(m, cardinality, sar.getCardinality());
                assertNotNull(m, sar);
                for (long v = rangeStart; v <= rangeEnd; ++v) {
                    vs.add(v);
                    assertTrue(m + ", v==" + v, sar.contains(v));
                }
                rangeStart = rangeEnd + 2 + step;
                rangeEnd = rangeStart + 1 + step;
            }

            // Cover removing non-existing above and below.
            sar = sar.remove(0);
            assertNotNull(sar);
            assertEquals(cardinality, sar.getCardinality());
            sar.validate();
            sar = sar.remove(1000000);
            assertNotNull(sar);
            assertEquals(cardinality, sar.getCardinality());
            sar.validate();

            Shuffle.shuffleArray(new Random(seed0), vs);
            for (int i = 0; i < vs.size(); ++i) {
                final long v = vs.get(i);
                sar = sar.remove(v);
                assertNotNull(sar);
                final String m = "v==" + v;
                assertFalse(m, sar.contains(v));
                --cardinality;
                assertEquals(m, cardinality, sar.getCardinality());
            }

            assertEquals(0, sar.getCardinality());
            sar.remove(1000);
            assertNotNull(sar);
            sar.validate();
        }
    }

    @Test
    public void testRemoveSingleRange() {
        final long start = 2;
        for (long rlen = 1; rlen <= 7; ++rlen) {
            final long end = start + rlen - 1;
            for (long v = 0; v < start + rlen + 1; ++v) {
                final String m = "rlen==" + rlen + ", v==" + v;
                SortedRanges sar = SortedRanges.makeSingleRange(start, end);
                sar = sar.tryCompact(4);
                long cardinality = end - start + 1;
                assertEquals(m, cardinality, sar.getCardinality());
                sar.validate();
                sar = sar.remove(v);
                assertNotNull(m, sar);
                final boolean contained = start <= v && v <= end;
                if (contained) {
                    --cardinality;
                }
                assertEquals(m, cardinality, sar.getCardinality());
                sar.validate();
            }
        }
    }

    @Test
    public void testRemoveSingles() {
        SortedRanges sar = new SortedRangesLong(2);
        sar.add(1);
        assertNotNull(sar);
        sar.add(3);
        assertNotNull(sar);
        sar.remove(2);
        assertEquals(2, sar.getCardinality());
        assertTrue(sar.contains(1));
        assertTrue(sar.contains(3));
        assertNotNull(sar);

        final long[] vs = new long[] {1, 3, 5, 7, 9};
        final long[] nonvs = new long[] {2, 4, 6, 8};
        sar = new SortedRangesLong(2);
        long cardinality = 0;
        for (long v : vs) {
            sar = sar.add(v);
            assertNotNull(sar);
            assertTrue(sar.contains(v));
            ++cardinality;
            sar.validate();
            assertEquals(cardinality, sar.getCardinality());
        }

        for (long v : nonvs) {
            assertFalse(sar.contains(v));
            sar = sar.remove(v);
            assertNotNull(sar);
            assertFalse(sar.contains(v));
            sar.validate();
            assertEquals(cardinality, sar.getCardinality());
        }

        Shuffle.shuffleArray(new Random(seed0), vs);
        for (long v : vs) {
            sar = sar.remove(v);
            assertNotNull(sar);
            assertFalse(sar.contains(v));
            --cardinality;
            sar.validate();
            assertEquals(cardinality, sar.getCardinality());
        }
    }

    @Test
    public void testRemoveRangeSimple() {
        SortedRanges sar = SortedRanges.makeSingleRange(1, 19);
        sar = sar.removeRange(0, 1);
        assertTrue(sar.containsRange(2, 9));
        assertEquals(18, sar.getCardinality());
        sar = sar.removeRange(2, 2);
        assertTrue(sar.containsRange(3, 19));
        assertEquals(17, sar.getCardinality());
        sar = sar.removeRange(2, 4);
        assertTrue(sar.containsRange(5, 19));
        assertEquals(15, sar.getCardinality());
        sar = sar.removeRange(5, 7);
        assertTrue(sar.containsRange(8, 19));
        assertEquals(12, sar.getCardinality());
        sar = sar.removeRange(9, 10);
        assertTrue(sar.contains(8));
        assertTrue(sar.containsRange(11, 19));
        assertEquals(10, sar.getCardinality());
        sar = sar.removeRange(8, 19);
        assertTrue(sar.isEmpty());
        assertEquals(0, sar.getCardinality());

        sar = new SortedRangesLong(2);
        sar = sar.add(3);
        sar = sar.addRange(18, 19);
        assertTrue(sar.contains(3));
        assertTrue(sar.containsRange(18, 19));
        assertEquals(3, sar.getCardinality());
        sar = sar.removeRange(19, 23);
        assertTrue(sar.contains(3));
        assertTrue(sar.contains(18));
        assertFalse(sar.contains(19));
        assertEquals(2, sar.getCardinality());
        sar = sar.removeRange(18, 18);
        assertTrue(sar.contains(3));
        assertFalse(sar.contains(18));
        assertEquals(1, sar.getCardinality());

        sar = new SortedRangesLong(2);
        sar = sar.add(3);
        sar = sar.add(5);
        sar = sar.add(7);
        sar = sar.add(9);
        sar = sar.add(11);
        assertEquals(5, sar.getCardinality());
        sar = sar.removeRange(4, 10);
        assertFalse(sar.contains(5) || sar.contains(7) || sar.contains(9));
        assertEquals(2, sar.getCardinality());

        sar = new SortedRangesLong(2);
        sar = sar.addRange(2, 4);
        sar = sar.addRange(5, 6);
        sar = sar.addRange(8, 9);
        sar = sar.addRange(11, 12);
        assertEquals(9, sar.getCardinality());
        sar = sar.removeRange(8, 13);
        assertFalse(sar.contains(9));
        assertFalse(sar.contains(11));
        assertFalse(sar.contains(12));
        assertEquals(5, sar.getCardinality());
        sar = sar.add(9);
        sar = sar.add(11);
        sar = sar.add(13);
        assertEquals(8, sar.getCardinality());
        sar = sar.removeRange(11, 14);
        assertFalse(sar.contains(11));
        assertFalse(sar.contains(13));
        assertEquals(6, sar.getCardinality());

        sar = new SortedRangesLong(2);
        sar = sar.addRange(2, 5);
        assertTrue(sar.containsRange(2, 5));
        assertEquals(4, sar.getCardinality());
        sar = sar.removeRange(4, 4);
        assertTrue(sar.containsRange(2, 3));
        assertTrue(sar.contains(5));
        assertEquals(3, sar.getCardinality());
    }

    private static final long[] sar0vs = new long[] {2, -4, 6, -12, 20, 22, -23, 25, 27, 30, -31, 40, -60};

    private static long[][] vs2segments(final long[] vs) {
        final ArrayList<long[]> buf = new ArrayList<>();
        long pendingStart = -1;
        for (int i = 0; i < vs.length; ++i) {
            final long v = vs[i];
            if (v < 0) {
                buf.add(new long[] {pendingStart, -v});
                pendingStart = -1;
            } else {
                if (pendingStart != -1) {
                    buf.add(new long[] {pendingStart, pendingStart});
                }
                pendingStart = v;
            }
        }
        if (pendingStart != -1) {
            buf.add(new long[] {pendingStart, pendingStart});
        }
        return buf.toArray(new long[buf.size()][]);
    }

    private static final long[][] segments0 = vs2segments(sar0vs);

    private static SortedRanges makeSortedArray0() {
        return vs2sar(sar0vs);
    }

    private static SortedRanges makeSortedArray(final long[][] segments) {
        SortedRanges sar = new SortedRangesLong(2);
        long card = 0;
        for (long[] segment : segments) {
            final long s = segment[0];
            final long e = segment[1];
            card += e - s + 1;
            sar = sar.addRange(s, e);
        }
        assertEquals(card, sar.getCardinality());
        return sar;
    }

    @Test
    public void testRemoveRange() {
        final SortedRanges sar = makeSortedArray0();
        long card = sar.getCardinality();

        final long[][] removals = new long[][] {
                /* new long[]{ start, end, intersectionCard }, */
                new long[] {0, 7, 5},
                new long[] {0, 0, 0},
                new long[] {0, 1, 0},
                new long[] {61, 61, 0},
                new long[] {61, 62, 0},
                new long[] {2, 3, 2},
                new long[] {4, 5, 1},
                new long[] {29, 30, 1},
                new long[] {30, 30, 1},
                new long[] {31, 31, 1},
                new long[] {31, 32, 1},
                new long[] {30, 40, 3},
                new long[] {31, 41, 3},
                new long[] {60, 60, 1},
                new long[] {11, 15, 2},
                new long[] {4, 6, 2},
                new long[] {4, 11, 7},
                new long[] {12, 22, 3},
                new long[] {23, 30, 4},
                new long[] {42, 44, 3},
                new long[] {3, 3, 1},
                new long[] {12, 12, 1},
        };
        for (int i = 0; i < removals.length; ++i) {
            long[] removal = removals[i];
            SortedRanges sar2 = sar.deepCopy();
            final long rs = removal[0];
            final long re = removal[1];
            sar2 = sar2.removeRange(rs, re);
            final String m = "i==" + i + " && rs==" + rs + " && re==" + re;
            assertEquals(m, card - removal[2], sar2.getCardinality());
            for (long[] segment : segments0) {
                for (long v = segment[0]; v <= segment[1]; ++v) {
                    final String m2 = m + " && v==" + v;
                    if (rs <= v && v <= re) {
                        assertFalse(m2, sar2.contains(v));
                    } else {
                        assertTrue(m2, sar2.contains(v));
                    }
                }
            }
        }
    }

    @Test
    public void testIterator() {
        final SortedRanges sar = makeSortedArray0();
        final RowSet.Iterator it = sar.getIterator();
        for (int i = 0; i < segments0.length; ++i) {
            final long[] segment = segments0[i];
            final long s = segment[0];
            final long e = segment[1];
            for (long v = s; v <= e; ++v) {
                assertTrue(it.hasNext());
                final long itv = it.nextLong();
                assertEquals(v, itv);
            }
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorCases() {
        final long[] values = new long[] {3, 4, 5, 7};

        SortedRanges sar = new SortedRangesLong(2);
        for (long v : values) {
            sar = sar.add(v);
        }
        final RowSet.Iterator rit = sar.getIterator();
        for (long v : values) {
            assertTrue(rit.hasNext());
            assertEquals(v, rit.nextLong());
        }
        assertFalse(rit.hasNext());
    }

    @Test
    public void testRangeIterator() {
        final SortedRanges sar = makeSortedArray0();
        final RowSet.RangeIterator rit = sar.getRangeIterator();
        for (int i = 0; i < segments0.length; ++i) {
            final long[] segment = segments0[i];
            assertTrue(rit.hasNext());
            rit.next();
            final long s = segment[0];
            final long e = segment[1];
            assertEquals(s, rit.currentRangeStart());
            assertEquals(e, rit.currentRangeEnd());
        }
        assertFalse(rit.hasNext());
    }

    @Test
    public void testRangeIteratorSingle() {
        SortedRanges sar2 = new SortedRangesLong(2);
        sar2 = sar2.add(10);
        final RowSet.RangeIterator rit2 = sar2.getRangeIterator();
        assertTrue(rit2.hasNext());
        rit2.next();
        assertEquals(10, rit2.currentRangeStart());
        assertEquals(10, rit2.currentRangeEnd());
        assertFalse(rit2.hasNext());
    }

    @Test
    public void testRangeIteratorAdvance() {
        // on an empty one first.
        final SortedRanges emptySar = new SortedRangesLong(2);
        final RowSet.RangeIterator itOnEmpty = emptySar.getRangeIterator();
        assertFalse(itOnEmpty.advance(1));
        assertFalse(itOnEmpty.hasNext());

        final SortedRanges sar = makeSortedArray0();
        final RowSet.RangeIterator rit = sar.getRangeIterator();
        final RowSet.Iterator it = sar.getIterator();
        final long first = sar.first();
        assertEquals(2, first);
        final long last = sar.last();
        assertEquals(60, last);
        long curr = it.nextLong();
        long next = it.nextLong();
        for (long v = first - 1; v <= last; ++v) {
            final String m = "v==" + v;
            boolean b = rit.advance(v);
            while (v > curr) {
                curr = next;
                if (it.hasNext()) {
                    next = it.next();
                } else {
                    next = -1;
                }
            }
            assertEquals(m, curr != -1, b);
            assertEquals(m, curr, rit.currentRangeStart());
        }
    }

    @Test
    public void testRangeIteratorAdvanceCases() {
        SortedRanges sar = SortedRanges.makeSingleRange(1, 2);
        sar.addRange(4, 5);
        try (RowSet.RangeIterator rit = sar.getRangeIterator()) {
            assertFalse(rit.advance(6));
            assertFalse(rit.hasNext());
        }
        try (RowSet.RangeIterator rit = sar.getRangeIterator()) {
            assertTrue(rit.hasNext());
            rit.next();
            assertTrue(rit.hasNext());
            rit.next();
            assertFalse(rit.advance(6));
            assertFalse(rit.hasNext());
        }
        sar = sar.add(7);
        try (RowSet.RangeIterator rit = sar.getRangeIterator()) {
            assertTrue(rit.advance(7));
            assertFalse(rit.hasNext());
        }
    }

    @Test
    public void testSearchIterator() {
        final SortedRanges sar = makeSortedArray0();
        final RowSet.SearchIterator sit = sar.getSearchIterator();
        sar.forEachLong((final long v) -> {
            final String m = "v==" + v;
            assertTrue(m, sit.hasNext());
            assertEquals(m, v, sit.nextLong());
            return true;
        });
    }

    @Test
    public void testContainsRangeCases() {
        SortedRanges sar = new SortedRangesLong();
        assertFalse(sar.containsRange(1, 1));
        sar = sar.add(6);
        sar = sar.add(9);
        assertFalse(sar.containsRange(1, 1));
        assertTrue(sar.containsRange(6, 6));
        assertFalse(sar.containsRange(9, 10));
        assertFalse(sar.containsRange(6, 7));
    }

    @Test
    public void testAppendCases() {
        SortedRanges sar = new SortedRangesLong();
        sar = sar.append(5);
        assertTrue(sar.contains(5));
        assertEquals(1, sar.getCardinality());
        sar = sar.append(6);
        assertTrue(sar.containsRange(5, 6));
        assertEquals(2, sar.getCardinality());
        sar = sar.append(7);
        assertTrue(sar.containsRange(5, 7));
        assertEquals(3, sar.getCardinality());
        sar = sar.append(9);
        assertTrue(sar.containsRange(5, 7));
        assertTrue(sar.contains(9));
        assertEquals(4, sar.getCardinality());
        boolean threw = false;
        try {
            sar = sar.append(9);
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        assertTrue(threw);
        assertTrue(sar.containsRange(5, 7));
        assertTrue(sar.contains(9));
        assertEquals(4, sar.getCardinality());
        sar = new SortedRangesShort(4, 1L);
        sar = sar.add(1);
        sar = sar.appendRange(2, 4);
        assertEquals(4, sar.getCardinality());
        assertTrue(sar.containsRange(1, 4));
    }

    @Test
    public void testAppendRangesCases() {
        SortedRanges sar = new SortedRangesLong();
        sar = sar.appendRange(5, 6);
        assertTrue(sar.containsRange(5, 6));
        assertEquals(2, sar.getCardinality());
        sar = sar.appendRange(7, 8);
        assertTrue(sar.containsRange(5, 8));
        assertEquals(4, sar.getCardinality());
        sar = sar.appendRange(10, 11);
        assertTrue(sar.containsRange(5, 8));
        assertTrue(sar.containsRange(10, 11));
        assertEquals(6, sar.getCardinality());
        sar = new SortedRangesLong();
        sar = sar.append(3);
        sar = sar.appendRange(4, 5);
        assertTrue(sar.containsRange(3, 5));
        assertEquals(3, sar.getCardinality());
        sar = new SortedRangesLong();
        sar = sar.append(3);
        sar = sar.appendRange(5, 6);
        assertTrue(sar.contains(3));
        assertTrue(sar.containsRange(5, 6));
        assertEquals(3, sar.getCardinality());
        boolean threw = false;
        try {
            sar = sar.appendRange(6, 7);
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        assertTrue(threw);
        assertTrue(sar.contains(3));
        assertTrue(sar.containsRange(5, 6));
        assertEquals(3, sar.getCardinality());
    }


    private static final boolean print = false;

    private static void compare(final String msgPfx, final TLongSet set, final SortedRanges arr) {
        final boolean debug = true;
        final long[] minmax = new long[2];
        final int min = 0;
        final int max = 1;
        minmax[min] = Long.MAX_VALUE;
        minmax[max] = Long.MIN_VALUE;
        if (arr.getCardinality() == 0 && set.size() == 0) {
            return;
        }
        if (print) {
            System.out.println(ValidationSet.set2str("s = ", set));
            System.out.println("a = " + arr);
        }
        // noinspection ConstantConditions
        if (debug) {
            final long[] vs = new long[set.size()];
            set.toArray(vs);
            Arrays.sort(vs);
            for (int j = 0; j < vs.length; ++j) {
                final long v = vs[j];
                final boolean c = arr.contains(v);
                if (!c) {
                    // noinspection ConstantConditions
                    assertTrue(msgPfx + " && v==" + v + " && j==" + j + " && vslen==" + vs.length +
                            ", node.size()==" + arr.getCardinality(), c);
                }
            }
            minmax[min] = vs[0];
            minmax[max] = vs[vs.length - 1];
        } else {
            set.forEach(v -> {
                if (v < minmax[min]) {
                    minmax[min] = v;
                }
                if (v > minmax[max]) {
                    minmax[max] = v;
                }
                assertTrue(msgPfx + " && v==" + v, arr.contains(v));
                return true;
            });
        }
        if (set.size() != arr.getCardinality()) {
            System.out.println("set.size() = " + set.size() + ", arr.size() = " + arr.getCardinality());
            long lastv = -2;
            int iti = 0;
            final RowSet.RangeIterator it = arr.getRangeIterator();
            while (it.hasNext()) {
                it.next();
                ++iti;
                final long s = it.currentRangeStart();
                final long e = it.currentRangeEnd();
                final String imsg = msgPfx + " && iti==" + iti + " && s==" + s + " && e==" + e;
                for (long v = s; v <= e; ++v) {
                    assertTrue(imsg + " v==" + v, set.contains(v));
                }
                assertTrue(imsg + ", lastv==" + lastv, s - lastv >= 1);
                lastv = e;
            }
            assertEquals(msgPfx, set.size(), arr.getCardinality());
        }
        assertEquals(msgPfx, minmax[min], arr.first());
        assertEquals(msgPfx, minmax[max], arr.last());
    }

    private static SortedRanges populateRandom(
            final String pfxMsg, final Random r, final int count, final int min, final int max,
            final int clusterWidth, final int jumpPropOneIn, final TLongSet set,
            @SuppressWarnings("SameParameterValue") final boolean check) {
        assertEquals(0, set.size());
        SortedRanges sar = new SortedRangesLong(2);
        final int halfClusterWidth = clusterWidth / 2;
        final int d = max - min + 1 - clusterWidth;
        int cluster1Mid = min + halfClusterWidth;
        for (int i = 0; i < count; ++i) {
            final long k;
            if (r.nextInt(jumpPropOneIn) == 0) {
                k = cluster1Mid = halfClusterWidth + r.nextInt(d);
            } else {
                k = cluster1Mid + r.nextInt(halfClusterWidth);
            }
            final SortedRanges ans = sar.add(k);
            if (ans == null) {
                break;
            }
            sar = ans;
            set.add(k);
            compare(pfxMsg + " && i==" + i, set, sar);
        }
        if (check) {
            compare(pfxMsg, set, sar);
        }
        return sar.tryCompact(4);
    }

    private static void doTestRuns(IntConsumer testFun) {
        for (int i = 0; i < runs; ++i) {
            testFun.accept(seed0 + i);
        }
    }

    @Test
    public void testGetFind() {
        doTestRuns(SortedRangesTest::doTestGetFind);
    }

    private static void doTestGetFind(final int seed) {
        final int count = SortedRanges.INT_DENSE_MAX_CAPACITY - 1;
        final TLongSet set = ValidationSet.make(count);
        final String pfxMsg = "doTestGetFind seed == " + seed;
        System.out.println(pfxMsg);
        final SortedRanges sar = populateRandom(
                pfxMsg, new Random(seed), count, 10, 3000000, 150, 50,
                set, true);
        assertEquals(set.size(), sar.getCardinality());
        final long[] arr = new long[set.size()];
        set.toArray(arr);
        Arrays.sort(arr);

        for (int i = 0; i < arr.length; ++i) {
            long vi = arr[i];
            final long gi = sar.get(i);
            assertEquals("i == " + i, vi, gi);
        }
        assertEquals(-1, sar.get(arr.length));

        assertEquals(-1, sar.find(arr[0] - 1));
        for (int i = 0; i < arr.length - 1; ++i) {
            int j = i + 1;
            long vi = arr[i];
            final long posi = sar.find(vi);
            assertEquals("i == " + i, i, posi);
            long vj = arr[j];
            for (long v = vi + 1; v < vj; ++v) {
                final long pos = sar.find(v);
                assertEquals(pfxMsg + " i == " + i + " && v == " + v, -(i + 1) - 1, pos);
            }
        }
        assertEquals(arr.length - 1, sar.find(arr[arr.length - 1]));
        assertEquals(-arr.length - 1, sar.find(arr[arr.length - 1] + 1));
    }

    @Test
    public void testFindCases() {
        SortedRanges sr = new SortedRangesLong();
        sr = sr.addRange(5, 9);
        assertEquals(~5, sr.find(10));
    }

    private static void searchRangeCheck(
            final String msg, final long start, final long end, final long value, final long prev, final long result) {
        if (value < start) {
            assertEquals(msg, prev, result);
        } else if (value <= end) {
            assertEquals(msg, value, result);
        } else {
            assertEquals(msg, end, result);
        }
    }

    @Test
    public void testSearchIteratorBinarySearch() {
        SortedRanges sar = makeSortedArray0();
        final RowSet.SearchIterator sit = sar.getSearchIterator();
        long prev = -1L;
        final MutableLong mutVal = new MutableLong(-1L);
        final RowSet.TargetComparator comp =
                (final long key, final int dir) -> Long.signum(dir * (mutVal.getValue() - key));
        for (long[] segment : segments0) {
            final long start = segment[0];
            final long end = segment[1];
            final String m = "start==" + start + " && end==" + end;
            for (long v = start - 1; v <= end + 1; ++v) {
                final String m2 = m + " && v==" + v;
                mutVal.setValue(v);
                long result = sit.binarySearchValue(comp, 1);
                searchRangeCheck(m2, start, end, v, prev, result);
                final RowSet.SearchIterator brandNewIter = sar.getSearchIterator();
                result = brandNewIter.binarySearchValue(comp, 1);
                searchRangeCheck(m2, start, end, v, prev, result);
                final boolean expectedHasNext = v < sar.last();
                assertEquals(expectedHasNext, sit.hasNext());
                assertEquals(expectedHasNext, brandNewIter.hasNext());
                prev = sit.currentValue();
            }
        }
    }

    @Test
    public void testSearchIteratorBinarySearchCases() {
        SortedRanges sar = new SortedRangesLong(2);

        // search for last when single value is final entry
        sar.appendRange(4, 10);
        sar.append(25);
        sar.append(32);
        try (final RowSet.SearchIterator sit = sar.getSearchIterator()) {
            final long v = sar.last();
            final RowSet.TargetComparator comp =
                    (final long key, final int dir) -> Long.signum(dir * (v - key));
            final long r = sit.binarySearchValue(comp, 1);
            assertEquals(v, r);
        }

        // search for last when a range is the final entry
        sar.clear();
        sar.appendRange(4, 10);
        sar.appendRange(25, 32);
        try (final RowSet.SearchIterator sit = sar.getSearchIterator()) {
            final long v = sar.last();
            final RowSet.TargetComparator comp =
                    (final long key, final int dir) -> Long.signum(dir * (v - key));
            final long r = sit.binarySearchValue(comp, 1);
            assertEquals(v, r);
        }

        // search for value in the final range when a range is the final entry
        sar.clear();
        sar.appendRange(4, 10);
        sar.appendRange(25, 32);
        try (final RowSet.SearchIterator sit = sar.getSearchIterator()) {
            final long v = sar.last() - 1;
            final RowSet.TargetComparator comp =
                    (final long key, final int dir) -> Long.signum(dir * (v - key));
            final long r = sit.binarySearchValue(comp, 1);
            assertEquals(v, r);
        }
    }

    @Test
    public void testReverseIterator() {
        SortedRanges sar = makeSortedArray0();
        final long[] values = new long[(int) sar.getCardinality()];
        int i = 0;
        try (final RowSet.Iterator it = sar.getIterator()) {
            while (it.hasNext()) {
                final long v = it.nextLong();
                values[i++] = v;
            }
        }
        try (final RowSet.SearchIterator rit = sar.getReverseIterator()) {
            while (--i >= 0) {
                final String m = "i==" + i;
                assertTrue(m, rit.hasNext());
                final long v = rit.nextLong();
                assertEquals(m, values[i], v);
            }
            assertFalse(rit.hasNext());
        }
    }

    @Test
    public void testEmptyIteratorCases() {
        SortedRanges sar = new SortedRangesLong(2);
        try (final RowSet.Iterator it = sar.getIterator()) {
            assertFalse(it.hasNext());
        }
        try (final RowSet.RangeIterator it = sar.getRangeIterator()) {
            assertFalse(it.hasNext());
        }
        try (final RowSet.SearchIterator it = sar.getSearchIterator()) {
            assertFalse(it.hasNext());
        }
        try (final RowSet.SearchIterator it = sar.getReverseIterator()) {
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void testForEachLong() {
        SortedRanges sar = makeSortedArray0();
        try (final RowSet.Iterator it = sar.getIterator()) {
            sar.forEachLong((final long v) -> {
                assertTrue(it.hasNext());
                assertEquals(it.nextLong(), v);
                return true;
            });
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void testForEachLongRange() {
        SortedRanges sar = makeSortedArray0();
        try (final RowSet.RangeIterator it = sar.getRangeIterator()) {
            sar.forEachLongRange((final long start, final long end) -> {
                assertTrue(it.hasNext());
                it.next();
                final long itStart = it.currentRangeStart();
                final long itEnd = it.currentRangeEnd();
                assertEquals(start, itStart);
                assertEquals(end, itEnd);
                return true;
            });
            assertFalse(it.hasNext());
        }
    }

    private long cardFromValues(final SortedRanges sr) {
        final MutableLong card = new MutableLong(0);
        sr.forEachLongRange((final long start, final long end) -> {
            card.add(end - start + 1);
            return true;
        });
        return card.longValue();
    }

    @Test
    public void testGetKeysForPositions() {
        final SortedRanges sar = makeSortedArray0();
        final long[] positions = new long[] {1, 3, 5, 7, 8, 9, 14, 15, 20, 36};
        PrimitiveIterator.OfLong iterator = Arrays.stream(positions).iterator();
        final MutableInt mi = new MutableInt(0);
        sar.getKeysForPositions(iterator, new LongConsumer() {
            @Override
            public void accept(final long v) {
                final int miValue = mi.intValue();
                final String m = "v==" + v + ", miValue==" + miValue;
                assertEquals(m, sar.get(positions[miValue]), v);
                mi.increment();
            }
        });
        assertEquals(positions.length, mi.intValue());
    }

    private static class IterOfLongAdaptor implements PrimitiveIterator.OfLong {
        private final TLongIterator it;

        public IterOfLongAdaptor(final TLongIterator it) {
            this.it = it;
        }

        @Override
        public long nextLong() {
            return it.next();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }
    }

    @Test
    public void testGetKeysForPositionsTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY}) {
                SortedRanges msar = populateRandom2(
                        null, rand, count, 0.33, 1L << 33, 11, 15, 1, 6);
                if (rand.nextBoolean()) {
                    msar = msar.tryCompact(4);
                }
                final SortedRanges sar = msar;
                final double p = 0.1;
                final TLongArrayList positions = new TLongArrayList((int) Math.ceil(sar.getCardinality() * (p * 1.05)));
                for (int i = 0; i < sar.getCardinality(); ++i) {
                    if (rand.nextDouble() <= p) {
                        positions.add(i);
                    }
                }
                PrimitiveIterator.OfLong iterator = new IterOfLongAdaptor(positions.iterator());
                final MutableInt mi = new MutableInt(0);
                sar.getKeysForPositions(iterator, new LongConsumer() {
                    @Override
                    public void accept(final long v) {
                        final int miValue = mi.intValue();
                        final String m2 = m + " && v==" + v + ", miValue==" + miValue;
                        assertEquals(m2, sar.get(positions.get(miValue)), v);
                        mi.increment();
                    }
                });
                assertEquals(positions.size(), mi.intValue());
            }
        }
    }

    @Test
    public void testSubArrayByKeyTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            final int oneIn = 12;
            for (int d : new int[] {-1, 0, +1}) {
                final int count = SortedRanges.LONG_DENSE_MAX_CAPACITY + d;
                SortedRanges sar = populateRandom2(
                        null, rand, count, 0.25, 1L << 33, 11, 15, 1, 6);
                if (rand.nextBoolean()) {
                    sar = sar.tryCompact(4);
                }
                final long last = sar.last();
                final String m2 = m + " && count==" + count;
                for (long s = sar.first() - 1; s <= last + 1; ++s) {
                    if (rand.nextInt(oneIn) < oneIn - 1) {
                        continue;
                    }
                    for (long e = s; e <= last + 1; ++e) {
                        if (rand.nextInt(oneIn) < oneIn - 1) {
                            continue;
                        }
                        final String m3 = m2 + " && s==" + s + " && e==" + e;
                        final SortedRanges byIntersect = sar.deepCopy().retainRange(s, e);
                        final SortedRanges subSar = sar.subRangesByKey(s, e);
                        if (subSar == null) {
                            assertTrue(byIntersect == null || byIntersect.getCardinality() == 0L);
                            continue;
                        }
                        if (byIntersect == null) {
                            assertTrue(subSar == null || subSar.getCardinality() == 0L);
                            continue;
                        }
                        try {
                            SortedRanges.checkEquals(byIntersect, subSar);
                        } catch (Exception ex) {
                            fail(m3 + ", exception " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testRetainRange() {
        final SortedRanges sar = makeSortedArray0();
        final long last = sar.last();
        for (long s = sar.first() - 1; s <= last + 1; ++s) {
            for (long e = s; e <= last + 1; ++e) {
                final String m = "s==" + s + " && e==" + e;
                final SortedRanges bySubKeys = sar.subRangesByKey(s, e);
                final SortedRanges subSar = sar.deepCopy().retainRange(s, e);
                if (subSar == null) {
                    assertTrue(m, bySubKeys == null || 0L == bySubKeys.getCardinality());
                    continue;
                }
                if (bySubKeys == null) {
                    assertTrue(m, subSar == null || 0L == subSar.getCardinality());
                    continue;
                }
                SortedRanges.checkEquals(bySubKeys, subSar);
            }
        }
    }

    @Test
    public void testRetainRangeCases() {
        SortedRanges arr = SortedRanges.makeSingleRange(10, 20);
        arr = arr.retainRange(15, 21);
        assertEquals(6, arr.getCardinality());
        assertEquals(15, arr.first());
        assertEquals(20, arr.last());
        assertTrue(arr.containsRange(15, 20));
    }

    private static TLongArrayList subRangeByPos(final SortedRanges sar, final long startPos, final long endPos) {
        TLongArrayList a = new TLongArrayList();
        long pos = -1;
        try (final RowSet.Iterator it = sar.getIterator()) {
            while (it.hasNext()) {
                final long v = it.nextLong();
                ++pos;
                if (pos < startPos) {
                    continue;
                }
                if (pos > endPos) {
                    break;
                }
                a.add(v);
            }
        }
        return a;
    }

    @Test
    public void testSubArrayByPos() {
        final SortedRanges sar = makeSortedArray0();
        final long lastPos = sar.getCardinality() - 1;
        for (long s = 0; s <= lastPos + 1; ++s) {
            for (long e = s; e <= lastPos + 1; ++e) {
                final String m = "s==" + s + " && e==" + e;
                final TLongArrayList sarList = subRangeByPos(sar, s, e);
                final SortedRanges subSar = sar.subRangesByPos(s, e);
                if (subSar == null) {
                    assertEquals(m, sarList.size(), 0);
                    continue;
                }
                assertEquals(m, sarList.size(), subSar.getCardinality());
                int i = 0;
                try (final RowSet.Iterator itSubSar = subSar.getIterator()) {
                    while (itSubSar.hasNext()) {
                        final long sarValue = sarList.get(i);
                        final long subSarValue = itSubSar.nextLong();
                        assertEquals(m + " && i==" + i, sarValue, subSarValue);
                        i++;
                    }
                }
            }
        }
    }

    @Test
    public void testSubArrayByPosCases() {
        final SortedRanges sar = SortedRanges.makeSingleRange(2, 2);
        final SortedRanges subSar = sar.subRangesByPos(0, 3);
        assertEquals(1, subSar.getCardinality());
        assertEquals(2, subSar.get(0));
    }

    @Test
    public void testOverlapsRange() {
        final SortedRanges sar = makeSortedArray0();
        final boolean Y = true;
        final boolean N = false;
        final long[] starts = {0, 1, 1, 1, 1, 2, 3, 4, 4, 5, 12, 13, 13, 13, 61, 60, 60};
        final long[] ends = {1, 2, 3, 4, 5, 4, 4, 4, 5, 5, 19, 19, 20, 21, 63, 60, 61};
        final boolean[] exps = {N, Y, Y, Y, Y, Y, Y, Y, Y, N, Y, N, Y, Y, N, Y, Y};
        assertEquals(starts.length, ends.length);
        assertEquals(ends.length, exps.length);
        for (int i = 0; i < starts.length; ++i) {
            final long start = starts[i];
            final long end = ends[i];
            final boolean expectation = exps[i];
            final boolean ans = sar.overlapsRange(start, end);
            assertEquals(expectation, ans);
        }
    }

    @Test
    public void testOverlaps() {
        final SortedRanges sar = makeSortedArray0();
        final boolean Y = true;
        final boolean N = false;
        SortedRanges noOver = new SortedRangesLong(2);
        noOver = noOver.addRange(0, 1);
        noOver = noOver.add(5);
        noOver = noOver.add(21);
        noOver = noOver.add(26);
        noOver = noOver.addRange(28, 29);
        noOver = noOver.addRange(32, 39);
        final boolean ans = sar.overlaps(noOver.getRangeIterator());
        assertFalse(ans);
        SortedRanges sar2 = noOver.deepCopy().add(2);
        assertTrue(sar.overlaps(sar2.getRangeIterator()));
        sar2 = noOver.deepCopy().add(4);
        assertTrue(sar.overlaps(sar2.getRangeIterator()));
        sar2 = noOver.deepCopy().addRange(13, 20);
        assertTrue(sar.overlaps(sar2.getRangeIterator()));
        sar2 = noOver.deepCopy().addRange(31, 39);
        assertTrue(sar.overlaps(sar2.getRangeIterator()));
        sar2 = noOver.deepCopy().addRange(32, 40);
        assertTrue(sar.overlaps(sar2.getRangeIterator()));
        sar2 = noOver.deepCopy().add(60);
        assertTrue(sar.overlaps(sar2.getRangeIterator()));
    }

    @Test
    public void testRetain() {
        SortedRanges sar = makeSortedArray0();
        SortedRanges other = SortedRanges.makeSingleRange(4, 6);
        other.add(2);
        other.addRange(13, 15);
        other.addRange(17, 20);
        other.add(23);
        other.add(26);
        other.addRange(28, 32);
        other.addRange(41, 42);
        other.add(60);
        OrderedLongSet tix = sar.retain(other);
        assertTrue(tix instanceof SortedRanges);
        assertTrue(sar == tix);
        sar = (SortedRanges) tix;
        try (final RowSet.Iterator retainedIter = sar.getIterator()) {
            while (retainedIter.hasNext()) {
                final long v = retainedIter.nextLong();
                assertTrue(sar.contains(v));
                assertTrue(other.contains(v));
            }
        }
        try (final RowSet.Iterator sarIter = sar.getIterator()) {
            while (sarIter.hasNext()) {
                final long v = sarIter.nextLong();
                assertEquals(other.contains(v), sar.contains(v));
            }
        }
    }

    @Test
    public void testReverseIteratorAdvance() {
        final SortedRanges sar = makeSortedArray0();
        final RowSet.RangeIterator forwardRangeIter = sar.getRangeIterator();
        long prevLast = -1;
        while (forwardRangeIter.hasNext()) {
            forwardRangeIter.next();
            final long start = forwardRangeIter.currentRangeStart();
            final long end = forwardRangeIter.currentRangeEnd();
            for (long v = start - 1; v <= end + 1; ++v) {
                if (v < 0) {
                    continue;
                }
                final String m = " && v==" + v;
                final RowSet.SearchIterator reverseIter = sar.getReverseIterator();
                final boolean valid = reverseIter.advance(v);
                if (start <= v && v <= end) {
                    assertTrue(m, valid);
                    assertEquals(m, v, reverseIter.currentValue());
                } else if (v == start - 1) {
                    if (prevLast == -1) {
                        assertFalse(m, valid);
                    } else {
                        assertTrue(m, valid);
                        assertEquals(m, prevLast, reverseIter.currentValue());
                    }
                } else { // v == end + 1
                    assertTrue(m, valid);
                    assertEquals(m, end, reverseIter.currentValue());
                }
            }
            prevLast = end;
        }
    }

    @Test
    public void testReverseIteratorAdvanceCases() {
        SortedRanges sar = new SortedRangesLong(2);
        RowSet.SearchIterator reverseIter = sar.getReverseIterator();
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());
        sar.addRange(3, 4);
        reverseIter = sar.getReverseIterator();
        assertTrue(reverseIter.hasNext());
        reverseIter.next();
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());
        reverseIter = sar.getReverseIterator();
        assertTrue(reverseIter.hasNext());
        reverseIter.next();
        assertTrue(reverseIter.hasNext());
        reverseIter.next();
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());

        sar = SortedRanges.makeSingleRange(2, 2);
        reverseIter = sar.getReverseIterator();
        assertTrue(reverseIter.hasNext());
        reverseIter.next();
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());

        sar = SortedRanges.makeSingleRange(3, 5);
        reverseIter = sar.getReverseIterator();
        assertTrue(reverseIter.advance(6));
        assertTrue(reverseIter.hasNext());
        assertTrue(reverseIter.advance(5));
        assertTrue(reverseIter.hasNext());
        assertTrue(reverseIter.advance(4));
        assertTrue(reverseIter.hasNext());
        assertTrue(reverseIter.advance(3));
        assertFalse(reverseIter.hasNext());
        assertFalse(reverseIter.advance(2));
        assertFalse(reverseIter.hasNext());
    }

    @Test
    public void testIntersect() {
        final SortedRanges sar = makeSortedArray0();
        SortedRanges other = SortedRanges.makeSingleRange(4, 6);
        other.add(2);
        other.addRange(13, 15);
        other.addRange(17, 20);
        other.add(23);
        other.add(26);
        other.addRange(28, 32);
        other.addRange(41, 42);
        other.add(60);
        final SortedRanges sarCopy = sar.deepCopy();
        final OrderedLongSet intersect = sarCopy.intersectOnNew(other);
        // Ensure sarCopy wasn't modified.
        assertEquals(sar.getCardinality(), sarCopy.getCardinality());
        try (final RowSet.Iterator sarIter = sar.getIterator();
                final RowSet.Iterator sarCopyIter = sarCopy.getIterator()) {
            while (sarIter.hasNext()) {
                assertTrue(sarCopyIter.hasNext());
                assertEquals(sarIter.nextLong(), sarCopyIter.nextLong());
            }
            assertFalse(sarCopyIter.hasNext());
        }
        try (final RowSet.Iterator intersectIter = intersect.ixIterator()) {
            while (intersectIter.hasNext()) {
                final long v = intersectIter.nextLong();
                assertTrue(sar.contains(v));
                assertTrue(other.contains(v));
            }
        }
        long card = 0;
        try (final RowSet.Iterator sarIter = sar.getIterator()) {
            while (sarIter.hasNext()) {
                final long v = sarIter.nextLong();
                final boolean inOther = other.contains(v);
                if (inOther) {
                    ++card;
                }
                assertEquals(inOther, intersect.ixContainsRange(v, v));
            }
        }
        assertEquals(card, intersect.ixCardinality());
    }

    @Test
    public void testSubsetOf() {
        final SortedRanges otherSar = makeSortedArray0();
        SortedRanges sar = SortedRanges.makeSingleRange(19, 19);
        assertFalse(sar.subsetOf(otherSar.getRangeIterator()));
        sar = SortedRanges.makeSingleRange(20, 20);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.addRange(40, 42);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.add(12);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.addRange(45, 46);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.add(58);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.add(59);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.add(57);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.add(60);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.removeRange(57, 59);
        assertTrue(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.add(32);
        assertFalse(sar.subsetOf(otherSar.getRangeIterator()));
        sar = sar.remove(32);
        sar = sar.add(29);
        assertFalse(sar.subsetOf(otherSar.getRangeIterator()));
    }

    @Test
    public void testInvertRange() {
        for (int vsi = 0; vsi < vss.length; ++vsi) {
            final String m = "vsi==" + vsi;
            final SortedRanges sar = vs2sar(vss[vsi]);
            final RowSet.RangeIterator rit = sar.getRangeIterator();
            long pos = 0;
            while (rit.hasNext()) {
                rit.next();
                final long start = rit.currentRangeStart();
                final long end = rit.currentRangeEnd();
                for (long s = start; s <= end; ++s) {
                    for (long e = s; e <= end; ++e) {
                        final String m2 = m + " && s==" + s + " && e==" + e;
                        final long spos = pos + s - start;
                        final long epos = pos + e - start;
                        for (long maxPos = spos - 1; maxPos <= epos + 1; ++maxPos) {
                            if (maxPos < 0) {
                                continue;
                            }
                            final String m3 = m2 + " && maxPos==" + maxPos;
                            final OrderedLongSet r = sar.invertRangeOnNew(s, e, maxPos);
                            if (maxPos < spos) {
                                assertTrue(m3, r.ixIsEmpty());
                                continue;
                            }
                            assertFalse(m3, r.ixIsEmpty());
                            assertEquals(m3, spos, r.ixFirstKey());
                            final long re = Math.min(epos, maxPos);
                            assertEquals(m3, re, r.ixLastKey());
                            assertEquals(m3, re - spos + 1, r.ixCardinality());
                        }
                    }
                }
                pos += end - start + 1;
            }
        }
    }

    private static final long[] vs0 = new long[] {3, 6, 7, 9, 12, 20, 23, 40, 41, 51, 52, 53, 54, 55, 56, 57, 58, 59};
    private static final long[] vs1 = new long[] {1, 5, 32, 39};
    private static final long[] vs2 = new long[] {1, 2, 5, 13, 61};
    private static final long[] vs3 = new long[] {27, 28, 29, 30, 39, 40, 41, 59, 60, 61, 62};
    private static final long[][] vss = new long[][] {sar0vs, vs0, vs1, vs2, vs3};

    private static void checkInvert(
            final String prefixMsg, final SortedRanges sar, final RowSet.RangeIterator ixrit,
            final RowSet.Iterator ixit,
            final long maxPosition) {
        final OrderedLongSetBuilderSequential b = new OrderedLongSetBuilderSequential();
        final boolean r = sar.invertOnNew(ixrit, b, maxPosition);
        final String m = "maxPosition==" + maxPosition;
        assertTrue(m, r);
        final RowSet rix = new WritableRowSetImpl(b.getOrderedLongSet());
        final RowSet.Iterator rit = rix.iterator();
        while (ixit.hasNext()) {
            final long ixv = ixit.nextLong();
            final long findResult = sar.find(ixv);
            if (findResult > maxPosition) {
                break;
            }
            assertTrue(rit.hasNext());
            final long pos = rit.nextLong();
            assertEquals(prefixMsg + "ixv==" + ixv, findResult, pos);
        }
        assertFalse(rit.hasNext());
    }

    @Test
    public void testInvert0() {
        final SortedRanges sar = makeSortedArray0();
        final long[] vs = vs0;
        final RowSet ix = RowSetFactory.fromKeys(vs);
        for (long maxPosition = 0; maxPosition <= ix.size(); ++maxPosition) { // go one over the last position.
            final RowSet.RangeIterator ixit = ix.rangeIterator();
            checkInvert("", sar, ixit, ix.iterator(), maxPosition);
        }
    }

    @Test
    public void testInvert() {
        final int nruns = 20;
        for (int seed = seed0; seed < seed0 + runs; ++seed) {
            final Random rand = new Random(seed);
            final String m = "seed==" + seed;
            for (int run = 0; run < nruns; ++run) {
                final String m2 = m + " && run==" + run;
                for (int vsi = 0; vsi < vss.length; ++vsi) {
                    final String m3 = m2 + " && vsi==" + vsi;
                    final long[] vs = vss[vsi];
                    final SortedRanges sar = vs2sar(vs);
                    final int sarCard = (int) sar.getCardinality();
                    SortedRanges invOp = sar.makeMyTypeAndOffset(sar.count());
                    for (int i = 0; i < sarCard; ++i) {
                        if (rand.nextBoolean()) {
                            final long v = sar.get(i);
                            invOp = invOp.append(v);
                        }
                    }
                    if (invOp.isEmpty()) {
                        continue;
                    }
                    for (long maxPosition = 0; maxPosition <= sarCard + 2; ++maxPosition) {
                        final String m4 = m3 + " && maxPosition==" + maxPosition;
                        checkInvert(m4 + " && ", sar, invOp.getRangeIterator(), invOp.getIterator(), maxPosition);
                    }
                }
            }
        }
    }

    @Test
    public void testInsertSimple() {
        doTestInsertOrUnionSimple(true);
    }

    @Test
    public void testUnionSimple() {
        doTestInsertOrUnionSimple(false);
    }

    private static void doTestInsertOrUnionSimple(final boolean isInsert) {
        final String m = "isInsert==" + isInsert;
        SortedRanges orig = makeSortedArray0();
        for (int vsi = 1; vsi < vss.length; ++vsi) {
            final String m2 = m + " && vsi==" + vsi;
            final long[] vs = vss[vsi];
            SortedRanges op2 = new SortedRangesLong();
            for (long v : vs) {
                op2 = op2.append(v);
            }
            op2 = op2.tryCompact(4);
            SortedRanges op1;
            if (isInsert) {
                op1 = orig.deepCopy();
            } else {
                op1 = orig;
            }
            op1 = op1.tryCompact(4);
            doTestInsertOrUnion(m2, isInsert, orig, op1, op2);
        }
    }

    private static void doTestInsertOrUnion(final String m2, final boolean isInsert, final SortedRanges orig,
            final SortedRanges op1, final SortedRanges op2) {
        final OrderedLongSet result;
        if (isInsert) {
            final OrderedLongSet tix = op1.insertImpl(op2);
            assertTrue(m2, tix instanceof SortedRanges);
            result = tix;
        } else {
            result = SortedRanges.unionOnNew(op1, op2);
        }
        for (RowSet.RangeIterator rit = result.ixRangeIterator(); rit.hasNext();) {
            rit.next();
            final long s = rit.currentRangeStart();
            final long e = rit.currentRangeEnd();
            for (long v = s; v <= e; ++v) {
                final String m3 = m2 + " && v==" + v;
                final boolean inOrig = orig.contains(v);
                if (inOrig) {
                    continue;
                }
                final boolean inOp2 = op2.contains(v);
                assertTrue(m3, inOp2);
            }
        }
        final SortedRanges[] srs = new SortedRanges[] {op2, orig};
        for (int i = 0; i < srs.length; ++i) {
            final String m3 = m2 + " && i==" + i;
            final SortedRanges sr = srs[i];
            for (RowSet.RangeIterator rit = sr.getRangeIterator(); rit.hasNext();) {
                rit.next();
                final long s = rit.currentRangeStart();
                final long e = rit.currentRangeEnd();
                final String m4 = m3 + " && s==" + s + " && e==" + e;
                assertTrue(m4, result.ixContainsRange(s, e));
            }
        }
        assertEquals(m2,
                orig.getCardinality() + op2.getCardinality() - orig.intersectOnNew(op2).ixCardinality(),
                result.ixCardinality());
    }

    @Test
    public void testRetainAndIntersect() {
        for (final boolean isRetain : new boolean[] {false, true}) {
            final String m = "isRetain==" + isRetain;
            SortedRanges orig = makeSortedArray0();
            for (int vsi = 1; vsi < vss.length; ++vsi) {
                final String m2 = m + " && vsi==" + vsi;
                final long[] vs = vss[vsi];
                SortedRanges op2 = new SortedRangesLong();
                for (long v : vs) {
                    op2 = op2.append(v);
                }
                final SortedRanges op1;
                if (isRetain) {
                    op1 = orig.deepCopy();
                } else {
                    op1 = orig;
                }
                final OrderedLongSet result;
                if (isRetain) {
                    result = op1.retain(op2);
                    assertTrue(m2, result.ixIsEmpty() ||
                            (result instanceof SingleRange) ||
                            (result instanceof SortedRanges && op1 == result));
                } else {
                    result = op1.intersectOnNew(op2);
                }
                try (final RowSet.RangeIterator rit = result.ixRangeIterator()) {
                    while (rit.hasNext()) {
                        rit.next();
                        final long s = rit.currentRangeStart();
                        final long e = rit.currentRangeEnd();
                        for (long v = s; v <= e; ++v) {
                            final String m3 = m2 + " && v==" + v;
                            final boolean inOrig = orig.contains(v);
                            assertTrue(m3, inOrig);
                            final boolean inOp2 = op2.contains(v);
                            assertTrue(m3, inOp2);
                        }
                    }
                }
                long intersectCount = 0;
                try (final RowSet.RangeIterator rit = orig.getRangeIterator()) {
                    while (rit.hasNext()) {
                        rit.next();
                        final long s = rit.currentRangeStart();
                        final long e = rit.currentRangeEnd();
                        for (long v = s; v <= e; ++v) {
                            final String m3 = m2 + " && v==" + v;
                            final boolean inOp2 = op2.contains(v);
                            if (inOp2) {
                                ++intersectCount;
                            }
                            assertEquals(m3, inOp2, result.ixContainsRange(v, v));
                        }
                    }
                }
                assertEquals(m2, intersectCount, result.ixCardinality());
            }
        }
    }

    @Test
    public void testRemoveAndMinus() {
        for (final boolean isRemove : new boolean[] {false, true}) {
            final String m = "isRemove==" + isRemove;
            SortedRanges orig = makeSortedArray0();
            for (int vsi = 1; vsi < vss.length; ++vsi) {
                final String m2 = m + " && vsi==" + vsi;
                final long[] vs = vss[vsi];
                SortedRanges op2 = new SortedRangesLong();
                for (long v : vs) {
                    op2 = op2.append(v);
                }
                final SortedRanges op1;
                if (isRemove) {
                    op1 = orig.deepCopy();
                } else {
                    op1 = orig;
                }
                final SortedRanges result;
                if (isRemove) {
                    final OrderedLongSet r = op1.remove(op2);
                    assertNotNull(r);
                    assertTrue(r instanceof SortedRanges);
                    result = (SortedRanges) r;
                } else {
                    OrderedLongSet r = op1.minusOnNew(op2);
                    if (r == null) {
                        continue;
                    }
                    assertTrue(r instanceof SortedRanges);
                    result = (SortedRanges) r;
                }
                for (RowSet.RangeIterator rit = result.getRangeIterator(); rit.hasNext();) {
                    rit.next();
                    final long s = rit.currentRangeStart();
                    final long e = rit.currentRangeEnd();
                    for (long v = s; v <= e; ++v) {
                        final String m3 = m2 + " && v==" + v;
                        final boolean inOrig = orig.contains(v);
                        final boolean inOp2 = op2.contains(v);
                        assertTrue(m3, inOrig && !inOp2);
                    }
                }
                long card = 0;
                for (RowSet.RangeIterator rit = orig.getRangeIterator(); rit.hasNext();) {
                    rit.next();
                    final long s = rit.currentRangeStart();
                    final long e = rit.currentRangeEnd();
                    for (long v = s; v <= e; ++v) {
                        final String m3 = m2 + " && v==" + v;
                        final boolean inOp2 = op2.contains(v);
                        if (!inOp2) {
                            ++card;
                        }
                        assertEquals(m3, !inOp2, result.contains(v));
                    }
                }
                assertEquals(m2, card, result.getCardinality());
            }
        }
    }

    private static SortedRanges vs2sar(final long[] vs) {
        SortedRanges sar = SortedRanges.tryMakeForKnownRangeFinalCapacityLowerBound(
                vs.length, vs.length, 0, Math.abs(vs[vs.length - 1]), false);
        long pendingStart = -1;
        for (long v : vs) {
            if (v < 0) {
                sar = sar.appendRange(pendingStart, -v);
                pendingStart = -1;
            } else {
                if (pendingStart != -1) {
                    sar = sar.append(pendingStart);
                }
                pendingStart = v;
            }
        }
        if (pendingStart != -1) {
            sar = sar.append(pendingStart);
        }
        return sar.tryCompact(4);
    }

    private static TLongSet sar2set(final SortedRanges sar) {
        final TLongSet s = new TLongHashSet((int) sar.getCardinality());
        for (RowSet.Iterator it = sar.getIterator(); it.hasNext();) {
            final long v = it.nextLong();
            s.add(v);
        }
        return s;
    }

    private static TLongSet vs2set(final long[] vs) {
        final TLongSet s = new TLongHashSet(vs.length);
        for (long v : vs) {
            s.add(v);
        }
        return s;
    }

    private static void setAddRange(final TLongSet set, final long s, final long e) {
        for (long v = s; v <= e; ++v) {
            set.add(v);
        }
    }

    @Test
    public void testAddRageCases() {
        final long[] vs = new long[] {2, 3, 4, 6, 7, 8, 13, 15, 17, 21, 22};
        for (int i = 0; i < vs.length; ++i) {
            for (int j = i; j < vs.length; ++j) {
                final long vi = vs[i];
                final long vj = vs[j];
                for (long s = vi - 1; s <= vi + 1; ++s) {
                    if (s < 0) {
                        continue;
                    }
                    final String m = "s==" + s;
                    for (long e = vj - 1; e <= vj + 1; ++e) {
                        if (e < s) {
                            continue;
                        }
                        final String m2 = m + " && e==" + e;
                        SortedRanges sar = vs2sar(vs);
                        sar = sar.addRange(s, e);
                        final TLongSet expected = vs2set(vs);
                        setAddRange(expected, s, e);
                        final TLongSet result = sar2set(sar);
                        assertEquals(m2, expected, result);
                    }
                }
            }
        }
    }

    @Test
    public void testAddCases2() {
        final long[] vs = new long[] {2, 3, 4, 6, 7, 8, 13, 15, 17, 21, 22};
        for (int i = 0; i < vs.length; ++i) {
            final long vi = vs[i];
            final String m = "vi==" + vi;
            for (long v = vi - 2; v <= vi + 2; ++v) {
                if (v < 0) {
                    continue;
                }
                final String m2 = m + " && v==" + v;
                SortedRanges sar = vs2sar(vs);
                sar = sar.add(v);
                final TLongSet expected = vs2set(vs);
                expected.add(v);
                final TLongSet result = sar2set(sar);
                assertEquals(m2, expected, result);
            }
        }
    }

    @Test
    public void testManyRanges() {
        SortedRanges sr = SortedRanges.makeEmpty();
        final long nRanges = SortedRanges.SHORT_MAX_CAPACITY / 2;
        final long v0 = (1 << 16) + 1;
        for (int i = 0; i < nRanges; ++i) {
            final long s = v0 + 3 * i;
            final long e = v0 + 3 * i + 1;
            sr = sr.appendRange(s, e);
            assertNotNull(sr);
        }
        assertEquals(2 * nRanges, sr.getCardinality());
        assertEquals(v0, sr.first());
        assertEquals(v0 + 3 * (nRanges - 1) + 1, sr.last());
        assertEquals(sr.first(), sr.get(0));
        assertEquals(sr.last(), sr.get(sr.getCardinality() - 1));
    }

    private static void checkEquals(final String m, final TLongArrayList vs, final SortedRanges sr) {
        checkEquals(m, vs, vs.size(), sr);
    }

    private static void checkEquals(final String m, final TLongArrayList vs, final int vsSize, final SortedRanges sr) {
        sr.validate();
        assertEquals(m, vsSize, sr.getCardinality());
        final MutableInt pos = new MutableInt(0);
        vs.forEach((final long v) -> {
            if (v == -1) {
                return true;
            }
            final int j = sr.unpackedBinarySearch(v, pos.intValue());
            assertTrue(m + " && v==" + v, j >= 0);
            pos.setValue(j);
            return true;
        });
    }

    @Test
    public void testTypeBoundaries() {
        final int[] counts = {
                SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY,
                SortedRanges.SHORT_MAX_CAPACITY};
        for (int run = 0; run < runs; ++run) {
            final String m = "run==" + run;
            final Random rand = new Random(seed0 + run);
            for (int count : counts) {
                final String m2 = m + " && count==" + count;
                final TLongArrayList vs = new TLongArrayList(count);
                SortedRanges sr = populateRandom2(vs, rand, count, 0.25, 0, 1, 4, 1, 4);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                final long lastData = sr.unpackedGet(sr.count - 1);
                final boolean lastIsNeg = lastData < 0;
                final long lastValue = lastIsNeg ? -lastData : lastData;
                for (long newVal : new long[] {lastValue, lastValue + 1, lastValue + 2, lastValue + 3}) {
                    final String m3 = m2 + " && newVal==" + newVal;
                    SortedRanges sr2 = sr.deepCopy();
                    sr2 = sr2.add(newVal);
                    if (count == SortedRanges.SHORT_MAX_CAPACITY &&
                            ((lastIsNeg && newVal > lastValue + 1) || (!lastIsNeg && newVal > lastValue))) {
                        assertNull(m3, sr2);
                    } else {
                        if (newVal != lastValue) {
                            vs.add(newVal);
                        }
                        checkEquals(m3, vs, sr2);
                        if (newVal != lastValue) {
                            vs.remove(newVal);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testSubsetOfTypes() {
        final int[] counts = {
                SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY,
                SortedRanges.SHORT_MAX_CAPACITY - 2};
        for (int run = 0; run < runs; ++run) {
            final String m = "run==" + run;
            final Random rand = new Random(seed0 + run);
            COUNTS: for (int count : counts) {
                final String m2 = m + " && count==" + count;
                final TLongArrayList vs = new TLongArrayList(count);
                SortedRanges sr = populateRandom2(vs, rand, count, 0.3, 1L << 33, 1, 7, 1, 7);
                SortedRanges sr2 = sr.deepCopy();
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                long card;
                int i = 0;
                while ((card = sr2.getCardinality()) > 1) {
                    final long v = sr2.get(rand.nextInt((int) card));
                    sr2 = sr2.remove(v);
                    if (sr2 == null) {
                        continue COUNTS;
                    }
                    assertEquals(card - 1, sr2.getCardinality());
                    assertTrue(m2 + " && i==" + i, sr2.subsetOf(sr.getRangeIterator()));
                    ++i;
                }
                final OrderedLongSet r = sr.ixMinusOnNew(
                        SingleRange.make(
                                sr2.first() - 2, sr2.last() + 2));
                assertNotNull(r);
                assertTrue(m2, r instanceof SortedRanges);
                sr2 = (SortedRanges) r;
                for (long pos = 0; pos < sr2.getCardinality(); ++pos) {
                    SortedRanges sr3 = sr.deepCopy();
                    final long newValNotInSr = sr2.get(pos);
                    sr3 = sr3.add(newValNotInSr);
                    assertFalse(m2 + " && pos==" + pos, !sr3.subsetOf(sr.getRangeIterator()));
                }
            }
        }
    }

    @Test
    public void testMinusWithTypeBoundaries() {
        final long offset1 = 1L << 33;
        SortedRanges sr1 = new SortedRangesInt(2, offset1);
        sr1 = sr1.addRange(offset1, offset1 + (1 << 15) - 1);
        assertEquals((1 << 15), sr1.getCardinality());
        final long offset2 = offset1 + 3;
        // result will have (nelems + 1) ranges;
        final int nelems = SortedRanges.INT_DENSE_MAX_CAPACITY - 1;
        SortedRanges sr2 = new SortedRangesInt(nelems, offset2);
        for (int i = 0; i < nelems; ++i) {
            sr2 = sr2.add(offset2 + 3 * i);
        }
        assertEquals(nelems, sr2.getCardinality());
        final OrderedLongSet r = sr1.minusOnNew(sr2);
        assertNotNull(r);
        assertTrue(r instanceof SortedRangesShort);
        final SortedRanges sr3 = (SortedRanges) r;
        assertEquals(sr1.getCardinality() - sr2.getCardinality(), sr3.getCardinality());
        final SortedRanges sr2final = sr2;
        final MutableLong card = new MutableLong(0);
        sr1.forEachLong((final long v) -> {
            final boolean inSr2 = sr2final.contains(v);
            if (!inSr2) {
                card.increment();
            }
            assertEquals("v==" + v, !inSr2, sr3.contains(v));
            return true;
        });
        assertEquals(card.longValue(), sr3.getCardinality());
    }

    @Test
    public void testUnionWithTypeChanges() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int size : new int[] {SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY}) {
                final String m2 = m + " && size==" + size;
                SortedRanges msr1 = populateRandom2(
                        null, rand, size, 0.25, 2, 2, 5, 1, 5);
                if (rand.nextBoolean()) {
                    msr1 = msr1.tryCompact(4);
                }
                final SortedRanges sr1 = msr1;
                SortedRanges msr2 = populateRandom2(
                        null, rand, size, 0.25, 2, 2, 5, 1, 5);
                if (rand.nextBoolean()) {
                    msr2 = msr2.tryCompact(4);
                }
                final SortedRanges sr2 = msr2;
                final OrderedLongSet union = SortedRanges.unionOnNew(sr1, sr2);
                for (SortedRanges sr : new SortedRanges[] {sr1, sr2}) {
                    sr.forEachLongRange((final long start, final long end) -> {
                        assertTrue(m2 + " && start==" + start + " && end==" + end, union.ixContainsRange(start, end));
                        return true;
                    });
                }
                union.ixForEachLongRange((final long start, final long end) -> {
                    for (long v = start; v <= end; ++v) {
                        final boolean in1 = sr1.contains(v);
                        final boolean in2 = sr2.contains(v);
                        assertTrue(m + " && v==" + v, in1 || in2);
                    }
                    return true;
                });
                assertEquals(m2,
                        sr1.getCardinality() + sr2.getCardinality() -
                                sr1.intersectOnNew(sr2).ixCardinality(),
                        union.ixCardinality());

            }
        }
    }

    @Test
    public void testInsertWithTypeChanges() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int size : new int[] {SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY}) {
                final String m2 = m + " && size==" + size;
                final int spaceBase = rand.nextBoolean() ? 0 : Short.MAX_VALUE / 32;
                SortedRanges msr1 = populateRandom2(
                        null, rand, size, 0.25, 2, 2, 5, spaceBase + 1, spaceBase + 7);
                if (rand.nextBoolean()) {
                    msr1 = msr1.tryCompact(4);
                }
                final SortedRanges sr1 = msr1;
                SortedRanges msr2 = populateRandom2(
                        null, rand, size, 0.25, 2, 2, 5, spaceBase + 1, spaceBase + 7);
                if (rand.nextBoolean()) {
                    msr2 = msr2.tryCompact(4);
                }
                final SortedRanges sr2 = msr2;
                final SortedRanges sr1cp = sr1.deepCopy();
                final OrderedLongSet result = sr1cp.insertImpl(sr2);
                if (result instanceof SortedRanges) {
                    sr1cp.validate();
                    continue;
                }
                result.ixValidate();
                for (SortedRanges sr : new SortedRanges[] {sr1, sr2}) {
                    sr.forEachLongRange((final long start, final long end) -> {
                        assertTrue(m2 + " && start==" + start + " && end==" + end, result.ixContainsRange(start, end));
                        return true;
                    });
                }
                result.ixForEachLongRange((final long start, final long end) -> {
                    for (long v = start; v <= end; ++v) {
                        final boolean in1 = sr1.contains(v);
                        final boolean in2 = sr2.contains(v);
                        assertTrue(m + " && v==" + v, in1 || in2);
                    }
                    return true;
                });
                assertEquals(m2,
                        sr1.getCardinality() + sr2.getCardinality() -
                                sr1.intersectOnNew(sr2).ixCardinality(),
                        result.ixCardinality());

            }
        }
    }

    @Test
    public void testIntersectWithTypeChanges() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int size : new int[] {SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY}) {
                final String m2 = m + " && size==" + size;
                SortedRanges msr1 = populateRandom2(
                        null, rand, size, 0.25, 2, 11, 15, 1, 9);
                if (rand.nextBoolean()) {
                    msr1 = msr1.tryCompact(4);
                }
                final SortedRanges sr1 = msr1;
                SortedRanges msr2 = populateRandom2(
                        null, rand, size, 0.25, 2, 7, 20, 1, 5);
                if (rand.nextBoolean()) {
                    msr2 = msr2.tryCompact(4);
                }
                final SortedRanges sr2 = msr2;
                final OrderedLongSet intersect = sr1.intersectOnNew(sr2);
                intersect.ixValidate();
                final MutableLong card = new MutableLong(0);
                sr1.forEachLong((final long v) -> {
                    final boolean inSr2 = sr2.contains(v);
                    if (inSr2) {
                        card.increment();
                    }
                    assertEquals(m2 + " && v==" + v, inSr2, intersect.ixContainsRange(v, v));
                    return true;
                });
                assertEquals(m, card.longValue(), intersect.ixCardinality());
                intersect.ixForEachLong((final long v) -> {
                    assertTrue(m2 + " && v==" + v, sr1.contains(v));
                    assertTrue(m2 + " && v==" + v, sr2.contains(v));
                    return true;
                });
            }
        }
    }

    @Test
    public void testGetFindTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final TLongArrayList arr = new TLongArrayList(count);
                SortedRanges sar = populateRandom2(
                        arr, rand, count, 0.25, 2, 7, 11, 1, 5);
                if (rand.nextBoolean()) {
                    sar = sar.tryCompact(4);
                }
                for (int i = 0; i < arr.size(); ++i) {
                    long vi = arr.get(i);
                    final long gi = sar.get(i);
                    assertEquals(m + " && i==" + i, vi, gi);
                }
                assertEquals(-1, sar.get(arr.size()));
                assertEquals(-1, sar.find(arr.get(0) - 1));
                for (int i = 0; i < arr.size() - 1; ++i) {
                    final int j = i + 1;
                    final long vi = arr.get(i);
                    final long posi = sar.find(vi);
                    final String m2 = m + " && i==" + i;
                    assertEquals(m2, i, posi);
                    final long vj = arr.get(j);
                    for (long v = vi + 1; v < vj; ++v) {
                        final long pos = sar.find(v);
                        assertEquals(m2 + " && v == " + v, -(i + 1) - 1, pos);
                    }
                }
                assertEquals(arr.size() - 1, sar.find(arr.get(arr.size() - 1)));
                assertEquals(-arr.size() - 1, sar.find(arr.get(arr.size() - 1) + 1));
            }
        }
    }

    @Test
    public void testRemoveSinglesTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final TLongArrayList arr = new TLongArrayList(count);
                SortedRanges sar = populateRandom2(
                        arr, rand, count, 0.25, 2, 7, 11, 1, 5);
                if (rand.nextBoolean()) {
                    sar = sar.tryCompact(4);
                }
                int posSize = arr.size();
                final int[] positions = new int[posSize];
                for (int i = 0; i < posSize; ++i) {
                    positions[i] = i;
                }
                int rmSize = arr.size();
                while (posSize > 0) {
                    final int iRm = rand.nextInt(posSize);
                    final int posRm = positions[iRm];
                    --posSize;
                    if (iRm != posSize) {
                        positions[iRm] = positions[posSize];
                    }
                    final long vRm = arr.get(posRm);
                    final SortedRanges ans = sar.remove(vRm);
                    if (ans != null) {
                        final String m2 = m + " && vRm==" + vRm;
                        sar = ans;
                        arr.set(posRm, -1);
                        --rmSize;
                        checkEquals(m2, arr, rmSize, sar);
                    }
                }
            }
        }
    }

    @Test
    public void testRemoveRangesTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final TLongArrayList arr = new TLongArrayList(count);
                SortedRanges sar = populateRandom2(
                        arr, rand, count, 0.25, 2, 7, 11, 1, 5);
                if (rand.nextBoolean()) {
                    sar = sar.tryCompact(4);
                }
                final TLongArrayList arrCopy = new TLongArrayList(arr);
                while (arrCopy.size() > 1) {
                    final int posRmStart = rand.nextInt(arrCopy.size() - 1);
                    final int posRmEnd = posRmStart + 1 + rand.nextInt(arrCopy.size() - 1 - posRmStart);
                    final int cardRm = posRmEnd - posRmStart + 1;
                    final long vRmStart = arrCopy.get(posRmStart);
                    final long vRmEnd = arrCopy.get(posRmEnd);
                    arrCopy.remove(posRmStart, cardRm);
                    final SortedRanges ans = sar.removeRange(vRmStart, vRmEnd);
                    final String m2 = m + " && vRmStart==" + vRmStart + " && vRmEnd==" + vRmEnd;
                    if (ans != null) {
                        final int posStart = arr.binarySearch(vRmStart);
                        final int posEnd = arr.binarySearch(vRmEnd, posStart + 1, arr.size());
                        arr.remove(posStart, posEnd - posStart + 1);
                        sar = ans;
                    }
                    checkEquals(m2, arr, sar);
                }
            }
        }
    }

    @Test
    public void testUnionTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY, SortedRanges.INT_DENSE_MAX_CAPACITY}) {
                SortedRanges sr1 = populateRandom2(
                        null, rand, count, 0.25, 2, 11, 15, 1, 6);
                if (rand.nextBoolean()) {
                    sr1 = sr1.tryCompact(4);
                }
                SortedRanges sr2 = populateRandom2(
                        null, rand, count, 0.25, 10, 14, 19, 3, 4);
                if (rand.nextBoolean()) {
                    sr2 = sr2.tryCompact(4);
                }
                final OrderedLongSet result = SortedRanges.unionOnNew(sr1, sr2);
                for (RowSet.RangeIterator rit = result.ixRangeIterator(); rit.hasNext();) {
                    rit.next();
                    final long s = rit.currentRangeStart();
                    final long e = rit.currentRangeEnd();
                    for (long v = s; v <= e; ++v) {
                        final String m2 = m + " && v==" + v;
                        final boolean inSr1 = sr1.contains(v);
                        if (inSr1) {
                            continue;
                        }
                        final boolean inSr2 = sr2.contains(v);
                        assertTrue(m2, inSr2);
                    }
                }
                final SortedRanges[] srs = new SortedRanges[] {sr1, sr2};
                for (int i = 0; i < srs.length; ++i) {
                    final String m2 = m + " && i==" + i;
                    final SortedRanges sr = srs[i];
                    for (RowSet.RangeIterator rit = sr.getRangeIterator(); rit.hasNext();) {
                        rit.next();
                        final long s = rit.currentRangeStart();
                        final long e = rit.currentRangeEnd();
                        final String m3 = m2 + " && s==" + s + " && e==" + e;
                        assertTrue(m3, result.ixContainsRange(s, e));
                    }
                }
                assertEquals(m, sr1.getCardinality() + sr2.getCardinality() -
                        sr1.intersectOnNew(sr2).ixCardinality(),
                        result.ixCardinality());
            }
        }
    }

    @Test
    public void testIteratorTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY,
                    SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final String m2 = m + " && count==" + count;
                SortedRanges sr = populateRandom2(
                        null, rand, count, 0.25, 2, 1, 10, 1, 6);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                final RspBitmap rb = sr.toRsp();
                final RowSet.Iterator srIter = sr.getIterator();
                final RowSet.Iterator rbIter = rb.ixIterator();
                int i = 0;
                while (srIter.hasNext()) {
                    final String m3 = m2 + " && i==" + i;
                    assertTrue(m3, rbIter.hasNext());
                    assertEquals(m3, rbIter.next(), srIter.next());
                }
                assertFalse(m, srIter.hasNext());
            }
        }
    }

    @Test
    public void testSearchIteratorTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY,
                    SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final String m2 = m + " && count==" + count;
                SortedRanges sr = populateRandom2(
                        null, rand, count, 0.25, 2, 1, 10, 1, 6);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                final RspBitmap rb = sr.toRsp();
                final RowSet.Iterator srIter = sr.getSearchIterator();
                final RowSet.Iterator rbIter = rb.ixSearchIterator();
                int i = 0;
                while (srIter.hasNext()) {
                    final String m3 = m2 + " && i==" + i;
                    assertTrue(m3, rbIter.hasNext());
                    assertEquals(m3, rbIter.next(), srIter.next());
                }
                assertFalse(m, srIter.hasNext());
            }
        }
    }

    @Test
    public void testRangeIteratorTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY,
                    SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final String m2 = m + " && count==" + count;
                SortedRanges sr = populateRandom2(
                        null, rand, count, 0.25, 2, 1, 10, 1, 6);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                final RspBitmap rb = sr.toRsp();
                final RowSet.RangeIterator srIter = sr.getRangeIterator();
                final RowSet.RangeIterator rbIter = rb.ixRangeIterator();
                int i = 0;
                while (srIter.hasNext()) {
                    final String m3 = m2 + " && i==" + i;
                    assertTrue(m3, rbIter.hasNext());
                    srIter.next();
                    rbIter.next();
                    assertEquals(m3, rbIter.currentRangeStart(), srIter.currentRangeStart());
                    assertEquals(m3, rbIter.currentRangeEnd(), srIter.currentRangeEnd());
                }
                assertFalse(m, srIter.hasNext());
            }
        }
    }

    private void checkOkAgainstIndex(final String m, final RowSequence rs, final RowSet ix) {
        assertEquals(m, ix.size(), rs.size());
        final RowSet rsIx = rs.asRowSet();
        assertEquals(m, ix.size(), rsIx.size());
        assertTrue(m, rsIx.subsetOf(ix));
        try (final RowSet.RangeIterator rit = ix.rangeIterator()) {
            rs.forEachRowKeyRange((final long start, final long end) -> {
                final String m2 = m + " && start==" + start + " && end==" + end;
                assertTrue(m2, rit.hasNext());
                rit.next();
                assertEquals(m2, start, rit.currentRangeStart());
                assertEquals(m2, end, rit.currentRangeEnd());
                return true;
            });
        }
        try (final RowSet.Iterator it = ix.iterator()) {
            rs.forEachRowKey((final long v) -> {
                final String m2 = m + " && v==" + v;
                assertTrue(m2, it.hasNext());
                assertEquals(m2, v, it.nextLong());
                return true;
            });
        }
    }

    @Test
    public void testGetRowSequenceByKeyRange() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {Math.min(SortedRanges.LONG_DENSE_MAX_CAPACITY / 2, 128)}) {
                final String m2 = m + " && count==" + count;
                final long offset = (1L << 33) + 2;
                SortedRanges sr = populateRandom2(new SortedRangesInt(count, offset),
                        null, rand, count, 0.25, offset, 1, 11, 5, 11);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                final long first = sr.first();
                final long last = sr.last();
                final int oneIn = 12;
                for (long start = Math.max(first - 1, 0); start <= last + 1; ++start) {
                    if (rand.nextInt(oneIn) < oneIn - 1) {
                        continue;
                    }
                    final String m3 = m2 + " && start==" + start;
                    for (long end = start; end <= last + 1; ++end) {
                        final String m4 = m3 + " && end==" + end;
                        try (final RowSequence rs = sr.getRowSequenceByKeyRange(start, end);
                                final RowSet ix = new WritableRowSetImpl(sr.ixSubindexByKeyOnNew(start, end))) {
                            assertEquals(m4, ix.firstRowKey(), rs.firstRowKey());
                            assertEquals(m4, ix.lastRowKey(), rs.lastRowKey());
                            assertEquals(m4, ix.size(), rs.size());
                            checkOkAgainstIndex(m4, rs, ix);
                            for (int dStart = -1; dStart <= 1; ++dStart) {
                                for (int dEnd = -1; dEnd <= 1; ++dEnd) {
                                    final long rs2Start = start + dStart;
                                    final long rs2End = end + dEnd;
                                    if (rs2End < rs2Start) {
                                        continue;
                                    }
                                    final String m5 = m4 + " && dStart==" + dStart + " && dEnd==" + dEnd;
                                    final RowSequence rs2 = rs.getRowSequenceByKeyRange(rs2Start, rs2End);
                                    if (rs2End < rs.firstRowKey() || rs.lastRowKey() < rs2Start) {
                                        assertEquals(0, rs2.size());
                                        continue;
                                    }
                                    final long rStart = Math.min(rs.lastRowKey(), Math.max(rs2Start, rs.firstRowKey()));
                                    final long rEnd = Math.max(rs.firstRowKey(), Math.min(rs2End, rs.lastRowKey()));
                                    final RowSet ix2 =
                                            new WritableRowSetImpl(sr.ixSubindexByKeyOnNew(rStart, rEnd));
                                    checkOkAgainstIndex(m5, rs2, ix2);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testRowSequenceNextWithLengthTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY,
                    SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final String m2 = m + " && count==" + count;
                SortedRanges sr = populateRandom2(
                        null, rand, count, 0.25, 1L << 33, 1, 10, 1, 11);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                final int step = 1 + rand.nextInt(11);
                final long dStart0 = rand.nextInt(step + 1);
                final long dEnd0 = rand.nextInt(step + 1);
                final long rs0SrLastPos = sr.getCardinality() - 1 - dEnd0;
                final long rs0SrCard = rs0SrLastPos - dStart0 + 1;
                final long dStart = dStart0 + rand.nextInt(step + 1);
                final long dEnd = dEnd0 + rand.nextInt(step + 1);
                final long rsSrLastPos = sr.getCardinality() - 1 - dEnd;
                final long rsSrCard = rsSrLastPos - dStart + 1;
                try (final RowSequence rsSr0 = sr.getRowSequenceByPosition(dStart0, rs0SrCard);
                        final RowSequence rsSr = rsSr0.getRowSequenceByPosition(dStart - dStart0, rsSrCard)) {
                    try (final RowSequence.Iterator rsIt = rsSr.getRowSequenceIterator()) {
                        long accum = 0;
                        int i = 0;
                        while (true) {
                            final String m3 = m2 + " && i==" + i;
                            if (accum < rsSrCard) {
                                assertTrue(m3, rsIt.hasMore());
                            } else {
                                assertFalse(m3, rsIt.hasMore());
                                break;
                            }
                            final RowSequence rs = rsIt.getNextRowSequenceWithLength(step);
                            final long rsCard = Math.min(step, rsSrCard - accum);
                            assertEquals(m3, rsCard, rs.size());
                            final RowSet subSr = new WritableRowSetImpl(
                                    sr.ixSubindexByPosOnNew(dStart + accum, dStart + accum + rsCard /* exclusive */));
                            assertEquals(m3, rsCard, subSr.size());
                            checkOkAgainstIndex(m3, rs, subSr);
                            accum += rsCard;
                            ++i;
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testRowSequenceNextThroughTypes() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY,
                    SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final String m2 = m + " && count==" + count;
                SortedRanges sr = populateRandom2(
                        null, rand, count, 0.25, 2, 1, 11, 1, 11);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                final int step = 7;
                final long dStart = 1 + rand.nextInt(step + 1);
                final long dEnd = 1 + rand.nextInt(step + 1);
                final long rsSrLastPos = sr.getCardinality() - 1 - dEnd;
                final long rsSrCard = rsSrLastPos - dStart + 1;
                try (final RowSequence rsSr = sr.getRowSequenceByPosition(dStart, rsSrCard)) {
                    long accum = 0;
                    try (final RowSequence.Iterator rsIt = rsSr.getRowSequenceIterator()) {
                        while (true) {
                            final String m3 = m2 + " && accum==" + accum;
                            if (accum < rsSrCard) {
                                assertTrue(m3, rsIt.hasMore());
                            } else {
                                assertFalse(m3, rsIt.hasMore());
                                break;
                            }
                            final long rsCard = Math.min(1 + rand.nextInt(step), rsSrCard - accum);
                            final RowSet subSr = new WritableRowSetImpl(
                                    sr.ixSubindexByPosOnNew(dStart + accum, dStart + accum + rsCard /* exclusive */));
                            final long last = subSr.lastRowKey();
                            final long target;
                            if (!sr.contains(last + 1) && rand.nextBoolean()) {
                                target = last + 1;
                            } else {
                                target = last;
                            }
                            final RowSequence rs = rsIt.getNextRowSequenceThrough(target);
                            assertEquals(m3, rsCard, rs.size());
                            checkOkAgainstIndex(m3, rs, subSr);
                            accum += rsCard;
                        }
                    }
                }
            }
        }
    }

    private static SortedRanges populateRandom2(
            final TLongArrayList vsOut, final Random rand, final int count, final double singlesDensity,
            final long offset,
            final int rangeLenMin, final int rangeLenMax,
            final int spaceMin, final int spaceMax) {
        return populateRandom2(SortedRanges.makeEmpty(),
                vsOut, rand, count, singlesDensity, offset, rangeLenMin, rangeLenMax, spaceMin, spaceMax);
    }

    private static SortedRanges populateRandom2(
            final SortedRanges srIn,
            final TLongArrayList vsOut, final Random rand, final int count, final double singlesDensity,
            final long offset,
            final int rangeLenMin, final int rangeLenMax,
            final int spaceMin, final int spaceMax) {
        SortedRanges sr = srIn;
        final int dRangeLen = rangeLenMax - rangeLenMin;
        final int dSpace = spaceMax - spaceMin;
        long prevValue = offset - 1;
        while (sr.count < count) {
            final int spaceFromPrev = spaceMin + rand.nextInt(dSpace);
            final long v = prevValue + spaceFromPrev;
            if (sr.count == count - 1 || rand.nextDouble() <= singlesDensity) {
                SortedRanges sr1 = sr.add(v);
                if (sr1 == null) {
                    break;
                }
                sr = sr1;
                if (vsOut != null) {
                    vsOut.add(v);
                }
                prevValue = v;
            } else {
                final int rangeLen = rangeLenMin + rand.nextInt(dRangeLen);
                final long end = v + rangeLen;
                SortedRanges sr1 = sr.addRange(v, end);
                if (sr1 == null) {
                    break;
                }
                sr = sr1;
                if (vsOut != null) {
                    for (long x = v; x <= end; ++x) {
                        vsOut.add(x);
                    }
                }
                prevValue = end;
            }
        }
        if (vsOut != null) {
            checkEquals("", vsOut, sr);
        }
        return sr;
    }

    private static SortedRanges rvs2sr(final long[] vs) {
        return rvs2sr(vs, SortedRanges.makeEmpty());
    }

    private static SortedRanges rvs2sr(final long[] vs, SortedRanges sr) {
        long pendingStart = -1;
        for (long v : vs) {
            if (v < 0) {
                sr = sr.addRange(pendingStart, -v);
                pendingStart = -1;
            } else {
                if (pendingStart != -1) {
                    sr = sr.add(pendingStart);
                }
                pendingStart = v;
            }
        }
        if (pendingStart != -1) {
            sr.add(pendingStart);
        }
        return sr;
    }

    private static long[] rvs0 = new long[] {
            2, 4, -5, 8, -12, 14, -18, 20, 22, -23, 26, -27, 29, -30, 32, -36, 39, -43, 45, -47, 49, 53, -56, 60, 62,
            -64, 66, -67, 69, 71,
            -73, 75, -80, 82, -83, 85, -89, 91, -92, 94, -98, 100, 102, -106, 109, 112, -113
    };
    private static long[] rvs1 = new long[] {
            14, 20, 22, 26, 29, 33, -34, 36, 39, 45, -46, 49, 55, 64, 67, 69, 71, -73, 78, -79, 82, 86, -88, 91, 94,
            102, -103, 109
    };

    @Test
    public void testMinusRegression0() {
        final SortedRanges sr0 = rvs2sr(rvs0);
        final SortedRanges sr1 = rvs2sr(rvs1);
        final OrderedLongSet r2 = sr1.minusOnNew(sr0);
        assertTrue(r2.ixIsEmpty());
    }

    @Test
    public void testIntersectsCase0() {
        final SortedRanges sr0 = rvs2sr(rvs0);
        final SortedRanges sr1 = rvs2sr(rvs1);
        final OrderedLongSet sr2 = sr0.intersectOnNew(sr1);
        assertEquals(sr1.getCardinality(), sr2.ixCardinality());
        final OrderedLongSet r3 = sr1.minusOnNew(sr2);
        assertTrue(r3.ixIsEmpty());
    }

    // check base.minus(minusArg) == result
    private static void checkMinus(final SortedRanges base, final SortedRanges minusArg, final SortedRanges result) {
        result.forEachLong((final long v) -> {
            final String m = "v==" + v;
            assertTrue(m, base.contains(v));
            assertFalse(m, minusArg.contains(v));
            return true;
        });
        base.forEachLong((final long v) -> {
            final String m = "v==" + v;
            assertEquals(m, minusArg.contains(v), !result.contains(v));
            return true;
        });
    }

    @Test
    public void testIntersectRegression0() {
        final SortedRanges sr0 = rvs2sr(new long[] {
                1073741776, -1073741798, 1073741805, -1073741812, 1073741824, -1073741860});
        final SortedRanges sr1 = rvs2sr(new long[] {
                1073741793, -1073741816, 1073741818, 1073741821, -1073741822, 1073741824, -1073741854});
        final OrderedLongSet sr2 = sr0.intersectOnNew(sr1);
        sr2.ixValidate();
    }

    @Test
    public void testMinusRegression2() {
        final SortedRanges sr0 =
                rvs2sr(new long[] {0, -5, 8, 16, -19, 22, 27, 29, 34, -35, 38, 40, 45, 48, -50, 53, -55, 60, -106});
        final SortedRanges sr1 =
                rvs2sr(new long[] {5, 16, 29, 34, -35, 40, 45, 50, 54, 62, 66, 77, 80, -81, 83, -85, 88, -96, 98, 105});
        final OrderedLongSet r = sr0.deepCopy().remove(sr1);
        assertNotNull(r);
        final SortedRanges sr2 = (SortedRanges) r;
        checkMinus(sr0, sr1, sr2);
    }

    @Test
    public void testSubsetOfRegression0() {
        final SortedRanges sr0 = SortedRanges.makeSingleRange(0, 17);
        final SortedRanges sr1 = rvs2sr(new long[] {1, -6, 9, -11, 13, -15});
        assertTrue(sr1.subsetOf(sr0.getRangeIterator()));
    }

    @Test
    public void testNextRowSequenceWithLengthForCoverage0() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(1, 3);
        sr = sr.add(5);
        try (final RowSequence.Iterator rsIt = sr.getRowSequenceIterator()) {
            RowSequence rs = rsIt.getNextRowSequenceWithLength(3);
            checkOkAgainstIndex("", rs, new WritableRowSetImpl(sr.subRangesByKey(1, 3)));
            rs = rsIt.getNextRowSequenceWithLength(4);
            checkOkAgainstIndex("", rs, new WritableRowSetImpl(sr.subRangesByKey(5, 5)));
        }
    }

    @Test
    public void testNextRowSequenceWithLengthForCoverage1() {
        SortedRanges sr = SortedRanges.makeEmpty();
        sr = sr.addRange(1, 3);
        sr = sr.add(7);
        sr = sr.addRange(10, 15);
        sr = sr.add(20);
        try (final RowSequence.Iterator rsIt = sr.getRowSequenceIterator()) {
            RowSequence rs = rsIt.getNextRowSequenceWithLength(3);
            checkOkAgainstIndex("", rs, new WritableRowSetImpl(sr.subRangesByKey(1, 3)));
            rs = rsIt.getNextRowSequenceWithLength(9);
            checkOkAgainstIndex("", rs, new WritableRowSetImpl(sr.subRangesByKey(7, 20)));
        }
    }

    @Test
    public void testArraySizes() {
        SortedRangesShort srs = new SortedRangesShort(2, 0);
        SortedRanges sr = srs;
        sr = sr.add(1);
        sr = sr.add(3);
        sr = sr.add(5);
        assertEquals(sr.cardinality, 3);
        assertEquals(6, sr.dataLength());
        sr = sr.add(7);
        sr = sr.add(9);
        sr = sr.add(11);
        sr = sr.add(13);
        assertEquals(sr.cardinality, 7);
        assertEquals(10, srs.dataLength());
    }

    @Test
    public void testRemoveRanges() {
        SortedRanges sr0 = SortedRanges.makeSingleRange(30, 35);
        sr0 = sr0.appendRange(60, 63);
        SortedRanges sr1 = SortedRanges.makeSingleRange(30, 31);
        sr1 = sr1.addRange(34, 34);
        final OrderedLongSet r = sr0.ixRemove(sr1);
        assertEquals(7, r.ixCardinality());
    }

    @Test
    public void testCompactTriesToPack() {
        SortedRanges sr = SortedRangesLong.makeSingleRange(1, Short.MAX_VALUE + 2);
        sr = sr.tryCompactUnsafe(4);
        assertTrue(sr instanceof SortedRangesInt);

        sr = SortedRangesLong.makeSingleRange(0, Short.MAX_VALUE);
        sr = sr.tryCompactUnsafe(4);
        assertTrue(sr instanceof SortedRangesShort);
    }

    @Test
    public void testCompact() {
        final long v = Integer.MAX_VALUE + 1L;
        SortedRanges sr = SortedRanges.makeSingleRange(0, v);
        sr = sr.appendRange(v + 4, v + 5);
        sr = sr.remove(v + 4);
        sr = sr.remove(v + 5);
        sr = sr.tryCompact(0);
        assertTrue(sr instanceof SortedRangesLong);
        final SortedRangesLong srl = (SortedRangesLong) sr;
        assertEquals(2, srl.data.length);

        sr = SortedRanges.makeSingleRange(0, Integer.MAX_VALUE);
        sr = sr.tryCompactUnsafe(4);
        assertTrue(sr instanceof SortedRangesInt);
    }

    @Test
    public void testRemoveRangeOutOfPackedRange() {
        final long first = Integer.MAX_VALUE + 1L;
        final long last = Integer.MAX_VALUE + 1L + Short.MAX_VALUE;
        SortedRanges sr = SortedRanges.makeSingleRange(first, last);
        sr = sr.tryCompact(4);
        assertTrue(sr instanceof SortedRangesShort);
        sr = sr.removeRange(Integer.MAX_VALUE - 100, Integer.MAX_VALUE - 2);
        sr.validate();
        assertEquals(last - first + 1, sr.getCardinality());
        assertEquals(first, sr.first());
        assertEquals(last, sr.last());
    }

    @Test
    public void testAddRangeAtPackedRange() {
        final long first = Integer.MAX_VALUE + 1L;
        final long last = Integer.MAX_VALUE + 1L + Short.MAX_VALUE;
        SortedRanges sr = SortedRanges.makeSingleRange(first, last);
        sr = sr.tryCompact(4);
        assertTrue(sr instanceof SortedRangesShort);
        final long rangeStart = last - 1;
        final long rangeEnd = last;
        sr = sr.addRange(rangeStart, rangeEnd);
        sr.validate();
        assertEquals(last - first + 1, sr.getCardinality());
        assertEquals(first, sr.first());
        assertEquals(last, sr.last());
    }

    @Test
    public void testSubRangesByPos() {
        final long first = Integer.MAX_VALUE + 1L;
        final long last = Integer.MAX_VALUE + 1L + Short.MAX_VALUE;
        SortedRanges sr = SortedRanges.makeSingleRange(first, last);
        sr = sr.tryCompact(4);
        assertTrue(sr instanceof SortedRangesShort);
        final SortedRanges sr2 = sr.subRangesByPos(1, 1);
        sr2.validate();
        assertEquals(1, sr2.getCardinality());
        assertEquals(Integer.MAX_VALUE + 2L, sr2.first());
    }

    @Test
    public void testInsertCoverage1() {
        final long offset = 10;
        for (boolean singles : new boolean[] {false, true}) {
            final String m1 = "singles=" + Boolean.toString(singles);
            SortedRanges sr0 = SortedRanges.makeEmpty();
            if (singles) {
                sr0 = sr0.add(offset + 1);
                sr0 = sr0.add(offset + 3);
            } else {
                sr0 = sr0.addRange(offset + 1, offset + 3);
            }
            SortedRanges sr1 = SortedRanges.makeEmpty();
            final MutableObject<SortedRanges> mu = new MutableObject<>(sr0);
            for (int i = 0; i < SortedRanges.SHORT_MAX_CAPACITY / 2; ++i) {
                final String m2 = m1 + ", i=" + i;
                final long base = offset + 4 * i;
                if (singles) {
                    sr1 = sr1.add(base + 1);
                    sr1 = sr1.add(base + 3);
                } else {
                    sr1 = sr1.addRange(base + 1, base + 3);
                }
                final OrderedLongSet result = mu.getValue().insertImpl(sr1);
                if (result instanceof SingleRange) {
                    mu.setValue(SortedRanges.makeSingleRange(result.ixFirstKey(), result.ixLastKey()));
                } else {
                    assertTrue(m2, result instanceof SortedRanges);
                    mu.setValue((SortedRanges) result);
                }
                sr0 = mu.getValue();
                sr0.validate();
                assertEquals(m2, sr1.getCardinality(), sr0.getCardinality());
            }
            final long start = offset + 4 * Short.MAX_VALUE + 1 + 1;
            final long end = offset + 4 * Short.MAX_VALUE + 1 + 3;
            SortedRanges sr2 = SortedRanges.makeSingleRange(start, end);
            final OrderedLongSet result = mu.getValue().insertImpl(sr2);
            assertFalse(result instanceof SortedRanges);
            assertEquals(sr0.getCardinality() + 3, result.ixCardinality());
            assertTrue(result.ixContainsRange(start, end));
            assertTrue(sr0.ixSubsetOf(result));
        }
    }

    @Test
    public void testAddRangeRandomRanges() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY,
                    SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final String m2 = m + " && count==" + count;
                final int space = 11;
                SortedRanges sr = populateRandom2(
                        null, rand, count, 0.25, 2, 1, space, 1, space);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                int c = 20;
                while (--c >= 0 && sr.last() - sr.first() + 1 > sr.getCardinality()) {
                    long v0 = rand.nextInt((int) (sr.last() + 2));
                    long v1;
                    if (rand.nextBoolean()) {
                        v1 = v0 + rand.nextInt(space + 2);
                    } else {
                        v1 = v0 + rand.nextInt((int) (sr.last() - v0) + 2);
                    }
                    sr = sr.addRange(v0, v1);
                    if (sr == null) {
                        break;
                    }
                    sr.validate();
                }
            }
        }
    }

    @Test
    public void testAddAroundRangeBoundaries() {
        for (int run = 0; run < runs; ++run) {
            final int seed = seed0 + run;
            final Random rand = new Random(seed);
            final String m = "run==" + run;
            for (int count : new int[] {
                    SortedRanges.LONG_DENSE_MAX_CAPACITY,
                    SortedRanges.INT_DENSE_MAX_CAPACITY,
                    SortedRanges.SHORT_MAX_CAPACITY}) {
                final String m2 = m + " && count==" + count;
                final int space = 11;
                SortedRanges sr = populateRandom2(
                        null, rand, count, 0.25, 2, 1, space, 1, space);
                if (rand.nextBoolean()) {
                    sr = sr.tryCompact(4);
                }
                int c = 20;
                SR_LOOP: while (--c >= 0) {
                    if (sr.getCardinality() == sr.last() - sr.first() + 1) {
                        break;
                    }
                    final OrderedLongSet complement =
                            SortedRanges.makeSingleRange(sr.first(), sr.last()).ixMinusOnNew(sr);
                    final RowSet.RangeIterator riter = complement.ixRangeIterator();
                    while (riter.hasNext()) {
                        riter.next();
                        final long start = riter.currentRangeStart();
                        final long end = riter.currentRangeEnd();
                        if (rand.nextBoolean()) {
                            final long v0 = start - 1 + rand.nextInt(3);
                            long v1 = end - 2 + rand.nextInt(3);
                            if (v1 < v0) {
                                v1 = v0;
                            }
                            sr = sr.addRange(v0, v1);
                        } else {
                            final long v0 = start - 2 + rand.nextInt((int) (end - start) + 4);
                            sr = sr.add(v0);
                        }
                        if (sr == null) {
                            break SR_LOOP;
                        }
                        sr.validate();
                    }
                }
            }
        }
    }

    @Test
    public void testRetainRefCountRegress() {
        final RspBitmap ix1 = new RspBitmap(20, 24);
        SortedRanges sr = new SortedRangesLong();
        sr = sr.add(18);
        sr = sr.add(20);
        SortedRanges sr2 = (SortedRanges) sr.cowRef();
        assertEquals(2, sr.refCount());
        sr.ixRetain(ix1);
        assertEquals(2, sr.refCount());
        assertEquals(2, sr2.refCount());
    }

    @Test
    public void testRetainRangeRefCountRegress() {
        SortedRanges sr = new SortedRangesLong();
        sr = sr.add(18);
        sr = sr.add(20);
        SortedRanges sr2 = (SortedRanges) sr.cowRef();
        assertEquals(2, sr.refCount());
        sr.ixRetainRange(21, 24);
        assertEquals(2, sr.refCount());
        assertEquals(2, sr2.refCount());
        sr.ixRetainRange(10, 17);
        assertEquals(2, sr.refCount());
        assertEquals(2, sr2.refCount());
    }

    @Test
    public void testUnionCoverage1() {
        SortedRanges sr0 = SortedRanges.makeSingleRange(45, 45).addRange(55, 56);
        SortedRanges sr1 = SortedRanges.makeSingleRange(10, 20).addRange(30, 40);
        for (SortedRanges arg : new SortedRanges[] {sr0, sr1}) {
            SortedRanges tis = (arg == sr0) ? sr1 : sr0;
            final OrderedLongSet r = tis.ixUnionOnNew(arg);
            assertNotNull(r);
            r.ixValidate();
            assertEquals(11 + 11 + 1 + 2, r.ixCardinality());
            assertTrue(sr0.ixSubsetOf(r));
            assertTrue(sr1.ixSubsetOf(r));
        }
    }

    @Test
    public void testUnionCoverage2() {
        SortedRanges sr0 = SortedRanges.makeSingleRange(10, 20).addRange(31, 40);
        SortedRanges sr1 = SortedRanges.makeSingleRange(20, 30).addRange(40, 50);
        final long expectedCard =
                sr0.ixCardinality() +
                        sr1.ixCardinality() -
                        sr0.intersectOnNew(sr1).ixCardinality();
        for (SortedRanges arg : new SortedRanges[] {sr0, sr1}) {
            SortedRanges tis = (arg == sr0) ? sr1 : sr0;
            final OrderedLongSet r = tis.ixUnionOnNew(arg);
            r.ixValidate();
            assertNotNull(r);
            assertEquals(expectedCard, r.ixCardinality());
            assertTrue(sr0.ixSubsetOf(r));
            assertTrue(sr1.ixSubsetOf(r));
        }
    }

    @Test
    public void testUnionCoverage3() {
        SortedRanges sr0 = SortedRanges.makeSingleRange(10, 20).addRange(31, 40);
        SortedRanges sr1 = SortedRanges.makeSingleRange(20, 30);
        final long expectedCard =
                sr0.ixCardinality() +
                        sr1.ixCardinality() -
                        sr0.intersectOnNew(sr1).ixCardinality();
        for (SortedRanges arg : new SortedRanges[] {sr0, sr1}) {
            SortedRanges tis = (arg == sr0) ? sr1 : sr0;
            final OrderedLongSet r = tis.ixUnionOnNew(arg);
            r.ixValidate();
            assertNotNull(r);
            assertEquals(expectedCard, r.ixCardinality());
            assertTrue(sr0.ixSubsetOf(r));
            assertTrue(sr1.ixSubsetOf(r));
        }
    }

    @Test
    public void testIntersectCoverage() {
        SortedRanges sr0 = SortedRanges.makeSingleRange(10, 20);
        SortedRanges sr1 = SortedRanges.makeSingleRange(20, 20);
        final long expectedCard =
                sr0.ixCardinality() +
                        sr1.ixCardinality() -
                        sr0.ixUnionOnNew(sr1).ixCardinality();
        for (SortedRanges arg : new SortedRanges[] {sr0, sr1}) {
            SortedRanges tis = (arg == sr0) ? sr1 : sr0;
            final OrderedLongSet r = tis.intersectOnNew(arg);
            r.ixValidate();
            assertNotNull(r);
            assertEquals(expectedCard, r.ixCardinality());
            assertTrue(r.ixSubsetOf(sr0));
            assertTrue(r.ixSubsetOf(sr1));
        }
    }

    @Test
    public void testBackToLongAfterPackedShortSingle() {
        SortedRanges sr0 = SortedRanges.makeSingleRange(0, 20);
        sr0 = sr0.tryPack();
        assertTrue(sr0 instanceof SortedRangesShort);
        for (int i = 2; i < SortedRanges.LONG_DENSE_MAX_CAPACITY - 1; ++i) {
            sr0 = sr0.append(30 + i * 2);
        }
        assertTrue(sr0 instanceof SortedRangesShort);
        SortedRanges sr1 = sr0.deepCopy();
        sr1 = sr1.append(Short.MAX_VALUE + 1);
        assertNotNull(sr1);
        assertTrue(sr1 instanceof SortedRangesLong);
        assertTrue(sr1.contains(Short.MAX_VALUE + 1));
        assertTrue(sr0.subsetOf(sr1.getRangeIterator()));

        sr0 = sr0.append(Short.MAX_VALUE - 2);
        assertNotNull(sr0);
        assertNull(sr0.append(Short.MAX_VALUE + 1));
    }

    @Test
    public void testBackToLongAfterPackedShortRange() {
        SortedRanges sr0 = SortedRanges.makeSingleRange(0, 20);
        sr0 = sr0.tryPack();
        assertTrue(sr0 instanceof SortedRangesShort);
        for (int i = 2; i < SortedRanges.LONG_DENSE_MAX_CAPACITY - 2; ++i) {
            sr0 = sr0.append(30 + i * 2);
        }
        assertTrue(sr0 instanceof SortedRangesShort);
        SortedRanges sr1 = sr0.deepCopy();
        sr1 = sr1.appendRange(Short.MAX_VALUE, Short.MAX_VALUE + 1);
        assertNotNull(sr1);
        assertTrue(sr1 instanceof SortedRangesLong);
        assertTrue(sr1.containsRange(Short.MAX_VALUE, Short.MAX_VALUE + 1));
        assertTrue(sr0.subsetOf(sr1.getRangeIterator()));

        sr0 = sr0.append(Short.MAX_VALUE - 1);
        assertNotNull(sr0);
        assertNull(sr0.appendRange(Short.MAX_VALUE, Short.MAX_VALUE + 1));
    }


    @Test
    public void testAppendRangeTypeChange() {
        final long offset = 1_000_000;
        SortedRangesShort srs0 = new SortedRangesShort(2, offset);
        SortedRanges sr = srs0.appendRange(offset, offset + 1);
        sr = sr.remove(offset + 1);
        SortedRanges ans = sr.appendRange(offset + Short.MAX_VALUE + 10, offset + Short.MAX_VALUE + 20);
        if (ans != null) {
            ans.validate();
        }
    }

    @Test
    public void testRemoveSinglesLeavesEmpty() {
        final SortedRanges sr0 = vs2sar(new long[] {10, 20, 30, 40});
        final SortedRanges sr1 = vs2sar(new long[] {8, 10, 15, 20, 30, 40});
        final OrderedLongSet tix = sr0.ixRemove(sr1);
        assertTrue(tix.ixIsEmpty());
    }

    @Test
    public void testRemoveWithChangedTypeAndOffsetNotMatchingFirst() {
        SortedRanges sr0 = new SortedRangesInt(0, 0);
        // Offset and start do not match.
        final long start = 1 << 16;
        for (int i = 0; i < SortedRanges.INT_DENSE_MAX_CAPACITY - 2; ++i) {
            sr0 = sr0.append(start + 2L * i);
        }
        final long lastRangeStart = start + Short.MAX_VALUE - 4;
        final long lastRangeEnd = lastRangeStart + 4;
        sr0 = sr0.appendRange(lastRangeStart, lastRangeEnd);
        final long preCard = sr0.getCardinality();
        sr0 = sr0.removeRange(lastRangeStart + 2, lastRangeStart + 2);
        assertNotNull(sr0);
        sr0.validate();
        assertEquals(preCard - 1, sr0.getCardinality());
    }

    @Test
    public void testAppendRangePacked() {
        SortedRanges sr0 = new SortedRangesLong(SortedRanges.LONG_DENSE_MAX_CAPACITY);
        for (int i = 0; i < SortedRanges.LONG_DENSE_MAX_CAPACITY; ++i) {
            sr0 = sr0.append(2L * i);
        }
        final long last = 2L * (SortedRanges.LONG_DENSE_MAX_CAPACITY - 1);
        sr0 = sr0.appendRange(last + 1, last + 3);
        sr0.validate();
        assertTrue(sr0.contains(last + 2));
    }

    @Test
    public void testSearchIteratorAdvaceRegression0() {
        SortedRanges sr = vs2sar(new long[] {10, -20, 30, -40, 1000, -1002});
        try (RowSet.SearchIterator it = sr.getSearchIterator()) {
            boolean more = it.advance(30);
            assertTrue(more);
            assertTrue(it.hasNext());
            assertEquals(30, it.currentValue());
            more = it.advance(1001);
            assertTrue(more);
            assertTrue(it.hasNext());
            more = it.advance(2000);
            assertFalse(more);
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void testRangeIteratorAdvaceRegression0() {
        SortedRanges sr = vs2sar(new long[] {10, -20, 30, -40, 1000, -1002});
        try (RowSet.RangeIterator it = sr.getRangeIterator()) {
            boolean more = it.advance(30);
            assertTrue(more);
            assertTrue(it.hasNext());
            assertEquals(30, it.currentRangeStart());
            more = it.advance(1001);
            assertTrue(more);
            assertFalse(it.hasNext());
            more = it.advance(2000);
            assertFalse(more);
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void testReverseRangeIteratorAdvanceRegression0() {
        SortedRanges sr = vs2sar(new long[] {10, -20, 30, -40, 1000, -1002});
        try (RowSet.SearchIterator it = sr.getReverseIterator()) {
            boolean more = it.advance(30);
            assertTrue(more);
            assertTrue(it.hasNext());
            assertEquals(30, it.currentValue());
            more = it.advance(11);
            assertTrue(more);
            assertTrue(it.hasNext());
            more = it.advance(5);
            assertFalse(more);
            assertFalse(it.hasNext());
        }
    }
}
