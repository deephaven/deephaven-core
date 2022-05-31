package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.Random;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertEquals;

public class TestTwoValuesContainer {
    @Test
    public void testAddRange() {
        final TwoValuesContainer tv = new TwoValuesContainer((short) 10, (short) 12);
        final int min = 7;
        final int max = 15;
        for (int first = min; first <= max; ++first) {
            for (int last = first; last <= max; ++last) {
                final Container c = tv.add(first, last + 1);
                boolean has10 = first <= 10 && 10 <= last;
                boolean has12 = first <= 12 && 12 <= last;
                final int expectedCard;
                if (has10 && has12) {
                    expectedCard = last - first + 1;
                } else if (has10 || has12) {
                    expectedCard = last - first + 2;
                } else {
                    expectedCard = last - first + 3;
                }
                final String m = "first==" + first + " && last==" + last;
                assertEquals(m, expectedCard, c.getCardinality());
                assertTrue(m, c.contains((short) 10));
                assertTrue(m, c.contains((short) 12));
                assertTrue(m, c.contains(first, last + 1));
            }
        }
    }

    @Test
    public void testIterators() {
        final TwoValuesContainer tv = new TwoValuesContainer((short) 10, (short) 12);
        {
            final RangeIterator rit = tv.getShortRangeIterator(0);
            assertTrue(rit.hasNext());
            rit.next();
            assertEquals(10, rit.start());
            assertEquals(11, rit.end());
            assertTrue(rit.hasNext());
            rit.next();
            assertEquals(12, rit.start());
            assertEquals(13, rit.end());
            assertFalse(rit.hasNext());
        }
        {
            final SearchRangeIterator rit2 = tv.getShortRangeIterator(0);
            assertTrue(rit2.advance(10));
            assertEquals(10, rit2.start());
            assertTrue(rit2.hasNext());
            assertTrue(rit2.advance(11));
            assertEquals(12, rit2.start());
            assertFalse(rit2.hasNext());
            assertTrue(rit2.advance(12));
            assertFalse(rit2.hasNext());
            assertEquals(12, rit2.start());
            assertFalse(rit2.advance(13));
            assertFalse(rit2.hasNext());
        }
        {
            final SearchRangeIterator rit3 = tv.getShortRangeIterator(1);
            assertTrue(rit3.hasNext());
            rit3.next();
            assertEquals(12, rit3.start());
            assertEquals(13, rit3.end());
            assertFalse(rit3.hasNext());
        }
    }

    @Test
    public void testForEach() {
        int v0 = 32769;
        int v1 = 32769 + 2;
        final TwoValuesContainer tv = new TwoValuesContainer((short) v0, (short) v1);
        final short[] vs = new short[2];
        final MutableInteger mi = new MutableInteger();
        mi.value = 0;
        tv.forEach(0, (final short v) -> {
            vs[mi.value++] = v;
            return true;
        });
        assertEquals(2, mi.value);
        assertTrue((short) v0 == vs[0] && (short) v1 == vs[1]);
        mi.value = 0;
        tv.forEach(1, (final short v) -> {
            vs[mi.value++] = v;
            return true;
        });
        assertEquals(1, mi.value);
        assertTrue((short) v1 == vs[0]);
        mi.value = 0;
        tv.forEach(2, (final short v) -> {
            vs[mi.value++] = v;
            return true;
        });
        assertEquals(0, mi.value);
        assertTrue((short) v1 == vs[0]);
    }

    @Test
    public void testShortBatchIterator() {
        final int v0 = 32769;
        final int v1 = 32769 + 10;
        final short[] vs = new short[] {(short) v0, (short) v1};
        final TwoValuesContainer c = new TwoValuesContainer(vs[0], vs[1]);
        for (int skip = 0; skip <= 1; ++skip) {
            final String m = "skip==" + skip;
            final ContainerShortBatchIterator it = c.getShortBatchIterator(skip);
            assertEquals(m, skip != 2, it.hasNext());
            if (skip == 2) {
                continue;
            }
            final short[] buf = new short[4];
            final int offset = 1;
            final int n = it.next(buf, offset, 3);
            assertEquals(m, 2 - skip, n);
            for (int i = skip, j = 0; i < 2; ++i, ++j) {
                final String m2 = m + " && i==" + i;
                assertTrue(m2, vs[i] == buf[offset + j]);
            }
        }
    }

    @Test
    public void testSearchIterator() {
        final int v0 = 32769;
        final int v1 = v0 + 2;
        final TwoValuesContainer tv = new TwoValuesContainer((short) v0, (short) v1);
        final SearchRangeIterator sit = tv.getShortRangeIterator(0);
        assertTrue(sit.hasNext());
        assertFalse(sit.search((int v) -> v0 - 1 - v));
        assertTrue(sit.hasNext());
        assertEquals(v0, sit.start());
        assertTrue(sit.search((int v) -> v0 - v));
        assertTrue(sit.hasNext());
        assertEquals(v0, sit.start());
        assertTrue(sit.search((int v) -> v0 + 1 - v));
        assertTrue(sit.hasNext());
        assertEquals(v0, sit.start());
        assertTrue(sit.search((int v) -> v0 + 2 - v));
        assertFalse(sit.hasNext());
        assertEquals(v1, sit.start());
    }

    @Test
    public void testContainerShortBatchIterForEach() {
        final int v0 = 32769;
        final int v1 = 32769 + 2;
        final TwoValuesContainer tv = new TwoValuesContainer((short) v0, (short) v1);
        final ContainerShortBatchIterator cbit = tv.getShortBatchIterator(0);
        assertTrue(cbit.hasNext());
        final short[] values = new short[2];
        final MutableInteger pos = new MutableInteger();
        pos.value = 0;
        cbit.forEach((final short v) -> {
            values[pos.value++] = v;
            return true;
        });
        assertEquals(2, pos.value);
        assertEquals((short) v0, values[0]);
        assertEquals((short) v1, values[1]);
    }

    @Test
    public void testContainerShortBatchIterNext() {
        final int v0 = 32769;
        final int v1 = 32769 + 2;
        final TwoValuesContainer tv = new TwoValuesContainer((short) v0, (short) v1);
        final ContainerShortBatchIterator cbit = tv.getShortBatchIterator(0);
        assertTrue(cbit.hasNext());
        final short[] values = new short[2];
        final int n = cbit.next(values, 0, 10);
        assertEquals(2, n);
        assertEquals((short) v0, values[0]);
        assertEquals((short) v1, values[1]);
    }

    @Test
    public void testGetFind() {
        final TwoValuesContainer tv = new TwoValuesContainer((short) 10, (short) 12);
        int findResult = tv.find((short) 9);
        assertEquals(-1, findResult);
        findResult = tv.find((short) 10);
        assertEquals(0, findResult);
        assertEquals(10, tv.select(0));
        findResult = tv.find((short) 11);
        assertEquals(~1, findResult);
        findResult = tv.find((short) 12);
        assertEquals(1, findResult);
        assertEquals(12, tv.select(1));
        findResult = tv.find((short) 13);
        assertEquals(~2, findResult);
    }

    @Test
    public void testFindRanges() {
        final TwoValuesContainer tv = new TwoValuesContainer((short) 10, (short) 12);
        final int[] ranges = new int[2];
        final RangeConsumer consumer = new RangeConsumer() {
            int i = 0;

            @Override
            public void accept(final int begin, final int end) {
                ranges[i++] = begin;
                ranges[i++] = end;
            }
        };
        assertFalse(tv.findRanges(
                consumer,
                tv.getShortRangeIterator(0),
                2));
        assertEquals(0, ranges[0]);
        assertEquals(2, ranges[1]);
    }

    @Test
    public void testReverseIterator() {
        final TwoValuesContainer tv = new TwoValuesContainer((short) 10, (short) 12);
        {
            final ShortAdvanceIterator rit = tv.getReverseShortIterator();
            assertTrue(rit.hasNext());
            assertEquals(12, rit.nextAsInt());
            assertTrue(rit.hasNext());
            assertEquals(10, rit.nextAsInt());
            assertFalse(rit.hasNext());
        }
        {
            final ShortAdvanceIterator rit2 = tv.getReverseShortIterator();
            assertTrue(rit2.advance(13));
            assertEquals(12, rit2.currAsInt());
            assertTrue(rit2.hasNext());
            assertTrue(rit2.advance(11));
            assertEquals(10, rit2.currAsInt());
            assertFalse(rit2.hasNext());
        }
        {
            final ShortAdvanceIterator rit3 = tv.getReverseShortIterator();
            assertTrue(rit3.advance(10));
            assertEquals(10, rit3.currAsInt());
            assertFalse(rit3.hasNext());
        }
    }

    @Test
    public void testAdvanceRegression0() {
        final TwoValuesContainer tv = new TwoValuesContainer((short) 0, (short) 2);
        final SearchRangeIterator rit = tv.getShortRangeIterator(0);
        assertTrue(rit.hasNext());
        rit.next();
        assertTrue(rit.advance(1));
        assertEquals(2, rit.start());
        assertEquals(3, rit.end());
        assertFalse(rit.hasNext());
    }

    @Test
    public void testShortRangeForEachRegression0() {
        final short v0 = (short) 1;
        final short v1 = (short) 3;
        final TwoValuesContainer tv = new TwoValuesContainer(v0, v1);
        tv.forEachRange(1, (final short start, final short end) -> {
            assertTrue(v1 == start);
            assertTrue(v1 == end);
            return true;
        });
    }

    @Test
    public void testFindRanges2() {
        final short v0 = (short) 1;
        final short v1 = (short) 3;
        final TwoValuesContainer tv = new TwoValuesContainer(v0, v1);
        final Container c2 = Container.singleRange(3, 4);
        final MutableInteger count = new MutableInteger();
        count.value = 0;
        final RangeConsumer r = (final int posStart, final int posEnd) -> {
            assertEquals(0, count.value);
            ++count.value;
            assertEquals(1, posStart);
            assertEquals(2, posEnd);
        };
        final boolean v = tv.findRanges(r, c2.getShortRangeIterator(0), 1);
        assertTrue(v);
    }

    @Test
    public void testFindRanges3() {
        final short v0 = (short) 10;
        final short v1 = (short) 30;
        final TwoValuesContainer tv = new TwoValuesContainer(v0, v1);
        final Container c2 = tv.deepCopy();
        final MutableInteger count = new MutableInteger();
        count.value = 0;
        final RangeConsumer r = (final int posStart, final int posEnd) -> {
            assertEquals(0, count.value);
            ++count.value;
            assertEquals(0, posStart);
            assertEquals(1, posEnd);
        };
        final boolean v = tv.findRanges(r, c2.getShortRangeIterator(0), 0);
        assertTrue(v);
    }

    @Test
    public void testOrCaseRegression0() {
        final int v0 = 10;
        final int v1 = 30;
        final TwoValuesContainer tv0 = new TwoValuesContainer((short) v0, (short) v1);
        final int v2 = 13;
        final int v3 = 15;
        final TwoValuesContainer tv1 = new TwoValuesContainer((short) v2, (short) v3);
        final Container c = tv0.or(tv1);
        assertEquals(4, c.getCardinality());
        final Container expected = new ArrayContainer(new short[] {(short) v0, (short) v2, (short) v3, (short) v1});
        assertTrue(expected.sameContents(c));
    }

    @Test
    public void testAndNotUsesHints() {
        final int v0 = 10;
        final int v1 = 30;
        final TwoValuesContainer tv = new TwoValuesContainer((short) v0, (short) v1);
        for (int r0start = v0 - 1; r0start <= v0 + 2; ++r0start) {
            for (int r1start = v1 - 1; r1start <= v1 + 2; ++r1start) {
                final String m = "r0start==" + r0start + " && r1start==" + r1start;
                final RunContainer r = new RunContainer(r0start, 15, r1start, 35);
                final Container ans = r.andNot(tv);
                final Container expected = r.andNot(tv.toLargeContainer());
                assertTrue(ans.sameContents(expected));
            }
        }
    }

    @Test
    public void testToBitmapContainer() {
        final Random rand = new Random(1);
        for (int v0 = rand.nextInt(64); v0 < 0xFFFF; v0 += 64 + rand.nextInt(64)) {
            for (int v1 = v0 + 2 + rand.nextInt(16); v1 <= 0xFFFF; v1 += 64 + rand.nextInt(64)) {
                final TwoValuesContainer tv = new TwoValuesContainer((short) v0, (short) v1);
                final BitmapContainer bc = tv.toBitmapContainer();
                bc.validate();
                assertTrue(tv.sameContents(bc));
            }
        }
    }
}
