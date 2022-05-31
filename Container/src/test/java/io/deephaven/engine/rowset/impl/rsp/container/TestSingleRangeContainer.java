package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.Random;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSingleRangeContainer {
    @Test
    public void testShortITerator() {
        final Container c = Container.singleRange(3, 7);
        final ShortIterator it = c.getShortIterator();
        assertTrue(it.hasNext());
        assertEquals(3, it.next());
        assertTrue(it.hasNext());
        assertEquals(4, it.next());
        assertTrue(it.hasNext());
        assertEquals(5, it.next());
        assertTrue(it.hasNext());
        assertEquals(6, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testNot() {
        final Container c = Container.singleRange(30, 60);
        final Container c2 = c.not(40, 50);
        assertEquals(20, c2.getCardinality());
        assertTrue(c2.contains(30, 40));
        assertTrue(c2.contains(50, 60));
    }

    @Test
    public void testAddRange() {
        final Container c = Container.singleRange(10, 12);
        final Container c1 = c.add(7, 9);
        assertEquals(4, c1.getCardinality());
        assertTrue(c1.contains(7, 9));
        assertTrue(c1.contains(10, 12));
        final Container c2 = c.add(7, 10);
        assertEquals(5, c2.getCardinality());
        assertTrue(c2.contains(7, 12));
        final Container c3 = c.add(12, 14);
        assertEquals(4, c3.getCardinality());
        assertTrue(c3.contains(10, 14));
        final Container c4 = c.add(14, 16);
        assertEquals(4, c4.getCardinality());
        assertTrue(c4.contains(10, 12));
        assertTrue(c4.contains(14, 16));
    }

    @Test
    public void testSearchIterator() {
        final Container c = new SingleRangeContainer(10, 13);
        SearchRangeIterator sit = c.getShortRangeIterator(0);
        assertTrue(sit.hasNext());
        assertEquals(10, sit.start());
        assertEquals(13, sit.end());

        sit = c.getShortRangeIterator(2);
        assertTrue(sit.hasNext());
        sit.next();
        assertEquals(12, sit.start());
        assertEquals(13, sit.end());

        sit = c.getShortRangeIterator(0);
        assertTrue(sit.hasNext());
        assertFalse(sit.search((int v) -> 9 - v));
        assertFalse(sit.hasNext());
        assertEquals(10, sit.start());
        assertTrue(sit.search((int v) -> 11 - v));
        assertFalse(sit.hasNext());
        assertEquals(11, sit.start());
        assertTrue(sit.search((int v) -> 12 - v));
        assertFalse(sit.hasNext());
        assertEquals(12, sit.start());
        assertTrue(sit.search((int v) -> 13 - v));
        assertFalse(sit.hasNext());
        assertEquals(12, sit.start());
    }

    @Test
    public void testGetFind() {
        final Container c = Container.singleRange(10, 13);
        for (int i = 8; i <= 14; ++i) {
            final int findResult = c.find((short) i);
            final String m = "i==" + i;
            if (i < 10) {
                assertEquals(m, -1, findResult);
            } else if (i >= 13) {
                assertEquals(m, ~c.getCardinality(), findResult);
            } else {
                assertEquals(m, i, c.select(findResult));
            }
        }
    }

    @Test
    public void testGetShortRangeIterator() {
        final Container c = Container.singleRange(10, 13);
        final SearchRangeIterator sit = c.getShortRangeIterator(1);
        assertTrue(sit.hasNext());
        sit.next();
        assertEquals(11, sit.start());
        assertEquals(13, sit.end());
        assertFalse(sit.hasNext());

        final SearchRangeIterator sit2 = c.getShortRangeIterator(2);
        assertTrue(sit2.hasNext());
    }

    @Test
    public void testReverseIterator() {
        final Container c = Container.singleRange(10, 15);
        final ShortAdvanceIterator rit = c.getReverseShortIterator();
        for (int i = 14; i >= 10; --i) {
            assertTrue(rit.hasNext());
            assertEquals(i, rit.nextAsInt());
        }
        assertFalse(rit.hasNext());
        final ShortAdvanceIterator rit2 = c.getReverseShortIterator();
        assertTrue(rit2.advance(10));
        assertEquals(10, rit2.currAsInt());
        assertFalse(rit2.hasNext());
    }

    @Test
    public void testShortBatchIterator() {
        final int rfirst = 10;
        final int rlast = 20;
        final int rlen = rlast - rfirst + 1;
        final SingleRangeContainer c = new SingleRangeContainer(rfirst, rlast + 1);
        final int skip = 3;
        final ContainerShortBatchIterator it = c.getShortBatchIterator(skip);
        assertTrue(it.hasNext());
        final short[] buf = new short[rlen + 5];
        final int offset = 3;

        int step = 3;
        int n = it.next(buf, offset, step);
        assertEquals(step, n);
        int p = offset;
        int v = 10 + skip;
        for (int i = 0; i < step; ++i) {
            assertTrue("i==" + i, ((short) v) == buf[p]);
            v++;
            p++;
        }
        n = it.next(buf, offset, step);
        assertEquals(step, n);
        p = offset;
        for (int i = 0; i < step; ++i) {
            assertTrue("i==" + i, ((short) v) == buf[p]);
            v++;
            p++;
        }
        n = it.next(buf, offset, step);
        assertEquals(n, 2);
        p = offset;
        for (int i = 0; i < 2; ++i) {
            assertTrue("i==" + i, ((short) v) == buf[p]);
            v++;
            p++;
        }
    }

    @Test
    public void testShortRangeIteratorSearchRegression0() {
        final Container c = Container.singleRange(1, 10);
        final ContainerUtil.TargetComparator comp = (final int v) -> 1 - v;
        final SearchRangeIterator sit = c.getShortRangeIterator(0);
        assertTrue(sit.search(comp));
        assertEquals(1, sit.start());
        assertFalse(sit.hasNext());
    }

    @Test
    public void testShortRangeIteratorSearchRegression1() {
        final Container c = Container.singleRange(1, 3);
        final ContainerUtil.TargetComparator comp = (final int v) -> (1 - v) < 0 ? -1 : 1;
        final SearchRangeIterator sit = c.getShortRangeIterator(0);
        assertTrue(sit.search(comp));
        assertEquals(1, sit.start());
        assertFalse(sit.hasNext());
    }

    @Test
    public void testSelectByRankRangeRegression0() {
        final Container c = Container.singleRange(1, 3);
        final Container r = c.select(0, 1);
        assertEquals(1, r.getCardinality());
    }

    @Test
    public void testFindRanges() {
        final Container c = Container.singleRange(10, 19 + 1);
        final Container c2 = Container.singleRange(14, 18 + 1);
        final MutableInteger count = new MutableInteger();
        count.value = 0;
        final RangeConsumer r = (final int posStart, final int posEnd) -> {
            assertEquals(0, count.value);
            ++count.value;
            assertEquals(c2.first() - c.first(), posStart);
            assertEquals(8, posEnd);
        };
        final boolean v = c.findRanges(r, c2.getShortRangeIterator(0), 7);
        assertTrue(v);
    }

    @Test
    public void testShortRangeIteratorSearchRegression2() {
        final Container c = Container.singleRange(1, 4);
        final ContainerUtil.TargetComparator comp = (final int v) -> (1 - v) < 0 ? -1 : 1;
        final SearchRangeIterator sit = c.getShortRangeIterator(0);
        assertTrue(sit.search(comp));
        assertEquals(1, sit.start());
        assertFalse(sit.hasNext());
    }

    @Test
    public void testFindRangesRegression0() {
        final Container c = Container.singleRange(10, 18 + 1);
        final Container findArg = Container.singleRange(10, 12 + 1).add(15, 18 + 1);
        final MutableInteger count = new MutableInteger();
        count.value = 0;
        final RangeConsumer r = (final int posStart, final int posEnd) -> {
            ++count.value;
            assertEquals(1, count.value);
            assertEquals(0, posStart);
            assertEquals(2 + 1, posEnd);
        };
        final boolean v = c.findRanges(r, findArg.getShortRangeIterator(0), 3);
        assertTrue(v);
    }

    @Test
    public void testToBitmapContainer() {
        final Random rand = new Random(1);
        for (int v = rand.nextInt(16); v < 0xFFFF; v += rand.nextInt(16)) {
            final SingletonContainer tv = new SingletonContainer((short) v);
            final BitmapContainer bc = tv.toBitmapContainer();
            bc.validate();
            assertTrue(tv.sameContents(bc));
        }
    }
}
