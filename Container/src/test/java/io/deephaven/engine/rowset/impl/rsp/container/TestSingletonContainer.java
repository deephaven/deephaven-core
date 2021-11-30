package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSingletonContainer {
    @Test
    public void testFindGet() {
        final SingletonContainer c = new SingletonContainer((short) 10);
        int findResult = c.find((short) 9);
        assertEquals(~0, findResult);
        findResult = c.find((short) 10);
        assertEquals(0, findResult);
        assertEquals(10, c.select(0));
        findResult = c.find((short) 11);
        assertEquals(~1, findResult);
    }

    @Test
    public void testIterator() {
        final SingletonContainer c = new SingletonContainer((short) 10);
        final RangeIterator it = c.getShortRangeIterator(0);
        assertTrue(it.hasNext());
        it.next();
        assertEquals(10, it.start());
        assertEquals(11, it.end());
        assertFalse(it.hasNext());
    }

    @Test
    public void testShortBatchIterator() {
        final SingletonContainer c = new SingletonContainer((short) 20);
        final int skip = 0;
        final ContainerShortBatchIterator it = c.getShortBatchIterator(skip);
        assertTrue(it.hasNext());
        final short[] vs = new short[2];
        final int n = it.next(vs, 1, 1);
        assertEquals(1, n);
        assertTrue((short) 20 == vs[1]);
    }

    @Test
    public void testFindRanges() {
        final SingletonContainer c = new SingletonContainer((short) 10);
        assertTrue(c.findRanges(
                (final int start, final int end) -> {
                    assertEquals(0, start);
                    assertEquals(1, end);
                },
                c.getShortRangeIterator(0),
                0));
    }

    @Test
    public void testReverseIterator() {
        final SingletonContainer c = new SingletonContainer((short) 10);
        final ShortAdvanceIterator it = c.getReverseShortIterator();
        assertTrue(it.hasNext());
        assertEquals(10, it.nextAsInt());
        assertFalse(it.hasNext());

        final ShortAdvanceIterator it2 = c.getReverseShortIterator();
        assertTrue(it2.advance(10));
        assertEquals(10, it2.currAsInt());
        assertFalse(it2.hasNext());
    }

    @Test
    public void testNot() {
        final SingletonContainer c = new SingletonContainer((short) 10);
        final int min = 8;
        final int max = 12;
        for (int first = min; first <= max; ++first) {
            final String m = "first==" + first;
            for (int last = first; last <= max; ++last) {
                final String m2 = m + " && last==" + last;
                final int end = last + 1;
                final Container not = c.not(first, end);
                for (int i = min; i <= max; ++i) {
                    final String m3 = m2 + " && i==" + i;
                    final boolean expected;
                    if (first <= i && i <= last) {
                        expected = i != 10;
                    } else {
                        expected = i == 10;
                    }
                    assertEquals(m3, expected, not.contains((short) i));
                }
            }
        }
    }

    @Test
    public void testAdd() {
        final SingletonContainer c = new SingletonContainer((short) 10);
        for (int i = 8; i <= 12; ++i) {
            Container r = c.iset((short) i);
            if (i == 8 || i == 12) {
                assertTrue(r instanceof TwoValuesContainer);
            } else if (i == 9 || i == 11) {
                assertTrue(r instanceof SingleRangeContainer);
            } else {
                assertTrue(c == r);
            }
            if (i == 10) {
                assertEquals(1, r.getCardinality());
            } else {
                assertTrue(r.contains((short) i));
                assertEquals(2, r.getCardinality());
            }
            assertTrue(r.contains((short) 10));
        }
    }

    @Test
    public void testAddRange() {
        final SingletonContainer c = new SingletonContainer((short) 10);
        final int min = 7;
        final int max = 13;
        for (int first = min; first <= max; ++first) {
            final String m = "first==" + first;
            for (int last = first; last <= max; ++last) {
                final String m2 = m + " && last==" + last;
                Container r = c.add(first, last + 1);
                for (int i = min; i <= max; ++i) {
                    final String m3 = m2 + " && i==" + i;
                    final boolean expectedContains = i == 10 || (first <= i && i <= last);
                    assertEquals(m3, expectedContains, r.contains((short) i));
                    final int expectedCardinality = last - first + 1 + ((last < 10 || 10 < first) ? 1 : 0);
                    assertEquals(m3, expectedCardinality, r.getCardinality());
                }
                if ((first <= 11 && 9 <= last)) {
                    if (r.getCardinality() == 1) {
                        assertTrue(c == r);
                    } else {
                        assertTrue(m2, r instanceof SingleRangeContainer);
                    }
                } else {
                    if (r.getCardinality() == 2) {
                        assertTrue(m2, r instanceof TwoValuesContainer);
                    } else {
                        assertTrue(m2, r instanceof RunContainer);
                    }
                }
            }
        }
    }

    @Test
    public void testAddType() {
        final int v = 10;
        final SingletonContainer c = new SingletonContainer((short) v);
        final int min = v - 3;
        final int max = v + 3;
        for (int first = min; first <= max; ++first) {
            final String m = "first==" + first;
            for (int last = first; last <= max; ++last) {
                final String m2 = m + " && last==" + last;
                final Container r = c.iadd(first, last + 1);
                final int deltaCard = last - first + ((first <= v && v <= last) ? 0 : 1);
                if (deltaCard == 1) {
                    if (last < v - 1 || v + 1 < first) {
                        assertTrue(m2, r instanceof TwoValuesContainer);
                    } else {
                        assertTrue(m2, r instanceof SingleRangeContainer);
                    }
                    assertEquals(m2, 2, r.getCardinality());
                } else if (deltaCard == 0) {
                    assertTrue(m2, c == r);
                } else {
                    assertEquals(m2, 1 + deltaCard, r.getCardinality());
                    if (first <= v + 1 && v - 1 <= last) {
                        assertTrue(m2, r instanceof SingleRangeContainer);
                    } else {
                        assertTrue(m2, r instanceof ArrayContainer || r instanceof RunContainer);
                    }
                }
            }
        }
    }
}
