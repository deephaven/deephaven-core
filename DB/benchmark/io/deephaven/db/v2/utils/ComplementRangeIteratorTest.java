package io.deephaven.db.v2.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class ComplementRangeIteratorTest {
    @Test
    public void testSimpleComplement() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(10, 20);
        ix.insertRange(22, 40);
        ix.insertRange(50, 100);

        try (final ComplementRangeIterator cit = new ComplementRangeIterator(ix.rangeIterator())) {

            assertTrue(cit.hasNext());
            assertEquals(0, cit.next());
            assertEquals(0, cit.currentRangeStart());
            assertEquals(9, cit.currentRangeEnd());

            assertTrue(cit.hasNext());
            assertEquals(21, cit.next());
            assertEquals(21, cit.currentRangeStart());
            assertEquals(21, cit.currentRangeEnd());

            assertTrue(cit.hasNext());
            assertEquals(41, cit.next());
            assertEquals(41, cit.currentRangeStart());
            assertEquals(49, cit.currentRangeEnd());

            assertTrue(cit.hasNext());
            assertEquals(101, cit.next());
            assertEquals(101, cit.currentRangeStart());
            assertEquals(Long.MAX_VALUE, cit.currentRangeEnd());

            assertFalse(cit.hasNext());
        }
    }

    @Test
    public void testEmptyComplement() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(0, Long.MAX_VALUE);

        try (final ComplementRangeIterator cit = new ComplementRangeIterator(ix.rangeIterator())) {
            assertFalse(cit.hasNext());
        }
    }

    @Test
    public void testFullComplement() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        try (final ComplementRangeIterator cit = new ComplementRangeIterator(ix.rangeIterator())) {

            assertTrue(cit.hasNext());
            assertEquals(0, cit.next());
            assertEquals(0, cit.currentRangeStart());
            assertEquals(Long.MAX_VALUE, cit.currentRangeEnd());

            assertFalse(cit.hasNext());
        }
    }

    @Test
    public void testSingleRangeInput() {
        final long[] points = new long[] {0, 100, 200, Long.MAX_VALUE};
        for (int i = 0; i < points.length - 1; ++i) {
            final long start = points[i];
            final long end = points[i + 1];
            final Index ix = Index.FACTORY.getIndexByRange(start, end);
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(cit.hasNext());
                final String m = "i=" + i;
                if (i == 0) {
                    assertEquals(m, points[1] + 1, cit.next());
                    assertEquals(m, points[1] + 1, cit.currentRangeStart());
                    assertEquals(m, Long.MAX_VALUE, cit.currentRangeEnd());
                } else {
                    assertEquals(m, 0, cit.next());
                    assertEquals(m, 0, cit.currentRangeStart());
                    assertEquals(m, points[i] - 1, cit.currentRangeEnd());
                }

                if (i == 0 || i == points.length - 2) {
                    assertFalse(m, cit.hasNext());
                } else {
                    assertTrue(m, cit.hasNext());
                    assertEquals(m, points[i + 1] + 1, cit.next());
                    assertEquals(m, points[i + 1] + 1, cit.currentRangeStart());
                    assertEquals(m, Long.MAX_VALUE, cit.currentRangeEnd());
                }
            }
        }
    }

    @Test
    public void testInputTwoRangesContainsZero() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(0, 20);
        ix.insertRange(22, 40);

        try (final ComplementRangeIterator cit = new ComplementRangeIterator(ix.rangeIterator())) {
            assertTrue(cit.hasNext());
            assertEquals(21, cit.next());
            assertEquals(21, cit.currentRangeStart());
            assertEquals(21, cit.currentRangeEnd());

            assertTrue(cit.hasNext());
            assertEquals(41, cit.next());
            assertEquals(41, cit.currentRangeStart());
            assertEquals(Long.MAX_VALUE, cit.currentRangeEnd());

            assertFalse(cit.hasNext());
        }
    }

    @Test
    public void testInputTwoRangesContainsMax() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(10, 20);
        ix.insertRange(22, Long.MAX_VALUE);

        try (final ComplementRangeIterator cit = new ComplementRangeIterator(ix.rangeIterator())) {
            assertTrue(cit.hasNext());
            assertEquals(0, cit.next());
            assertEquals(0, cit.currentRangeStart());
            assertEquals(9, cit.currentRangeEnd());

            assertTrue(cit.hasNext());
            assertEquals(21, cit.next());
            assertEquals(21, cit.currentRangeStart());
            assertEquals(21, cit.currentRangeEnd());

            assertFalse(cit.hasNext());
        }
    }

    @Test
    public void testAdvanceSimple() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(10, 20);
        ix.insertRange(30, 40);

        for (long v : new long[] {0, 1, 5, 8, 9}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertTrue(m, cit.advance(v));
                assertEquals(m, v, cit.currentRangeStart());
                assertEquals(m, 9, cit.currentRangeEnd());
                assertTrue(m, cit.hasNext());
                assertEquals(m, 21, cit.next());
                assertEquals(m, 21, cit.currentRangeStart());
                assertEquals(m, 29, cit.currentRangeEnd());
            }
        }

        for (long v : new long[] {10, 11, 15, 20, 21, 26, 28, 29}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertTrue(m, cit.advance(v));
                assertEquals(m, Math.max(v, 21), cit.currentRangeStart());
                assertEquals(m, 29, cit.currentRangeEnd());
                assertTrue(m, cit.hasNext());
                assertEquals(m, 41, cit.next());
                assertEquals(m, 41, cit.currentRangeStart());
                assertEquals(m, Long.MAX_VALUE, cit.currentRangeEnd());
            }
        }

        for (long v : new long[] {30, 31, 35, 39, 40, 41, 600, Long.MAX_VALUE - 1,
                Long.MAX_VALUE}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertTrue(m, cit.advance(v));
                assertEquals(m, Math.max(v, 41), cit.currentRangeStart());
                assertEquals(m, Long.MAX_VALUE, cit.currentRangeEnd());
                assertFalse(m, cit.hasNext());
            }
        }
    }

    @Test
    public void testAdvanceInputContainsZero() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(0, 10);
        ix.insertRange(20, 40);

        for (long v : new long[] {0, 1, 5, 9, 10, 11, 12, 15, 18, 19}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertTrue(m, cit.advance(v));
                assertEquals(m, Math.max(11, v), cit.currentRangeStart());
                assertEquals(m, 19, cit.currentRangeEnd());
                assertTrue(m, cit.hasNext());
                assertEquals(m, 41, cit.next());
                assertEquals(m, 41, cit.currentRangeStart());
                assertEquals(m, Long.MAX_VALUE, cit.currentRangeEnd());
            }
        }
    }

    @Test
    public void testAdvanceInputContainsMax() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(5, 10);
        ix.insertRange(20, Long.MAX_VALUE);

        for (long v : new long[] {20, 21, 25, 1000000, Long.MAX_VALUE - 1, Long.MAX_VALUE}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertEquals(0, cit.next());
                assertFalse(m, cit.advance(v));
                assertFalse(m, cit.hasNext());
            }
        }
    }

    @Test
    public void testAdvanceEmpty() {
        final Index ix = Index.FACTORY.getIndexByRange(0, Long.MAX_VALUE);
        for (long v : new long[] {0, 1, 20, Long.MAX_VALUE - 1, Long.MAX_VALUE}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertFalse(m, cit.advance(v));
                assertFalse(m, cit.hasNext());
            }
        }
    }

    @Test
    public void testAdvanceCoverage1() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(0, 10);
        ix.insertRange(20, Long.MAX_VALUE);
        for (long v : new long[] {20, 21, 30, Long.MAX_VALUE}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertEquals(11, cit.next());
                assertFalse(cit.advance(v));
                assertFalse(cit.hasNext());
            }
        }
    }

    @Test
    public void testAdvanceCoverage2() {
        final Index ix = Index.FACTORY.getEmptyIndex();
        ix.insertRange(10, 20);
        ix.insertRange(30, 40);
        ix.insertRange(50, 60);
        for (long v : new long[] {30, 31, 35, 39, 40, 41, 42, 45, 48, 49}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertEquals(m, 0, cit.next());
                assertTrue(m, cit.advance(v));
                assertEquals(m, Math.max(41, v), cit.currentRangeStart());
                assertEquals(m, 49, cit.currentRangeEnd());
                assertTrue(m, cit.hasNext());
                assertEquals(m, 61, cit.next());
                assertEquals(m, 61, cit.currentRangeStart());
                assertEquals(m, Long.MAX_VALUE, cit.currentRangeEnd());
                assertFalse(m, cit.hasNext());
            }
        }

        for (long v : new long[] {40, 41, 42, 45, 48, 49}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertEquals(m, 0, cit.next());
                assertTrue(m, cit.advance(v));
                assertEquals(m, Math.max(41, v), cit.currentRangeStart());
                assertEquals(m, 49, cit.currentRangeEnd());
                assertTrue(m, cit.hasNext());
                assertEquals(m, 61, cit.next());
                assertEquals(m, 61, cit.currentRangeStart());
                assertEquals(m, Long.MAX_VALUE, cit.currentRangeEnd());
            }
        }

        ix.insertRange(70, 80);
        for (long v : new long[] {30, 31, 35, 39, 40, 41, 42, 45, 48, 49}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertEquals(m, 0, cit.next());
                assertTrue(m, cit.advance(v));
                assertEquals(m, Math.max(41, v), cit.currentRangeStart());
                assertEquals(m, 49, cit.currentRangeEnd());
                assertTrue(m, cit.hasNext());
                assertEquals(m, 61, cit.next());
                assertEquals(m, 61, cit.currentRangeStart());
                assertEquals(m, 69, cit.currentRangeEnd());
                assertTrue(m, cit.hasNext());
            }
        }

        ix.insertRange(70, Long.MAX_VALUE);
        for (long v : new long[] {50, 51, 55, 59, 60, 61, 62, 65, 68, 69}) {
            final String m = "v=" + v;
            try (final ComplementRangeIterator cit =
                new ComplementRangeIterator(ix.rangeIterator())) {
                assertTrue(m, cit.hasNext());
                assertEquals(m, 0, cit.next());
                assertTrue(m, cit.advance(v));
                assertEquals(m, Math.max(61, v), cit.currentRangeStart());
                assertEquals(m, 69, cit.currentRangeEnd());
                assertFalse(m, cit.hasNext());
            }
        }
    }
}
