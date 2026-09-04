package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link RankCursor} answers every query exactly as the container's own {@code select}, {@code find} and
 * {@code getShortRangeIterator} do, whatever order the queries arrive in and however the three kinds of query are
 * interleaved on one cursor.
 */
public class TestRankCursor {

    private static Container bitmapContainer(final Random r, final double density, final int lo, final int hi) {
        Container c = new BitmapContainer();
        for (int v = lo; v < hi; ++v) {
            if (r.nextDouble() < density) {
                c = c.iset((short) v);
            }
        }
        assertTrue("fixture is a BitmapContainer, got " + c.getClass().getSimpleName(),
                c instanceof BitmapContainer);
        return c;
    }

    private static Container runContainer(final Random r, final int runs, final int maxRunLength) {
        Container c = new RunContainer();
        int v = r.nextInt(8);
        for (int i = 0; i < runs && v < 65536; ++i) {
            final int len = 1 + r.nextInt(maxRunLength);
            final int end = Math.min(65536, v + len);
            c = c.iadd(v, end);
            v = end + 1 + r.nextInt(12);
        }
        assertTrue("fixture is a RunContainer, got " + c.getClass().getSimpleName(), c instanceof RunContainer);
        return c;
    }

    private static Container arrayContainer(final Random r, final int count) {
        Container c = new ArrayContainer();
        for (int i = 0; i < count; ++i) {
            c = c.iset((short) r.nextInt(65536));
        }
        assertTrue("fixture is an ArrayContainer, got " + c.getClass().getSimpleName(),
                c instanceof ArrayContainer);
        return c;
    }

    private static List<int[]> ranges(final SearchRangeIterator it) {
        final List<int[]> out = new ArrayList<>();
        while (it.hasNext()) {
            it.next();
            out.add(new int[] {it.start(), it.end()});
        }
        return out;
    }

    private static void assertSameRanges(final String what, final Container c, final RankCursor cursor,
            final int rank) {
        final List<int[]> expected = ranges(c.getShortRangeIterator(rank));
        final List<int[]> actual = ranges(cursor.getShortRangeIterator(rank));
        assertEquals(what + " range count from rank " + rank, expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            assertEquals(what + " range " + i + " start from rank " + rank, expected.get(i)[0], actual.get(i)[0]);
            assertEquals(what + " range " + i + " end from rank " + rank, expected.get(i)[1], actual.get(i)[1]);
        }
    }

    private static void check(final String what, final Container c, final Random r) {
        final int card = c.getCardinality();
        final RankCursor cursor = new RankCursor();
        cursor.reset(c);

        // Ascending ranks, the intended use.
        for (int rank = 0; rank < card; ++rank) {
            assertEquals(what + " select(" + rank + ")", c.select(rank), cursor.select(rank));
        }
        // Ranks in random order, which makes the cursor start over.
        for (int i = 0; i < 2000; ++i) {
            final int rank = r.nextInt(card);
            assertEquals(what + " select(" + rank + ") random", c.select(rank), cursor.select(rank));
        }

        // Every value, ascending, present or not.
        for (int v = 0; v < 65536; ++v) {
            assertEquals(what + " find(" + v + ")", c.find((short) v), cursor.find((short) v));
        }
        // Values in random order.
        for (int i = 0; i < 2000; ++i) {
            final int v = r.nextInt(65536);
            assertEquals(what + " find(" + v + ") random", c.find((short) v), cursor.find((short) v));
        }

        // Range iterators from ascending ranks, and from random ranks.
        for (int rank = 0; rank < card; rank += 1 + r.nextInt(Math.max(1, card / 200))) {
            assertSameRanges(what + " ascending", c, cursor, rank);
        }
        for (int i = 0; i < 200; ++i) {
            assertSameRanges(what + " random", c, cursor, r.nextInt(card));
        }
        assertFalse(what + " range iterator past the end", cursor.getShortRangeIterator(card).hasNext());

        // The three kinds of query interleaved on one cursor, in random order.
        for (int i = 0; i < 3000; ++i) {
            switch (r.nextInt(3)) {
                case 0: {
                    final int rank = r.nextInt(card);
                    assertEquals(what + " mixed select(" + rank + ")", c.select(rank), cursor.select(rank));
                    break;
                }
                case 1: {
                    final int v = r.nextInt(65536);
                    assertEquals(what + " mixed find(" + v + ")", c.find((short) v), cursor.find((short) v));
                    break;
                }
                default: {
                    final int rank = r.nextInt(card);
                    final SearchRangeIterator expected = c.getShortRangeIterator(rank);
                    final SearchRangeIterator actual = cursor.getShortRangeIterator(rank);
                    assertEquals(what + " mixed range iterator hasNext from " + rank, expected.hasNext(),
                            actual.hasNext());
                    if (expected.hasNext()) {
                        expected.next();
                        actual.next();
                        assertEquals(what + " mixed range start from " + rank, expected.start(), actual.start());
                        assertEquals(what + " mixed range end from " + rank, expected.end(), actual.end());
                    }
                    break;
                }
            }
        }
    }

    @Test
    public void testDenseBitmap() {
        final Random r = new Random(1);
        check("dense bitmap", bitmapContainer(r, 0.5, 0, 65536), r);
    }

    @Test
    public void testSparseBitmap() {
        final Random r = new Random(2);
        check("sparse bitmap", bitmapContainer(r, 0.08, 0, 65536), r);
    }

    /** Empty words before, between, and after the values. */
    @Test
    public void testBitmapWithEmptyWords() {
        final Random r = new Random(3);
        Container c = new BitmapContainer();
        for (int v = 3000; v < 9000; v += 1 + r.nextInt(3)) {
            c = c.iset((short) v);
        }
        for (int v = 40000; v < 41000; ++v) {
            c = c.iset((short) v);
        }
        c = c.iset((short) 65535);
        assertTrue(c instanceof BitmapContainer);
        check("bitmap with empty words", c, r);
    }

    @Test
    public void testManyShortRuns() {
        final Random r = new Random(4);
        check("many short runs", runContainer(r, 1500, 12), r);
    }

    @Test
    public void testFewLongRuns() {
        final Random r = new Random(5);
        check("few long runs", runContainer(r, 6, 8000), r);
    }

    @Test
    public void testSingleRun() {
        final Random r = new Random(6);
        Container c = new RunContainer();
        c = c.iadd(1000, 30000);
        assertTrue(c instanceof RunContainer);
        check("single run", c, r);
    }

    @Test
    public void testArrayContainerDelegates() {
        final Random r = new Random(7);
        check("array", arrayContainer(r, 3000), r);
    }

    @Test
    public void testSmallContainersDelegate() {
        final Random r = new Random(8);
        check("singleton", Container.singleton((short) 12345), r);
        check("two values", Container.twoValues((short) 7, (short) 40000), r);
        check("single range", Container.singleRange(500, 60000), r);
    }
}
