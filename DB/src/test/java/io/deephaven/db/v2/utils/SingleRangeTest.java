package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.util.LongChunkAppender;
import io.deephaven.db.v2.sources.chunk.util.LongChunkIterator;
import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.singlerange.SingleRangeOrderedKeys;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.function.Consumer;

import static io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import static io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import static org.junit.Assert.*;

public class SingleRangeTest {
    @Test
    public void testIterator() {
        final long start = 10;
        final long end = 21;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        final Index.Iterator it = ix.iterator();
        long expected = start;
        while (it.hasNext()) {
            assertEquals(expected, it.nextLong());
            ++expected;
        }
        assertEquals(end + 1, expected);
    }

    @Test
    public void testRangeIterator() {
        final long start = 10;
        final long end = 21;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        final Index.RangeIterator it = ix.rangeIterator();
        assertTrue(it.hasNext());
        it.next();
        assertEquals(start, it.currentRangeStart());
        assertEquals(end, it.currentRangeEnd());
        assertFalse(it.hasNext());
    }

    @Test
    public void testRemoveIndex() {
        final long start = 10;
        final long end = 21;
        TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        Index ix2 = new TreeIndex(SingleRange.make(start - 1, end + 1));
        ix.remove(ix2);
        assertEquals(TreeIndexImpl.EMPTY, ix.getImpl());

        ix = new TreeIndex(SingleRange.make(start, end));
        ix2 = new TreeIndex(SingleRange.make(start - 1, end - 1));
        ix.remove(ix2);
        assertTrue(ix.getImpl() instanceof SingleRange);
        assertEquals(end, ix.firstKey());
        assertEquals(end, ix.lastKey());

        ix = new TreeIndex(SingleRange.make(start, end));
        ix2 = new TreeIndex(SingleRange.make(start + 1, end + 1));
        ix.remove(ix2);
        assertTrue(ix.getImpl() instanceof SingleRange);
        assertEquals(start, ix.firstKey());
        assertEquals(start, ix.lastKey());

        ix = new TreeIndex(SingleRange.make(start, end));
        ix2 = new TreeIndex(SingleRange.make(start + 1, end - 1));
        ix.remove(ix2);
        assertTrue(ix.getImpl() instanceof SortedRanges);
        assertEquals(2, ix.size());
        assertEquals(start, ix.firstKey());
        assertEquals(end, ix.lastKey());

        ix = new TreeIndex(SingleRange.make(start, end));
        ix2 = Index.FACTORY.getEmptyIndex();
        ix2.insertRange(10, 10);
        ix2.insertRange(21, 21);
        ix.remove(ix2);
        assertEquals(10, ix.size());
        assertTrue(ix.containsRange(11, 20));
    }

    @Test
    public void testRemoveKey() {
        final long start = 10;
        final long end = 21;
        TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        ix.remove(9);
        assertTrue(ix.getImpl() instanceof SingleRange);
        assertEquals(10, ix.firstKey());
        assertEquals(21, ix.lastKey());
        ix.remove(22);
        assertTrue(ix.getImpl() instanceof SingleRange);
        assertEquals(10, ix.firstKey());
        assertEquals(21, ix.lastKey());
        ix.remove(10);
        assertTrue(ix.getImpl() instanceof SingleRange);
        assertEquals(11, ix.firstKey());
        assertEquals(21, ix.lastKey());
        ix.remove(21);
        assertTrue(ix.getImpl() instanceof SingleRange);
        assertEquals(11, ix.firstKey());
        assertEquals(20, ix.lastKey());
        ix.remove(12);
        assertTrue(ix.getImpl() instanceof SortedRanges);
        assertEquals(9, ix.size());
        assertTrue(ix.containsRange(11, 11));
        assertTrue(ix.containsRange(13, 20));
        TreeIndex ix2 = new TreeIndex(SingleRange.make(9, 9));
        ix2.remove(9);
        assertEquals(ix2.getImpl(), TreeIndexImpl.EMPTY);
    }

    private static void insertResultsInSingle(
            final long start1,
            final long end1,
            final long start2,
            final long end2,
            final long startExpected,
            final long endExpected) {
        final TreeIndex ix = new TreeIndex(SingleRange.make(start1, end1));
        Runnable check = () -> {
            assertTrue(ix.getImpl() instanceof SingleRange);
            assertEquals(startExpected, ix.firstKey());
            assertEquals(endExpected, ix.lastKey());
        };
        final TreeIndex ix2 = new TreeIndex(SingleRange.make(start2, end2));
        ix.insert(ix2);
        check.run();
        ix.clear();
        ix.insertRange(start1, end1);
        ix.insertRange(start2, end2);
        check.run();
    }

    @Test
    public void testInsertIndexAndInsertRange() {
        insertResultsInSingle(10, 20, 21, 22, 10, 22);
        insertResultsInSingle(10, 20, 8, 9, 8, 20);
        final long start = 10;
        final long end = 20;
        TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        TreeIndex ix2 = new TreeIndex(SingleRange.make(start - 3, start - 2));
        ix.insert(ix2);
        assertTrue(ix.getImpl() instanceof SortedRanges);
        assertEquals(ix.size(), end - start + 1 + 2);
        assertEquals(start - 3, ix.firstKey());
        assertEquals(-3L, ix.find(start - 1));
        assertEquals(end, ix.lastKey());

        ix = new TreeIndex(SingleRange.make(start, end));
        ix2 = new TreeIndex(SingleRange.make(end + 2, end + 3));
        ix.insert(ix2);
        assertTrue(ix.getImpl() instanceof SortedRanges);
        assertEquals(ix.size(), end - start + 1 + 2);
        assertEquals(start, ix.firstKey());
        assertEquals(-12L, ix.find(end + 1));
        assertEquals(end + 3, ix.lastKey());
    }

    private static void removeResultsCheck(
            final long start1,
            final long end1,
            final long start2,
            final long end2,
            final Consumer<Index> check) {
        final TreeIndex ix = new TreeIndex(SingleRange.make(start1, end1));
        final TreeIndex ix2 = new TreeIndex(SingleRange.make(start2, end2));
        ix.remove(ix2);
        check.accept(ix);
        ix.clear();
        ix.insertRange(start1, end1);
        ix.removeRange(start2, end2);
        check.accept(ix);
        ix.clear();
        ix.insertRange(start1, end1);
        final Index ix3 = ix.minus(ix2);
        check.accept(ix3);
    }

    private static void removeResultsCheck(
            final long start1,
            final long end1,
            final long start2,
            final long end2) {
        final Consumer<Index> check;
        if (start1 < start2 && end2 < end1) {
            // hole.
            check = (final Index t) -> {
                final TreeIndexImpl timpl = ((ImplementedByTreeIndexImpl) t).getImpl();
                assertTrue((timpl instanceof RspBitmap) || (timpl instanceof SortedRanges));
                int rangeCount = 0;
                final Index.RangeIterator it = t.rangeIterator();
                while (it.hasNext()) {
                    it.next();
                    final long start = it.currentRangeStart();
                    final long end = it.currentRangeEnd();
                    if (rangeCount == 0) {
                        assertEquals(start1, start);
                        assertEquals(start2 - 1, end);
                    } else {
                        assertEquals(end2 + 1, start);
                        assertEquals(end1, end);
                    }
                    ++rangeCount;
                    if (rangeCount > 2) {
                        fail();
                    }
                }
            };
        } else if (start2 <= start1 && end1 <= end2) {
            // completely removed
            check = (final Index t) -> {
                assertTrue(((ImplementedByTreeIndexImpl) t).getImpl() == TreeIndexImpl.EMPTY);
                assertTrue(t.empty());
            };
        } else {
            // results in single range
            check = (final Index t) -> {
                assertTrue(((ImplementedByTreeIndexImpl) t).getImpl() instanceof SingleRange);
                if (start1 < start2) {
                    assertEquals(start1, t.firstKey());
                    assertEquals(start2 - 1, t.lastKey());
                } else {
                    // end2 < end1
                    assertEquals(end2 + 1, t.firstKey());
                    assertEquals(end1, t.lastKey());
                }
            };
        }
        removeResultsCheck(start1, end1, start2, end2, check);
    }

    @Test
    public void testRemoveIndexAndRemoveRangeAndMinus() {
        removeResultsCheck(10, 20, 7, 9);
        removeResultsCheck(10, 20, 21, 22);
        removeResultsCheck(10, 20, 8, 10);
        removeResultsCheck(10, 20, 20, 22);
        removeResultsCheck(10, 20, 10, 20);
        removeResultsCheck(10, 20, 8, 11);
        removeResultsCheck(10, 20, 15, 25);
        TreeIndex ix = new TreeIndex(SingleRange.make(10, 20));
        TreeIndex ix2 = new TreeIndex(SingleRange.make(10, 20));
        ix.remove(ix2);
        assertEquals(TreeIndexImpl.EMPTY, ix.getImpl());
        ix.insertRange(10, 20);
        assertEquals(11, ix.size());
        ix.removeRange(10, 20);
        assertEquals(TreeIndexImpl.EMPTY, ix.getImpl());
        ix.insertRange(10, 20);
        assertEquals(11, ix.size());
        removeResultsCheck(10, 20, 11, 19);
    }

    @Test
    public void testInvert() {
        final long start = 10;
        final long end = 30;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        final RspBitmap rb = new RspBitmap();
        for (long i = start + 1; i <= end; i += 2) {
            rb.add(i);
        }
        final Index keys = new TreeIndex(rb);
        final Index r = ix.invert(keys, 10);
        assertEquals(5, r.size());
        assertTrue(r.subsetOf(Index.FACTORY.getIndexByValues(1, 3, 5, 7, 9)));

        keys.clear();
        keys.insertRange(10, 30);
        final Index r2 = ix.invert(keys, 10);
        assertEquals(11, r2.size());
        assertTrue(r.subsetOf(Index.FACTORY.getIndexByRange(0, 10)));

        keys.clear();
        keys.insert(30);
        final Index r3 = ix.invert(keys, 100);
        assertEquals(1, r3.size());
        assertEquals(20, r3.firstKey());
    }

    @Test
    public void testOrderedKeyIterator() {
        final long start = 10;
        final long end = 30;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        final OrderedKeys.Iterator okit = ix.getOrderedKeysIterator();
        final WritableLongChunk<OrderedKeyRanges> chunk = WritableLongChunk.makeWritableChunk(2);
        int offset = 0;
        long prevRelPos = -1;
        final int step = 2;
        while (okit.hasMore()) {
            final long relPos = okit.getRelativePosition();
            if (prevRelPos != -1) {
                assertEquals(step, relPos - prevRelPos);
            }
            final long expectedStart = start + offset;
            final long expectedEnd = (expectedStart + 1 > end) ? expectedStart : expectedStart + 1;
            assertEquals(expectedStart, okit.peekNextKey());
            final OrderedKeys oks = okit.getNextOrderedKeysWithLength(step);
            oks.fillKeyRangesChunk(chunk);
            assertEquals(2, chunk.size());
            assertEquals(expectedStart, chunk.get(0));
            assertEquals(expectedEnd, chunk.get(1));
            offset += 2;
            prevRelPos = relPos;
        }
    }

    @Test
    public void testOrderedKeys() {
        final long start = 10;
        final long end = 30;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        OrderedKeys ok = ix.getOrderedKeysByKeyRange(8, 9);
        assertEquals(OrderedKeys.EMPTY, ok);
        ok = ix.getOrderedKeysByKeyRange(31, 32);
        assertEquals(OrderedKeys.EMPTY, ok);
        ok = ix.getOrderedKeysByKeyRange(9, 10);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(10, ok.firstKey());
        assertEquals(10, ok.lastKey());
        ok = ix.getOrderedKeysByKeyRange(9, 11);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(10, ok.firstKey());
        assertEquals(11, ok.lastKey());
        ok = ix.getOrderedKeysByKeyRange(10, 11);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(10, ok.firstKey());
        assertEquals(11, ok.lastKey());
        ok = ix.getOrderedKeysByKeyRange(30, 31);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(30, ok.firstKey());
        assertEquals(30, ok.lastKey());
        ok = ix.getOrderedKeysByKeyRange(29, 31);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(29, ok.firstKey());
        assertEquals(30, ok.lastKey());
        ok = ix.getOrderedKeysByKeyRange(29, 30);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(29, ok.firstKey());
        assertEquals(30, ok.lastKey());
        ok = ix.getOrderedKeysByKeyRange(11, 29);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(11, ok.firstKey());
        assertEquals(29, ok.lastKey());

        ok = ix.getOrderedKeysByPosition(0, 0);
        assertEquals(OrderedKeys.EMPTY, ok);
        ok = ix.getOrderedKeysByPosition(0, 1);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(10, ok.firstKey());
        assertEquals(10, ok.lastKey());
        ok = ix.getOrderedKeysByPosition(1, 2);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(11, ok.firstKey());
        assertEquals(12, ok.lastKey());
        ok = ix.getOrderedKeysByPosition(20, 1);
        assertTrue(ok instanceof SingleRangeOrderedKeys);
        assertEquals(30, ok.firstKey());
        assertEquals(30, ok.lastKey());
        ok = ix.getOrderedKeysByPosition(21, 1);
        assertEquals(OrderedKeys.EMPTY, ok);
    }

    @Test
    public void testSubindexByKey() {
        final long start = 10;
        final long end = 30;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        Index r = ix.subindexByKey(5, 15);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(10, 15)));
        r = ix.subindexByKey(5, 15);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(10, 15)));
        r = ix.subindexByKey(15, 20);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(15, 20)));
        r = ix.subindexByKey(25, 35);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(25, 30)));
        r = ix.subindexByKey(11, 29);
        assertEquals(19, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(11, 29)));
    }

    @Test
    public void testSubindexByPos() {
        final long start = 10;
        final long end = 30;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        Index r = ix.subindexByPos(0, 5 + 1);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(10, 15)));
        r = ix.subindexByPos(5, 10 + 1);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(15, 20)));
        r = ix.subindexByPos(15, 35 + 1);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(25, 30)));
        r = ix.subindexByPos(1, 19 + 1);
        assertEquals(19, r.size());
        assertTrue(r.overlaps(Index.FACTORY.getIndexByRange(11, 29)));
    }

    private static void checkBinarySearch(final TreeIndex ix) {
        assertFalse(ix.empty());
        final long start = ix.firstKey();
        final long end = ix.lastKey();
        assertEquals(end - start + 1, ix.size());
        final Index.SearchIterator it = ix.searchIterator();
        assertTrue(it.hasNext());
        assertEquals(-1, it.binarySearchValue((final long key, final int dir) -> (int) ((start - 1) - key), 1));
        for (long v = start; v <= end; ++v) {
            final long compValue = v;
            assertEquals(v, it.binarySearchValue((final long key, final int dir) -> (int) ((compValue) - key), 1));
        }
        assertEquals(end, it.binarySearchValue((final long key, final int dir) -> (int) ((end + 1) - key), 1));

    }

    @Test
    public void testSimpleBinarySearch() {
        final long start = 10;
        final long end = 30;
        final TreeIndex six = new TreeIndex(SingleRange.make(start, end));
        checkBinarySearch(six);
        final TreeIndex rbix = new TreeIndex(new RspBitmap(start, end));
        checkBinarySearch(rbix);
    }

    @Test
    public void testGetKeysForPositions() {
        final long start = 10;
        final long end = 30;
        final long card = end - start + 1;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        final long[] positions = new long[] {0, 1, card - 2, card - 1};
        final long[] expected = new long[] {10, 11, 29, 30};
        final long[] result = new long[4];
        final WritableLongChunk<OrderedKeyIndices> resultsChunk = WritableLongChunk.writableChunkWrap(result);
        final LongChunk<OrderedKeyIndices> positionsChunk = WritableLongChunk.chunkWrap(positions);
        ix.getKeysForPositions(new LongChunkIterator(positionsChunk), new LongChunkAppender(resultsChunk));
        assertArrayEquals(expected, result);
    }

    @Test
    public void testOrderedKeysIteratorAdvanceRegression0() {
        final long start = 1000;
        final long end = 3000;
        final TreeIndex ix = new TreeIndex(SingleRange.make(start, end));
        final OrderedKeys.Iterator okit = ix.getOrderedKeysIterator();
        assertTrue(okit.hasMore());
        final boolean valid = okit.advance(500);
        assertTrue(valid);
        assertEquals(1000, okit.peekNextKey());
    }
}
