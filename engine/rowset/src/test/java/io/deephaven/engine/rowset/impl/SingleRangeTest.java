/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.*;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.util.LongChunkAppender;
import io.deephaven.chunk.util.LongChunkIterator;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRangeRowSequence;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import org.junit.Test;

import java.util.function.Consumer;

import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import static org.junit.Assert.*;

public class SingleRangeTest {
    @Test
    public void testIterator() {
        final long start = 10;
        final long end = 21;
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        final RowSet.Iterator it = ix.iterator();
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
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        final RowSet.RangeIterator it = ix.rangeIterator();
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
        WritableRowSetImpl ix = new WritableRowSetImpl(SingleRange.make(start, end));
        WritableRowSet ix2 = new WritableRowSetImpl(SingleRange.make(start - 1, end + 1));
        ix.remove(ix2);
        assertEquals(OrderedLongSet.EMPTY, ix.getInnerSet());

        ix = new WritableRowSetImpl(SingleRange.make(start, end));
        ix2 = new WritableRowSetImpl(SingleRange.make(start - 1, end - 1));
        ix.remove(ix2);
        assertTrue(ix.getInnerSet() instanceof SingleRange);
        assertEquals(end, ix.firstRowKey());
        assertEquals(end, ix.lastRowKey());

        ix = new WritableRowSetImpl(SingleRange.make(start, end));
        ix2 = new WritableRowSetImpl(SingleRange.make(start + 1, end + 1));
        ix.remove(ix2);
        assertTrue(ix.getInnerSet() instanceof SingleRange);
        assertEquals(start, ix.firstRowKey());
        assertEquals(start, ix.lastRowKey());

        ix = new WritableRowSetImpl(SingleRange.make(start, end));
        ix2 = new WritableRowSetImpl(SingleRange.make(start + 1, end - 1));
        ix.remove(ix2);
        assertTrue(ix.getInnerSet() instanceof SortedRanges);
        assertEquals(2, ix.size());
        assertEquals(start, ix.firstRowKey());
        assertEquals(end, ix.lastRowKey());

        ix = new WritableRowSetImpl(SingleRange.make(start, end));
        ix2 = RowSetFactory.empty();
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
        WritableRowSetImpl ix = new WritableRowSetImpl(SingleRange.make(start, end));
        ix.remove(9);
        assertTrue(ix.getInnerSet() instanceof SingleRange);
        assertEquals(10, ix.firstRowKey());
        assertEquals(21, ix.lastRowKey());
        ix.remove(22);
        assertTrue(ix.getInnerSet() instanceof SingleRange);
        assertEquals(10, ix.firstRowKey());
        assertEquals(21, ix.lastRowKey());
        ix.remove(10);
        assertTrue(ix.getInnerSet() instanceof SingleRange);
        assertEquals(11, ix.firstRowKey());
        assertEquals(21, ix.lastRowKey());
        ix.remove(21);
        assertTrue(ix.getInnerSet() instanceof SingleRange);
        assertEquals(11, ix.firstRowKey());
        assertEquals(20, ix.lastRowKey());
        ix.remove(12);
        assertTrue(ix.getInnerSet() instanceof SortedRanges);
        assertEquals(9, ix.size());
        assertTrue(ix.containsRange(11, 11));
        assertTrue(ix.containsRange(13, 20));
        WritableRowSetImpl ix2 = new WritableRowSetImpl(SingleRange.make(9, 9));
        ix2.remove(9);
        assertEquals(ix2.getInnerSet(), OrderedLongSet.EMPTY);
    }

    private static void insertResultsInSingle(
            final long start1,
            final long end1,
            final long start2,
            final long end2,
            final long startExpected,
            final long endExpected) {
        final WritableRowSetImpl ix = new WritableRowSetImpl(SingleRange.make(start1, end1));
        Runnable check = () -> {
            assertTrue(ix.getInnerSet() instanceof SingleRange);
            assertEquals(startExpected, ix.firstRowKey());
            assertEquals(endExpected, ix.lastRowKey());
        };
        final RowSet ix2 = new WritableRowSetImpl(SingleRange.make(start2, end2));
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
        WritableRowSetImpl ix = new WritableRowSetImpl(SingleRange.make(start, end));
        RowSet ix2 = new WritableRowSetImpl(SingleRange.make(start - 3, start - 2));
        ix.insert(ix2);
        assertTrue(ix.getInnerSet() instanceof SortedRanges);
        assertEquals(ix.size(), end - start + 1 + 2);
        assertEquals(start - 3, ix.firstRowKey());
        assertEquals(-3L, ix.find(start - 1));
        assertEquals(end, ix.lastRowKey());

        ix = new WritableRowSetImpl(SingleRange.make(start, end));
        ix2 = new WritableRowSetImpl(SingleRange.make(end + 2, end + 3));
        ix.insert(ix2);
        assertTrue(ix.getInnerSet() instanceof SortedRanges);
        assertEquals(ix.size(), end - start + 1 + 2);
        assertEquals(start, ix.firstRowKey());
        assertEquals(-12L, ix.find(end + 1));
        assertEquals(end + 3, ix.lastRowKey());
    }

    private static void removeResultsCheck(
            final long start1,
            final long end1,
            final long start2,
            final long end2,
            final Consumer<RowSet> check) {
        final WritableRowSet ix = new WritableRowSetImpl(SingleRange.make(start1, end1));
        final RowSet ix2 = new WritableRowSetImpl(SingleRange.make(start2, end2));
        ix.remove(ix2);
        check.accept(ix);
        ix.clear();
        ix.insertRange(start1, end1);
        ix.removeRange(start2, end2);
        check.accept(ix);
        ix.clear();
        ix.insertRange(start1, end1);
        final RowSet ix3 = ix.minus(ix2);
        check.accept(ix3);
    }

    private static void removeResultsCheck(
            final long start1,
            final long end1,
            final long start2,
            final long end2) {
        final Consumer<RowSet> check;
        if (start1 < start2 && end2 < end1) {
            // hole.
            check = (final RowSet t) -> {
                final OrderedLongSet timpl = ((WritableRowSetImpl) t).getInnerSet();
                assertTrue((timpl instanceof RspBitmap) || (timpl instanceof SortedRanges));
                int rangeCount = 0;
                final RowSet.RangeIterator it = t.rangeIterator();
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
            check = (final RowSet t) -> {
                assertTrue(((WritableRowSetImpl) t).getInnerSet() == OrderedLongSet.EMPTY);
                assertTrue(t.isEmpty());
            };
        } else {
            // results in single range
            check = (final RowSet t) -> {
                assertTrue(((WritableRowSetImpl) t).getInnerSet() instanceof SingleRange);
                if (start1 < start2) {
                    assertEquals(start1, t.firstRowKey());
                    assertEquals(start2 - 1, t.lastRowKey());
                } else {
                    // end2 < end1
                    assertEquals(end2 + 1, t.firstRowKey());
                    assertEquals(end1, t.lastRowKey());
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
        WritableRowSetImpl ix = new WritableRowSetImpl(SingleRange.make(10, 20));
        RowSet ix2 = new WritableRowSetImpl(SingleRange.make(10, 20));
        ix.remove(ix2);
        assertEquals(OrderedLongSet.EMPTY, ix.getInnerSet());
        ix.insertRange(10, 20);
        assertEquals(11, ix.size());
        ix.removeRange(10, 20);
        assertEquals(OrderedLongSet.EMPTY, ix.getInnerSet());
        ix.insertRange(10, 20);
        assertEquals(11, ix.size());
        removeResultsCheck(10, 20, 11, 19);
    }

    @Test
    public void testInvert() {
        final long start = 10;
        final long end = 30;
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        final RspBitmap rb = new RspBitmap();
        for (long i = start + 1; i <= end; i += 2) {
            rb.add(i);
        }
        final WritableRowSet keys = new WritableRowSetImpl(rb);
        final RowSet r = ix.invert(keys, 10);
        assertEquals(5, r.size());
        assertTrue(r.subsetOf(RowSetFactory.fromKeys(1, 3, 5, 7, 9)));

        keys.clear();
        keys.insertRange(10, 30);
        final RowSet r2 = ix.invert(keys, 10);
        assertEquals(11, r2.size());
        assertTrue(r.subsetOf(RowSetFactory.fromRange(0, 10)));

        keys.clear();
        keys.insert(30);
        final RowSet r3 = ix.invert(keys, 100);
        assertEquals(1, r3.size());
        assertEquals(20, r3.firstRowKey());
    }

    @Test
    public void testOrderedKeyIterator() {
        final long start = 10;
        final long end = 30;
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        final RowSequence.Iterator rsIt = ix.getRowSequenceIterator();
        final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(2);
        int offset = 0;
        long prevRelPos = -1;
        final int step = 2;
        while (rsIt.hasMore()) {
            final long relPos = rsIt.getRelativePosition();
            if (prevRelPos != -1) {
                assertEquals(step, relPos - prevRelPos);
            }
            final long expectedStart = start + offset;
            final long expectedEnd = (expectedStart + 1 > end) ? expectedStart : expectedStart + 1;
            assertEquals(expectedStart, rsIt.peekNextKey());
            final RowSequence rs = rsIt.getNextRowSequenceWithLength(step);
            rs.fillRowKeyRangesChunk(chunk);
            assertEquals(2, chunk.size());
            assertEquals(expectedStart, chunk.get(0));
            assertEquals(expectedEnd, chunk.get(1));
            offset += 2;
            prevRelPos = relPos;
        }
    }

    @Test
    public void testRowSequence() {
        final long start = 10;
        final long end = 30;
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        RowSequence rs = ix.getRowSequenceByKeyRange(8, 9);
        assertEquals(RowSequenceFactory.EMPTY, rs);
        rs = ix.getRowSequenceByKeyRange(31, 32);
        assertEquals(RowSequenceFactory.EMPTY, rs);
        rs = ix.getRowSequenceByKeyRange(9, 10);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(10, rs.firstRowKey());
        assertEquals(10, rs.lastRowKey());
        rs = ix.getRowSequenceByKeyRange(9, 11);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(10, rs.firstRowKey());
        assertEquals(11, rs.lastRowKey());
        rs = ix.getRowSequenceByKeyRange(10, 11);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(10, rs.firstRowKey());
        assertEquals(11, rs.lastRowKey());
        rs = ix.getRowSequenceByKeyRange(30, 31);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(30, rs.firstRowKey());
        assertEquals(30, rs.lastRowKey());
        rs = ix.getRowSequenceByKeyRange(29, 31);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(29, rs.firstRowKey());
        assertEquals(30, rs.lastRowKey());
        rs = ix.getRowSequenceByKeyRange(29, 30);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(29, rs.firstRowKey());
        assertEquals(30, rs.lastRowKey());
        rs = ix.getRowSequenceByKeyRange(11, 29);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(11, rs.firstRowKey());
        assertEquals(29, rs.lastRowKey());

        rs = ix.getRowSequenceByPosition(0, 0);
        assertEquals(RowSequenceFactory.EMPTY, rs);
        rs = ix.getRowSequenceByPosition(0, 1);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(10, rs.firstRowKey());
        assertEquals(10, rs.lastRowKey());
        rs = ix.getRowSequenceByPosition(1, 2);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(11, rs.firstRowKey());
        assertEquals(12, rs.lastRowKey());
        rs = ix.getRowSequenceByPosition(20, 1);
        assertTrue(rs instanceof SingleRangeRowSequence);
        assertEquals(30, rs.firstRowKey());
        assertEquals(30, rs.lastRowKey());
        rs = ix.getRowSequenceByPosition(21, 1);
        assertEquals(RowSequenceFactory.EMPTY, rs);
    }

    @Test
    public void testSubindexByKey() {
        final long start = 10;
        final long end = 30;
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        RowSet r = ix.subSetByKeyRange(5, 15);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(10, 15)));
        r = ix.subSetByKeyRange(5, 15);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(10, 15)));
        r = ix.subSetByKeyRange(15, 20);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(15, 20)));
        r = ix.subSetByKeyRange(25, 35);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(25, 30)));
        r = ix.subSetByKeyRange(11, 29);
        assertEquals(19, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(11, 29)));
    }

    @Test
    public void testSubindexByPos() {
        final long start = 10;
        final long end = 30;
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        RowSet r = ix.subSetByPositionRange(0, 5 + 1);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(10, 15)));
        r = ix.subSetByPositionRange(5, 10 + 1);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(15, 20)));
        r = ix.subSetByPositionRange(15, 35 + 1);
        assertEquals(6, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(25, 30)));
        r = ix.subSetByPositionRange(1, 19 + 1);
        assertEquals(19, r.size());
        assertTrue(r.overlaps(RowSetFactory.fromRange(11, 29)));
    }

    private static void checkBinarySearch(final RowSet ix) {
        assertFalse(ix.isEmpty());
        final long start = ix.firstRowKey();
        final long end = ix.lastRowKey();
        assertEquals(end - start + 1, ix.size());
        final RowSet.SearchIterator it = ix.searchIterator();
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
        final RowSet six = new WritableRowSetImpl(SingleRange.make(start, end));
        checkBinarySearch(six);
        final RowSet rbix = new WritableRowSetImpl(new RspBitmap(start, end));
        checkBinarySearch(rbix);
    }

    @Test
    public void testGetKeysForPositions() {
        final long start = 10;
        final long end = 30;
        final long card = end - start + 1;
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        final long[] positions = new long[] {0, 1, card - 2, card - 1};
        final long[] expected = new long[] {10, 11, 29, 30};
        final long[] result = new long[4];
        final WritableLongChunk<OrderedRowKeys> resultsChunk = WritableLongChunk.writableChunkWrap(result);
        final LongChunk<OrderedRowKeys> positionsChunk = WritableLongChunk.chunkWrap(positions);
        ix.getKeysForPositions(new LongChunkIterator(positionsChunk), new LongChunkAppender(resultsChunk));
        assertArrayEquals(expected, result);
    }

    @Test
    public void testRowSequenceIteratorAdvanceRegression0() {
        final long start = 1000;
        final long end = 3000;
        final RowSet ix = new WritableRowSetImpl(SingleRange.make(start, end));
        final RowSequence.Iterator rsIt = ix.getRowSequenceIterator();
        assertTrue(rsIt.hasMore());
        final boolean valid = rsIt.advance(500);
        assertTrue(valid);
        assertEquals(1000, rsIt.peekNextKey());
    }

    @Test
    public void testSaturation() {
        final long first = 1073741821;
        final long last = first + 6;
        final SingleRange sr = SingleRange.make(first, last);
        final OrderedLongSet res = sr.ixSubindexByPosOnNew(5, Long.MAX_VALUE);
        assertEquals(last - 1, res.ixFirstKey());
        assertEquals(last, res.ixLastKey());
    }
}
