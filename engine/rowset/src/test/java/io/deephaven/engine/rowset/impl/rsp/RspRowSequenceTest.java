/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.impl.RowSequenceTestBase;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.testutil.rowset.RowSetTstUtils;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.*;

public class RspRowSequenceTest extends RowSequenceTestBase {

    @Override
    protected RowSequence create(long... values) {
        final RspBitmap rb = RspBitmap.makeEmpty();
        for (long v : values) {
            rb.addUnsafe(v);
        }
        rb.finishMutationsAndOptimize();
        return new WritableRowSetImpl(rb);
    }

    @Test
    public void testAverageRunLength() {
        assertEquals(1, RowSequenceFactory.EMPTY.getAverageRunLengthEstimate());
        final RspBitmap rb = new RspBitmap();
        final RowSequence rs = rb.getRowSequenceByKeyRange(0, 1);
        assertEquals(1, rs.getAverageRunLengthEstimate());
        rs.close();
    }

    @Test
    public void testIteratorSimpleByKeys() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(1, 65534);
        rb.add(65536);
        rb.add(65536 * 2 - 1);
        rb.add(65536 * 2 + 2);
        rb.addRange(65536 * 3 - 1, 65536 * 6 + 2);
        try (final RowSequence.Iterator it = rb.getRowSequenceIterator()) {
            long v = 65536;
            final RspBitmap res = new RspBitmap();
            while (it.hasMore()) {
                final RowSequence rs = it.getNextRowSequenceThrough(v);
                if (rs.size() != 0) {
                    try (final WritableLongChunk<OrderedRowKeyRanges> chunk =
                            WritableLongChunk.makeWritableChunk(65536 * 6)) {
                        rs.fillRowKeyRangesChunk(chunk);
                        assertEquals(0, chunk.size() % 2);
                        int i = 0;
                        while (i < chunk.size()) {
                            final long s = chunk.get(i++);
                            final long e = chunk.get(i++);
                            res.addRange(s, e);
                        }
                    }
                }
                v += 65536;
            }
            assertEquals(rb, res);
        }
    }

    @Test
    public void testIteratorSimpleByPosition() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(1, 65534);
        rb.add(65536);
        rb.add(65536 * 2 - 1);
        rb.add(65536 * 2 + 2);
        rb.addRange(65536 * 3 - 1, 65536 * 6 + 2);
        try (final RowSequence.Iterator it = rb.getRowSequenceIterator()) {
            long v = 65536;
            final RspBitmap res = new RspBitmap();
            final long len = rb.getCardinality() / 4;
            while (it.hasMore()) {
                final RowSequence rs = it.getNextRowSequenceWithLength(len);
                if (rs.size() != 0) {
                    try (final WritableLongChunk<OrderedRowKeyRanges> chunk =
                            WritableLongChunk.makeWritableChunk(2 * (int) len)) {
                        rs.fillRowKeyRangesChunk(chunk);
                        assertEquals(0, chunk.size() % 2);
                        int i = 0;
                        while (i < chunk.size()) {
                            final long s = chunk.get(i++);
                            final long e = chunk.get(i++);
                            res.addRange(s, e);
                        }
                    }
                }
                v += 65536;
            }
            assertEquals(rb, res);
        }
    }

    @Test
    public void testAsIndexUnderlyingIndexFullSpansOnly() {
        RspBitmap rb = new RspBitmap();
        final long sk = 7 * BLOCK_SIZE;
        rb = rb.addRange(sk, sk + BLOCK_LAST);
        // so far single full span.
        final long len = 20;
        final long[] startOffsets = {0, 1, 4, BLOCK_LAST - 19, BLOCK_LAST - 20};
        RowSequence rs;
        RowSet ix;
        for (long startOffset : startOffsets) {
            rs = rb.getRowSequenceByPosition(startOffset, len);
            ix = rs.asRowSet();
            ix.validate();
            final String m = "startOffset==" + startOffset;
            assertEquals(m, len, ix.size());
            assertEquals(m, sk + startOffset, ix.firstRowKey());
            assertEquals(m, sk + startOffset + len - 1, ix.lastRowKey());
        }
        // a second full block span.
        rb = rb.addRange(sk + BLOCK_SIZE, sk + BLOCK_SIZE + BLOCK_LAST);

        for (long startOffset : startOffsets) {
            rs = rb.getRowSequenceByPosition(startOffset, len);
            ix = rs.asRowSet();
            ix.validate();
            final String m = "startOffset==" + startOffset;
            assertEquals(m, len, ix.size());
            assertEquals(m, sk + startOffset, ix.firstRowKey());
            assertEquals(m, sk + startOffset + len - 1, ix.lastRowKey());
            rs.close();
            rs = rb.getRowSequenceByPosition(startOffset, BLOCK_SIZE - (startOffset - 1));
            ix = rs.asRowSet();
            ix.validate(m);
            assertEquals(m, BLOCK_SIZE - (startOffset - 1), ix.size());
            assertEquals(m, sk + startOffset, ix.firstRowKey());
            assertEquals(m, sk + BLOCK_SIZE, ix.lastRowKey());
            rs.close();
        }
    }

    @Test
    public void testAsIndexUnderlyingIndexBoundarySingleBlockFullSpans() {
        RspBitmap rb = new RspBitmap();
        final long sk = 11 * BLOCK_SIZE;
        rb = rb.addRange(sk, sk + BLOCK_LAST);
        rb = rb.addRange(sk + 4 * BLOCK_SIZE + 4, sk + 4 * BLOCK_SIZE + 40);
        rb = rb.addRange(sk + 10 * BLOCK_SIZE, sk + 10 * BLOCK_SIZE + BLOCK_LAST);
        RowSequence rs = rb.getRowSequenceByPosition(BLOCK_SIZE / 2, BLOCK_SIZE);
        RowSet ix = rs.asRowSet();
        ix.validate();
        assertEquals(BLOCK_SIZE, ix.size());

        rb = rb.addRange(sk + BLOCK_SIZE, sk + BLOCK_SIZE + BLOCK_LAST);
        rs = rb.getRowSequenceByPosition(BLOCK_SIZE / 2, 2 * BLOCK_SIZE);
        ix = rs.asRowSet();
        ix.validate();
        assertEquals(2 * BLOCK_SIZE, ix.size());
        rs.close();

        rb = rb.addRange(sk + 9 * BLOCK_SIZE, sk + 9 * BLOCK_SIZE + BLOCK_LAST);
        rs = rb.getRowSequenceByPosition(BLOCK_SIZE / 2, 3 * BLOCK_SIZE);
        ix = rs.asRowSet();
        ix.validate();
        assertEquals(3 * BLOCK_SIZE, ix.size());
        rs.close();

        rb = rb.addRange(sk + 6 * BLOCK_SIZE, sk + 6 * BLOCK_SIZE + 2);
        rs = rb.getRowSequenceByPosition(BLOCK_SIZE / 2, 3 * BLOCK_SIZE);
        ix = rs.asRowSet();
        ix.validate();
        assertEquals(3 * BLOCK_SIZE, ix.size());
        rs.close();

        rs = rb.getRowSequenceByPosition(rb.find(sk + 6 * BLOCK_SIZE + 2), 1 + 2 * BLOCK_SIZE);
        ix = rs.asRowSet();
        ix.validate();
        assertEquals(1 + 2 * BLOCK_SIZE, ix.size());
        rs.close();
    }

    @Test
    public void testAsIndexMultiBlockFirstFullBlockSpan() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 7 * BLOCK_SIZE;
        rb.addRange(sk, sk + 5 * BLOCK_SIZE - 1);
        assertEquals(5 * BLOCK_SIZE, rb.getCardinality());
        final RowSequence rs = rb.getRowSequenceByPosition(1, 5 * BLOCK_SIZE - 2);
        final RowSet ix = rs.asRowSet();
        ix.validate();
        assertEquals(5 * BLOCK_SIZE - 2, ix.size());
        rs.close();
    }

    @Test
    public void testAsIndexEndOffsetMatchesCardinalityOfEndingContainer() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 5 * BLOCK_SIZE;
        rb.addRange(sk + 5, 6 * BLOCK_SIZE + 4);
        final RowSequence rs = rb.getRowSequenceByKeyRange(sk + 60000, 6 * BLOCK_SIZE + 4);
        final RowSet ix = rs.asRowSet();
        ix.validate();
        assertEquals(sk + 60000, ix.firstRowKey());
        assertEquals(6 * BLOCK_SIZE + 4, ix.lastRowKey());
        assertEquals(ix.lastRowKey() - ix.firstRowKey() + 1, ix.size());
        rs.close();
    }

    @Test
    public void testAsIndexSingleContainerFromFirstFullBlockSpanOfMoreThanOneBlock() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 10 * BLOCK_SIZE;
        rb.addRange(sk, sk + 10 * BLOCK_SIZE);
        final long first = 16 * BLOCK_SIZE + 1;
        final long last = 17 * BLOCK_SIZE - 1;
        final RowSequence rs = rb.getRowSequenceByKeyRange(first, last);
        final RowSet ix = rs.asRowSet();
        ix.validate();
        assertEquals(first, ix.firstRowKey());
        assertEquals(last, ix.lastRowKey());
        assertEquals(last - first + 1, ix.size());
        rs.close();
    }

    @Test
    public void testAsIndexFullBlockSpanLen4For3AtFront() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 3 * BLOCK_SIZE;
        rb.addRange(sk, sk + 4 * BLOCK_SIZE - 1);
        final long first = sk + BLOCK_SIZE;
        final long last = sk + 4 * BLOCK_SIZE - 1;
        final RowSequence rs = rb.getRowSequenceByKeyRange(first, last);
        final RowSet ix = rs.asRowSet();
        ix.validate();
        assertEquals(first, ix.firstRowKey());
        assertEquals(last, ix.lastRowKey());
        assertEquals(last - first + 1, ix.size());
        rs.close();
    }

    @Test
    public void testAsIndexFullBlockSpanLen4PlusSomeFor1AtFront() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 3 * BLOCK_SIZE;
        rb.addRange(sk, sk + 4 * BLOCK_SIZE);
        final long first = sk + BLOCK_SIZE;
        final long last = sk + 4 * BLOCK_SIZE;
        final RowSequence rs = rb.getRowSequenceByKeyRange(first, last);
        final RowSet ix = rs.asRowSet();
        ix.validate();
        assertEquals(first, ix.firstRowKey());
        assertEquals(last, ix.lastRowKey());
        assertEquals(last - first + 1, ix.size());
    }

    @Test
    public void testFromKeyRangeWhereKeyNotInContainer() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 3 * BLOCK_SIZE;
        rb.addRange(sk + 10, sk + 21);
        rb.addRange(sk + BLOCK_SIZE, sk + 3 * BLOCK_SIZE + 1);
        final long startKey = sk + 22;
        final long endKey = sk + 3 * BLOCK_SIZE + 1;
        final RowSequence rs = rb.getRowSequenceByKeyRange(startKey, endKey);
        final RowSet ix = rs.asRowSet();
        ix.validate();
        final RspBitmap rb2 = (RspBitmap) rb.ixSubindexByKeyOnNew(startKey, endKey);
        rb2.andEquals(rb);
        final RowSet ix2 = new WritableRowSetImpl(rb2);
        assertEquals(ix2.size(), ix.size());
        assertEquals(0, ix2.minus(ix).size());
        rs.close();
    }

    @Test
    public void testFillKeyIndicesChunk() {
        final RspBitmap rb = new RspBitmap();
        final int ks = 7;
        final int lastOddVal = BLOCK_SIZE * ks + 1;
        for (int i = 1; i < lastOddVal; i += 4) {
            rb.addRangeUnsafeNoWriteCheck(i, i + 2);
        }
        rb.addRangeUnsafeNoWriteCheck(BLOCK_SIZE, 2 * BLOCK_SIZE - 1);
        rb.addRangeUnsafeNoWriteCheck(3 * BLOCK_SIZE, 4 * BLOCK_SIZE - 1);
        rb.finishMutationsAndOptimize();
        for (long kref = 0; kref <= (ks + 1) * BLOCK_SIZE; kref += BLOCK_SIZE) {
            final long last = rb.last();
            for (long kendpoint : new long[] {0, last}) {
                final long k1 = Math.min(kref, kendpoint);
                final long k2 = Math.max(kref, kendpoint);
                for (int dk1 = -1; dk1 <= 1; ++dk1) {
                    for (int dk2 = -1; dk2 <= 1; ++dk2) {
                        final long startKey = k1 + dk1;
                        final long endKey = k2 + dk2;
                        if (startKey < 0 || endKey < startKey) {
                            continue;
                        }
                        try (final RowSequence rs = rb.getRowSequenceByKeyRange(startKey, endKey)) {
                            final RspBitmap expected = rb.subrangeByValue(startKey, endKey);
                            final RspBitmap res = new RspBitmap();
                            final RowSequence.Iterator it = rs.getRowSequenceIterator();
                            final int chunkSz = 13;
                            try (final WritableLongChunk<OrderedRowKeys> chunk =
                                    WritableLongChunk.makeWritableChunk(chunkSz)) {
                                while (it.hasMore()) {
                                    final RowSequence rsIt = it.getNextRowSequenceWithLength(chunkSz);
                                    chunk.setSize(chunkSz);
                                    rsIt.fillRowKeyChunk(chunk);
                                    final int sz = chunk.size();
                                    for (int i = 0; i < sz; ++i) {
                                        res.addUnsafeNoWriteCheck(chunk.get(i));
                                    }
                                }
                            }
                            final String m = "startKey==" + startKey + " && endKey==" + endKey;
                            res.finishMutationsAndOptimize();
                            assertEquals(m, expected, res);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testFillKeyRangesChunk() {
        final RspBitmap rb = new RspBitmap();
        final int ks = 7;
        final int lastOddVal = BLOCK_SIZE * ks + 1;
        for (int i = 1; i < lastOddVal; i += 4) {
            rb.addRangeUnsafeNoWriteCheck(i, i + 2);
        }
        rb.addRangeUnsafeNoWriteCheck(BLOCK_SIZE, 2 * BLOCK_SIZE - 1);
        rb.addRangeUnsafeNoWriteCheck(3 * BLOCK_SIZE, 4 * BLOCK_SIZE - 1);
        rb.finishMutationsAndOptimize();
        for (long kref = 0; kref <= (ks + 1) * BLOCK_SIZE; kref += BLOCK_SIZE) {
            final long last = rb.last();
            for (long kendpoint : new long[] {0, last}) {
                final long k1 = Math.min(kref, kendpoint);
                final long k2 = Math.max(kref, kendpoint);
                for (int dk1 = -1; dk1 <= 1; ++dk1) {
                    for (int dk2 = -1; dk2 <= 1; ++dk2) {
                        final long startKey = k1 + dk1;
                        final long endKey = k2 + dk2;
                        if (startKey < 0 || endKey < startKey) {
                            continue;
                        }
                        try (final RowSequence rs = rb.getRowSequenceByKeyRange(startKey, endKey)) {
                            final RspBitmap expected = rb.subrangeByValue(startKey, endKey);
                            final RspBitmap res = new RspBitmap();
                            final RowSequence.Iterator it = rs.getRowSequenceIterator();
                            final int chunkSz = 13;
                            try (final WritableLongChunk<OrderedRowKeyRanges> chunk =
                                    WritableLongChunk.makeWritableChunk(chunkSz)) {
                                while (it.hasMore()) {
                                    final RowSequence rsIt = it.getNextRowSequenceWithLength(chunkSz / 2);
                                    chunk.setSize(chunkSz);
                                    rsIt.fillRowKeyRangesChunk(chunk);
                                    final int sz = chunk.size();
                                    for (int i = 0; i < sz; i += 2) {
                                        res.addRangeUnsafeNoWriteCheck(chunk.get(i), chunk.get(i + 1));
                                    }
                                }
                            }
                            res.finishMutationsAndOptimize();
                            final String m = "startKey==" + startKey + " && endKey==" + endKey;
                            assertEquals(m, expected, res);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testForEachLongRange2() {
        final RspBitmap rb = new RspBitmap();
        final int ks = 7;
        final int lastOddVal = BLOCK_SIZE * ks + 1;
        for (int i = 1; i < lastOddVal; i += 4) {
            rb.addRange(i, i + 2);
        }
        rb.addRange(BLOCK_SIZE, 2 * BLOCK_SIZE - 1);
        rb.addRange(3 * BLOCK_SIZE, 4 * BLOCK_SIZE - 1);
        for (long kref = 0; kref <= (ks + 1) * BLOCK_SIZE; kref += BLOCK_SIZE) {
            final long last = rb.last();
            for (long kendpoint : new long[] {0, last}) {
                final long k1 = Math.min(kref, kendpoint);
                final long k2 = Math.max(kref, kendpoint);
                for (int dk1 = -1; dk1 <= 1; ++dk1) {
                    for (int dk2 = -1; dk2 <= 1; ++dk2) {
                        final long startKey = k1 + dk1;
                        final long endKey = k2 + dk2;
                        if (startKey < 0 || endKey < startKey) {
                            continue;
                        }
                        try (final RowSequence rs = rb.getRowSequenceByKeyRange(startKey, endKey)) {
                            final RspBitmap expected = rb.subrangeByValue(startKey, endKey);
                            final RspBitmap res = new RspBitmap();
                            final RowSequence.Iterator it = rs.getRowSequenceIterator();
                            while (it.hasMore()) {
                                final RowSequence rsIt = it.getNextRowSequenceWithLength(13);
                                rsIt.forAllRowKeyRanges(res::addRange);
                            }
                            final String m = "startKey==" + startKey + " && endKey==" + endKey;
                            assertEquals(m, expected.getCardinality(), res.getCardinality());
                            assertEquals(m, expected, res);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testInvariantRegression0() {
        final RspBitmap rb = new RspBitmap();
        rb.addValues(175, 225, 288, 351, 429, 523, 562, 131247, 131297, 131360, 131423, 131501, 131595, 131634);
        final RowSequence.Iterator it = rb.getRowSequenceIterator();
        int maxKey = BLOCK_SIZE - 1;
        while (it.hasMore()) {
            final RowSequence rs = it.getNextRowSequenceThrough(maxKey);
            final String m = "maxKey==" + maxKey;
            switch (maxKey) {
                case BLOCK_SIZE - 1:
                case 3 * BLOCK_SIZE - 1:
                    assertEquals(m, 7, rs.size());
                    break;
                case 2 * BLOCK_SIZE - 1:
                    assertEquals(m, 0, rs.size());
                    assertEquals(m, RowSequenceFactory.EMPTY, rs);
                    break;
                default:
                    fail(m + "; Shouldn't get here.");
            }
            maxKey += BLOCK_SIZE;
        }
    }

    @Test
    public void testIteratorAdvance() {
        final long start = 1099511627776L;
        final long end = 1099511634270L;
        final long beyond = 2199023255552L;
        assertTrue(beyond > end);
        final RspBitmap rb = new RspBitmap(start, end);
        final RowSequence.Iterator it = rb.getRowSequenceIterator();
        assertTrue(it.hasMore());
        final RowSequence rs1 = it.getNextRowSequenceThrough(end - 1);
        assertEquals(end - start + 1 - 1, rs1.size());
        final long relPosPre = it.getRelativePosition();
        final boolean advanced = it.advance(beyond);
        final long relPosPost = it.getRelativePosition();
        assertFalse(advanced);
        assertEquals(1, relPosPost - relPosPre);
    }

    @Test
    public void testAdvanceRegressionNegativeSizeLeft() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(0, 2);
        rb.addRange(BLOCK_SIZE, BLOCK_SIZE + 2);
        rb.addRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 2);
        try (final RowSequence.Iterator it1 = rb.getRowSequenceIterator()) {
            assertTrue(it1.hasMore());
            final RowSequence rs1 = it1.getNextRowSequenceWithLength(6);
            assertEquals(6, rs1.size());
            try (final RowSequence.Iterator it2 = rs1.getRowSequenceIterator()) {
                assertTrue(it2.hasMore());
                final boolean advanced = it2.advance(2 * BLOCK_SIZE + 2);
                assertFalse(advanced);
            }
        }
    }

    @Test
    public void testThroughRegression() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(0, 3);
        rb.addRange(BLOCK_SIZE, BLOCK_SIZE + 5);
        rb.addRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 3);
        final RowSequence.Iterator it = rb.getRowSequenceIterator();
        assertTrue(it.hasMore());
        final RowSequence rs0 = it.getNextRowSequenceThrough(BLOCK_SIZE + 5);
        assertEquals(10, rs0.size());
        final RowSequence rs1 = it.getNextRowSequenceThrough(2 * BLOCK_SIZE + 3);
        assertEquals(4, rs1.size());
    }

    @Test
    public void testAdvaceBugRsp() {
        advanceBug(RowSetTstUtils.makeEmptyRsp());
    }

    @Test
    public void testForEachLongRangeInSpanRegression0() {
        final RspBitmap rb = new RspBitmap();
        // We need a number of ranges that matches bufSz/2 in the implementation for
        // RspArray.forEachLongRangeInSpanWithOffsetAndMaxCardinality.
        // Further, we need to stop in a partial range short of the full filled buf.
        rb.addRange(1, 2);
        rb.addRange(4, 5);
        rb.addRange(7, 8); // we stop here (7 below in getRowSequenceByRange), so that there is one range after us.
        rb.addRange(10, 11);
        final RowSequence rs = rb.getRowSequenceByKeyRange(1, 7);
        rs.forAllRowKeyRanges((final long start, final long end) -> {
            assertTrue(end >= start);
        });
    }

    @Test
    public void testAsKeyRangesChunkRegression0() {
        WritableRowSet rowSet = RowSetTstUtils.makeSingleRange(130972, 131071);
        rowSet.insert(262144);

        final long CHUNK_SIZE = 4096;

        try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            assertTrue(it.hasMore());
            final RowSequence subKeys = it.getNextRowSequenceWithLength(CHUNK_SIZE);
            final LongChunk<OrderedRowKeyRanges> ranges = subKeys.asRowKeyRangesChunk();
            assertFalse(it.hasMore());
            assertEquals(4, ranges.size());
            assertEquals(130972, ranges.get(0));
            assertEquals(131071, ranges.get(1));
            assertEquals(262144, ranges.get(2));
            assertEquals(262144, ranges.get(3));
        }
    }

    @Test
    public void testRspIteratorCloseLeak() {
        ChunkPoolReleaseTracking.enableStrict();
        final RspBitmap rb = new RspBitmap();
        rb.add(BLOCK_SIZE);
        rb.add(2 * BLOCK_SIZE);
        long count = 0;
        try (final RowSequence.Iterator rsIter = rb.getRowSequenceIterator()) {
            long last = 0;
            while (rsIter.hasMore()) {
                final RowSequence rs = rsIter.getNextRowSequenceThrough(last + BLOCK_SIZE);
                last = rs.lastRowKey();
                final LongChunk<OrderedRowKeys> chunk = rs.asRowKeyChunk();
                count += chunk.size();
            }
        }
        assertEquals(rb.getCardinality(), count);
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    @Test
    public void testAsKeyRangesChunkRegression0Rsp() {
        WritableRowSet rowSet = RowSetTstUtils.makeSingleRange(130972, 131071);
        rowSet.insert(262144);

        final long CHUNK_SIZE = 4096;

        try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            assertTrue(it.hasMore());
            final RowSequence subKeys = it.getNextRowSequenceWithLength(CHUNK_SIZE);
            final LongChunk<OrderedRowKeyRanges> ranges = subKeys.asRowKeyRangesChunk();
            assertFalse(it.hasMore());
            assertEquals(4, ranges.size());
            assertEquals(130972, ranges.get(0));
            assertEquals(131071, ranges.get(1));
            assertEquals(262144, ranges.get(2));
            assertEquals(262144, ranges.get(3));
        }
    }

    @Test
    public void testAsKeyRangesChunkRegression0SortedRanges() {
        WritableRowSet rowSet = RowSetFactory.empty();
        rowSet.insertRange(130972, 131071);
        rowSet.insert(262144);

        final long CHUNK_SIZE = 4096;

        try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            assertTrue(it.hasMore());
            final RowSequence subKeys = it.getNextRowSequenceWithLength(CHUNK_SIZE);
            final LongChunk<OrderedRowKeyRanges> ranges = subKeys.asRowKeyRangesChunk();
            assertFalse(it.hasMore());
            assertEquals(4, ranges.size());
            assertEquals(130972, ranges.get(0));
            assertEquals(131071, ranges.get(1));
            assertEquals(262144, ranges.get(2));
            assertEquals(262144, ranges.get(3));
        }
    }

    @Test
    public void testAdvanceRegression0() {
        final int v = 0x10000;
        RspBitmap rb = new RspBitmap();
        rb = rb.add(v);
        rb = rb.add(2 * v + 1);
        for (long target = 2 * v; target <= 2 * v + 1; ++target) {
            try (final RowSequence.Iterator rsIter = rb.getRowSequenceIterator()) {
                assertTrue(rsIter.advance(target));
                final RowSequence rs = rsIter.getNextRowSequenceWithLength(1);
                assertEquals(1, rs.size());
                assertEquals(2 * v + 1, rs.firstRowKey());
            }
        }
        rb = rb.addRange(3 * v + 10, 3 * v + 13);
        try (final RowSequence.Iterator rsIter = rb.getRowSequenceIterator()) {
            assertTrue(rsIter.advance(2 * v + 2));
            final RowSequence rs = rsIter.getNextRowSequenceWithLength(2);
            assertEquals(2, rs.size());
            assertEquals(3 * v + 10, rs.firstRowKey());
        }
    }

    @Test
    public void testAdvance() {
        final int v = 0x10000;
        final int count = 400;
        final int maxRangesPerBlock = 3;
        final int maxRangeLen = 3;
        final int seed = 1;
        final String m0 = "seed==" + seed;
        final Random rand = new Random(seed);
        final int runs = 20;
        final float fullBlockProb = 0.3F;
        for (int run = 0; run < runs; ++run) {
            final String m = m0 + " && run==" + run;

            // Populate the RspBitmap.
            final RspBitmap rb = new RspBitmap();
            for (int c = 0; c < count; ++c) {
                if (rand.nextFloat() < fullBlockProb) {
                    rb.addRange(c * v, (c + 1) * v - 1);
                    continue;
                }
                final int nRanges = rand.nextInt(maxRangesPerBlock + 1);
                for (int range = 0; range < nRanges; ++range) {
                    final int rlen = 1 + rand.nextInt(maxRangeLen);
                    final long start = c * v + range * 20 + rand.nextInt(3);
                    final long end = start + rlen - 1;
                    rb.addRange(start, end);
                }
            }
            try (final RowSequence.Iterator rsIter = rb.getRowSequenceIterator();
                    final RspRangeIterator rit = rb.getRangeIterator()) {
                long prevEnd = -2;
                long lastRelativePos = -rb.getCardinality() - 1;
                int nrange = 0;
                while (rit.hasNext()) {
                    rit.next();
                    final long start = rit.start();
                    final long end = rit.end();
                    final String m2 = m + " && nrange==" + nrange;
                    assertTrue(m2, prevEnd + 1 < start);
                    final long advanceTarget = Math.max(prevEnd + 1, 0);
                    assertTrue(m2, rsIter.advance(advanceTarget));
                    assertEquals(m2, start, rsIter.peekNextKey());
                    final long relativePos = rsIter.getRelativePosition();
                    assertTrue(relativePos > lastRelativePos);
                    try (final RowSequence.Iterator rsIter2 = rb.getRowSequenceIterator()) {
                        assertTrue(m2, rsIter2.advance(advanceTarget));
                        assertEquals(m2, start, rsIter2.peekNextKey());
                    }
                    if (start - 1 > 0 && start - 1 != advanceTarget) {
                        try (final RowSequence.Iterator rsIter2 = rb.getRowSequenceIterator()) {
                            assertTrue(m2, rsIter2.advance(start - 1));
                            assertEquals(m2, start, rsIter2.peekNextKey());
                        }
                    }
                    try (final RowSequence.Iterator rsIter2 = rb.getRowSequenceIterator()) {
                        assertTrue(m2, rsIter2.advance(start));
                        assertEquals(m2, start, rsIter2.peekNextKey());
                    }
                    try (final RowSequence.Iterator rsIter2 = rb.getRowSequenceIterator()) {
                        assertTrue(m2, rsIter2.advance(end));
                        assertEquals(m2, end, rsIter2.peekNextKey());
                    }
                    prevEnd = end;
                    lastRelativePos = relativePos;
                    ++nrange;
                }
            }
        }
    }

    @Test
    public void testGetNextThroughSameContainerBack() {
        final int v = 0x10000;
        final RspBitmap rb = new RspBitmap();
        rb.add(v);
        rb.addRange(2 * v + 1, 2 * v + 3);
        try (final RowSequence.Iterator rsIter = rb.getRowSequenceIterator()) {
            final RowSequence rs = rsIter.getNextRowSequenceWithLength(2);
            assertEquals(2, rs.size());
            assertEquals(v, rs.firstRowKey());
            assertEquals(2 * v + 1, rs.lastRowKey());
            final RowSequence rs2 = rsIter.getNextRowSequenceThrough(2 * v);
            assertEquals(0, rs2.size());
        }
    }

    @Test
    public void testIteratorGetNextRowSequenceThroughRegression0() {
        RspBitmap rb = RspBitmap.makeSingleRange(10, 15);
        rb = rb.appendRange(BLOCK_SIZE + 1, BLOCK_SIZE + 10);
        // rb has two containers, we are going to iterate to a maxKey of BLOCK_SIZE, which is not present
        // and if it was, would be at the beginning of the second container.
        try (RowSequence.Iterator it = rb.ixGetRowSequenceIterator()) {
            assertTrue(it.hasMore());
            RowSequence rs = it.getNextRowSequenceThrough(BLOCK_SIZE);
            assertEquals(15, rs.lastRowKey());
            assertTrue(it.hasMore());
            rs = it.getNextRowSequenceThrough(BLOCK_SIZE + 20);
            assertEquals(BLOCK_SIZE + 10, rs.lastRowKey());
            assertFalse(it.hasMore());
        }
    }

    @Test
    public void testAdvanceSingleBlocks() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(1);
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (RowSequence.Iterator it = rb.ixGetRowSequenceIterator()) {
            assertTrue(it.hasMore());
            final boolean has = it.advance(BLOCK_SIZE + 1);
            assertTrue(has);
            final RowSequence rs = it.getNextRowSequenceThrough(3 * BLOCK_SIZE + 1);
            assertEquals(BLOCK_SIZE + 3, rs.size());
            assertFalse(rs.isContiguous());
            assertEquals(BLOCK_SIZE + 10, rs.firstRowKey());
            assertEquals(3 * BLOCK_SIZE + 1, rs.lastRowKey());
            assertFalse(it.hasMore());
        }
    }

    @Test
    public void testFindOrPrevCoverage() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(1);
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (RowSequence.Iterator it = rb.ixGetRowSequenceIterator()) {
            assertTrue(it.hasMore());
            RowSequence rs = it.getNextRowSequenceThrough(BLOCK_SIZE + 10);
            assertEquals(2, rs.size());
            assertFalse(rs.isContiguous());
            assertEquals(1, rs.firstRowKey());
            assertEquals(BLOCK_SIZE + 10, rs.lastRowKey());
            assertTrue(it.hasMore());
        }
    }

    @Test
    public void testFindOrPrevCoverage2() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(1);
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (RowSequence.Iterator it = rb.ixGetRowSequenceIterator()) {
            assertTrue(it.hasMore());
            RowSequence rs = it.getNextRowSequenceThrough(BLOCK_SIZE + 9);
            assertEquals(1, rs.size());
            assertEquals(1, rs.firstRowKey());
            assertTrue(it.hasMore());
        }
    }

    @Test
    public void testFindOrPrevCoverage3() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(1);
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (RowSequence.Iterator it = rb.ixGetRowSequenceIterator()) {
            assertTrue(it.hasMore());
            RowSequence rs = it.getNextRowSequenceThrough(BLOCK_SIZE + 11);
            assertEquals(2, rs.size());
            assertEquals(1, rs.firstRowKey());
            assertEquals(BLOCK_SIZE + 10, rs.lastRowKey());
            assertTrue(it.hasMore());
        }
    }

    @Test
    public void testFindOrPrevCoverage4() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (RowSequence.Iterator it = rb.ixGetRowSequenceIterator()) {
            assertTrue(it.hasMore());
            RowSequence rs = it.getNextRowSequenceThrough(BLOCK_SIZE + 9);
            assertEquals(0, rs.size());
            assertTrue(it.hasMore());
            rs = it.getNextRowSequenceThrough(BLOCK_SIZE + 11);
            assertEquals(1, rs.size());
            assertTrue(it.hasMore());
        }
    }
}
