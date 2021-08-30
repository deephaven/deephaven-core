package io.deephaven.db.v2.utils.rsp;

import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.db.v2.utils.*;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.db.v2.utils.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.db.v2.utils.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.*;

public class RspOrderedKeysTest extends OrderedKeysTestBase {

    @Override
    protected OrderedKeys create(long... values) {
        final RspBitmap rb = RspBitmap.makeEmpty();
        for (long v : values) {
            rb.addUnsafe(v);
        }
        rb.finishMutationsAndOptimize();
        return new TreeIndex(rb);
    }

    @Test
    public void testAverageRunLength() {
        assertEquals(1, OrderedKeys.EMPTY.getAverageRunLengthEstimate());
        final RspBitmap rb = new RspBitmap();
        final OrderedKeys rok = rb.getOrderedKeysByKeyRange(0, 1);
        assertEquals(1, rok.getAverageRunLengthEstimate());
        rok.close();
    }

    @Test
    public void testIteratorSimpleByKeys() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(1, 65534);
        rb.add(65536);
        rb.add(65536 * 2 - 1);
        rb.add(65536 * 2 + 2);
        rb.addRange(65536 * 3 - 1, 65536 * 6 + 2);
        try (final OrderedKeys.Iterator it = rb.getOrderedKeysIterator()) {
            long v = 65536;
            final RspBitmap res = new RspBitmap();
            while (it.hasMore()) {
                final OrderedKeys oks = it.getNextOrderedKeysThrough(v);
                if (oks.size() != 0) {
                    try (final WritableLongChunk<OrderedKeyRanges> chunk =
                        WritableLongChunk.makeWritableChunk(65536 * 6)) {
                        oks.fillKeyRangesChunk(chunk);
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
        try (final OrderedKeys.Iterator it = rb.getOrderedKeysIterator()) {
            long v = 65536;
            final RspBitmap res = new RspBitmap();
            final long len = rb.getCardinality() / 4;
            while (it.hasMore()) {
                final OrderedKeys oks = it.getNextOrderedKeysWithLength(len);
                if (oks.size() != 0) {
                    try (final WritableLongChunk<OrderedKeyRanges> chunk =
                        WritableLongChunk.makeWritableChunk(2 * (int) len)) {
                        oks.fillKeyRangesChunk(chunk);
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
        OrderedKeys ok;
        Index ix;
        for (long startOffset : startOffsets) {
            ok = rb.getOrderedKeysByPosition(startOffset, len);
            ix = ok.asIndex();
            ix.validate();
            final String m = "startOffset==" + startOffset;
            assertEquals(m, len, ix.size());
            assertEquals(m, sk + startOffset, ix.firstKey());
            assertEquals(m, sk + startOffset + len - 1, ix.lastKey());
        }
        // a second full block span.
        rb = rb.addRange(sk + BLOCK_SIZE, sk + BLOCK_SIZE + BLOCK_LAST);

        for (long startOffset : startOffsets) {
            ok = rb.getOrderedKeysByPosition(startOffset, len);
            ix = ok.asIndex();
            ix.validate();
            final String m = "startOffset==" + startOffset;
            assertEquals(m, len, ix.size());
            assertEquals(m, sk + startOffset, ix.firstKey());
            assertEquals(m, sk + startOffset + len - 1, ix.lastKey());
            ok.close();
            ok = rb.getOrderedKeysByPosition(startOffset, BLOCK_SIZE - (startOffset - 1));
            ix = ok.asIndex();
            ix.validate(m);
            assertEquals(m, BLOCK_SIZE - (startOffset - 1), ix.size());
            assertEquals(m, sk + startOffset, ix.firstKey());
            assertEquals(m, sk + BLOCK_SIZE, ix.lastKey());
            ok.close();
        }
    }

    @Test
    public void testAsIndexUnderlyingIndexBoundarySingleBlockFullSpans() {
        RspBitmap rb = new RspBitmap();
        final long sk = 11 * BLOCK_SIZE;
        rb = rb.addRange(sk, sk + BLOCK_LAST);
        rb = rb.addRange(sk + 4 * BLOCK_SIZE + 4, sk + 4 * BLOCK_SIZE + 40);
        rb = rb.addRange(sk + 10 * BLOCK_SIZE, sk + 10 * BLOCK_SIZE + BLOCK_LAST);
        OrderedKeys ok = rb.getOrderedKeysByPosition(BLOCK_SIZE / 2, BLOCK_SIZE);
        Index ix = ok.asIndex();
        ix.validate();
        assertEquals(BLOCK_SIZE, ix.size());

        rb = rb.addRange(sk + BLOCK_SIZE, sk + BLOCK_SIZE + BLOCK_LAST);
        ok = rb.getOrderedKeysByPosition(BLOCK_SIZE / 2, 2 * BLOCK_SIZE);
        ix = ok.asIndex();
        ix.validate();
        assertEquals(2 * BLOCK_SIZE, ix.size());
        ok.close();

        rb = rb.addRange(sk + 9 * BLOCK_SIZE, sk + 9 * BLOCK_SIZE + BLOCK_LAST);
        ok = rb.getOrderedKeysByPosition(BLOCK_SIZE / 2, 3 * BLOCK_SIZE);
        ix = ok.asIndex();
        ix.validate();
        assertEquals(3 * BLOCK_SIZE, ix.size());
        ok.close();

        rb = rb.addRange(sk + 6 * BLOCK_SIZE, sk + 6 * BLOCK_SIZE + 2);
        ok = rb.getOrderedKeysByPosition(BLOCK_SIZE / 2, 3 * BLOCK_SIZE);
        ix = ok.asIndex();
        ix.validate();
        assertEquals(3 * BLOCK_SIZE, ix.size());
        ok.close();

        ok = rb.getOrderedKeysByPosition(rb.find(sk + 6 * BLOCK_SIZE + 2), 1 + 2 * BLOCK_SIZE);
        ix = ok.asIndex();
        ix.validate();
        assertEquals(1 + 2 * BLOCK_SIZE, ix.size());
        ok.close();
    }

    @Test
    public void testAsIndexMultiBlockFirstFullBlockSpan() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 7 * BLOCK_SIZE;
        rb.addRange(sk, sk + 5 * BLOCK_SIZE - 1);
        assertEquals(5 * BLOCK_SIZE, rb.getCardinality());
        final OrderedKeys ok = rb.getOrderedKeysByPosition(1, 5 * BLOCK_SIZE - 2);
        final Index ix = ok.asIndex();
        ix.validate();
        assertEquals(5 * BLOCK_SIZE - 2, ix.size());
        ok.close();
    }

    @Test
    public void testAsIndexEndOffsetMatchesCardinalityOfEndingContainer() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 5 * BLOCK_SIZE;
        rb.addRange(sk + 5, 6 * BLOCK_SIZE + 4);
        final OrderedKeys ok = rb.getOrderedKeysByKeyRange(sk + 60000, 6 * BLOCK_SIZE + 4);
        final Index ix = ok.asIndex();
        ix.validate();
        assertEquals(sk + 60000, ix.firstKey());
        assertEquals(6 * BLOCK_SIZE + 4, ix.lastKey());
        assertEquals(ix.lastKey() - ix.firstKey() + 1, ix.size());
        ok.close();
    }

    @Test
    public void testAsIndexSingleContainerFromFirstFullBlockSpanOfMoreThanOneBlock() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 10 * BLOCK_SIZE;
        rb.addRange(sk, sk + 10 * BLOCK_SIZE);
        final long first = 16 * BLOCK_SIZE + 1;
        final long last = 17 * BLOCK_SIZE - 1;
        final OrderedKeys ok = rb.getOrderedKeysByKeyRange(first, last);
        final Index ix = ok.asIndex();
        ix.validate();
        assertEquals(first, ix.firstKey());
        assertEquals(last, ix.lastKey());
        assertEquals(last - first + 1, ix.size());
        ok.close();
    }

    @Test
    public void testAsIndexFullBlockSpanLen4For3AtFront() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 3 * BLOCK_SIZE;
        rb.addRange(sk, sk + 4 * BLOCK_SIZE - 1);
        final long first = sk + BLOCK_SIZE;
        final long last = sk + 4 * BLOCK_SIZE - 1;
        final OrderedKeys ok = rb.getOrderedKeysByKeyRange(first, last);
        final Index ix = ok.asIndex();
        ix.validate();
        assertEquals(first, ix.firstKey());
        assertEquals(last, ix.lastKey());
        assertEquals(last - first + 1, ix.size());
        ok.close();
    }

    @Test
    public void testAsIndexFullBlockSpanLen4PlusSomeFor1AtFront() {
        final RspBitmap rb = new RspBitmap();
        final long sk = 3 * BLOCK_SIZE;
        rb.addRange(sk, sk + 4 * BLOCK_SIZE);
        final long first = sk + BLOCK_SIZE;
        final long last = sk + 4 * BLOCK_SIZE;
        final OrderedKeys ok = rb.getOrderedKeysByKeyRange(first, last);
        final Index ix = ok.asIndex();
        ix.validate();
        assertEquals(first, ix.firstKey());
        assertEquals(last, ix.lastKey());
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
        final OrderedKeys ok = rb.getOrderedKeysByKeyRange(startKey, endKey);
        final Index ix = ok.asIndex();
        ix.validate();
        final RspBitmap rb2 = (RspBitmap) rb.ixSubindexByKeyOnNew(startKey, endKey);
        rb2.andEquals(rb);
        final Index ix2 = new TreeIndex(rb2);
        assertEquals(ix2.size(), ix.size());
        assertEquals(0, ix2.minus(ix).size());
        ok.close();
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
                        try (final OrderedKeys ok = rb.getOrderedKeysByKeyRange(startKey, endKey)) {
                            final RspBitmap expected = rb.subrangeByValue(startKey, endKey);
                            final RspBitmap res = new RspBitmap();
                            final OrderedKeys.Iterator it = ok.getOrderedKeysIterator();
                            final int chunkSz = 13;
                            try (final WritableLongChunk<OrderedKeyIndices> chunk =
                                WritableLongChunk.makeWritableChunk(chunkSz)) {
                                while (it.hasMore()) {
                                    final OrderedKeys okit =
                                        it.getNextOrderedKeysWithLength(chunkSz);
                                    chunk.setSize(chunkSz);
                                    okit.fillKeyIndicesChunk(chunk);
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
                        try (final OrderedKeys ok = rb.getOrderedKeysByKeyRange(startKey, endKey)) {
                            final RspBitmap expected = rb.subrangeByValue(startKey, endKey);
                            final RspBitmap res = new RspBitmap();
                            final OrderedKeys.Iterator it = ok.getOrderedKeysIterator();
                            final int chunkSz = 13;
                            try (final WritableLongChunk<OrderedKeyRanges> chunk =
                                WritableLongChunk.makeWritableChunk(chunkSz)) {
                                while (it.hasMore()) {
                                    final OrderedKeys okit =
                                        it.getNextOrderedKeysWithLength(chunkSz / 2);
                                    chunk.setSize(chunkSz);
                                    okit.fillKeyRangesChunk(chunk);
                                    final int sz = chunk.size();
                                    for (int i = 0; i < sz; i += 2) {
                                        res.addRangeUnsafeNoWriteCheck(chunk.get(i),
                                            chunk.get(i + 1));
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
                        try (final OrderedKeys ok = rb.getOrderedKeysByKeyRange(startKey, endKey)) {
                            final RspBitmap expected = rb.subrangeByValue(startKey, endKey);
                            final RspBitmap res = new RspBitmap();
                            final OrderedKeys.Iterator it = ok.getOrderedKeysIterator();
                            while (it.hasMore()) {
                                final OrderedKeys okit = it.getNextOrderedKeysWithLength(13);
                                okit.forAllLongRanges(res::addRange);
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
        rb.addValues(175, 225, 288, 351, 429, 523, 562, 131247, 131297, 131360, 131423, 131501,
            131595, 131634);
        final OrderedKeys.Iterator it = rb.getOrderedKeysIterator();
        int maxKey = BLOCK_SIZE - 1;
        while (it.hasMore()) {
            final OrderedKeys oks = it.getNextOrderedKeysThrough(maxKey);
            final String m = "maxKey==" + maxKey;
            switch (maxKey) {
                case BLOCK_SIZE - 1:
                case 3 * BLOCK_SIZE - 1:
                    assertEquals(m, 7, oks.size());
                    break;
                case 2 * BLOCK_SIZE - 1:
                    assertEquals(m, 0, oks.size());
                    assertEquals(m, OrderedKeys.EMPTY, oks);
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
        final OrderedKeys.Iterator it = rb.getOrderedKeysIterator();
        assertTrue(it.hasMore());
        final OrderedKeys oks1 = it.getNextOrderedKeysThrough(end - 1);
        assertEquals(end - start + 1 - 1, oks1.size());
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
        try (final OrderedKeys.Iterator it1 = rb.getOrderedKeysIterator()) {
            assertTrue(it1.hasMore());
            final OrderedKeys ok1 = it1.getNextOrderedKeysWithLength(6);
            assertEquals(6, ok1.size());
            try (final OrderedKeys.Iterator it2 = ok1.getOrderedKeysIterator()) {
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
        final OrderedKeys.Iterator it = rb.getOrderedKeysIterator();
        assertTrue(it.hasMore());
        final OrderedKeys ok0 = it.getNextOrderedKeysThrough(BLOCK_SIZE + 5);
        assertEquals(10, ok0.size());
        final OrderedKeys ok1 = it.getNextOrderedKeysThrough(2 * BLOCK_SIZE + 3);
        assertEquals(4, ok1.size());
    }

    @Test
    public void testAdvaceBugRsp() {
        advanceBug(TreeIndex.makeEmptyRsp());
    }

    @Test
    public void testForEachLongRangeInSpanRegression0() {
        final RspBitmap rb = new RspBitmap();
        // We need a number of ranges that matches bufSz/2 in the implementation for
        // RspArray.forEachLongRangeInSpanWithOffsetAndMaxCardinality.
        // Further, we need to stop in a partial range short of the full filled buf.
        rb.addRange(1, 2);
        rb.addRange(4, 5);
        rb.addRange(7, 8); // we stop here (7 below in getOrderedKeysByRange), so that there is one
                           // range after us.
        rb.addRange(10, 11);
        final OrderedKeys ok = rb.getOrderedKeysByKeyRange(1, 7);
        ok.forAllLongRanges((final long start, final long end) -> {
            assertTrue(end >= start);
        });
    }

    @Test
    public void testAsKeyRangesChunkRegression0() {
        Index index = TreeIndex.makeSingleRange(130972, 131071);
        index.insert(262144);

        final long CHUNK_SIZE = 4096;

        try (final OrderedKeys.Iterator it = index.getOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            final OrderedKeys subKeys = it.getNextOrderedKeysWithLength(CHUNK_SIZE);
            final LongChunk<OrderedKeyRanges> ranges = subKeys.asKeyRangesChunk();
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
        try (final OrderedKeys.Iterator okIter = rb.getOrderedKeysIterator()) {
            long last = 0;
            while (okIter.hasMore()) {
                final OrderedKeys ok = okIter.getNextOrderedKeysThrough(last + BLOCK_SIZE);
                last = ok.lastKey();
                final LongChunk<OrderedKeyIndices> chunk = ok.asKeyIndicesChunk();
                count += chunk.size();
            }
        }
        assertEquals(rb.getCardinality(), count);
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    @Test
    public void testAsKeyRangesChunkRegression0Rsp() {
        Index index = TreeIndex.makeSingleRange(130972, 131071);
        index.insert(262144);

        final long CHUNK_SIZE = 4096;

        try (final OrderedKeys.Iterator it = index.getOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            final OrderedKeys subKeys = it.getNextOrderedKeysWithLength(CHUNK_SIZE);
            final LongChunk<OrderedKeyRanges> ranges = subKeys.asKeyRangesChunk();
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
        Index index = Index.FACTORY.getEmptyIndex();
        index.insertRange(130972, 131071);
        index.insert(262144);

        final long CHUNK_SIZE = 4096;

        try (final OrderedKeys.Iterator it = index.getOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            final OrderedKeys subKeys = it.getNextOrderedKeysWithLength(CHUNK_SIZE);
            final LongChunk<OrderedKeyRanges> ranges = subKeys.asKeyRangesChunk();
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
            try (final OrderedKeys.Iterator okIter = rb.getOrderedKeysIterator()) {
                assertTrue(okIter.advance(target));
                final OrderedKeys ok = okIter.getNextOrderedKeysWithLength(1);
                assertEquals(1, ok.size());
                assertEquals(2 * v + 1, ok.firstKey());
            }
        }
        rb = rb.addRange(3 * v + 10, 3 * v + 13);
        try (final OrderedKeys.Iterator okIter = rb.getOrderedKeysIterator()) {
            assertTrue(okIter.advance(2 * v + 2));
            final OrderedKeys ok = okIter.getNextOrderedKeysWithLength(2);
            assertEquals(2, ok.size());
            assertEquals(3 * v + 10, ok.firstKey());
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
            try (final OrderedKeys.Iterator okIter = rb.getOrderedKeysIterator();
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
                    assertTrue(m2, okIter.advance(advanceTarget));
                    assertEquals(m2, start, okIter.peekNextKey());
                    final long relativePos = okIter.getRelativePosition();
                    assertTrue(relativePos > lastRelativePos);
                    try (final OrderedKeys.Iterator okIter2 = rb.getOrderedKeysIterator()) {
                        assertTrue(m2, okIter2.advance(advanceTarget));
                        assertEquals(m2, start, okIter2.peekNextKey());
                    }
                    if (start - 1 > 0 && start - 1 != advanceTarget) {
                        try (final OrderedKeys.Iterator okIter2 = rb.getOrderedKeysIterator()) {
                            assertTrue(m2, okIter2.advance(start - 1));
                            assertEquals(m2, start, okIter2.peekNextKey());
                        }
                    }
                    try (final OrderedKeys.Iterator okIter2 = rb.getOrderedKeysIterator()) {
                        assertTrue(m2, okIter2.advance(start));
                        assertEquals(m2, start, okIter2.peekNextKey());
                    }
                    try (final OrderedKeys.Iterator okIter2 = rb.getOrderedKeysIterator()) {
                        assertTrue(m2, okIter2.advance(end));
                        assertEquals(m2, end, okIter2.peekNextKey());
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
        try (final OrderedKeys.Iterator okIter = rb.getOrderedKeysIterator()) {
            final OrderedKeys ok = okIter.getNextOrderedKeysWithLength(2);
            assertEquals(2, ok.size());
            assertEquals(v, ok.firstKey());
            assertEquals(2 * v + 1, ok.lastKey());
            final OrderedKeys ok2 = okIter.getNextOrderedKeysThrough(2 * v);
            assertEquals(0, ok2.size());
        }
    }

    @Test
    public void testIteratorGetNextOrderedKeysThroughRegression0() {
        RspBitmap rb = RspBitmap.makeSingleRange(10, 15);
        rb = rb.appendRange(BLOCK_SIZE + 1, BLOCK_SIZE + 10);
        // rb has two containers, we are going to iterate to a maxKey of BLOCK_SIZE, which is not
        // present
        // and if it was, would be at the beginning of the second container.
        try (OrderedKeys.Iterator it = rb.ixGetOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            OrderedKeys ok = it.getNextOrderedKeysThrough(BLOCK_SIZE);
            assertEquals(15, ok.lastKey());
            assertTrue(it.hasMore());
            ok = it.getNextOrderedKeysThrough(BLOCK_SIZE + 20);
            assertEquals(BLOCK_SIZE + 10, ok.lastKey());
            assertFalse(it.hasMore());
        }
    }

    @Test
    public void testAdvanceSingleBlocks() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(1);
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (OrderedKeys.Iterator it = rb.ixGetOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            final boolean has = it.advance(BLOCK_SIZE + 1);
            assertTrue(has);
            final OrderedKeys ok = it.getNextOrderedKeysThrough(3 * BLOCK_SIZE + 1);
            assertEquals(BLOCK_SIZE + 3, ok.size());
            assertFalse(ok.isContiguous());
            assertEquals(BLOCK_SIZE + 10, ok.firstKey());
            assertEquals(3 * BLOCK_SIZE + 1, ok.lastKey());
            assertFalse(it.hasMore());
        }
    }

    @Test
    public void testFindOrPrevCoverage() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(1);
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (OrderedKeys.Iterator it = rb.ixGetOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            OrderedKeys ok = it.getNextOrderedKeysThrough(BLOCK_SIZE + 10);
            assertEquals(2, ok.size());
            assertFalse(ok.isContiguous());
            assertEquals(1, ok.firstKey());
            assertEquals(BLOCK_SIZE + 10, ok.lastKey());
            assertTrue(it.hasMore());
        }
    }

    @Test
    public void testFindOrPrevCoverage2() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(1);
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (OrderedKeys.Iterator it = rb.ixGetOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            OrderedKeys ok = it.getNextOrderedKeysThrough(BLOCK_SIZE + 9);
            assertEquals(1, ok.size());
            assertEquals(1, ok.firstKey());
            assertTrue(it.hasMore());
        }
    }

    @Test
    public void testFindOrPrevCoverage3() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(1);
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (OrderedKeys.Iterator it = rb.ixGetOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            OrderedKeys ok = it.getNextOrderedKeysThrough(BLOCK_SIZE + 11);
            assertEquals(2, ok.size());
            assertEquals(1, ok.firstKey());
            assertEquals(BLOCK_SIZE + 10, ok.lastKey());
            assertTrue(it.hasMore());
        }
    }

    @Test
    public void testFindOrPrevCoverage4() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE + 1);
        try (OrderedKeys.Iterator it = rb.ixGetOrderedKeysIterator()) {
            assertTrue(it.hasMore());
            OrderedKeys ok = it.getNextOrderedKeysThrough(BLOCK_SIZE + 9);
            assertEquals(0, ok.size());
            assertTrue(it.hasMore());
            ok = it.getNextOrderedKeysThrough(BLOCK_SIZE + 11);
            assertEquals(1, ok.size());
            assertTrue(it.hasMore());
        }
    }
}
