/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

/**
 * OrderedKeys implementation tests can extend this to verify that OrderedKeys behavior is as expected.
 */
public abstract class OrderedKeysTestBase {

    private SafeCloseableList itemsToCloseOnTearDownCase;

    @Before
    public void setupCase() {
        itemsToCloseOnTearDownCase = new SafeCloseableList();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDownCase() {
        itemsToCloseOnTearDownCase.close();
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    protected abstract OrderedKeys create(long... values);

    final <TYPE extends SafeCloseable> TYPE closeOnTearDownCase(@NotNull final TYPE item) {
        itemsToCloseOnTearDownCase.add(item);
        return item;
    }

    long[] indicesFromRanges(long... ranges) {
        assertEquals(0, ranges.length % 2);
        long numElements = 0;
        for (int idx = 0; idx < ranges.length; idx += 2) {
            numElements += ranges[idx + 1] - ranges[idx] + 1;
        }
        assertTrue(numElements < LongChunk.MAXIMUM_SIZE);
        int offset = 0;
        final long[] elements = new long[(int) numElements];
        for (int idx = 0; idx + 1 < ranges.length; idx += 2) {
            final long range = ranges[idx + 1] - ranges[idx] + 1;
            for (long jdx = 0; jdx < range; ++jdx) {
                elements[offset++] = ranges[idx] + jdx;
            }
        }
        return elements;
    }

    @Test
    public void testOKIteratorGetNextMaxKeyOverruns() {
        final long[] indices = indicesFromRanges(0, 4, 10, 20, 40, 60);
        try (final OrderedKeys ok1 = create(indices)) {
            try (final OrderedKeys.Iterator it1 = ok1.getOrderedKeysIterator()) {
                final OrderedKeys ok2 = it1.getNextOrderedKeysThrough(20);
                try (final OrderedKeys.Iterator it2 = ok2.getOrderedKeysIterator()) {
                    final OrderedKeys ok3 = it2.getNextOrderedKeysThrough(600);
                    assertEquals(20, ok3.lastKey());
                }
            }
        }
    }

    @Test
    public void testCanConstructOrderedKeys() {
        final long[] indices = indicesFromRanges(0, 4, Long.MAX_VALUE - 4, Long.MAX_VALUE);
        try (final OrderedKeys OK = create(indices)) {
            assertContentsByIndices(indices, OK);
        }
    }

    public static final long[] ranges0 = {1, 10, 21, 40, 101, 142};

    public static final long[] array(final long... vs) {
        return vs;
    }

    public static long[] shift(final long[] vs, final long offset) {
        final long[] ans = new long[vs.length];
        for (int i = 0; i < vs.length; ++i) {
            ans[i] = vs[i] + offset;
        }
        return ans;
    }

    public static final long offset0 = (1 << 16) - 4;

    @Test
    public void testGetOrderedKeysByPositionEdgedAtBeginning() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(0, 20)) {
            assertContentsByIndices(indicesFromRanges(shift(array(1, 10, 21, 30), offset0)), byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByPositionEdgeInMiddle() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(10, 20)) {
            assertContentsByIndices(indicesFromRanges(shift(array(21, 40), offset0)), byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByPositionMiddle() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(8, 24)) {
            assertContentsByIndices(indicesFromRanges(shift(array(9, 10, 21, 40, 101, 102), offset0)), byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByPositionEdgedAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(28, 44)) {
            assertContentsByIndices(indicesFromRanges(shift(array(39, 40, 101, 142), offset0)), byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByPositionBeginAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(72, 1024)) {
            assertContentsByIndices(indicesFromRanges(), byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByPositionOverlapAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(60, 1024)) {
            assertContentsByIndices(indicesFromRanges(shift(array(131, 142), offset0)), byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByPositionBeyondEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(100, 1)) {
            assertContentsByIndices(indicesFromRanges(), byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByPositionOverlapEntire() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(0, 1024)) {
            assertContentsByIndices(indices, byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByPositionExactRange() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byPos = OK.getOrderedKeysByPosition(0, indices.length)) {
            assertContentsByIndices(indices, byPos);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeBeforeBeginning() {
        final long[] indices = indicesFromRanges(shift(array(21, 40, 101, 142), offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byKey = OK.getOrderedKeysByKeyRange(offset0, 20 + offset0)) {
            assertContentsByIndices(indicesFromRanges(), byKey);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeOverlapBeginning() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byKey = OK.getOrderedKeysByKeyRange(offset0, offset0 + 5)) {
            assertContentsByIndices(indicesFromRanges(offset0 + 1, offset0 + 5), byKey);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeEdgeAtBeginning() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byRange = OK.getOrderedKeysByKeyRange(offset0 + 1, offset0 + 5)) {
            assertContentsByIndices(indicesFromRanges(offset0 + 1, offset0 + 5), byRange);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeEdgeInMiddle() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byRange = OK.getOrderedKeysByKeyRange(offset0 + 10, offset0 + 30)) {
            assertContentsByIndices(indicesFromRanges(shift(array(10, 10, 21, 30), offset0)), byRange);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeMiddle() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byRange = OK.getOrderedKeysByKeyRange(offset0 + 24, offset0 + 34)) {
            assertContentsByIndices(indicesFromRanges(offset0 + 24, offset0 + 34), byRange);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeEdgedAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byRange = OK.getOrderedKeysByKeyRange(offset0 + 120, offset0 + 142)) {
            assertContentsByIndices(indicesFromRanges(offset0 + 120, offset0 + 142), byRange);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeBeginAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byRange = OK.getOrderedKeysByKeyRange(offset0 + 143, offset0 + 1024)) {
            assertContentsByIndices(indicesFromRanges(), byRange);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeOverlapEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byRange = OK.getOrderedKeysByKeyRange(offset0 + 25, offset0 + 1024)) {
            assertContentsByIndices(indicesFromRanges(shift(array(25, 40, 101, 142), offset0)), byRange);
        }
    }

    @Test
    public void testGetOrderedKeysByRangeBeyondEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final OrderedKeys OK = create(indices);
                final OrderedKeys byRange = OK.getOrderedKeysByKeyRange(offset0 + 1024, offset0 + 2048)) {
            assertContentsByIndices(indicesFromRanges(), byRange);
        }
    }

    @Test
    public void testFillIndices() {
        for (long[] ranges : new long[][] {ranges0, ranges1, ranges2}) {
            final long[] indices = indicesFromRanges(shift(ranges, offset0));
            try (final WritableLongChunk<OrderedKeyIndices> chunk =
                    WritableLongChunk.makeWritableChunk(indices.length)) {
                try (final OrderedKeys OK = create(indices)) {
                    OK.fillKeyIndicesChunk(chunk);
                }
                final LongChunk<OrderedKeyIndices> expectedChunk = LongChunk.chunkWrap(indices, 0, indices.length);
                assertChunksEqual(expectedChunk, chunk);
            }
        }
    }

    @Test
    public void testFillRanges() {
        final long[][] rangesessess = new long[][] {ranges0, ranges1, ranges2};
        for (int r = 0; r < rangesessess.length; ++r) {
            final long[] ranges = rangesessess[r];
            final long[] indices = indicesFromRanges(shift(ranges, offset0));
            try (final WritableLongChunk<OrderedKeyRanges> chunk =
                    WritableLongChunk.makeWritableChunk(indices.length)) {
                try (final OrderedKeys OK = create(indices)) {
                    OK.fillKeyRangesChunk(chunk);
                }
                int i = 0;
                assertEquals(0, chunk.size() & 1);
                for (int j = 0; j < chunk.size(); j += 2) {
                    final long s = chunk.get(j);
                    final long e = chunk.get(j + 1);
                    for (long v = s; v <= e; ++v) {
                        assertEquals("r==" + r + " && i==" + i + " && v==" + v, indices[i++], v);
                    }
                }
                assertEquals(indices.length, i);
            }
        }
    }

    @Test
    public void testAverageRunLenSingleRun() {
        final long[] indices = indicesFromRanges(1, 100);
        try (final OrderedKeys OK = create(indices)) {
            assertEquals(indices.length, OK.getAverageRunLengthEstimate());
        }
    }

    @Test
    public void testAverageRunLenSingleElements() {
        final long[] indices = new long[100];
        for (int idx = 0; idx < indices.length; ++idx) {
            indices[idx] = 2 * idx;
        }
        try (final OrderedKeys OK = create(indices)) {
            assertEquals(1, OK.getAverageRunLengthEstimate());
        }
    }

    static final long[] ranges1 = {11, 20, 51, 60, 128, 256};

    @Test
    public void testIteratorAdvanceAndPeekOnlyAllElements() {
        final long[] indices = indicesFromRanges(ranges1);
        try (final OrderedKeys OK = create(indices)) {
            try (final OrderedKeys.Iterator it = OK.getOrderedKeysIterator()) {
                for (int offset = 0; offset < indices.length; ++offset) {
                    assertEquals(indices[offset], it.peekNextKey());
                    final boolean expectMore = offset + 1 < indices.length;
                    final String m = "offset==" + offset;
                    assertEquals(m, expectMore, it.advance(expectMore ? indices[offset + 1] : indices[offset] + 1));
                    assertEquals(m, expectMore, it.hasMore());
                }
            }
        }
    }

    @Test
    public void testIteratorAdvanceAndPeekOnlyCertainElements() {
        final long[] indices = indicesFromRanges(ranges1);
        try (final OrderedKeys OK = create(indices)) {
            try (final OrderedKeys.Iterator it = OK.getOrderedKeysIterator()) {
                for (int offset = 0; offset < indices.length; ++offset) {
                    if (indices[offset] % 5 != 0)
                        continue;
                    assertTrue(it.advance(indices[offset]));
                    assertEquals(indices[offset], it.peekNextKey());
                    assertTrue(it.hasMore());
                }
            }
        }
    }

    @Test
    public void testIteratorGetNextByLenOnly() {
        final int stepSize = 5;
        final long[] indices = indicesFromRanges(ranges1);
        try (final OrderedKeys OK = create(indices)) {
            try (final OrderedKeys.Iterator it = OK.getOrderedKeysIterator()) {
                for (int offset = 0; offset < indices.length; offset += stepSize) {
                    final long pos0 = it.getRelativePosition();
                    final OrderedKeys subOK = it.getNextOrderedKeysWithLength(stepSize);
                    final long pos1 = it.getRelativePosition();
                    assertEquals(subOK.size(), pos1 - pos0);
                    assertContentsByIndices(sliceLongArray(indices, offset,
                            Math.min(indices.length, offset + stepSize)), subOK);
                }
            }
        }
    }

    @Test
    public void testIteratorGetNextByKeyOnly() {
        final long stepSize = 5;
        final long[] indices = indicesFromRanges(ranges1);
        try (final OrderedKeys OK = create(indices)) {
            try (final OrderedKeys.Iterator it = OK.getOrderedKeysIterator()) {
                for (long key = indices[0] - (indices[0] % stepSize); key < indices[indices.length - 1]; key +=
                        stepSize) {
                    final long endKey = key + stepSize - 1;
                    final long pos0 = it.getRelativePosition();
                    final OrderedKeys subOK = it.getNextOrderedKeysThrough(endKey);
                    final long pos1 = it.getRelativePosition();
                    final String m = "key==" + key;
                    assertEquals(m, subOK.size(), pos1 - pos0);
                    int startOffset = Arrays.binarySearch(indices, key);
                    if (startOffset < 0)
                        startOffset = ~startOffset;
                    int endOffset = Arrays.binarySearch(indices, endKey);
                    endOffset = (endOffset < 0) ? ~endOffset : endOffset + 1;
                    assertContentsByIndices(m, sliceLongArray(indices, startOffset, endOffset), subOK);
                }
            }
        }
    }

    @Test
    public void testIteratorAdvanceThenLen() {
        final int stepSize = 50;
        final long[] indices = indicesFromRanges(ranges1);
        try (final OrderedKeys OK = create(indices)) {
            try (final OrderedKeys.Iterator it = OK.getOrderedKeysIterator()) {
                it.advance(150);
                final long pos0 = it.getRelativePosition();
                final OrderedKeys subOK = it.getNextOrderedKeysWithLength(50);
                final long pos1 = it.getRelativePosition();
                assertEquals(subOK.size(), pos1 - pos0);
                final int startOffset = Arrays.binarySearch(indices, 150);
                assertContentsByIndices(sliceLongArray(indices, startOffset, startOffset + stepSize), subOK);
            }
        }
    }

    @Test
    public void testIteratorAdvanceThenKeyThrough() {
        final long[] indices = indicesFromRanges(ranges1);
        try (final OrderedKeys OK = create(indices)) {
            try (final OrderedKeys.Iterator it = OK.getOrderedKeysIterator()) {
                it.advance(150);
                final OrderedKeys subOK = it.getNextOrderedKeysThrough(212);
                final int startOffset = Arrays.binarySearch(indices, 150);
                final int endOffset = Arrays.binarySearch(indices, 212);
                assertContentsByIndices(sliceLongArray(indices, startOffset, endOffset + 1), subOK);
            }
        }
    }

    @Test
    public void testIteratorKeyThroughThenLen() {
        final long[] indices = indicesFromRanges(ranges1);
        try (final OrderedKeys OK = create(indices)) {
            try (final OrderedKeys.Iterator it = OK.getOrderedKeysIterator()) {
                final int offset = Arrays.binarySearch(indices, 150) + 1;
                OrderedKeys subOK = it.getNextOrderedKeysThrough(150);
                assertContentsByIndices(sliceLongArray(indices, 0, offset), subOK);
                subOK = it.getNextOrderedKeysWithLength(20);
                assertContentsByIndices(sliceLongArray(indices, offset, offset + 20), subOK);
            }
        }
    }

    @Test
    public void testIteratorLenThenKeyThrough() {
        final long[] indices = indicesFromRanges(ranges1);
        try (final OrderedKeys OK = create(indices)) {
            try (final OrderedKeys.Iterator it = OK.getOrderedKeysIterator()) {
                final int offset = 35;
                OrderedKeys subOK = it.getNextOrderedKeysWithLength(offset);
                assertContentsByIndices(sliceLongArray(indices, 0, offset), subOK);
                subOK = it.getNextOrderedKeysThrough(150);
                final int endOffset = Arrays.binarySearch(indices, 150);
                assertContentsByIndices(sliceLongArray(indices, offset, endOffset + 1), subOK);
            }
        }
    }

    @Test
    public void testGetAverageRunLengthEstimate() {
        final Index ix = TreeIndex.makeEmptyRsp();
        final long k0 = 1 << 14;
        final long rlen = 16;
        final long nRanges = 256;
        for (int i = 0; i < nRanges; ++i) {
            final long s = i * k0;
            final long e = s + rlen;
            ix.insertRange(s, e);
        }
        long estimate = ix.getAverageRunLengthEstimate();
        assertEquals(1.0, rlen / (double) estimate, 0.1);
        try (final OrderedKeys.Iterator okit = ix.getOrderedKeysIterator()) {
            int r = 0;
            while (okit.hasMore()) {
                final String m = "r==" + r;
                final OrderedKeys oks = okit.getNextOrderedKeysWithLength(ix.size() / 2 + 1);
                assertEquals(1.0, rlen / (double) estimate, 0.1);
                ++r;
            }
        }
    }

    protected long[] sliceLongArray(final long[] src, final int startIndex, final int endIndex) {
        final long[] ret = new long[endIndex - startIndex];
        for (int offset = 0; offset < ret.length; ++offset) {
            ret[offset] = src[startIndex + offset];
        }
        return ret;
    }

    protected void assertContentsByIndices(final long[] expected,
            final OrderedKeys orderedKeys) {
        assertContentsByIndices(null, expected, orderedKeys);
    }

    protected void assertContentsByIndices(
            final String msg, final long[] expected, final OrderedKeys orderedKeys) {
        final LongChunk<OrderedKeyIndices> expectedIndices = LongChunk.chunkWrap(expected);
        try (final WritableLongChunk<OrderedKeyRanges> expectedRanges =
                ChunkUtils.convertToOrderedKeyRanges(expectedIndices)) {

            // size must be identical
            assertEquals(msg, expectedIndices.size(), orderedKeys.size());

            // Check Ordered Indices Chunk
            assertChunksEqual(expectedIndices, orderedKeys.asKeyIndicesChunk());
            // Check Ordered Ranges Chunk
            assertChunksEqual(expectedRanges, orderedKeys.asKeyRangesChunk());
            // Check Index
            final MutableInt idx = new MutableInt(0);
            final Index ix = orderedKeys.asIndex();
            assertTrue(msg, ix.forEachLong((value) -> {
                assertEquals(msg + " && value==" + value, expectedIndices.get(idx.intValue()), value);
                idx.add(1);
                return true;
            }));

            // Check fillKeyIndices
            try (final WritableLongChunk<OrderedKeyIndices> writableOKIndices =
                    WritableLongChunk.makeWritableChunk(expectedIndices.size())) {
                orderedKeys.fillKeyIndicesChunk(writableOKIndices);
                assertChunksEqual(expectedIndices, writableOKIndices);
            }
            // Check fillKeyRanges
            try (final WritableLongChunk<OrderedKeyRanges> writableOKRanges =
                    WritableLongChunk.makeWritableChunk(expectedRanges.size())) {
                orderedKeys.fillKeyRangesChunk(writableOKRanges);
                assertChunksEqual(expectedRanges, writableOKRanges);
            }
        }

        if (expectedIndices.size() > 0) {
            // Check first and last key.
            assertEquals(msg, expectedIndices.get(0), orderedKeys.firstKey());
            assertEquals(msg, expectedIndices.get(expectedIndices.size() - 1), orderedKeys.lastKey());

            // Check averageRunLength is reasonable (note: undefined if size is 0)
            final long runLen = orderedKeys.getAverageRunLengthEstimate();
            assertTrue(msg, runLen > 0);
            assertTrue(msg, runLen <= expectedIndices.size());
        }
    }

    protected <ATTR extends Any> void assertChunksEqual(final String msg, final LongChunk<ATTR> expected,
            final LongChunk<ATTR> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            assertEquals(msg, expected.get(i), actual.get(i));
        }
    }

    protected <ATTR extends Any> void assertChunksEqual(final LongChunk<ATTR> expected,
            final LongChunk<ATTR> actual) {
        assertChunksEqual(null, expected, actual);
    }

    public static final long k2 = 65536;
    public static final long[] ranges2 = {k2 + 10, k2 + 105, 2 * k2, 5 * k2 + 4, 7 * k2 - 2, 7 * k2 + 1, 8 * k2 - 1,
            8 * k2 - 1, 10 * k2, 12 * k2 + 3};

    private interface IndexBoundaryTest {
        void run(final String ctxt, final OrderedKeys ok, final Index ix, final long s, final long e);
    }

    private void testIndexBoundaries(final long[] ranges, final IndexBoundaryTest test) {
        final long[] r = ranges;
        final long[] indices = indicesFromRanges(r);
        try (final OrderedKeys ok = create(indices)) {
            final Index ix = ok.asIndex();
            for (int ri = 0; ri < r.length / 2; ri += 2) {
                final long si = r[ri];
                final long ei = r[ri + 1];
                for (long s = si - 1; s <= si + 1; ++s) {
                    for (long e = ei - 1; e <= ei + 1; ++e) {
                        if (e < s || s < 0) {
                            continue;
                        }
                        test.run("s==" + s + " && e==" + e, ok, ix, s, e);
                    }
                }
            }
        }
    }

    @Test
    public void testForEachLong() {
        testIndexBoundaries(ranges2,
                (final String ctxt, final OrderedKeys ok, final Index ix, final long s, final long e) -> {
                    final Index expected = ix.subindexByKey(s, e);
                    try (final OrderedKeys ok1 = ok.getOrderedKeysByKeyRange(s, e)) {
                        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
                        ok1.forEachLong((final long v) -> {
                            b.appendKey(v);
                            return true;
                        });
                        final Index result = b.getIndex();
                        assertEquals(ctxt, expected.size(), result.size());
                        final Index d = expected.minus(result);
                        assertEquals(ctxt, 0, d.size());
                    }
                });
    }

    @Test
    public void testForAllLongs() {
        testIndexBoundaries(ranges2,
                (final String ctxt, final OrderedKeys ok, final Index ix, final long s, final long e) -> {
                    final Index expected = ix.subindexByKey(s, e);
                    try (final OrderedKeys ok1 = ok.getOrderedKeysByKeyRange(s, e)) {
                        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
                        ok1.forAllLongs(b::appendKey);
                        final Index result = b.getIndex();
                        assertEquals(ctxt, expected.size(), result.size());
                        final Index d = expected.minus(result);
                        assertEquals(ctxt, 0, d.size());
                    }
                });
    }

    @Test
    public void testForEachLongRange() {
        testIndexBoundaries(ranges2,
                (final String ctxt, final OrderedKeys ok, final Index ix, final long s, final long e) -> {
                    final Index expected = ix.subindexByKey(s, e);
                    try (final OrderedKeys ok1 = ok.getOrderedKeysByKeyRange(s, e)) {
                        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
                        ok1.forEachLongRange((final long start, final long end) -> {
                            b.appendRange(start, end);
                            return true;
                        });
                        final Index result = b.getIndex();
                        assertEquals(ctxt, expected.size(), result.size());
                        final Index d = expected.minus(result);
                        assertEquals(ctxt, 0, d.size());
                    }
                });
    }

    @Test
    public void testForAllLongRanges() {
        testIndexBoundaries(ranges2,
                (final String ctxt, final OrderedKeys ok, final Index ix, final long s, final long e) -> {
                    final Index expected = ix.subindexByKey(s, e);
                    try (final OrderedKeys ok1 = ok.getOrderedKeysByKeyRange(s, e)) {
                        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
                        ok1.forAllLongRanges(b::appendRange);
                        final Index result = b.getIndex();
                        assertEquals(ctxt, expected.size(), result.size());
                        final Index d = expected.minus(result);
                        assertEquals(ctxt, 0, d.size());
                    }
                });
    }


    protected void advanceBug(final Index index) {
        final long regionBits = 40;
        final long regionSize = 1L << regionBits;
        final long subRegionBitMask = regionSize - 1;
        final int chunkCapacity = 4096;

        LongStream.range(0, 20).forEach(ri -> index.insertRange(ri * regionSize, ri * regionSize + 99_999));
        try (final OrderedKeys.Iterator outerOKI = index.getOrderedKeysIterator()) {
            while (outerOKI.hasMore()) {
                final OrderedKeys next = outerOKI.getNextOrderedKeysWithLength(chunkCapacity);
                final long firstKey = next.firstKey();
                final long lastKey = next.lastKey();
                if ((firstKey >> regionBits) == (lastKey >> regionBits)) {
                    // Same region - nothing special to do here
                    continue;
                }
                try (final OrderedKeys.Iterator innerOKI = next.getOrderedKeysIterator()) {
                    int totalConsumed = 0;
                    while (innerOKI.hasMore()) {
                        final long nextKey = innerOKI.peekNextKey();
                        final long target = (nextKey & ~subRegionBitMask) + regionSize;
                        final long consumed = innerOKI.advanceAndGetPositionDistance(target);
                        if (consumed + totalConsumed > chunkCapacity) {
                            throw new IllegalStateException(
                                    "Consumed " + consumed + ", after already consuming " + totalConsumed
                                            + ", exceeds capacity " + chunkCapacity);
                        }
                        totalConsumed += consumed;
                    }
                }
            }
        }
    }

    @Test
    public void testAdvancePastEnd() {
        final long[] r = ranges2;
        final long[] indices = indicesFromRanges(r);
        try (final OrderedKeys ok = create(indices);
                final OrderedKeys.Iterator okIter = ok.getOrderedKeysIterator()) {
            final long last = ok.lastKey();
            final OrderedKeys ok2 = okIter.getNextOrderedKeysThrough(last);
            assertFalse(okIter.hasMore());
            assertFalse(okIter.advance(last + 1));
        }
    }

    @Test
    public void testNextOrderedKeysThroughPastEnd() {
        final long[] r = ranges2;
        final long[] indices = indicesFromRanges(r);
        try (final OrderedKeys ok = create(indices);
                final OrderedKeys.Iterator okIter = ok.getOrderedKeysIterator()) {
            final long last = ok.lastKey();
            final OrderedKeys ok2 = okIter.getNextOrderedKeysThrough(last);
            assertFalse(okIter.hasMore());
            final OrderedKeys ok3 = okIter.getNextOrderedKeysThrough(last + 1);
            assertEquals(0, ok3.size());
        }
    }
}
