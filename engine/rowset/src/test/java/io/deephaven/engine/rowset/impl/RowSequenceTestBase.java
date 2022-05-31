/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
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
 * RowSequence implementation tests can extend this to verify that RowSequence behavior is as expected.
 */
public abstract class RowSequenceTestBase {

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

    protected abstract RowSequence create(long... values);

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
    public void testrsIteratorGetNextMaxKeyOverruns() {
        final long[] indices = indicesFromRanges(0, 4, 10, 20, 40, 60);
        try (final RowSequence rs1 = create(indices)) {
            try (final RowSequence.Iterator it1 = rs1.getRowSequenceIterator()) {
                final RowSequence rs2 = it1.getNextRowSequenceThrough(20);
                try (final RowSequence.Iterator it2 = rs2.getRowSequenceIterator()) {
                    final RowSequence rs3 = it2.getNextRowSequenceThrough(600);
                    assertEquals(20, rs3.lastRowKey());
                }
            }
        }
    }

    @Test
    public void testCanConstructRowSequence() {
        final long[] indices = indicesFromRanges(0, 4, Long.MAX_VALUE - 4, Long.MAX_VALUE);
        try (final RowSequence OK = create(indices)) {
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
    public void testGetRowSequenceByPositionEdgedAtBeginning() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(0, 20)) {
            assertContentsByIndices(indicesFromRanges(shift(array(1, 10, 21, 30), offset0)), byPos);
        }
    }

    @Test
    public void testGetRowSequenceByPositionEdgeInMiddle() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(10, 20)) {
            assertContentsByIndices(indicesFromRanges(shift(array(21, 40), offset0)), byPos);
        }
    }

    @Test
    public void testGetRowSequenceByPositionMiddle() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(8, 24)) {
            assertContentsByIndices(indicesFromRanges(shift(array(9, 10, 21, 40, 101, 102), offset0)), byPos);
        }
    }

    @Test
    public void testGetRowSequenceByPositionEdgedAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(28, 44)) {
            assertContentsByIndices(indicesFromRanges(shift(array(39, 40, 101, 142), offset0)), byPos);
        }
    }

    @Test
    public void testGetRowSequenceByPositionBeginAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(72, 1024)) {
            assertContentsByIndices(indicesFromRanges(), byPos);
        }
    }

    @Test
    public void testGetRowSequenceByPositionOverlapAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(60, 1024)) {
            assertContentsByIndices(indicesFromRanges(shift(array(131, 142), offset0)), byPos);
        }
    }

    @Test
    public void testGetRowSequenceByPositionBeyondEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(100, 1)) {
            assertContentsByIndices(indicesFromRanges(), byPos);
        }
    }

    @Test
    public void testGetRowSequenceByPositionOverlapEntire() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(0, 1024)) {
            assertContentsByIndices(indices, byPos);
        }
    }

    @Test
    public void testGetRowSequenceByPositionExactRange() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byPos = OK.getRowSequenceByPosition(0, indices.length)) {
            assertContentsByIndices(indices, byPos);
        }
    }

    @Test
    public void testGetRowSequenceByRangeBeforeBeginning() {
        final long[] indices = indicesFromRanges(shift(array(21, 40, 101, 142), offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byKey = OK.getRowSequenceByKeyRange(offset0, 20 + offset0)) {
            assertContentsByIndices(indicesFromRanges(), byKey);
        }
    }

    @Test
    public void testGetRowSequenceByRangeOverlapBeginning() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byKey = OK.getRowSequenceByKeyRange(offset0, offset0 + 5)) {
            assertContentsByIndices(indicesFromRanges(offset0 + 1, offset0 + 5), byKey);
        }
    }

    @Test
    public void testGetRowSequenceByRangeEdgeAtBeginning() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byRange = OK.getRowSequenceByKeyRange(offset0 + 1, offset0 + 5)) {
            assertContentsByIndices(indicesFromRanges(offset0 + 1, offset0 + 5), byRange);
        }
    }

    @Test
    public void testGetRowSequenceByRangeEdgeInMiddle() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byRange = OK.getRowSequenceByKeyRange(offset0 + 10, offset0 + 30)) {
            assertContentsByIndices(indicesFromRanges(shift(array(10, 10, 21, 30), offset0)), byRange);
        }
    }

    @Test
    public void testGetRowSequenceByRangeMiddle() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byRange = OK.getRowSequenceByKeyRange(offset0 + 24, offset0 + 34)) {
            assertContentsByIndices(indicesFromRanges(offset0 + 24, offset0 + 34), byRange);
        }
    }

    @Test
    public void testGetRowSequenceByRangeEdgedAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byRange = OK.getRowSequenceByKeyRange(offset0 + 120, offset0 + 142)) {
            assertContentsByIndices(indicesFromRanges(offset0 + 120, offset0 + 142), byRange);
        }
    }

    @Test
    public void testGetRowSequenceByRangeBeginAtEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byRange = OK.getRowSequenceByKeyRange(offset0 + 143, offset0 + 1024)) {
            assertContentsByIndices(indicesFromRanges(), byRange);
        }
    }

    @Test
    public void testGetRowSequenceByRangeOverlapEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byRange = OK.getRowSequenceByKeyRange(offset0 + 25, offset0 + 1024)) {
            assertContentsByIndices(indicesFromRanges(shift(array(25, 40, 101, 142), offset0)), byRange);
        }
    }

    @Test
    public void testGetRowSequenceByRangeBeyondEnd() {
        final long[] indices = indicesFromRanges(shift(ranges0, offset0));
        try (final RowSequence OK = create(indices);
                final RowSequence byRange = OK.getRowSequenceByKeyRange(offset0 + 1024, offset0 + 2048)) {
            assertContentsByIndices(indicesFromRanges(), byRange);
        }
    }

    @Test
    public void testFillIndices() {
        for (long[] ranges : new long[][] {ranges0, ranges1, ranges2}) {
            final long[] indices = indicesFromRanges(shift(ranges, offset0));
            try (final WritableLongChunk<OrderedRowKeys> chunk =
                    WritableLongChunk.makeWritableChunk(indices.length)) {
                try (final RowSequence OK = create(indices)) {
                    OK.fillRowKeyChunk(chunk);
                }
                final LongChunk<OrderedRowKeys> expectedChunk =
                        LongChunk.chunkWrap(indices, 0, indices.length);
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
            try (final WritableLongChunk<OrderedRowKeyRanges> chunk =
                    WritableLongChunk.makeWritableChunk(indices.length)) {
                try (final RowSequence OK = create(indices)) {
                    OK.fillRowKeyRangesChunk(chunk);
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
        try (final RowSequence OK = create(indices)) {
            assertEquals(indices.length, OK.getAverageRunLengthEstimate());
        }
    }

    @Test
    public void testAverageRunLenSingleElements() {
        final long[] indices = new long[100];
        for (int idx = 0; idx < indices.length; ++idx) {
            indices[idx] = 2 * idx;
        }
        try (final RowSequence OK = create(indices)) {
            assertEquals(1, OK.getAverageRunLengthEstimate());
        }
    }

    static final long[] ranges1 = {11, 20, 51, 60, 128, 256};

    @Test
    public void testIteratorAdvanceAndPeekOnlyAllElements() {
        final long[] indices = indicesFromRanges(ranges1);
        try (final RowSequence OK = create(indices)) {
            try (final RowSequence.Iterator it = OK.getRowSequenceIterator()) {
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
        try (final RowSequence OK = create(indices)) {
            try (final RowSequence.Iterator it = OK.getRowSequenceIterator()) {
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
        try (final RowSequence OK = create(indices)) {
            try (final RowSequence.Iterator it = OK.getRowSequenceIterator()) {
                for (int offset = 0; offset < indices.length; offset += stepSize) {
                    final long pos0 = it.getRelativePosition();
                    final RowSequence subOK = it.getNextRowSequenceWithLength(stepSize);
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
        try (final RowSequence OK = create(indices)) {
            try (final RowSequence.Iterator it = OK.getRowSequenceIterator()) {
                for (long key = indices[0] - (indices[0] % stepSize); key < indices[indices.length - 1]; key +=
                        stepSize) {
                    final long endKey = key + stepSize - 1;
                    final long pos0 = it.getRelativePosition();
                    final RowSequence subOK = it.getNextRowSequenceThrough(endKey);
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
        try (final RowSequence OK = create(indices)) {
            try (final RowSequence.Iterator it = OK.getRowSequenceIterator()) {
                it.advance(150);
                final long pos0 = it.getRelativePosition();
                final RowSequence subOK = it.getNextRowSequenceWithLength(50);
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
        try (final RowSequence OK = create(indices)) {
            try (final RowSequence.Iterator it = OK.getRowSequenceIterator()) {
                it.advance(150);
                final RowSequence subOK = it.getNextRowSequenceThrough(212);
                final int startOffset = Arrays.binarySearch(indices, 150);
                final int endOffset = Arrays.binarySearch(indices, 212);
                assertContentsByIndices(sliceLongArray(indices, startOffset, endOffset + 1), subOK);
            }
        }
    }

    @Test
    public void testIteratorKeyThroughThenLen() {
        final long[] indices = indicesFromRanges(ranges1);
        try (final RowSequence OK = create(indices)) {
            try (final RowSequence.Iterator it = OK.getRowSequenceIterator()) {
                final int offset = Arrays.binarySearch(indices, 150) + 1;
                RowSequence subOK = it.getNextRowSequenceThrough(150);
                assertContentsByIndices(sliceLongArray(indices, 0, offset), subOK);
                subOK = it.getNextRowSequenceWithLength(20);
                assertContentsByIndices(sliceLongArray(indices, offset, offset + 20), subOK);
            }
        }
    }

    @Test
    public void testIteratorLenThenKeyThrough() {
        final long[] indices = indicesFromRanges(ranges1);
        try (final RowSequence OK = create(indices)) {
            try (final RowSequence.Iterator it = OK.getRowSequenceIterator()) {
                final int offset = 35;
                RowSequence subOK = it.getNextRowSequenceWithLength(offset);
                assertContentsByIndices(sliceLongArray(indices, 0, offset), subOK);
                subOK = it.getNextRowSequenceThrough(150);
                final int endOffset = Arrays.binarySearch(indices, 150);
                assertContentsByIndices(sliceLongArray(indices, offset, endOffset + 1), subOK);
            }
        }
    }

    @Test
    public void testGetAverageRunLengthEstimate() {
        final WritableRowSet ix = RowSetTstUtils.makeEmptyRsp();
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
        try (final RowSequence.Iterator rsIt = ix.getRowSequenceIterator()) {
            int r = 0;
            while (rsIt.hasMore()) {
                final String m = "r==" + r;
                final RowSequence rs = rsIt.getNextRowSequenceWithLength(ix.size() / 2 + 1);
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
            final RowSequence rowSequence) {
        assertContentsByIndices(null, expected, rowSequence);
    }

    protected void assertContentsByIndices(
            final String msg, final long[] expected, final RowSequence rowSequence) {
        final LongChunk<OrderedRowKeys> expectedIndices = LongChunk.chunkWrap(expected);
        try (final WritableLongChunk<OrderedRowKeyRanges> expectedRanges =
                RowKeyChunkUtils.convertToOrderedKeyRanges(expectedIndices)) {

            // size must be identical
            assertEquals(msg, expectedIndices.size(), rowSequence.size());

            // Check Ordered Indices Chunk
            assertChunksEqual(expectedIndices, rowSequence.asRowKeyChunk());
            // Check Ordered Ranges Chunk
            assertChunksEqual(expectedRanges, rowSequence.asRowKeyRangesChunk());
            // Check RowSet
            final MutableInt idx = new MutableInt(0);
            final RowSet ix = rowSequence.asRowSet();
            assertTrue(msg, ix.forEachRowKey((value) -> {
                assertEquals(msg + " && value==" + value, expectedIndices.get(idx.intValue()), value);
                idx.add(1);
                return true;
            }));

            // Check fillKeyIndices
            try (final WritableLongChunk<OrderedRowKeys> writableOKIndices =
                    WritableLongChunk.makeWritableChunk(expectedIndices.size())) {
                rowSequence.fillRowKeyChunk(writableOKIndices);
                assertChunksEqual(expectedIndices, writableOKIndices);
            }
            // Check fillKeyRanges
            try (final WritableLongChunk<OrderedRowKeyRanges> writableOKRanges =
                    WritableLongChunk.makeWritableChunk(expectedRanges.size())) {
                rowSequence.fillRowKeyRangesChunk(writableOKRanges);
                assertChunksEqual(expectedRanges, writableOKRanges);
            }
        }

        if (expectedIndices.size() > 0) {
            // Check first and last key.
            assertEquals(msg, expectedIndices.get(0), rowSequence.firstRowKey());
            assertEquals(msg, expectedIndices.get(expectedIndices.size() - 1), rowSequence.lastRowKey());

            // Check averageRunLength is reasonable (note: undefined if size is 0)
            final long runLen = rowSequence.getAverageRunLengthEstimate();
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
        void run(final String ctxt, final RowSequence rs, final RowSet ix, final long s, final long e);
    }

    private void testIndexBoundaries(final long[] ranges, final IndexBoundaryTest test) {
        final long[] r = ranges;
        final long[] indices = indicesFromRanges(r);
        try (final RowSequence rs = create(indices)) {
            final RowSet ix = rs.asRowSet();
            for (int ri = 0; ri < r.length / 2; ri += 2) {
                final long si = r[ri];
                final long ei = r[ri + 1];
                for (long s = si - 1; s <= si + 1; ++s) {
                    for (long e = ei - 1; e <= ei + 1; ++e) {
                        if (e < s || s < 0) {
                            continue;
                        }
                        test.run("s==" + s + " && e==" + e, rs, ix, s, e);
                    }
                }
            }
        }
    }

    @Test
    public void testForEachLong() {
        testIndexBoundaries(ranges2,
                (final String ctxt, final RowSequence rs, final RowSet ix, final long s, final long e) -> {
                    final RowSet expected = ix.subSetByKeyRange(s, e);
                    try (final RowSequence rs1 = rs.getRowSequenceByKeyRange(s, e)) {
                        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
                        rs1.forEachRowKey((final long v) -> {
                            b.appendKey(v);
                            return true;
                        });
                        final RowSet result = b.build();
                        assertEquals(ctxt, expected.size(), result.size());
                        final RowSet d = expected.minus(result);
                        assertEquals(ctxt, 0, d.size());
                    }
                });
    }

    @Test
    public void testForAllLongs() {
        testIndexBoundaries(ranges2,
                (final String ctxt, final RowSequence rs, final RowSet ix, final long s, final long e) -> {
                    final RowSet expected = ix.subSetByKeyRange(s, e);
                    try (final RowSequence rs1 = rs.getRowSequenceByKeyRange(s, e)) {
                        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
                        rs1.forAllRowKeys(b::appendKey);
                        final RowSet result = b.build();
                        assertEquals(ctxt, expected.size(), result.size());
                        final RowSet d = expected.minus(result);
                        assertEquals(ctxt, 0, d.size());
                    }
                });
    }

    @Test
    public void testForEachLongRange() {
        testIndexBoundaries(ranges2,
                (final String ctxt, final RowSequence rs, final RowSet ix, final long s, final long e) -> {
                    final RowSet expected = ix.subSetByKeyRange(s, e);
                    try (final RowSequence rs1 = rs.getRowSequenceByKeyRange(s, e)) {
                        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
                        rs1.forEachRowKeyRange((final long start, final long end) -> {
                            b.appendRange(start, end);
                            return true;
                        });
                        final RowSet result = b.build();
                        assertEquals(ctxt, expected.size(), result.size());
                        final RowSet d = expected.minus(result);
                        assertEquals(ctxt, 0, d.size());
                    }
                });
    }

    @Test
    public void testForAllLongRanges() {
        testIndexBoundaries(ranges2,
                (final String ctxt, final RowSequence rs, final RowSet ix, final long s, final long e) -> {
                    final RowSet expected = ix.subSetByKeyRange(s, e);
                    try (final RowSequence rs1 = rs.getRowSequenceByKeyRange(s, e)) {
                        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
                        rs1.forAllRowKeyRanges(b::appendRange);
                        final RowSet result = b.build();
                        assertEquals(ctxt, expected.size(), result.size());
                        final RowSet d = expected.minus(result);
                        assertEquals(ctxt, 0, d.size());
                    }
                });
    }


    protected void advanceBug(final WritableRowSet rowSet) {
        final long regionBits = 40;
        final long regionSize = 1L << regionBits;
        final long subRegionBitMask = regionSize - 1;
        final int chunkCapacity = 4096;

        LongStream.range(0, 20).forEach(ri -> rowSet.insertRange(ri * regionSize, ri * regionSize + 99_999));
        try (final RowSequence.Iterator outerOKI = rowSet.getRowSequenceIterator()) {
            while (outerOKI.hasMore()) {
                final RowSequence next = outerOKI.getNextRowSequenceWithLength(chunkCapacity);
                final long firstKey = next.firstRowKey();
                final long lastKey = next.lastRowKey();
                if ((firstKey >> regionBits) == (lastKey >> regionBits)) {
                    // Same region - nothing special to do here
                    continue;
                }
                try (final RowSequence.Iterator innerOKI = next.getRowSequenceIterator()) {
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
        try (final RowSequence rs = create(indices);
                final RowSequence.Iterator rsIter = rs.getRowSequenceIterator()) {
            final long last = rs.lastRowKey();
            final RowSequence rs2 = rsIter.getNextRowSequenceThrough(last);
            assertFalse(rsIter.hasMore());
            assertFalse(rsIter.advance(last + 1));
        }
    }

    @Test
    public void testNextRowSequenceThroughPastEnd() {
        final long[] r = ranges2;
        final long[] indices = indicesFromRanges(r);
        try (final RowSequence rs = create(indices);
                final RowSequence.Iterator rsIter = rs.getRowSequenceIterator()) {
            final long last = rs.lastRowKey();
            final RowSequence rs2 = rsIter.getNextRowSequenceThrough(last);
            assertFalse(rsIter.hasMore());
            final RowSequence rs3 = rsIter.getNextRowSequenceThrough(last + 1);
            assertEquals(0, rs3.size());
        }
    }
}
