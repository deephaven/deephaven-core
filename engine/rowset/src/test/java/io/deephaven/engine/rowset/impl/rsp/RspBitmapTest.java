/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl.rsp;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import io.deephaven.base.testing.Shuffle;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.*;
import io.deephaven.engine.rowset.impl.rsp.container.ArrayContainer;
import io.deephaven.engine.rowset.impl.rsp.container.Container;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BITS_PER_BLOCK;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class RspBitmapTest {
    private static final int runs = 1;
    private static final int lastRun = 1; // to help offset the seed when doing multiple runs.
    private static final int seed0 = 50150 + lastRun;

    @Test
    public void testSimpleKeepAddingTillAllOnes() {
        final RspBitmap rb = new RspBitmap();
        long start = 0;
        for (int shift = 1; shift <= 16; ++shift) {
            final long end = (1 << shift);
            rb.addRangeUnsafeNoWriteCheck(0, start, end - 1);
            start = end;
        }
        rb.addRangeUnsafeNoWriteCheck(0, 1 << 15, 1 << 16 - 1);
        for (int i = 0; i < (1 << 16); ++i) {
            assertTrue(rb.contains(i));
        }
    }

    private void assertContainerAt(final int pos, final RspBitmap rb) {
        final Object span = rb.getKvs().getSpans()[pos];
        assertTrue(span instanceof Container || span instanceof short[]);
    }

    private long getContainerCardinality(final int pos, final RspBitmap rb) {
        try (RspArray.SpanView view = new RspArray.WorkData().borrowSpanView(rb, pos)) {
            return view.getContainer().getCardinality();
        }
    }

    private void assertFullBlocksSpanAt(final int pos, final RspBitmap rb, final long flen) {
        assertEquals(flen, rb.getFullBlockSpanLenAt(pos));
    }

    @Test
    public void testSimpleAddFirst2SpansThen1() {
        final RspBitmap rb = new RspBitmap();
        final long[] ks = {0, 1 << 15, 1 << 16, 2 * (1 << 16) - 2};
        rb.addValues(ks);
        for (long k : ks) {
            assertTrue(rb.contains(k));
        }
        final RspArray ra = rb.getKvs();
        assertEquals(2, ra.getSize());
        assertContainerAt(0, rb);
        assertContainerAt(1, rb);
        rb.addRangeUnsafeNoWriteCheck(1, (1 << 15) - 1);
        rb.addRangeUnsafeNoWriteCheck((1 << 15) + 1, (1 << 16) - 1);
        assertEquals(2, ra.getSize());
        assertFullBlocksSpanAt(0, rb, 1);
        assertContainerAt(1, rb);
        rb.addRangeUnsafeNoWriteCheck((1 << 16) + 1, 2 * (1 << 16) - 2 - 1);
        assertEquals(2, ra.getSize());
        assertFullBlocksSpanAt(0, rb, 1);
        assertContainerAt(1, rb);
        assertEquals((1 << 16) - 1, getContainerCardinality(1, rb));
        rb.add(2 * (1 << 16) - 1);
        assertEquals(1, ra.getSize());
        assertFullBlocksSpanAt(0, rb, 2);
    }

    @Test
    public void testMergeFullBlockSpans() {
        final int tgtflen = 10;
        final int[] blocks = new int[tgtflen - 2];
        for (int i = 1; i <= tgtflen - 2; ++i) {
            blocks[i - 1] = i;
        }
        Shuffle.shuffleArray(new Random(seed0), blocks);
        final RspBitmap rb = new RspBitmap();
        // Initial population: "almost full" single blocks.
        for (int bi = 0; bi < tgtflen; ++bi) {
            final long blockOffset = (1 << 16) * bi;
            rb.addRangeUnsafeNoWriteCheck(blockOffset + 1, blockOffset + BLOCK_SIZE - 1);
        }
        rb.finishMutations();
        assertEquals(tgtflen, rb.getKvs().getSize());
        for (int i = 0; i < tgtflen; ++i) {
            assertContainerAt(i, rb);
            assertEquals(BLOCK_SIZE - 1, getContainerCardinality(i, rb));
        }

        // For each block except first and last, add the single missing element that would make it full.
        // Follow the order in the randomly shuffled blocks array.
        for (int i = 0; i < tgtflen - 2; ++i) {
            final int bi = blocks[i];
            final long blockOffset = (1 << 16) * bi;
            rb.addUnsafe(blockOffset);
        }
        rb.finishMutations();
        // We should have 3 spans now.
        assertEquals(3, rb.getKvs().getSize());
        assertContainerAt(0, rb);
        assertFullBlocksSpanAt(1, rb, tgtflen - 2);
        assertContainerAt(2, rb);

        // Add the last 2 missing elements for a single, full blocks span of tgtflen.
        rb.addUnsafe(0);
        rb.addUnsafe((1 << 16) * (tgtflen - 1));
        rb.finishMutations();
        assertEquals(1, rb.getKvs().getSize());
        assertFullBlocksSpanAt(0, rb, tgtflen);
        rb.validate();
    }

    @Test
    public void testFillIntervalOneElementAtATime() {
        final int tgtflen = 3;
        final int elems[] = new int[tgtflen * BLOCK_SIZE];
        for (int i = 0; i < elems.length; ++i) {
            elems[i] = i;
        }
        Shuffle.shuffleArray(new Random(seed0), elems);
        final RspBitmap rb = new RspBitmap();
        for (int i = 0; i < elems.length; ++i) {
            rb.add(elems[i]);
            final String m = "i=" + i;
            assertEquals(m, i + 1, rb.getCardinality());
            rb.validate(m);
        }
        assertEquals(1, rb.getKvs().getSize());
        assertFullBlocksSpanAt(0, rb, tgtflen);
    }

    @Test
    public void testSimpleRemoveFromContainer() {
        final RspBitmap rb = new RspBitmap();
        final int n = 3;
        for (int i = 0; i < n; ++i) {
            final long start = i * BLOCK_SIZE;
            final long end = start + RspArray.BLOCK_LAST;
            rb.addRangeUnsafeNoWriteCheck(start, end - 1);
        }
        rb.finishMutations();
        final long startCardinality = (BLOCK_SIZE - 1) * n;
        assertEquals(startCardinality, rb.getCardinality());
        for (int i = 0; i < n; ++i) {
            rb.remove(i);
            final String m = "i=" + i;
            assertEquals(m, startCardinality - 1 - i, rb.getCardinality());
            assertEquals(m, n, rb.getKvs().getSize());
            assertFalse(m, rb.contains(i));
        }
    }

    @Test
    public void testRangeSpanningMultipleBlocks() {
        final int tgtflen = 20;
        final long startCardinality = tgtflen * BLOCK_SIZE;
        final RspBitmap rb = new RspBitmap();
        rb.addRange(0, startCardinality - 1);
        assertEquals(1, rb.getKvs().getSize());
        assertEquals(startCardinality, rb.getCardinality());
    }

    @Test
    public void testRemoveFromFullSpans() {
        final int tgtflen = 20;
        final int elems[] = new int[tgtflen];
        for (int i = 0; i < elems.length; ++i) {
            elems[i] = 5 + i * BLOCK_SIZE;
        }
        Shuffle.shuffleArray(new Random(seed0), elems);
        final RspBitmap rb = new RspBitmap();
        final long startCardinality = tgtflen * BLOCK_SIZE;
        rb.addRange(0, startCardinality - 1);
        int currSize = rb.getKvs().getSize();
        assertEquals(1, currSize);
        assertEquals(startCardinality, rb.getCardinality());
        for (int i = 0; i < elems.length; ++i) {
            rb.remove(elems[i]);
            final String m = "i=" + i;
            assertEquals(m, startCardinality - 1 - i, rb.getCardinality());
            rb.validate(m);
            final int newSize = rb.getKvs().getSize();
            if (i == 0) {
                assertTrue(m, newSize > currSize);
            } else {
                assertTrue(m, newSize >= currSize);
            }
            currSize = newSize;
        }
        assertEquals(tgtflen, currSize);
    }

    static long blockStart(final int bi) {
        return bi * BLOCK_SIZE;
    }

    static long blockEnd(final int bi) { // exclusive
        return (bi + 1) * BLOCK_SIZE;
    }

    static class RandomSpansSomeFullSomeAlmostFull {
        public final int[] full;
        public int nfull;
        public final int[] almost;
        public int nalmost;
        public final RspBitmap rb;

        public RandomSpansSomeFullSomeAlmostFull(final int nblocks, final Random r, final int firstBlock) {
            this(nblocks, r, firstBlock, 0, null);
        }

        public RandomSpansSomeFullSomeAlmostFull(
                final int nblocks, final Random r,
                final int firstBlock, final int nEmptyBlocks,
                final TLongSet set) {
            full = new int[nblocks];
            nfull = 0;
            almost = new int[nblocks];
            nalmost = 0;
            int nempty = 0;
            for (int i = 0; i < nblocks + nempty; ++i) {
                if (nempty < nEmptyBlocks && r.nextInt(nblocks + nempty) < nempty) {
                    // empty;
                    ++nempty;
                    continue;
                }
                if (r.nextBoolean()) {
                    full[nfull++] = i;
                } else {
                    almost[nalmost++] = i;
                }
            }
            rb = new RspBitmap();
            for (int i = 0; i < nfull; ++i) {
                final long blockStart = blockStart(firstBlock + full[i]);
                final long blockEnd = blockStart + BLOCK_SIZE; // exclusive
                rb.addRange(blockStart, blockEnd - 1);
                assertEquals("i=" + i,
                        (i + 1) * BLOCK_SIZE, rb.getCardinality());
                assertEquals(i + 1, rb.getKvs().getFullBlocksCount());
                if (set != null) {
                    for (long vv = blockStart; vv < blockEnd; ++vv) {
                        set.add(vv);
                    }
                }
            }
            for (int i = 0; i < nalmost; ++i) {
                final long blockStart = blockStart(firstBlock + almost[i]);
                final long blockEnd = blockStart + BLOCK_SIZE; // exclusive
                final long bs;
                final long be;
                if (r.nextBoolean()) {
                    bs = blockStart;
                    be = blockEnd - 1;
                } else {
                    bs = blockStart + 1;
                    be = blockEnd;
                }
                rb.addRange(bs, be - 1);
                if (set != null) {
                    for (long vv = bs; vv < be; ++vv) {
                        set.add(vv);
                    }
                }
                assertEquals("i=" + i,
                        nfull * BLOCK_SIZE + (i + 1) * (BLOCK_SIZE - 1),
                        rb.getCardinality());
                assertEquals(nfull, rb.getKvs().getFullBlocksCount());
            }
        }

        public RandomSpansSomeFullSomeAlmostFull(final int nblocks, final Random r) {
            this(nblocks, r, 0);
        }
    }

    public static void randomizedTest(final IntConsumer doTestFun) {
        randomizedTest(doTestFun, false);
    }

    public static void randomizedTest(final IntConsumer doTestFun, final boolean printSeed) {
        for (int seed = seed0; seed < seed0 + runs; ++seed) {
            if (printSeed) {
                System.out.println("seed=" + seed);
            }
            doTestFun.accept(seed);
        }
    }

    // To help with debugging when a particular seed value has uncovered an error.
    public static void randomizedTest(final IntConsumer doTestFun, final int seed) {
        doTestFun.accept(seed);
    }

    @Test
    public void testAppend() {
        randomizedTest(RspBitmapTest::doTestAppend);
    }

    private static void doTestAppend(final int seed) {
        final Random r = new Random(seed);
        final String m = "seed=" + seed;
        final int count = 1200000;
        final long[] rangeStart = new long[count];
        final long[] rangeEnd = new long[count];
        final RspBitmap rb = new RspBitmap();
        long last = -1;
        int nRanges = 0;
        long prevCard = 0;
        for (int i = 0; i < count; ++i) {
            final long s = last + 1 + r.nextInt(2 * BLOCK_SIZE);
            final long d;
            if (r.nextFloat() < 0.2) {
                d = r.nextInt(3 * BLOCK_SIZE);
            } else {
                d = r.nextInt(BLOCK_SIZE / 4);
            }
            final long e = s + d;
            rb.appendRange(s, e);
            final String m2 = m + ", s=" + s + ", e=" + e;
            final long card = rb.getCardinality();
            assertEquals(m2, prevCard + e - s + 1, card);
            prevCard = card;
            if (nRanges == 0 || rangeEnd[nRanges - 1] + 1 != s) {
                rangeStart[nRanges] = s;
                rangeEnd[nRanges] = e;
                ++nRanges;
            } else {
                rangeEnd[nRanges - 1] = e;
            }
            last = e;
        }
        rb.validate();
        final RspRangeIterator it = rb.getRangeIterator();
        int i = 0;
        last = -2;
        while (it.hasNext()) {
            it.next();
            final String m2 = m + ", i=" + i;
            final long s = it.start();
            assertTrue(m2, last + 1 < s);
            final long e = it.end();
            long rs = rangeStart[i];
            long re = rangeEnd[i];
            ++i;
            final String m3 = m2 + ", s=" + s + ", e=" + e + ", rs=" + rs + ", re=" + re;
            assertEquals(m3, rs, s);
            assertEquals(m3, re, e);
            last = e;
        }
        assertEquals(m, i, nRanges);
    }

    @Test
    public void testAppendRange2() {
        // Ensure multi block ranges work.
        final RspBitmap rb = new RspBitmap();
        rb.append(100 * BLOCK_SIZE); // ensure we are not appending in what follows.
        final long start = BLOCK_SIZE + 4;
        final long end = 3 * BLOCK_SIZE - 1;
        try {
            rb.appendRange(start, end); // end at block last.
            fail("Exception expected");
        } catch (IllegalArgumentException expected) {
        }
    }


    @Test
    public void testStressConversionBetweenSpanTypes() {
        randomizedTest(RspBitmapTest::doTestStressConversionBetweenSpanTypes);
    }

    private static void doTestStressConversionBetweenSpanTypes(final int seed) {
        final Random r = new Random(seed);
        final RandomSpansSomeFullSomeAlmostFull ss = new RandomSpansSomeFullSomeAlmostFull(60, r);
        long cardinality = ss.rb.getCardinality();
        final String m = "seed=" + seed;
        for (int t = 0; t < 1000; ++t) {
            final String m2 = m + ", t=" + t;
            if (r.nextBoolean()) {
                // Convert a full to almost.
                if (ss.nfull == 0) {
                    continue;
                }
                final int i = r.nextInt(ss.nfull);
                final long valToRemove;
                if (r.nextBoolean()) {
                    valToRemove = blockStart(ss.full[i]);
                } else {
                    valToRemove = blockEnd(ss.full[i]) - 1;
                }
                ss.rb.remove(valToRemove);
                assertEquals(m2, cardinality - 1, ss.rb.getCardinality());
                --cardinality;
                ss.almost[ss.nalmost++] = ss.full[i];
                ss.full[i] = ss.full[ss.nfull - 1];
                --ss.nfull;
            } else {
                // Convert an almost to full.
                if (ss.nalmost == 0) {
                    continue;
                }
                final int i = r.nextInt(ss.nalmost);
                final long blockFirst = blockStart(ss.almost[i]);
                final long blockLast = blockFirst + BLOCK_SIZE - 1;
                if (!ss.rb.contains(blockFirst)) {
                    ss.rb.add(blockFirst);
                } else if (!ss.rb.contains(blockLast)) {
                    ss.rb.add(blockLast);
                } else {
                    fail(m + ", i=" + i);
                }
                assertEquals(m2, cardinality + 1, ss.rb.getCardinality());
                ++cardinality;
                ss.full[ss.nfull++] = ss.almost[i];
                ss.almost[i] = ss.almost[ss.nalmost - 1];
                --ss.nalmost;
            }
            assertEquals(m, ss.nfull, ss.rb.getKvs().getFullBlocksCount());
        }
        ss.rb.validate();
    }

    private static RspBitmap getRandomSpansSomeFullSomeAlmostFull(final int nblocks, final Random r) {
        final RandomSpansSomeFullSomeAlmostFull ss = new RandomSpansSomeFullSomeAlmostFull(nblocks, r);
        return ss.rb;
    }

    @Test
    public void testSimpleIterator() {
        final RspBitmap rb = new RspBitmap();
        assertTrue(rb.isEmpty());
        final long k = new Random(seed0).nextLong();
        rb.add(k);
        assertTrue(rb.contains(k));
        assertEquals(1, rb.getCardinality());
        assertFalse(rb.isEmpty());
        {
            final RspRangeIterator rit = rb.getRangeIterator();
            assertTrue(rit.hasNext());
            rit.next();
            final long s = rit.start();
            final long e = rit.end();
            assertEquals(k, s);
            assertEquals(k, e);
            assertFalse(rit.hasNext());
        }
        rb.remove(k);
        assertFalse(rb.contains(k));
        assertEquals(0, rb.getCardinality());
        assertTrue(rb.isEmpty());
        final RspRangeIterator rit = rb.getRangeIterator();
        assertTrue(!rit.hasNext());
    }

    @Test
    public void testSimpleReverseIterator() {
        final RspBitmap rb = new RspBitmap();
        assertTrue(rb.isEmpty());
        final long k = new Random(seed0).nextLong();
        rb.add(k);
        assertTrue(rb.contains(k));
        assertEquals(1, rb.getCardinality());
        assertFalse(rb.isEmpty());
        {
            final RspReverseIterator rit = rb.getReverseIterator();
            assertTrue(rit.hasNext());
            rit.next();
            final long v = rit.current();
            assertEquals(k, v);
            assertFalse(rit.hasNext());
        }
        rb.remove(k);
        assertFalse(rb.contains(k));
        assertEquals(0, rb.getCardinality());
        assertTrue(rb.isEmpty());
        final RspReverseIterator rit = rb.getReverseIterator();
        assertTrue(!rit.hasNext());
    }

    private static RspBitmap getRandomRspBitmap(final int nblocks, final Random r, final float threshold,
            int toRemoveBound) {
        return getRandomRspBitmap(nblocks, r, threshold, toRemoveBound, 0, 0, null);
    }

    private static RspBitmap getRandomRspBitmap(
            final int nblocks, final Random r, final float threshold, final int toRemoveBound,
            final int firstBlock, final int nEmpty) {
        return getRandomRspBitmap(nblocks, r, threshold, toRemoveBound, firstBlock, nEmpty, null);
    }

    private static RspBitmap getRandomRspBitmap(
            final int nblocks, final Random r, final float threshold, final int toRemoveBound,
            final int firstBlock, final int nEmpty, final TLongSet set) {
        final RandomSpansSomeFullSomeAlmostFull ss =
                new RandomSpansSomeFullSomeAlmostFull(nblocks, r, firstBlock, nEmpty, set);
        final RspBitmap rb = ss.rb;
        // take some almost full blocks and remove a few elements from each as to create more ranges on each.
        for (int i = 0; i < ss.nalmost; ++i) {
            final boolean removeFromThisOne = r.nextFloat() < threshold;
            if (!removeFromThisOne) {
                continue;
            }
            final long ki = blockStart(firstBlock + ss.almost[i]);
            final int nToRemove = 1 + r.nextInt(toRemoveBound);
            for (int j = 0; j < nToRemove; ++j) {
                final int shortToRemove = r.nextInt(BLOCK_SIZE);
                final long toRemove = ki | (long) shortToRemove;
                rb.removeUnsafe(toRemove);
                if (set != null) {
                    set.remove(toRemove);
                }
            }
            rb.finishMutations();
        }
        return rb;
    }

    @Test
    public void testRangeIterator() {
        randomizedTest(RspBitmapTest::doTestRangeIterator);
    }

    private static void checkRangeIterator(final RspBitmap rb, final String m) {
        final long cardinality = rb.getCardinality();
        final RspRangeIterator rit = rb.getRangeIterator();
        long prevEnd = 0;
        int nrange = 0;
        long cardAcc = 0;
        while (rit.hasNext()) {
            rit.next();
            final long start = rit.start();
            final long end = rit.end();
            final String m2 = m + ", nrange=" + nrange;
            if (nrange > 0) {
                assertTrue(m2 + ", start=" + start + ", prevEnd=" + prevEnd, start - prevEnd > 1);
                for (long v = prevEnd + 1; v < start; ++v) {
                    assertFalse(rb.contains(v));
                }
            }
            for (long v = start; v <= end; ++v) {
                assertTrue(m2, rb.contains(v));
            }
            cardAcc += end - start + 1;
            prevEnd = end;
            ++nrange;
        }
        assertEquals(cardinality, cardAcc);
    }

    private static void doTestRangeIterator(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 60;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.75f, 3);
        final String m = "seed=" + seed;
        checkRangeIterator(rb, m);
    }

    @Test
    public void testIterator() {
        randomizedTest(RspBitmapTest::doTestIterator);
    }

    private static void checkIterator(final RspBitmap rb, final String m) {
        final long cardinality = rb.getCardinality();
        final RspIterator rit = rb.getIterator();
        long prev = -1;
        int n = 0;
        while (rit.hasNext()) {
            final long v = rit.nextLong();
            final String m2 = m + ", n=" + n + ", v=" + v;
            if (n > 0) {
                assertTrue(m2 + ", prev=" + prev + ", v=" + v, prev < v);
            }
            assertTrue(m2, rb.contains(v));
            prev = v;
            ++n;
        }
        assertEquals(cardinality, n);
    }

    private static void doTestIterator(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 60;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.75f, 3);
        final String m = "seed=" + seed;
        checkIterator(rb, m);
    }

    @Test
    public void testIteratorAdvance() {
        randomizedTest(RspBitmapTest::doTestIteratorAdvance);
    }

    private static void doTestIteratorAdvance(final int seed) {
        final Random r = new Random(seed + 2);
        final int nblocks = 400;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 1 / 2.0f, 3);
        final long lastValue = rb.last();
        final RspRangeIterator rit1 = rb.getRangeIterator();
        final RspRangeIterator rit2 = rb.getRangeIterator();
        final float pAdvance = 0.05f;
        int range = 0;
        final String m = "seed=" + seed;
        while (rit1.hasNext()) {
            final String m2 = m + ", range=" + range;
            rit1.next();
            final boolean doAdvance = r.nextFloat() < pAdvance;
            if (!doAdvance) {
                continue;
            }
            final int choice = r.nextInt(5);
            final long vAdvance;
            if (choice == 0) {
                vAdvance = rit1.end();
            } else if (choice == 1 && rit1.end() - rit1.start() > 1) {
                vAdvance = rit1.start() + 1;
            } else if (choice == 2 && rit1.end() - rit1.start() > 1) {
                vAdvance = rit1.end() - 1;
            } else if (choice == 3 && rit1.end() - rit1.start() > 1) {
                vAdvance = (rit1.start() + rit1.end()) / 2;
            } else {
                vAdvance = rit1.start();
            }
            final boolean valid = rit2.advance(vAdvance);
            assertEquals(m2, vAdvance <= lastValue, valid);
            if (valid) {
                assertTrue(m2, rit2.end() >= vAdvance);
            }
            ++range;
        }
        assertFalse(m, rit2.advance(lastValue + 1));
        assertFalse(m, rit2.hasNext());
    }

    @Test
    public void testReverseIteratorAdvance() {
        randomizedTest(RspBitmapTest::doTestReverseIteratorAdvance);
    }

    private static void doTestReverseIteratorAdvance(final int seed) {
        final Random r = new Random(seed + 2);
        final int npasses = 40;
        for (int pass = 0; pass < npasses; ++pass) {
            doTestReverseIteratorAdvanceOnePass(seed, pass, r);
        }
    }

    private static void doTestReverseIteratorAdvanceOnePass(final int seed, final int passIdx, final Random r) {
        final int nblocks = 50;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 1 / 2.0f, 3);
        final RspRangeIterator forwardRangeIter = rb.getRangeIterator();
        final String m = "seed==" + seed + ", passIdx=" + passIdx;
        long prevLast = -1;
        final double pInternal = 0.01;
        boolean prevAtBlockBoundary = true;
        while (forwardRangeIter.hasNext()) {
            forwardRangeIter.next();
            final long start = forwardRangeIter.start();
            final long end = forwardRangeIter.end();
            for (long v = start - 1; v <= end + 1; ++v) {
                if (v < 0) {
                    continue;
                }
                if (!prevAtBlockBoundary && r.nextDouble() > pInternal) {
                    continue;
                }
                final long blockMod = v & BLOCK_LAST;
                prevAtBlockBoundary =
                        (blockMod == 0 || blockMod == 1 || blockMod == (BLOCK_LAST - 1) || blockMod == BLOCK_LAST);
                final String m2 = m + " && v==" + v;
                final RspReverseIterator reverseIter = rb.getReverseIterator();
                final boolean valid = reverseIter.advance(v);
                if (start <= v && v <= end) {
                    assertTrue(m2, valid);
                    assertEquals(m2, v, reverseIter.current());
                } else if (v == start - 1) {
                    if (prevLast == -1) {
                        assertFalse(m2, valid);
                    } else {
                        assertTrue(m2, valid);
                        assertEquals(m2, prevLast, reverseIter.current());
                    }
                } else { // v == end + 1
                    assertTrue(m2, valid);
                    assertEquals(m2, end, reverseIter.current());
                }
            }
            prevLast = end;
        }
    }

    @Test
    public void testReverseIteratorAdvanceCases() {
        RspBitmap rb = new RspBitmap();
        RspReverseIterator reverseIter = rb.getReverseIterator();
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());
        rb.addRange(3, 4);
        reverseIter = rb.getReverseIterator();
        assertTrue(reverseIter.hasNext());
        reverseIter.next();
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());
        reverseIter = rb.getReverseIterator();
        assertTrue(reverseIter.hasNext());
        reverseIter.next();
        assertTrue(reverseIter.hasNext());
        reverseIter.next();
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());

        rb = new RspBitmap(2, 2);
        reverseIter = rb.getReverseIterator();
        assertTrue(reverseIter.hasNext());
        reverseIter.next();
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());

        rb = new RspBitmap(BLOCK_SIZE, BLOCK_SIZE + 1);
        rb.add(2 * BLOCK_SIZE);
        reverseIter = rb.getReverseIterator();
        assertTrue(reverseIter.hasNext());
        assertFalse(reverseIter.advance(1));
        assertFalse(reverseIter.hasNext());

        rb = new RspBitmap(2 * BLOCK_SIZE - 2, 2 * BLOCK_SIZE - 1);
        rb.addRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE);
        reverseIter = rb.getReverseIterator();
        assertTrue(reverseIter.advance(2 * BLOCK_SIZE - 2));
        assertFalse(reverseIter.hasNext());

        rb = new RspBitmap();
        rb.add(BLOCK_LAST - 1);
        rb.add(2 * BLOCK_SIZE);
        reverseIter = rb.getReverseIterator();
        assertTrue(reverseIter.advance(BLOCK_LAST));
        assertEquals(BLOCK_LAST - 1, reverseIter.current());
        assertFalse(reverseIter.hasNext());
    }

    private static void forceContainerType(final RspBitmap rb, final String containerName, final int safeRangeStart,
            final long safeRangeEnd) {
        switch (containerName) {
            case "bitmap": {
                for (int v = safeRangeStart; v <= safeRangeEnd; v += 2) {
                    rb.add(v);
                }
            }
                break;
            case "run": {
                rb.addRange(safeRangeStart, safeRangeEnd);
            }
                break;
            default:
                return;
        }
    }

    private static void checkReverseIteratorAdvance(final RspBitmap rb, final long[] alls, final String m) {
        for (long v : alls) {
            final RspReverseIterator rit = rb.getReverseIterator();
            final RspBitmap chopped = rb.subrangeByValue(0, v);
            final boolean valid = rit.advance(v);
            final String m2 = m + " && m.hashCode()==" + m.hashCode() + " && v==" + v;
            assertEquals(m2, !chopped.isEmpty(), valid);
            if (valid) {
                assertEquals(m2, chopped.last(), rit.current());
                assertEquals(m2, chopped.getCardinality() > 1, rit.hasNext());
                if (rit.hasNext()) {
                    rit.next();
                    final RspBitmap reChopped = chopped.subrangeByValue(0, chopped.last() - 1);
                    assertEquals(m2, reChopped.last(), rit.current());
                }
            } else {
                if (!rb.isEmpty()) {
                    assertEquals(m2, rb.first(), rit.current());
                }
            }
        }
    }

    private static void checkRangeIteratorAdvance(final RspBitmap rb, final long[] alls, final String m) {
        for (long v : alls) {
            final String m2 = m + " && m.hashCode()==" + m.hashCode() + " && v==" + v;
            final RspRangeIterator rit = rb.getRangeIterator();
            final RspBitmap chopped = rb.subrangeByValue(v, Long.MAX_VALUE);
            final boolean valid = rit.advance(v);
            assertEquals(m2, !chopped.isEmpty(), valid);
            if (valid) {
                final RspRangeIterator choppedIter = chopped.getRangeIterator();
                assertTrue(choppedIter.hasNext());
                choppedIter.next();
                assertEquals(m2, choppedIter.start(), rit.start());
                assertEquals(m2, choppedIter.end(), rit.end());
                assertEquals(choppedIter.hasNext(), rit.hasNext());
                if (rit.hasNext()) {
                    rit.next();
                    choppedIter.next();
                    assertEquals(m2, choppedIter.start(), rit.start());
                    assertEquals(m2, choppedIter.end(), rit.end());
                }
            }
        }
    }

    @Test
    public void testIteratorAdvanceBlockBoundaries() {
        final long[] b0s = new long[] {BLOCK_LAST - 4, BLOCK_LAST - 3, BLOCK_LAST - 2, BLOCK_LAST - 1, BLOCK_LAST};
        final long[] b1s = new long[] {BLOCK_SIZE, BLOCK_SIZE + 1, BLOCK_SIZE + 2, BLOCK_SIZE + 3, BLOCK_SIZE + 4};
        final long[] alls = new long[b0s.length + b1s.length];
        System.arraycopy(b0s, 0, alls, 0, b0s.length);
        System.arraycopy(b1s, 0, alls, b0s.length, b1s.length);
        final int b0bits = 1 << b0s.length;
        final int b1bits = 1 << b1s.length;
        final String[] containerNames = {"run", "array", "bitmap", "full"};
        for (String b0ContainerName : containerNames) {
            for (String b1ContainerName : containerNames) {
                for (int b0 = 0; b0 <= b0bits; ++b0) {
                    if (b0ContainerName.charAt(0) == 'f') {
                        if (b0 != b0bits) {
                            continue;
                        }
                    } else if (b0 == b0bits) {
                        continue;
                    }
                    for (int b1 = 0; b1 <= b1bits; ++b1) {
                        if (b1ContainerName.charAt(0) == 'f') {
                            if (b1 != b1bits) {
                                continue;
                            }
                        } else if (b1 == b1bits) {
                            continue;
                        }
                        final RspBitmap rb = new RspBitmap();
                        if (b0 < b0bits) {
                            forceContainerType(rb, b0ContainerName, 0, BLOCK_LAST - 6);
                            for (int i = 0; i < b0s.length; ++i) {
                                if ((b0 & (1 << i)) != 0) {
                                    rb.add(b0s[i]);
                                }
                            }
                        } else {
                            rb.addRange(0, BLOCK_LAST);
                        }
                        if (b1 < b1bits) {
                            forceContainerType(rb, b1ContainerName, BLOCK_SIZE + 6, BLOCK_SIZE + BLOCK_LAST);
                            for (int i = 0; i < b1s.length; ++i) {
                                if ((b1 & (1 << i)) != 0) {
                                    rb.add(b1s[i]);
                                }
                            }
                        } else {
                            rb.addRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
                        }
                        final String m =
                                "b0ContainerName.charAt(0)=='" + b0ContainerName.charAt(0) +
                                        "' && b1ContainerName.charAt(0)=='" + b1ContainerName.charAt(0) +
                                        "' && b0==" + b0 + " && b1==" + b1;
                        checkReverseIteratorAdvance(rb, alls, m);
                        checkRangeIteratorAdvance(rb, alls, m);
                    }
                }
            }
        }

    }

    @Test
    public void testReverseIteratorCases() {
        RspBitmap rb = new RspBitmap(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        RspReverseIterator reverseIter = rb.getReverseIterator();
        for (long v = BLOCK_SIZE + BLOCK_LAST; v >= BLOCK_SIZE; --v) {
            assertTrue("v==" + v, reverseIter.hasNext());
            reverseIter.next();
            assertEquals(v, reverseIter.current());
        }
        assertFalse(reverseIter.hasNext());
    }

    @Test
    public void testSearchIteratorSingles() {
        for (int inc = 2; inc <= 8; inc += 2) {
            final RspBitmap rb = new RspBitmap();
            for (int i = 1; i < 2000; i += inc) {
                for (int j = i; j <= i + inc - 2; ++j) {
                    rb.add(j);
                }
            }
            final RspRangeIterator it = rb.getRangeIterator();
            assertTrue(it.hasNext());
            it.next();
            for (long k = 500L; k < 1500L; ++k) {
                final String msg = "inc == " + inc + " && k == " + k;
                final long finalK = k;
                long expected = k;
                while (!rb.contains(expected))
                    --expected;
                final RowSetUtils.Comparator tc0 = (rKey) -> Long.compare(finalK, rKey);
                it.search(tc0);
                final long r0 = it.start();
                assertEquals(msg, expected, r0);
            }
        }
    }

    @Test
    public void testSearchIteratorRandom() {
        randomizedTest(RspBitmapTest::doTestSearchIteratorRandom);
    }

    private static void doTestSearchIteratorRandom(final int seed) {
        final int count = 10000;
        final TLongSet set = ValidationSet.make(count);
        final String pfxMsg = "doTestSearchIteratorSingle seed == " + seed;
        final Random r = new Random(seed);
        final RspBitmap rb = populateRandom(pfxMsg, r, count, 1, 10000, 150, 50, set, true);
        assertEquals(set.size(), rb.getCardinality());
        final long[] arr = new long[set.size()];
        set.toArray(arr);
        Arrays.sort(arr);
        {
            long min = rb.first() - 1;
            long max = rb.last() + 2;
            final RspRangeIterator it = rb.getRangeIterator();
            assertTrue(it.hasNext());
            it.next();
            for (long v = min; v <= max; ++v) {
                final long finalV = v;
                final boolean contains = set.contains(v);
                final int i = Arrays.binarySearch(arr, v);
                final long expected;
                if (i < 0) {
                    assertFalse(contains);
                    final int j = -i - 1;
                    expected = arr[Math.max(j - 1, 0)];
                } else {
                    assertEquals(arr[i], v);
                    expected = v;
                }
                final RowSetUtils.Comparator tc0 = (rKey) -> Long.compare(finalV, rKey);
                it.search(tc0);
                final long res = it.start();
                assertEquals(pfxMsg + " && v == " + v, expected, res);
            }
        }
        {
            for (int i = 0; i < arr.length - 1; ++i) {
                final String msg = pfxMsg + " && i == " + i;
                int j = i + 1;
                final long vi = arr[i];
                final long vj = arr[j];
                final long vjm1 = vj - 1;
                final RspRangeIterator it0 = rb.getRangeIterator();
                assertTrue(it0.hasNext());
                it0.next();
                final RowSetUtils.Comparator tc0 = (rKey) -> Long.compare(vjm1, rKey);
                it0.search(tc0);
                final long r0 = it0.start();
                assertEquals(msg, vi, r0);
            }
        }
    }

    @Test
    public void testIteratorAdvanceForCoverage() {
        final RspBitmap rb = new RspBitmap();
        rb.addRangeExclusiveEnd(0, BLOCK_LAST);
        rb.addRangeExclusiveEnd(BLOCK_SIZE + 1, BLOCK_SIZE + 5);
        rb.addRangeExclusiveEnd(BLOCK_SIZE + 10, BLOCK_SIZE + 25);
        RspRangeIterator rit = rb.getRangeIterator();
        assertTrue(rit.hasNext());
        rit.next();
        boolean r = rit.advance(BLOCK_SIZE + 26);
        assertFalse(r);
        assertFalse(rit.hasNext());
        rb.addRangeExclusiveEnd(2 * BLOCK_SIZE, 3 * BLOCK_SIZE);
        rit = rb.getRangeIterator();
        assertTrue(rit.hasNext());
        rit.next();
        r = rit.advance(BLOCK_SIZE + 26);
        assertTrue(r);
        assertEquals(2 * BLOCK_SIZE, rit.start());
        assertEquals(3 * BLOCK_SIZE - 1, rit.end());
        rit = rb.getRangeIterator();
        r = rit.advance(3 * BLOCK_SIZE);
        assertFalse(r);
        assertFalse(rit.hasNext());
    }

    @Test
    public void testEmptyGet() {
        final RspBitmap rb = new RspBitmap();
        assertEquals(-1, rb.get(900));
    }

    @Test
    public void testEmptyFind() {
        final RspBitmap rb = new RspBitmap();
        assertEquals(-1, rb.find(900));
    }

    @Test
    public void testGetFind() {
        randomizedTest(RspBitmapTest::doTestGetFind);
    }

    private static void doTestGetFind(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 60;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.95f, 2000);
        final RspRangeIterator rit = rb.getRangeIterator();
        long c = 0;
        long prevEnd = -1;
        int ri = 0;
        final String m = "seed=" + seed;
        while (rit.hasNext()) {
            rit.next();
            final String m2 = m + ", ri=" + ri;
            final long s = rit.start();
            final long e = rit.end();
            final long rc = e - s + 1;
            for (long pos = c; pos < c + rc; ++pos) {
                final long v = rb.get(pos);
                assertEquals(m2 + ",s=" + s + ", pos=" + pos, s + pos - c, v);
            }
            for (long v = prevEnd + 1; v < s; ++v) {
                final long p = rb.find(v);
                assertEquals(m2 + ",v=" + v, -c - 1, p);
            }
            for (long v = s; v <= e; ++v) {
                final long p = rb.find(v);
                assertEquals(m2 + ",v=" + v, c + v - s, p);
            }
            c += rc;
            prevEnd = e;
            ++ri;
        }
        assertEquals(-1, rb.get(c));
    }

    @Test
    public void testGetFind2() {
        randomizedTest(RspBitmapTest::doTestGetFind2);
    }

    private static void doTestGetFind2(final int seed) {
        final int count = 100000;
        final TLongSet set = ValidationSet.make(count);
        final String pfxMsg = "doTestGetFind2 seed == " + seed;
        final RspBitmap rb = populateRandom(pfxMsg, new Random(seed), count, 10, 30000000, 150, 50, set, true);
        assertEquals(set.size(), rb.getCardinality());
        rb.validate();
        final long[] arr = new long[set.size()];
        set.toArray(arr);
        Arrays.sort(arr);

        for (int i = 0; i < arr.length; ++i) {
            long vi = arr[i];
            final long gi = rb.get(i);
            assertEquals(pfxMsg + " && i==" + i, vi, gi);
        }
        assertEquals(-1, rb.get(arr.length));

        assertEquals(-1, rb.find(arr[0] - 1));
        for (int i = 0; i < arr.length - 1; ++i) {
            int j = i + 1;
            long vi = arr[i];
            final long posi = rb.find(vi);
            assertEquals(pfxMsg + " && i==" + i, i, posi);
            long vj = arr[j];
            for (long v = vi + 1; v < vj; ++v) {
                final long pos = rb.find(v);
                assertEquals(pfxMsg + " && i==" + i + " && v==" + v, -(i + 1) - 1, pos);
            }
        }
        assertEquals(arr.length - 1, rb.find(arr[arr.length - 1]));
        assertEquals(-arr.length - 1, rb.find(arr[arr.length - 1] + 1));
    }

    @Test
    public void testGetFind3() {
        final RspBitmap rb = new RspBitmap();
        rb.add(1);
        rb.addRange(BLOCK_SIZE, BLOCK_SIZE + 3);
        rb.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE - 1);
        rb.add(3 * BLOCK_SIZE + 10);
        final long card = rb.getCardinality();
        for (long i = 0; i < card; ++i) {
            final long v = rb.get(i);
            final long ii = rb.find(v);
            assertEquals("i==" + i, i, ii);
        }
        assertEquals(-1, rb.get(card));
        assertEquals(-1, rb.get(card + 1));

        rb.addRange(4 * BLOCK_SIZE, 6 * BLOCK_SIZE - 1);
        assertEquals(-1, rb.get(rb.getCardinality()));
        assertEquals(-1, rb.get(rb.getCardinality() + 1));

        rb.removeRange(0, 8 * BLOCK_SIZE - 1);
        assertEquals(-1, rb.get(0));
        assertEquals(-1, rb.get(1));
    }

    @Test
    public void testRemoveRange() {
        randomizedTest(RspBitmapTest::doTestRemoveRange);
    }

    private static void doTestRemoveRange(final int seed) {
        final String m = "seed==" + seed;
        final Random r = new Random(seed);
        final int nblocks = 6;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 2000);
        final long rbc = rb.getCardinality();
        for (long i = 0, j = rbc - 1; i < j; ++i, --j) {
            final String m2 = m + " && i==" + i + " && j==" + j;
            final long start = rb.get(i);
            int si = rb.getKvs().getSpanIndex(start);
            if (si < 0) {
                si = -si - 1;
            }
            if (rb.getKvs().getSpanCardinalityAtIndex(si) > 0) {
                if (r.nextFloat() > 1.0f / 20) { // skip most elements of a full block span.
                    continue;
                }
            }
            final long end = rb.get(j);
            final RspBitmap rbCopy = rb.deepCopy();
            rbCopy.removeRange(start, end);
            rbCopy.validate(m2);
            final long expectedCopyCard = rbc - (j - i + 1);
            assertEquals(m2, expectedCopyCard, rbCopy.getCardinality());
            if (i > 0) {
                final RspBitmap rbBefore = rb.subrangeByPos(0, i - 1);
                final RspBitmap rbCopyBefore = rbCopy.subrangeByPos(0, i - 1);
                final long rbBeforeCard = rbBefore.getCardinality();
                assertEquals(m2, i, rbBeforeCard);
                final long rbCopyBeforeCard = rbCopyBefore.getCardinality();
                assertEquals(m2, i, rbCopyBeforeCard);
                assertEquals(m2, rbBefore, rbCopyBefore);
            }
            if (j < rbc - 1) {
                final RspBitmap rbAfter = rb.subrangeByPos(j + 1, rbc - 1);
                final RspBitmap rbCopyAfter = rbCopy.subrangeByPos(i, expectedCopyCard - 1);
                final long rbAfterCard = rbAfter.getCardinality();
                final long rbAfterExpectedCard = rbc - 1 - j;
                assertEquals(m2, rbAfterExpectedCard, rbAfterCard);
                final long rbCopyAfterCard = rbCopyAfter.getCardinality();
                final long rbCopyAfterExpectedCard = expectedCopyCard - i;
                assertEquals(m2, rbCopyAfterExpectedCard, rbCopyAfterCard);
                assertEquals(m2, rbAfterCard, rbCopyAfterCard);
                assertEquals(m2, rbAfter, rbCopyAfter);
            }
        }
    }

    private void checkRemoveRange(final RspBitmap original, final long start, final long end) {
        final RspBitmap rb = original.deepCopy();
        rb.removeRange(start, end);
        rb.validate();
        final RspBitmap rbRange = new RspBitmap();
        rbRange.addRange(start, end);
        final RspBitmap expected = RspBitmap.andNot(original, rbRange);
        assertEquals(expected, rb);
    }

    @Test
    public void testRemoveRange2() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(3, 5);
        rb.add(1001);
        rb.add(BLOCK_LAST);
        rb.add(BLOCK_SIZE);
        rb.add(BLOCK_SIZE + 1);
        rb.add(BLOCK_SIZE + BLOCK_LAST);
        rb.add(2 * BLOCK_SIZE + 1);
        rb.addRange(3 * BLOCK_SIZE, 6 * BLOCK_SIZE - 1);
        rb.add(6 * BLOCK_SIZE);
        rb.addRange(7 * BLOCK_SIZE, 8 * BLOCK_SIZE - 1);
        rb.add(8 * BLOCK_SIZE + 1);
        checkRemoveRange(rb, 0, BLOCK_SIZE - 1);
        checkRemoveRange(rb, 0, BLOCK_SIZE);
        checkRemoveRange(rb, 0, BLOCK_SIZE + 1);
        checkRemoveRange(rb, BLOCK_SIZE - 1, BLOCK_SIZE - 1);
        checkRemoveRange(rb, BLOCK_SIZE - 1, BLOCK_SIZE);
        checkRemoveRange(rb, BLOCK_SIZE - 1, BLOCK_SIZE + 1);
        checkRemoveRange(rb, BLOCK_SIZE, BLOCK_SIZE);
        checkRemoveRange(rb, BLOCK_SIZE, BLOCK_SIZE + 1);
        checkRemoveRange(rb, 2 * BLOCK_SIZE + 2, 3 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 2 * BLOCK_SIZE + 2, 3 * BLOCK_SIZE);
        checkRemoveRange(rb, 2 * BLOCK_SIZE + 2, 3 * BLOCK_SIZE + 1);
        checkRemoveRange(rb, 2 * BLOCK_SIZE + 1, 3 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 3 * BLOCK_SIZE + 2, 5 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 3 * BLOCK_SIZE + 2, 5 * BLOCK_SIZE);
        checkRemoveRange(rb, 3 * BLOCK_SIZE + 2, 5 * BLOCK_SIZE + 1);
        checkRemoveRange(rb, 3 * BLOCK_SIZE + 1, 5 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 3 * BLOCK_SIZE - 3, 5 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 3 * BLOCK_SIZE - 3, 5 * BLOCK_SIZE);
        checkRemoveRange(rb, 3 * BLOCK_SIZE - 3, 5 * BLOCK_SIZE + 1);
        checkRemoveRange(rb, 3 * BLOCK_SIZE - 3, 5 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 5 * BLOCK_SIZE, 6 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 7 * BLOCK_SIZE, 8 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 7 * BLOCK_SIZE - 1, 8 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 7 * BLOCK_SIZE - 1, 8 * BLOCK_SIZE - 2);
        checkRemoveRange(rb, 7 * BLOCK_SIZE + 1, 8 * BLOCK_SIZE - 1);
        checkRemoveRange(rb, 7 * BLOCK_SIZE + 1, 8 * BLOCK_SIZE - 2);
        checkRemoveRange(rb, 7 * BLOCK_SIZE + 1, 8 * BLOCK_SIZE + 1);
    }

    @Test
    public void testRemoveRange3() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(BLOCK_SIZE, 3 * BLOCK_SIZE - 1);
        rb.addRange(10 * BLOCK_SIZE, 12 * BLOCK_SIZE - 1);
        // key for start not in keys for rb.
        checkRemoveRange(rb, 5 * BLOCK_SIZE, 11 * BLOCK_SIZE - 2);
        checkRemoveRange(rb, 12 * BLOCK_SIZE, 12 * BLOCK_SIZE + 1);
    }

    @Test
    public void testRemoveRange4() {
        final RspBitmap rb = new RspBitmap();
        final long d = 63332;
        rb.addRange(0, BLOCK_SIZE + d);
        rb.validate();
        final RspBitmap rb2 = rb.deepCopy();
        assertEquals(BLOCK_SIZE + d + 1, rb.getCardinality());
        rb.removeRange(0, 2 * BLOCK_SIZE);
        rb.validate();
        assertEquals(0, rb.getCardinality());
        final RspBitmap rb3 = new RspBitmap();
        rb3.addRange(0, 2 * BLOCK_SIZE);
        rb2.andNotEquals(rb3);
        assertEquals(0, rb2.getCardinality());
        final Object[] spans = rb2.getKvs().getSpans();
        for (int i = 0; i < spans.length; ++i) {
            assertNull(spans[i]);
        }
    }

    @Test
    public void testRemoveRange5() {
        randomizedTest(RspBitmapTest::doTestRemoveRange5);
    }

    private static long nextMultipleOfBlockSize(final long v) {
        final long div = v / BLOCK_SIZE;
        final long rem = v % BLOCK_SIZE;
        if (rem == 0) {
            return div;
        }
        return div * (BLOCK_SIZE + 1);
    }

    private static void doTestRemoveRange5(final int seed) {
        final String m = "seed==" + seed;
        final Random r = new Random(seed);
        final int nblocks = 30;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 2000);
        final long min = rb.first();
        final long max = rb.last();
        final long kEnd = nextMultipleOfBlockSize(max);
        for (long kLeft = nextMultipleOfBlockSize(min); kLeft <= kEnd; kLeft += BLOCK_SIZE) {
            for (int jitterLeft = -2; jitterLeft <= 2; ++jitterLeft) {
                for (int jitterRight = -2; jitterRight <= 2; ++jitterRight) {
                    for (long kRight = kLeft + BLOCK_SIZE; kRight < kEnd; kRight += BLOCK_SIZE) {
                        final long start = Math.max(kLeft + jitterLeft, 0);
                        final long end = Math.max(kRight + jitterRight, start);
                        final String m2 = m + " && start==" + start + " && end==" + end;
                        final RspBitmap rbCopy = rb.deepCopy();
                        rbCopy.removeRange(start, end);
                        assertFalse(rbCopy.containsRange(start, end));
                        rbCopy.validate(m2);
                        final RspBitmap range = new RspBitmap(start, end);
                        final RspBitmap minus = RspBitmap.andNot(rb, range);
                        assertFalse(minus.containsRange(start, end));
                        assertEquals(m2, minus, rbCopy);
                    }
                }
            }
        }
    }

    @Test
    public void testSubrangeByKey() {
        randomizedTest(RspBitmapTest::doTestSubrangeByKey);
    }

    private static void doTestSubrangeByKey(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 60;
        final TLongSet set = new TLongHashSet();
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 2000,
                0, 0, set);
        final long rbc = rb.getCardinality();
        final int ntests = 20;
        final String m = "seed=" + seed;
        for (int t = 0; t < ntests; ++t) {
            final long posStart = r.nextInt((int) rbc);
            final long posEnd = posStart + r.nextInt((int) (rbc - posStart));
            final long start = rb.get(posStart);
            final long end = rb.get(posEnd);
            final String m2 = m + ", t=" + t + ", start=" + start + ", end=" + end;
            RspBitmap rbs = rb.subrangeByValue(start, end);
            rbs.validate(m2);
            final TLongSet set2 = new TLongHashSet(set);
            final RspRangeIterator it = rbs.getRangeIterator();
            while (it.hasNext()) {
                it.next();
                final long s = it.start();
                final long e = it.end();
                assertTrue(m2 + ", s=" + s + ", e=" + e, start <= s && e <= end);
                for (long v = s; v <= e; ++v) {
                    final String m3 = m2 + ", v=" + v;
                    assertTrue(m3, set2.remove(v));
                }
            }
            set2.forEach((long v) -> {
                assertTrue(m2, v < start || end < v);
                return true;
            });
        }
    }

    @Test
    public void testSubrangeByKey2() {
        randomizedTest(RspBitmapTest::doTestSubrangeByKey2);
    }

    private static void findRangeCheck(final String pfxMsg, final long[] arr, final RspBitmap rb, final long start,
            final long end) {
        final RspBitmap result = rb.subrangeByValue(start, end);
        result.validate(pfxMsg);
        int sz = 0;
        final String msg = pfxMsg + " start == " + start + " && end == " + end;
        for (int j = 0; j < arr.length; ++j) {
            final long v = arr[j];
            final String emsg = msg + " && j == " + j + " && v == " + v;
            if (start <= v && v <= end) {
                assertTrue(emsg, result.contains(v));
                ++sz;
            } else {
                assertFalse(emsg, result.contains(v));
            }
        }
        assertEquals(msg, sz, result.getCardinality());
    }

    private static void doTestSubrangeByKey2(final int seed) {
        final int count = 100000;
        final TLongSet set = ValidationSet.make(count);
        final Random r = new Random(seed);
        final String pfxMsg = "doTestFindRange seed == " + seed;
        final RspBitmap rb = populateRandom(pfxMsg, r, count, 10, 30000000, 150, 50, set, true);
        assertEquals(set.size(), rb.getCardinality());
        final long[] arr = new long[set.size()];
        set.toArray(arr);
        Arrays.sort(arr);
        final long as = arr[0];
        final long ae = arr[arr.length - 1];
        final long[] starts = new long[] {as - 1, as, as + 1};
        final long[] ends = new long[] {ae - 1, ae, ae + 1};
        for (long s : starts) {
            for (long e : ends) {
                findRangeCheck(pfxMsg, arr, rb, s, e);
            }
        }
        final int icount = count / 10000;
        for (int i = 0; i < icount; ++i) {
            final int d = r.nextInt(arr.length);
            final int s = r.nextInt(arr.length - d);
            final int e = s + d;
            findRangeCheck(pfxMsg + " && i == " + i, arr, rb, arr[s], arr[e]);
        }
    }

    @Test
    public void testSubrangeByKey3() {
        randomizedTest(RspBitmapTest::doTestSubrangeByKey3);
    }

    private static void doTestSubrangeByKey3(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 40;
        final TLongSet set = new TLongHashSet();
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.70f, 200);
        final long rbc = rb.getCardinality();
        final String m = "seed=" + seed;
        for (int i = 0; i < rbc; ++i) {
            final long pos = i;
            final long k = rb.get(pos);
            final String m2 = m + ", pos=" + pos + ", k=" + k;
            RspBitmap rbs = rb.subrangeByValue(k, k);
            rbs.validate(m2);
            assertEquals(m2, 1, rbs.getCardinality());
            assertEquals(m2, k, rbs.get(0));
        }
    }

    @Test
    public void testSubrangeByKeyBeforeAfter() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 512 - 1);
        RspBitmap rbs = rb.subrangeByValue(0, 2 * BLOCK_SIZE - 1);
        assertEquals(0, rbs.getCardinality());
        rbs = rb.subrangeByValue(BLOCK_SIZE, 2 * BLOCK_SIZE);
        assertEquals(1, rbs.getCardinality());
        assertEquals(rb.first(), rbs.first());
        rbs = rb.subrangeByValue(rb.last() + 1, rb.last() + 10 * BLOCK_SIZE);
        assertEquals(0, rbs.getCardinality());
        rbs = rb.subrangeByValue(rb.last(), rb.last());
        assertEquals(1, rbs.getCardinality());
        assertEquals(rb.last(), rbs.first());
        final RspRangeIterator it = rb.getRangeIterator();
        long laste = -1;
        while (it.hasNext()) {
            it.next();
            final long s = it.start();
            final long e = it.end();
            if (laste != -1) {
                final long ss = laste + 1;
                final long ee = it.start() - 1;
                assertTrue(ss <= ee);
                rbs = rb.subrangeByValue(ss, ee);
                assertTrue(rbs.isEmpty());
                if (ss != ee) {
                    rbs = rb.subrangeByValue(ss, ss);
                    assertTrue(rbs.isEmpty());
                    rbs = rb.subrangeByValue(ee, ee);
                    assertTrue(rbs.isEmpty());
                }
            }
            for (long v = s; v <= e; ++v) {
                rbs = rb.subrangeByValue(v, v);
                assertEquals(1, rbs.getCardinality());
                assertEquals(v, rbs.first());
            }
            laste = e;
        }
    }

    @Test
    public void testAddAndAppendWorkTheSame() {
        randomizedTest(RspBitmapTest::doTestAddAndAppendWorkTheSame);
    }

    private static void doTestAddAndAppendWorkTheSame(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 120;
        TLongSet set = new TLongHashSet();
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 100,
                0, 0, set);
        RspRangeIterator it = rb.getRangeIterator();
        final RspBitmap rba = new RspBitmap();
        while (it.hasNext()) {
            it.next();
            for (long v = it.start(); v <= it.end(); ++v) {
                rba.append(v);
            }
        }
        assertEquals(rb.getCardinality(), rba.getCardinality());
        assertEquals(rb, rba);
        rb.validate();
        rba.validate();
    }

    @Test
    public void testAddAndAppendRangeWorkTheSame() {
        randomizedTest(RspBitmapTest::doTestAddAndAppendRangeWorkTheSame);
    }

    private static void doTestAddAndAppendRangeWorkTheSame(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 120;
        TLongSet set = new TLongHashSet();
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 100,
                0, 0, set);
        RspRangeIterator it = rb.getRangeIterator();
        final RspBitmap rba = new RspBitmap();
        while (it.hasNext()) {
            it.next();
            rba.appendRangeUnsafeNoWriteCheck(it.start(), it.end());
        }
        rba.finishMutations();
        assertEquals(rb.getCardinality(), rba.getCardinality());
        assertEquals(rb, rba);
    }

    @Test
    public void testSubrangeByPos() {
        randomizedTest(RspBitmapTest::doTestSubrangeByPos);
    }

    private static void doTestSubrangeByPos(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 120;
        TLongSet set = new TLongHashSet();
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 100,
                0, 0, set);
        final long[] vs = set.toArray();
        Arrays.sort(vs);
        set = null;
        final long rbc = rb.getCardinality();
        final int ntests = 20;
        final String m = "seed==" + seed;
        for (int t = 0; t < ntests; ++t) {
            final int istart = r.nextInt((int) rbc);
            final long iend = istart + r.nextInt((int) (rbc - istart));
            final String m2 = m + " && t==" + t + " && istart==" + istart + " && iend==" + iend;
            RspBitmap rbs = rb.subrangeByPos(istart, iend);
            final RspRangeIterator it = rbs.getRangeIterator();
            int j = 0;
            while (it.hasNext()) {
                it.next();
                final long s = it.start();
                final long e = it.end();
                for (long v = s; v <= e; ++v) {
                    final String m3 = m2 + " && j==" + j + " && v==" + v;
                    assertEquals(m3, vs[istart + j], v);
                    ++j;
                }
            }
        }
    }

    @Test
    public void testSubrangeByPos2() {
        randomizedTest(RspBitmapTest::doTestSubrangeByPos2);
    }

    private static void doTestSubrangeByPos2(final int seed) {
        final int count = 100000;
        final TLongSet set = ValidationSet.make(count);
        final String pfxMsg = "doTestGetRange seed == " + seed;
        final Random r = new Random(seed);
        final RspBitmap rb = populateRandom(pfxMsg, r, count, 10, 30000000, 150, 50, set, true);
        assertEquals(set.size(), rb.getCardinality());
        final long[] arr = new long[set.size()];
        set.toArray(arr);
        Arrays.sort(arr);
        final long[] pos0s = new long[] {0, 1};
        final long[] pos1s = new long[] {arr.length - 1, arr.length, arr.length + 1};
        for (long pos0 : pos0s) {
            for (long pos1 : pos1s) {
                subrangeByPosRangeCheck(pfxMsg, arr, rb, pos0, pos1);
            }
        }
        final int icount = count / 10000;
        for (int i = 0; i < icount; ++i) {
            final int d = r.nextInt(arr.length);
            final int s = r.nextInt(arr.length - d);
            final int e = s + d;
            subrangeByPosRangeCheck(pfxMsg + " && i == " + i, arr, rb, s, e);
        }
    }

    private static void subrangeByPosRangeCheck(final String pfxMsg, final long[] arr, final RspBitmap rb,
            final long pos0, final long pos1) {
        final RspBitmap result = rb.subrangeByPos(pos0, pos1);
        result.validate(pfxMsg);
        int sz = 0;
        final String msg = pfxMsg + " pos0 == " + pos0 + " && pos1 == " + pos1;
        for (int j = 0; j < arr.length; ++j) {
            final long v = arr[j];
            final String emsg = msg + " && j == " + j + " && v == " + v;
            if (pos0 <= j && j <= pos1) {
                assertTrue(emsg, result.contains(v));
                ++sz;
            } else {
                assertFalse(emsg, result.contains(v));
            }
        }
        assertEquals(msg, sz, result.getCardinality());
    }

    @Test
    public void testSubrangeByPosAfter() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 512 - 1);
        RspBitmap rbs = rb.subrangeByPos(512, 1024);
        assertEquals(0, rbs.getCardinality());
        rbs = rb.subrangeByPos(511, 511);
        assertEquals(1, rbs.getCardinality());
        assertEquals(2 * BLOCK_SIZE + 511, rbs.first());
    }

    @Test
    public void testSubsetOf() {
        randomizedTest(RspBitmapTest::doTestSubsetOf);
    }

    private static void doTestSubsetOf(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 60;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 200000);
        final long rbc = rb.getCardinality();
        final int ntests = 20;
        final String m = "seed=" + seed;
        for (int t = 0; t < ntests; ++t) {
            final String m2 = m + ", t=" + t;
            final RspBitmap rb2 = rb.deepCopy();
            long rb2c = rbc;
            final int nRemovals = r.nextInt(200);
            if (nRemovals == 0) {
                continue;
            }
            for (int ri = 0; ri < nRemovals; ++ri) {
                final String m3 = m2 + ", ri=" + ri;
                assertEquals(m3, rb2c, rb2.getCardinality());
                final long pos = r.nextInt((int) rb2c);
                final long v = rb2.get(pos);
                assertTrue(rb2.contains(v));
                long b4 = rb2.getCardinality();
                rb2.remove(v);
                final String m4 = m3 + ", pos=" + pos + ", v=" + v + ", b4=" + b4;
                assertFalse(m4, rb2.contains(v));
                assertEquals(m4, rb2c - 1, rb2.getCardinality());
                --rb2c;
            }
            assertEquals(m2, rbc - nRemovals, rb2.getCardinality());
            assertTrue(m2, rb2.subsetOf(rb));
            assertTrue(m2, rb.subsetOf(rb));
            assertFalse(m2, rb.subsetOf(rb2));
        }
    }

    @Test
    public void testOverlaps() {
        randomizedTest(RspBitmapTest::doTestOverlaps);
    }

    private static void doTestOverlaps(final int seed) {
        final Random r = new Random(seed);
        final int nblocks1 = 60;
        final int nblocks2 = 30;
        final RspBitmap rb1 = getRandomRspBitmap(nblocks1, r, 0.90f, 2000, 0, nblocks1 / 5);
        final RspBitmap rb2 = getRandomRspBitmap(nblocks2, r, 0.90f, 2000, nblocks1 + 1, nblocks2 / 5);
        assertFalse(rb1.overlaps(rb2));
        final int nadds = 200;
        final String m = "seed=" + seed;
        for (int i = 0; i < nadds; ++i) {
            final float f = i / (float) (nadds - 1); // between 0 and 1.
            final int b = (int) ((nblocks1 - 1) * f); // between 0 and (nblocks1 - 1).
            final int firstBlock = nblocks1 - 1 - b; // make firstBlock further to the left on every iteration pass.
            final float pFull = 0.3F;
            final boolean doFull = r.nextFloat() <= pFull;
            boolean expectOverlap = false;
            final RspBitmap rb2copy = rb2.deepCopy();
            if (doFull) {
                final long start = blockStart(firstBlock);
                rb2copy.addRangeExclusiveEnd(start, start + BLOCK_SIZE);
                expectOverlap = true;
            } else {
                final long start = blockStart(firstBlock) + r.nextInt(BLOCK_SIZE);
                final long end = start + 1 + r.nextInt(3); // note end may span a block boundary, if so, so be it.
                rb2copy.addRangeExclusiveEnd(start, end);
                for (long v = start; v < end; ++v) {
                    if (rb1.contains(v)) {
                        expectOverlap = true;
                        break;
                    }
                }
            }
            assertEquals(m + ", i=" + i, expectOverlap, rb1.overlaps(rb2copy));
        }
    }

    @Test
    public void testOverlaps2() {
        final RspBitmap r1 = new RspBitmap();
        r1.add(3);
        r1.addRange(BLOCK_SIZE, 2 * BLOCK_SIZE - 1);
        r1.add(2 * BLOCK_SIZE + 5);
        r1.addRange(3 * BLOCK_SIZE, 7 * BLOCK_SIZE - 1);
        r1.add(7 * BLOCK_SIZE);
        final RspBitmap r2 = new RspBitmap();
        r2.addRange(4 * BLOCK_SIZE, 20 * BLOCK_SIZE);
        boolean b = r1.overlaps(r2);
        assertTrue(b);
        final RspBitmap r3 = new RspBitmap();
        r3.add(6 * BLOCK_SIZE + 7);
        b = r1.overlaps(r3);
        assertTrue(b);
        b = r2.overlaps(r3);
        assertTrue(b);
    }

    @Test
    public void testOverlaps3() {
        final RspBitmap r1 = new RspBitmap();
        final RspBitmap r2 = new RspBitmap();
        r1.add(2 * BLOCK_SIZE + 10);
        r2.add(2 * BLOCK_SIZE + 11);
        assertFalse(r1.overlaps(r2));
        r1.add(3 * BLOCK_SIZE + 10);
        r2.add(4 * BLOCK_SIZE + 11);
        assertFalse(r1.overlaps(r2));
        assertFalse(r2.overlaps(r1));
        r1.addRange(5 * BLOCK_SIZE, 8 * BLOCK_SIZE - 1);
        r2.remove(2 * BLOCK_SIZE + 11);
        assertFalse(r1.overlaps(r2));
        assertFalse(r2.overlaps(r1));
        r2.add(2 * BLOCK_SIZE + 11);
        r2.add(9 * BLOCK_SIZE);
        assertFalse(r1.overlaps(r2));
        assertFalse(r2.overlaps(r1));
        r2.add(6 * BLOCK_SIZE + 1);
        assertTrue(r1.overlaps(r2));
        assertTrue(r2.overlaps(r1));
        r2.remove(6 * BLOCK_SIZE + 1);
        r2.add(7 * BLOCK_SIZE);
        assertTrue(r1.overlaps(r2));
        assertTrue(r2.overlaps(r1));
        r2.remove(7 * BLOCK_SIZE);
    }

    @Test
    public void testOverlaps4() {
        final RspBitmap r1 = new RspBitmap();
        final RspBitmap r2 = new RspBitmap();
        r1.add(6 * BLOCK_SIZE + 1);
        r1.addRange(8 * BLOCK_SIZE, 10 * BLOCK_SIZE - 1);
        r2.add(5 * BLOCK_SIZE + 1);
        r2.add(7 * BLOCK_SIZE + 2);
        assertFalse(r1.overlaps(r2));
        assertFalse(r2.overlaps(r1));
    }

    @Test
    public void testOverlaps5() {
        final RspBitmap r1 = new RspBitmap();
        final RspBitmap r2 = new RspBitmap();
        r1.addRange(6 * BLOCK_SIZE, 9 * BLOCK_SIZE - 1);
        r2.add(7 * BLOCK_SIZE);
        assertTrue(r1.overlaps(r2));
        assertTrue(r2.overlaps(r1));
    }

    @Test
    public void testIteratorSearch() {
        randomizedTest(RspBitmapTest::doTestIteratorSearch);
    }

    private static void doTestIteratorSearch(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 120;
        final TLongSet set = new TLongHashSet();
        final RspBitmap rb =
                getRandomRspBitmap(nblocks, r, 0.20f, 6000, 0, 12, set);
        doTestIteratorSearchImpl(seed, rb, set);
    }

    private static void doTestIteratorSearchImpl(final int seed, final RspBitmap rb, TLongSet set) {
        final String m = "seed==" + seed;
        final RspRangeIterator it = rb.getRangeIterator();
        assertTrue(it.hasNext());
        it.next();
        final long[] vs = new long[set.size()];
        set.toArray(vs);
        Arrays.sort(vs);
        set = null;
        long lastv = -1;
        for (long v : vs) {
            final String m2 = m + " && v==" + v;
            for (long w = lastv + 1; w < v; ++w) {
                if (v == 1245185) {
                    String m5 = "a";
                }
                final String m3 = m2 + " && w==" + w;
                final long preStart = it.start();
                final long ww = w;
                final RowSetUtils.Comparator comp = (k) -> Long.compare(ww, k);
                it.search(comp);
                assertEquals(m3, preStart, it.start());
                final RspRangeIterator it2 = rb.getRangeIterator();
                assertTrue(m3, it2.hasNext());
                it2.next();
                it2.search(comp);
                assertEquals(m3, preStart, it2.start());
            }
            final RowSetUtils.Comparator comp = (k) -> Long.compare(v, k);
            it.search(comp);
            assertEquals(m2, v, it.start());
            final RspRangeIterator it2 = rb.getRangeIterator();
            assertTrue(m2, it2.hasNext());
            it2.next();
            it2.search(comp);
            assertEquals(m2, v, it2.start());
            lastv = v;
        }
    }

    private static void testBiOp(
            final int seed,
            BiFunction<RspBitmap, RspBitmap, RspBitmap> bitmapFun,
            BiFunction<Boolean, Boolean, Boolean> boolFun) {
        final Random r = new Random(seed);
        final int nblocks = 60;
        final RspBitmap rb1 =
                getRandomRspBitmap(nblocks, r, 0.90f, 2000, 0, 12);
        final RspBitmap rb2 =
                getRandomRspBitmap(nblocks, r, 0.90f, 2000, nblocks / 2, 12);
        final String m = "seed==" + seed;
        testBiOp(m, bitmapFun, boolFun, rb1, rb2);
    }

    private static void testBiOp(
            final String m,
            BiFunction<RspBitmap, RspBitmap, RspBitmap> bitmapFun,
            BiFunction<Boolean, Boolean, Boolean> boolFun,
            final RspBitmap rb1,
            final RspBitmap rb2) {
        final RspBitmap rbres = bitmapFun.apply(rb1, rb2);
        rbres.validate(m);
        long calculatedCardinality = 0;
        final RowSetUtils.CombinedRangeIterator itCombined =
                new RowSetUtils.CombinedRangeIterator(rb1.ixRangeIterator(), rb2.ixRangeIterator());
        while (itCombined.hasNext()) {
            itCombined.next();
            final long s = itCombined.currentRangeStart();
            final long e = itCombined.currentRangeEnd();
            final RowSetUtils.CombinedRangeIterator.RangeMembership membership = itCombined.currentRangeMembership();
            final boolean in1 = membership.hasFirst();
            final boolean in2 = membership.hasSecond();
            final boolean boolResult = boolFun.apply(in1, in2);
            if (boolResult) {
                calculatedCardinality += e - s + 1;
            }
            final String m2 = m + " && s==" + s + " && e==" + e;
            for (long v = s; v <= e; ++v) {
                final boolean inr = rbres.contains(v);
                assertEquals(m2 + " && v==" + v,
                        boolResult, inr);
            }
        }
        assertEquals(calculatedCardinality, rbres.getCardinality());
    }

    @Test
    public void testOr() {
        final IntConsumer testFun =
                (final int seed) -> testBiOp(seed, RspBitmap::or, (Boolean b1, Boolean b2) -> b1 || b2);
        randomizedTest(testFun);
    }

    @Test
    public void testAnd() {
        final IntConsumer testFun =
                (final int seed) -> testBiOp(seed, RspBitmap::and, (Boolean b1, Boolean b2) -> b1 && b2);
        randomizedTest(testFun);
    }

    @Test
    public void testAndNot() {
        final IntConsumer testFun =
                (final int seed) -> testBiOp(seed, RspBitmap::andNot, (Boolean b1, Boolean b2) -> b1 && !b2);
        randomizedTest(testFun);
    }

    @Test
    public void testSetFullBlockSpanAtIndexRegression0() {
        final RspBitmap rb = new RspBitmap();
        rb.add(2);
        rb.add(BLOCK_SIZE + 2);
        rb.add(2 * BLOCK_SIZE + 2);
        rb.addRangeExclusiveEnd(30 * BLOCK_SIZE, 31 * BLOCK_SIZE);
        final long k = 196608;
        final int i = rb.getKvs().getSpanIndex(k);
        rb.getKvs().setOrInsertFullBlockSpanAtIndex(i, k, 2, null);
        rb.finishMutations();
        assertTrue(rb.getKvs().validate());
    }

    @Test
    public void testSetFullBlockSpanAtIndexRegression1() {
        final RspBitmap rb = new RspBitmap();
        rb.add(2);
        rb.addRangeExclusiveEnd(3538944, 3538944 + 2 * BLOCK_SIZE);
        final long v2 = 3538944 + 2 * BLOCK_SIZE + 2;
        rb.add(v2);
        assertTrue(rb.contains(v2));
        final RspBitmap rb2 = rb.deepCopy();
        final long k = 3538944 + BLOCK_SIZE;
        final int i = rb.getKvs().getSpanIndex(k);
        rb.getKvs().setOrInsertFullBlockSpanAtIndex(i, k, 1, null);
        assertTrue(rb.getKvs().validate());
        assertTrue(rb.contains(3538944));
        assertTrue(rb2.subsetOf(rb));
        final RspBitmap rb3 = new RspBitmap();
        rb3.addRangeExclusiveEnd(k, k + BLOCK_SIZE);
        assertTrue(rb3.subsetOf(rb));
    }

    private static TLongSet makeSet(final int sz) {
        return new TLongHashSet(sz, 0.5f, -1L);
    }

    private static class TestContext {
        private final TLongSet set;
        private RspBitmap rb = new RspBitmap();
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private final boolean fullCheck;
        private final boolean partialCheck;
        private final String pfxMsg;

        TestContext(final String pfxMsg, final int count, final boolean fullCheck, final boolean partialCheck) {
            set = makeSet(count);
            this.pfxMsg = pfxMsg;
            this.fullCheck = fullCheck;
            this.partialCheck = partialCheck;
        }

        RspBitmap bitmap() {
            return rb;
        }

        public TLongSet set() {
            return set;
        }

        public long min() {
            return min;
        }

        public long max() {
            return max;
        }

        private void fullCheck(final int i, final long v, final long v2) {
            final String msg = pfxMsg + " i==" + i + " && v==" + v + " && v2==" + v2;
            assertEquals(msg, set.size(), rb.getCardinality());
            assertEquals(msg, min, rb.first());
            assertEquals(msg, max, rb.last());
            for (long j = Math.max(min - 2, 0); j <= max + 2; ++j) {
                final boolean sc = set.contains(j);
                final boolean nc = rb.contains(j);
                if (sc != nc) {
                    assertEquals(msg + " && j==" + j, set.contains(j), rb.contains(j));
                }
            }
        }

        void endCheck() {
            rb.finishMutations();
            rb.validate(pfxMsg);
            if (fullCheck) {
                return;
            }
            // assertEquals(pfxMsg, set.size(), rb.getCardinality());
            // assertEquals(pfxMsg, min, rb.first());
            // assertEquals(pfxMsg, max, rb.last());
            for (long i = Math.max(min - 2, 0); i <= max + 2; ++i) {
                assertEquals(pfxMsg + " && i==" + i, set.contains(i), rb.contains(i));
            }
        }

        private void tryCheck(final int i, final long v, final long v2) {
            if (fullCheck) {
                fullCheck(i, v, v2);
            } else if (partialCheck) {
                rb.finishMutations();
                final String msg = pfxMsg + " i == " + i + " && v== " + v + " && v2 == " + v2;
                if (set.size() != rb.getCardinality())
                    assertEquals(msg, set.size(), rb.getCardinality());
                assertEquals(msg, min, rb.first());
                assertEquals(msg, max, rb.last());
                for (long j = v; j <= v2; ++j) {
                    final boolean sc = set.contains(j);
                    final boolean nc = rb.contains(j);
                    if (sc != nc) {
                        assertEquals(msg + ", j=" + j, set.contains(j), rb.contains(j));
                    }
                }
            }
        }

        void insert(final long v, final int i) {
            set.add(v);
            rb.addUnsafe(v);
            if (v < min)
                min = v;
            if (v > max)
                max = v;
            tryCheck(i, v, v);
        }

        void insertRange(final long v, final long v2, final int i) {
            for (long k = v; k <= v2; ++k)
                set.add(k);
            rb.addRange(v, v2);
            if (v < min)
                min = v;
            if (v2 > max)
                max = v2;
            tryCheck(i, v, v2);
        }

        public void remove(final long v, final int i) {
            set.remove(v);
            rb.remove(v);
        }
    }

    @Test
    public void testRandomSingles() {
        randomizedTest(RspBitmapTest::doTestRandomSingles);
    }

    private static void doTestRandomSingles(final int seed) {
        final int count = 20000;
        final int topMin = 0;
        final int topMax = 25000000;
        final Random r = new Random(seed);
        final boolean fullCheck = false;
        final boolean partialCheck = true;
        final String pfxMsg = "doTestRandomSingles seed == " + seed;
        final TestContext tc = new TestContext(pfxMsg, count, fullCheck, partialCheck);
        long lastv = -1;
        for (int i = 0; i < count; ++i) {
            final long v;
            if (lastv > 0 && r.nextBoolean()) {
                v = r.nextBoolean() ? lastv - 1 : lastv + 1;
            } else {
                v = r.nextInt(topMax - topMin + 1) + topMin;
            }
            tc.insert(v, i);
            lastv = v;
        }
        tc.endCheck();
    }

    @Test
    public void testSequentialSingles() {
        randomizedTest(RspBitmapTest::doTestSequentialSingles);
    }

    private static void doTestSequentialSingles(final int seed) {
        final int count = 20000;
        final int base = 3000;
        final boolean fullCheck = false;
        final boolean partialCheck = true;
        final String pfxMsg = "doTestSequentialSingles seed == " + seed;
        final TestContext tc = new TestContext(pfxMsg, count, fullCheck, partialCheck);
        final Random r = new Random(seed);
        final int step = 10;
        for (int i = base; i < count + base; i += 1 + r.nextInt(step)) {
            final long v = (i != base && r.nextInt(100) == 1) ? i : Math.max(tc.min() - 1, 0);
            tc.insert(v, i);
        }
        tc.endCheck();
    }

    @Test
    public void testRandomInserts() {
        randomizedTest(RspBitmapTest::doTestRandomInserts);
    }

    private static TestContext getRandom(
            final String pfxMsg,
            final int seed,
            final int count,
            final int vmin, final int vmax,
            final int rangeMin, final int rangeMax,
            final boolean fullCheck, final boolean partialCheck) {
        final Random r = new Random(seed);
        final TestContext tc = new TestContext(pfxMsg, count, fullCheck, partialCheck);
        for (int i = 0; i < count; ++i) {
            if (r.nextBoolean()) {
                final long v = r.nextInt(vmax - vmin + 1) + vmin;
                tc.insert(v, i);
            } else {
                final long v = r.nextInt(vmax - vmin + 1) + vmin;
                final long v2 = v + r.nextInt(rangeMax - rangeMin + 1) + rangeMin;
                tc.insertRange(v, v2, i);
            }
        }
        return tc;
    }

    private static void doTestRandomInserts(final int seed) {
        final int count = 20000;
        final int topMin = 3000;
        final int startMax = 250000;
        final int endMax = 25000000;
        final int rangeMin = 2;
        final int rangeMax = 30;
        final boolean fullCheck = false;
        final boolean partialCheck = false;
        final String pfxMsg = "doTestRandomInserts seed==" + seed;
        for (int topMax = startMax; topMax <= endMax; topMax *= 10) {
            final TestContext tc =
                    getRandom(pfxMsg, seed, count, topMin, topMax, rangeMin, rangeMax, fullCheck, partialCheck);
            tc.endCheck();
        }
    }

    @Test
    public void testIterator2() {
        randomizedTest(RspBitmapTest::doTestIterator2, 2228);
    }

    private static void doTestIterator2(final int seed) {
        final int count = 20000;
        final int topMin = 3000;
        final int topMax = 8000000;
        final int rangeMin = 2;
        final int rangeMax = 30;
        final boolean fullCheck = false;
        final boolean partialCheck = false;
        final String pfxMsg = "doTestRangeIterator seed == " + seed;
        final TestContext tc =
                getRandom(pfxMsg, seed, count, topMin, topMax, rangeMin, rangeMax, fullCheck, partialCheck);
        tc.endCheck();
        final RspBitmap rb = tc.bitmap();
        final TLongSet set = new TLongHashSet(tc.set());
        long last = -1;
        final RspRangeIterator it = rb.getRangeIterator();
        while (it.hasNext()) {
            it.next();
            final long start = it.start();
            final long end = it.end();
            for (long v = start; v <= end; ++v) {
                assertTrue(last < v);
                assertTrue(set.remove(v));
                last = v;
            }
        }
        assertEquals(pfxMsg, 0, set.size());
    }

    @Test
    public void testIterator3() {
        randomizedTest(RspBitmapTest::doTestIterator3);
    }

    private static void doTestIterator3(final int seed) {
        final int count = 20000;
        final int topMin = 3000;
        final int topMax = 8000000;
        final int rangeMin = 2;
        final int rangeMax = 30;
        final boolean fullCheck = false;
        final boolean partialCheck = false;
        final String msg = "doTestRangeIterator seed == " + seed;
        final TestContext tc = getRandom(msg, seed, count, topMin, topMax, rangeMin, rangeMax, fullCheck, partialCheck);
        tc.endCheck();
        final RspBitmap rb = tc.bitmap();
        final TLongSet set = tc.set();
        long last = -1;
        final RspRangeIterator it = rb.getRangeIterator();
        while (it.hasNext()) {
            it.next();
            final long start = it.start();
            final long end = it.end();
            assertTrue(start <= end);
            for (long v = start; v <= end; ++v) {
                final String emsg = msg + " && v == " + v;
                assertTrue(emsg, last < v);
                assertTrue(emsg, set.remove(v));
                last = v;
            }
        }
        assertTrue(msg, set.isEmpty());
    }

    @Test
    public void testReverseIterator() {
        randomizedTest(RspBitmapTest::doTestReverseIterator);
    }

    private static void doTestReverseIterator(final int seed) {
        final int count = 20000;
        final int topMin = 3000;
        final int topMax = 8000000;
        final int rangeMin = 2;
        final int rangeMax = 30;
        final boolean fullCheck = false;
        final boolean partialCheck = false;
        final String msg = "doTestRangeIterator seed == " + seed;
        final TestContext tc = getRandom(msg, seed, count, topMin, topMax, rangeMin, rangeMax, fullCheck, partialCheck);
        tc.endCheck();
        final RspBitmap rb = tc.bitmap();
        final TLongSet set = tc.set();
        long last = 0;
        boolean firstTime = true;
        final RspReverseIterator it = rb.getReverseIterator();
        while (it.hasNext()) {
            it.next();
            final long v = it.current();
            final String emsg = msg + " && v == " + v;
            if (!firstTime) {
                assertTrue(emsg + ", last=" + last, last > v);
            }
            assertTrue(emsg, set.remove(v));
            last = v;
            firstTime = false;
        }
        assertTrue(msg, set.isEmpty());
    }

    private static final boolean print = false;

    private static String bitmap2str(final String pfx, final RspBitmap rb) {
        StringBuilder sb = new StringBuilder(pfx);
        sb.append("{ ");
        boolean comma = false;
        final RspRangeIterator it = rb.getRangeIterator();
        while (it.hasNext()) {
            it.next();
            final long s = it.start();
            final long e = it.end();
            for (long j = s; j <= e; ++j) {
                if (comma) {
                    sb.append(", ");
                }
                sb.append(j);
                comma = true;
            }
            comma = true;
        }
        sb.append(" }");
        return sb.toString();
    }

    private static void compare(final String msgPfx, final TLongSet set, final RspBitmap rb) {
        final boolean debug = true;
        final long[] minmax = new long[2];
        final int min = 0;
        final int max = 1;
        minmax[min] = Long.MAX_VALUE;
        minmax[max] = Long.MIN_VALUE;
        if (rb.getCardinality() == 0 && set.size() == 0) {
            return;
        }
        if (print) {
            System.out.println(ValidationSet.set2str("s = ", set));
            System.out.println(bitmap2str("n = ", rb));
        }
        // noinspection ConstantConditions
        if (debug) {
            final long[] vs = new long[set.size()];
            set.toArray(vs);
            Arrays.sort(vs);
            for (int j = 0; j < vs.length; ++j) {
                final long v = vs[j];
                final boolean c = rb.contains(v);
                if (!c) {
                    // noinspection ConstantConditions
                    assertTrue(msgPfx + " && v == " + v + " && j == " + j +
                            " && vslen == " + vs.length + ", node.size() == " + rb.getCardinality(),
                            c);
                }
            }
            minmax[min] = vs[0];
            minmax[max] = vs[vs.length - 1];
        } else {
            set.forEach(v -> {
                if (v < minmax[min])
                    minmax[min] = v;
                if (v > minmax[max])
                    minmax[max] = v;
                assertTrue(msgPfx + ", v=" + v, rb.contains(v));
                return true;
            });
        }
        if (set.size() != rb.getCardinality()) {
            System.out.println("set.size() = " + set.size() + ", node.size() = " + rb.getCardinality());
            long lastv = -2;
            int iti = 0;
            final RspRangeIterator it = rb.getRangeIterator();
            while (it.hasNext()) {
                it.next();
                ++iti;
                final long s = it.start();
                final long e = it.end();
                final String imsg = msgPfx + " && iti == " + iti + " && s == " + s + " && e == " + e;
                for (long v = s; v <= e; ++v) {
                    assertTrue(imsg + " v == " + v, set.contains(v));
                }
                assertTrue(imsg + ", lastv == " + lastv, s - lastv >= 1);
                lastv = e;
            }
            assertEquals(msgPfx, set.size(), rb.getCardinality());
        }
        assertEquals(msgPfx, minmax[min], rb.first());
        assertEquals(msgPfx, minmax[max], rb.last());
    }

    private static RspBitmap populateRandom(
            final String pfxMsg, final Random r, final int count,
            final int min, final int max, final int clusterWidth, final int jumpPropOneIn) {
        return populateRandom(pfxMsg, r, count, min, max, clusterWidth, jumpPropOneIn, null, false);
    }

    private static RspBitmap populateRandom(
            final String pfxMsg, final Random r, final int count,
            final int min, final int max, final int clusterWidth, final int jumpPropOneIn,
            final TLongSet set, @SuppressWarnings("SameParameterValue") final boolean check) {
        if (set != null) {
            assertEquals(0, set.size());
        }
        final RspBitmap rb = new RspBitmap();
        final int halfClusterWidth = clusterWidth / 2;
        final int d = max - min + 1 - clusterWidth;
        int cluster1Mid = min + halfClusterWidth;
        for (int i = 0; i < count; ++i) {
            final long k;
            if (r.nextInt(jumpPropOneIn) == 0) {
                k = cluster1Mid = halfClusterWidth + r.nextInt(d);
            } else {
                k = cluster1Mid + r.nextInt(halfClusterWidth);
            }
            if (set != null) {
                set.add(k);
            }
            rb.addUnsafeNoWriteCheck(k);
        }
        rb.finishMutations();
        if (check && set != null) {
            compare(pfxMsg, set, rb);
        }
        return rb;
    }

    @Test
    public void testApplyOffset() {
        randomizedTest(RspBitmapTest::doTestApplyOffset);
    }

    private static void doTestApplyOffset(final int seed) {
        final String pfxMsg = "doTestApplyOffset seed=" + seed;
        final Random r = new Random(seed);
        final long[] offsets = new long[] {
                RspArray.BLOCK_SIZE - 1,
                RspArray.BLOCK_SIZE,
                10 * RspArray.BLOCK_SIZE,
                10 * RspArray.BLOCK_SIZE + 1,
        };
        final int count = 100000;
        for (long offset : offsets) {
            final TLongSet set = ValidationSet.make(count);
            final RspBitmap rb =
                    populateRandom(pfxMsg, r, count,
                            10, 30000000, 150, 50, set, true);
            final String m = pfxMsg + " && offset==" + offset;
            final RspBitmap orb = rb.applyOffsetOnNew(offset);
            orb.validate(m);
            assertEquals(set.size(), rb.getCardinality());
            final long[] minmax = new long[2];
            final int min = 0;
            final int max = 1;
            minmax[min] = Long.MAX_VALUE;
            minmax[max] = Long.MIN_VALUE;
            set.forEach(v -> {
                if (v < minmax[min])
                    minmax[min] = v;
                if (v > minmax[max])
                    minmax[max] = v;
                assertTrue(m + " && v==" + v, orb.contains(v + offset));
                return true;
            });
            assertEquals(m, minmax[min] + offset, orb.first());
            assertEquals(m, minmax[max] + offset, orb.last());
        }
    }

    @Test
    public void testExtensiveRemove() {
        randomizedTest(RspBitmapTest::doTestExtensiveRemove);
    }

    private static void doTestExtensiveRemove(final int seed) {
        final String m = "seed=" + seed;
        final Random r = new Random(seed);
        final int count = 10000000;
        final RspBitmap rb = populateRandom(m, r, count, 1, 10000, 150, 50);
        long rbc = rb.getCardinality();
        while (rbc > 0) {
            final long ri = r.nextInt((int) rbc);
            final long k = rb.get(ri);
            rb.remove(k);
            final String m2 = m + ", rbc=" + rbc;
            assertFalse(m2, rb.contains(k));
            assertEquals(m2, rbc - 1, rb.getCardinality());
            --rbc;
            rb.validate(m2);
        }
    }

    @Test
    public void testExtensiveRemove2() {
        randomizedTest(RspBitmapTest::doTestExtensiveRemove2);
    }

    private static void doTestExtensiveRemove2(final int seed) {
        final String m = "seed=" + seed;
        final Random r = new Random(seed);
        final int nblocks = 500;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 2000);
        long rbc = rb.getCardinality();
        int toRemove = 10000;
        for (int i = 0; i < toRemove; ++i) {
            final long ri = r.nextInt((int) rbc);
            final long k = rb.get(ri);
            rb.remove(k);
            final String m2 = m + ", i=" + i;
            assertFalse(m2, rb.contains(k));
            assertEquals(m2, rbc - 1, rb.getCardinality());
            rb.validate(m2);
            --rbc;
        }
    }

    @Test
    public void testExtensiveRemove3() {
        randomizedTest(RspBitmapTest::doTestExtensiveRemove3);
    }

    private static void doTestExtensiveRemove3(final int seed) {
        final String m = "seed=" + seed;
        final Random r = new Random(seed);
        final int nblocks = 6;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.50f, 200);
        long rbc = rb.getCardinality();
        int toRemove = 20000;
        for (int i = 0; i < toRemove; ++i) {
            final long ri = r.nextInt((int) rbc);
            final long k = rb.get(ri);
            rb.remove(k);
            final String m2 = m + ", rbc=" + rbc;
            assertFalse(m2, rb.contains(k));
            assertEquals(m2, rbc - 1, rb.getCardinality());
            rb.validate(m2);
            --rbc;
        }
    }

    @Test
    public void testInvert() {
        randomizedTest(RspBitmapTest::doTestInvert);
    }

    private static long[] pick(final Random r, final int n, final long[] vs) {
        if (n > vs.length) {
            throw new IllegalArgumentException("n > vs.length");
        }
        if (n == vs.length) {
            return vs;
        }
        final long[] vsCopy = Arrays.copyOf(vs, vs.length);
        Shuffle.shuffleArray(r, vsCopy);
        final long[] ans = new long[n];
        System.arraycopy(vsCopy, 0, ans, 0, n);
        return ans;
    }

    private static void doTestInvertForPicks(final String m, final long rbc, final RspBitmap rb,
            final long[] vs, final long[] picks) {
        final RspBitmap rbPicks = RspBitmap.makeEmpty();
        final TLongSet setPicks = new TLongHashSet(picks.length);
        for (final long k : picks) {
            rbPicks.add(k);
            setPicks.add(k);
        }
        assertEquals(m, setPicks.size(), rbPicks.getCardinality());
        final RspBitmap inverted = new RspBitmap();
        rb.invert(
                inverted::appendRangeUnsafeNoWriteCheck,
                rbPicks.ixRangeIterator(),
                rbc);
        inverted.finishMutationsAndOptimize();
        inverted.validate(m);
        // assertEquals(m, setPicks.size(), inverted.getCardinality());
        final RspRangeIterator invertedIt = inverted.getRangeIterator();
        while (invertedIt.hasNext()) {
            invertedIt.next();
            final long s = invertedIt.start();
            final long e = invertedIt.end();
            for (long pos = s; pos <= e; ++pos) {
                final String m2 = m + ", pos=" + pos;
                assertTrue(m2, pos < rbc);
                final long k = vs[(int) pos];
                assertTrue(m2 + ", k=" + k, setPicks.remove(k));
            }
        }
        if (!setPicks.isEmpty()) {
            final long[] min = new long[1];
            min[0] = Long.MAX_VALUE;
            setPicks.forEach((long v) -> {
                if (v < min[0])
                    min[0] = v;
                return true;
            });
            final long pos = rb.find(min[0]);
            fail(m + ", min=" + min[0] + ", pos=" + pos);
        }
    }

    private static void doTestInvert(final int seed) {
        final String pfxMsg = "doTestInvert seed==" + seed;
        final Random r = new Random(seed);
        for (int j = 0; j < 2; ++j) {
            final int nblocks = (j == 0) ? RspBitmap.accNullThreshold : (32 + RspBitmap.accNullThreshold);
            if (nblocks == 0) {
                continue;
            }
            TLongSet set = new TLongHashSet();
            final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.10f, 2000,
                    0, 0, set);
            final long[] vs = set.toArray();
            Arrays.sort(vs);
            set = null;
            final long rbc = rb.getCardinality();
            final int ntests = 30;
            for (int t = 0; t < ntests; ++t) {
                final String m = pfxMsg + " && t==" + t;
                final int npicks;
                if (t == 0) {
                    npicks = 0;
                } else if (t == ntests - 1) {
                    npicks = (int) rbc;
                } else {
                    npicks = 1 + r.nextInt((int) (rbc - 1));
                }
                long[] picks = pick(r, npicks, vs);
                doTestInvertForPicks(m, rbc, rb, vs, picks);
            }
        }
    }

    @Test
    public void testInvert2() {
        final RspBitmap rb = new RspBitmap();
        rb.add(101);
        rb.addRange(1000, 1003);
        rb.addRange(BLOCK_LAST - 2, BLOCK_LAST);
        rb.addRange(BLOCK_SIZE, 3 * BLOCK_SIZE - 1);
        rb.addRange(3 * BLOCK_SIZE + 1, 3 * BLOCK_SIZE + 3);
        rb.addRange(4 * BLOCK_SIZE, 6 * BLOCK_SIZE - 1);
        rb.addRange(6 * BLOCK_SIZE, 6 * BLOCK_SIZE + BLOCK_LAST);
        rb.add(7 * BLOCK_SIZE);
        rb.addRange(8 * BLOCK_SIZE, 10 * BLOCK_SIZE);
        final long rbc = rb.getCardinality();
        final long[] vs = new long[(int) rbc];
        final RspRangeIterator rit = rb.getRangeIterator();
        int j = 0;
        while (rit.hasNext()) {
            rit.next();
            final long s = rit.start();
            final long e = rit.end();
            for (long v = s; v <= e; ++v) {
                vs[j++] = v;
            }
        }
        assertEquals(rbc, j);
        final long[] picks = new long[2];
        final long[] ks = rb.getKvs().getKeys();
        for (long k : ks) {
            long pos = rb.find(k);
            if (pos < 0) {
                pos = -pos - 1;
                if (pos >= rbc) {
                    --pos;
                }
            }
            picks[0] = rb.get(pos);
            if (pos - 1 > 0) {
                picks[1] = rb.get(pos - 1);
            } else {
                picks[1] = rb.get(pos + 1);
            }
            doTestInvertForPicks(picks.toString(), rbc, rb, vs, picks);
        }
    }

    @Test
    public void testSetLastFullBlockSpanRegression() {
        final RspBitmap rb = new RspBitmap();
        rb.addRangeExclusiveEnd(0, BLOCK_SIZE * 2);
        rb.addRangeExclusiveEnd(BLOCK_SIZE * 2, BLOCK_SIZE * 2 + 10);
        rb.addRangeExclusiveEnd(BLOCK_SIZE * 2, BLOCK_SIZE * 3);
        assertEquals(3 * BLOCK_SIZE, rb.getCardinality());
    }

    private RspBitmap str2rb(final String s) {
        final String[] ranges = s.split(",");
        final RspBitmap rb = new RspBitmap();
        for (String range : ranges) {
            if (range.contains("-")) {
                String[] parts = range.split("-");
                long start = Long.parseLong(parts[0]);
                long end = Long.parseLong(parts[1]);
                rb.addRange(start, end);
            } else {
                long key = Long.parseLong(range);
                rb.add(key);
            }
        }
        return rb;
    }

    private final RspBitmap strs2rb(final String... ss) {
        RspBitmap rb = new RspBitmap();
        for (String s : ss) {
            rb.orEquals(str2rb(s));
        }
        return rb;
    }

    @Test
    public void testSomeSimpleOps() {
        final RspBitmap rb = str2rb("43,146,367,376,434,470-599,601-635");
        final RspBitmap toRemove = str2rb("470-599,601-635");
        final RspBitmap rb2 = rb.deepCopy();
        rb2.andNotEquals(toRemove);
        final RspBitmap rbExpected = str2rb("43,146,367,376,434");
        assertEquals(rbExpected, rb2);
        final RspBitmap noIntersection = strs2rb(
                "12,14-19,21-23,25-26,28-31,33-34,36-37",
                "39,41,45-53,55-56,58,60,62-65,67-72,74,76-78,80-81",
                "83-84,86,88-90,92,94-96,98,100,102-104,106,108,110-112",
                "114,116,118-119,121,123,125,127,129,131,133-134,136-138",
                "140-144,147,149-150,152-153,155,157-160,162-163,165,167-170",
                "172,174,176-177,179-180,182,184-186,188-189,191-193,195-197",
                "199,201-202,204,206-207,209,211,213-214,216,218,220-221,223",
                "225,227,229-236,238,240,242-243,245-246,248-252,254-255,257",
                "259-260,262-266,268,270,272-275,277,279,281,283-287,289,291-294",
                "296-300,302,304,306,308-311,313-318,320-322,324-325,327,329-330,332-336",
                "338,340-341,343-347,349,351-355,357,359,361-362,364,366,369-370,372-373",
                "375,378,380-384,386-387,389-393,395-396,398,400-401,403,405-407,409,411-412",
                "414-415,417,419-420,422-423,425-426,428-429,431,433,435-438,440-441,443,445",
                "447,449,451,453-455,457-464,466,468-469");
        final RspBitmap rb3 = rb.deepCopy();
        rb3.andEquals(noIntersection);
        assertEquals(0, rb3.getCardinality());
    }

    @Test
    public void testSomeSimpleOps2() {
        final RspBitmap rb0 = str2rb("7,9");
        final RspBitmap rb1 = str2rb("2,4,6");
        final RspBitmap rb2 = new RspBitmap();
        final RspBitmap rb3 = RspBitmap.andNot(rb0, rb2);
        final RspBitmap rb4 = RspBitmap.and(rb3, rb1);
        assertEquals(0, rb4.getCardinality());
    }

    @Test
    public void testSubrangeByPosRegression0() {
        final RspBitmap rb = new RspBitmap();
        final long sz = 2L * Integer.MAX_VALUE;
        rb.addRange(0, sz - 1);
        assertEquals(sz, rb.getCardinality());
        final RspBitmap rb2 = rb.subrangeByPos(4294967293L, 4294967293L);
        assertEquals(1, rb2.getCardinality());
        assertEquals(4294967293L, rb2.get(0));
    }

    @Test
    public void testSubrangeByPosRegression1() {
        final RspBitmap rb = new RspBitmap();
        final long k = 1073741824;
        rb.addValues(k + 1, k + 4);
        assertEquals(2, rb.getCardinality());
        assertEquals(k + 1, rb.get(0));
        assertEquals(k + 4, rb.get(1));
        final RspBitmap rb2 = rb.subrangeByPos(0, 9);
        assertEquals(2, rb2.getCardinality());
        assertEquals(k + 1, rb2.get(0));
        assertEquals(k + 4, rb2.get(1));
    }

    @Test
    public void testSubrangeByPosRegression2() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(0, 9);
        rb.add(BLOCK_SIZE + 2);
        final RspBitmap rb2 = rb.subrangeByPos(10, 10);
        assertEquals(1, rb2.getCardinality());
        assertEquals(BLOCK_SIZE + 2, rb2.get(0));
    }

    @Test
    public void testIteratorSearchRegression0() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(1, 8);
        final RspRangeIterator rit = rb.getRangeIterator();
        assertTrue(rit.hasNext());
        rit.next();
        RowSetUtils.Comparator comp = (long k) -> Long.compare(2, k);
        rit.search(comp);
        assertEquals(2, rit.start());
    }

    @Test
    public void testAndEqualsRegression0() {
        // This was throwing ArrayOutOfBounds.
        final RspBitmap rb1 = new RspBitmap();
        rb1.add(0);
        rb1.add(BLOCK_SIZE);
        rb1.add(2 * BLOCK_SIZE);
        final RspBitmap rb2 = new RspBitmap();
        rb2.addRange(0, 3 * BLOCK_SIZE - 1);
        rb1.andEquals(rb2);
        assertEquals(3, rb1.getCardinality());
    }

    @Test
    public void testAndNotBlockBoundaries() {
        final RspBitmap rb0 = new RspBitmap();
        rb0.addRange(0, 1);
        rb0.addRange(BLOCK_SIZE, 2 * BLOCK_SIZE - 1);
        rb0.validate();
        final RspBitmap rb1 = new RspBitmap();
        rb1.addRange(BLOCK_SIZE + 1, 2 * BLOCK_SIZE - 1);
        rb1.validate();
        rb0.andNotEquals(rb1);
        rb0.validate();
        assertEquals(3, rb0.getCardinality());
        final RspBitmap rb2 = new RspBitmap();
        rb2.add(BLOCK_SIZE);
        rb2.validate();
        rb0.andNotEquals(rb2);
        rb0.validate();
        assertEquals(2, rb0.getCardinality());
        rb0.andNotEquals(rb0.deepCopy());
        rb0.validate();
        assertEquals(0, rb0.getCardinality());
    }

    @Test
    public void testAndBlockBoundaries() {
        final RspBitmap rb0 = new RspBitmap();
        rb0.addRange(BLOCK_SIZE + 1000, 2 * BLOCK_SIZE);
        rb0.validate();
        final RspBitmap rb1 = new RspBitmap();
        rb1.addRange(BLOCK_SIZE, BLOCK_SIZE + 999);
        rb1.validate();
        rb0.andEquals(rb1);
        rb0.validate();
        assertEquals(0, rb0.getCardinality());
    }

    @Test
    public void testMinusRegression0() {
        final RspBitmap base = new RspBitmap();
        base.addRange(4194441, 8388608);
        final RspBitmap doesNotIsectBase = new RspBitmap();
        doesNotIsectBase.addRange(1048577, 2097152);
        final RspBitmap isect = RspBitmap.and(base, doesNotIsectBase);
        assertEquals(0L, isect.getCardinality());
        final RspBitmap r3 = RspBitmap.andNot(base, doesNotIsectBase);
        assertEquals(base.getCardinality(), r3.getCardinality());
        assertEquals(base, r3);
    }

    @Test
    public void testJustDisjoint() {
        RspBitmap r1 = new RspBitmap();
        r1.addRange(0, 65536);
        RspBitmap r2 = new RspBitmap();
        r2.addRange(65537, 65536 * 3 - 1);
        assertEquals(0, RspBitmap.and(r1, r2).getCardinality());
        assertEquals(0, RspBitmap.and(r2, r1).getCardinality());
        assertEquals(r1.getCardinality(), RspBitmap.andNot(r1, r2).getCardinality());
        assertEquals(r2.getCardinality(), RspBitmap.andNot(r2, r1).getCardinality());
        r1 = new RspBitmap();
        r1.addRange(0, 65536 - 1);
        r2 = new RspBitmap();
        r2.addRange(65536, 65536 * 2 - 1);
        assertEquals(0, RspBitmap.and(r1, r2).getCardinality());
        assertEquals(0, RspBitmap.and(r2, r1).getCardinality());
        assertEquals(r1.getCardinality(), RspBitmap.andNot(r1, r2).getCardinality());
        assertEquals(r2.getCardinality(), RspBitmap.andNot(r2, r1).getCardinality());
    }

    @Test
    public void testAndNotEqualsRegression0() {
        RspBitmap r1 = new RspBitmap();
        r1.add(10);
        r1.add(BLOCK_SIZE + 10);
        r1.add(2 * BLOCK_SIZE + 10);
        final long r1Card = r1.getCardinality(); // This is critical: force the card cache to populate.
        assertEquals(3L, r1Card);
        RspBitmap r2 = new RspBitmap();
        r2.addRange(BLOCK_SIZE, BLOCK_SIZE * 3 - 1);
        r1.andNotEquals(r2);
        assertEquals(1, r1.getCardinality());
        r1.addRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 9);
        assertEquals(11, r1.getCardinality());
    }

    @Test
    public void testAddRange2() {
        // Ensure range of exactly full block length works.
        final RspBitmap rb = new RspBitmap();
        rb.add(2 * BLOCK_SIZE); // ensure we are not appending for the following addRange.
        rb.addRange(BLOCK_SIZE, 2 * BLOCK_SIZE - 1);
        rb.validate();
        assertEquals(BLOCK_SIZE + 1, rb.getCardinality());
    }

    @Test
    public void testAddRange3() {
        // Ensure multi block ranges work.
        final RspBitmap rb = new RspBitmap();
        rb.add(100 * BLOCK_SIZE); // ensure we are not appending in what follows.
        final long start = BLOCK_SIZE + 4;
        final long end = 3 * BLOCK_SIZE - 1;
        rb.addRange(start, end); // end at block last.
        rb.validate();
        assertEquals(end - start + 1 + 1, rb.getCardinality());
        final long start2 = 5 * BLOCK_SIZE + 1;
        final long end2 = 8 * BLOCK_SIZE - 2;
        rb.addRange(start2, end2); // end not at block last.
        rb.validate();
        assertEquals(end - start + 1 + end2 - start2 + 1 + 1, rb.getCardinality());
        final long start3 = 9 * BLOCK_SIZE;
        final long end3 = 13 * BLOCK_SIZE - 1;
        rb.addRange(start3, end3);
        rb.validate();
        assertEquals(end - start + 1 + end2 - start2 + 1 + end3 - start3 + 1 + 1, rb.getCardinality());
    }


    @Test
    public void testAddRange4() {
        final RspBitmap rb = new RspBitmap();
        rb.add(3);
        rb.add(BLOCK_SIZE + 3);
        rb.add(2 * BLOCK_SIZE + 3);
        rb.add(3 * BLOCK_SIZE + 3);
        rb.add(4 * BLOCK_SIZE + 3);
        final long s = BLOCK_SIZE + 5;
        final long e = 4 * BLOCK_SIZE - 1;
        rb.addRange(s, e);
        assertEquals(e - s + 1 + 5 - 2, rb.getCardinality());
    }

    @Test
    public void testAddRange5() {
        final RspBitmap rb = new RspBitmap();
        rb.add(3);
        rb.add(BLOCK_SIZE + 3);
        rb.add(20 * BLOCK_SIZE + 3);
        rb.add(21 * BLOCK_SIZE + 3);
        rb.add(22 * BLOCK_SIZE + 3);
        final long s = BLOCK_SIZE + 5;
        final long e = 4 * BLOCK_SIZE - 1;
        rb.addRange(s, e);
        assertEquals(e - s + 1 + 5, rb.getCardinality());
    }

    @Test
    public void testAndEquals2() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(10, 20);
        rb.addRange(3 * BLOCK_SIZE, 5 * BLOCK_SIZE - 1);
        rb.addRange(10 * BLOCK_SIZE, 13 * BLOCK_SIZE - 7);
        rb.add(17 * BLOCK_SIZE + 10);
        rb.addRange(20 * BLOCK_SIZE + 10, 21 * BLOCK_SIZE - 20);
        final RspBitmap rb2 = new RspBitmap();
        // start and end neither matches rb's keys.
        rb2.addRange(6 * BLOCK_SIZE, 11 * BLOCK_SIZE - 7);
        rb.andEquals(rb2);
        assertEquals(rb.getCardinality(), 11 * BLOCK_SIZE - 7 - 10 * BLOCK_SIZE + 1);
    }

    @Test
    public void testSubsetOf2() {
        final RspBitmap rb1 = new RspBitmap();
        rb1.addRange(2 * BLOCK_SIZE + 8, 2 * BLOCK_SIZE + 20);
        final RspBitmap rb2 = new RspBitmap();
        rb2.addRange(2 * BLOCK_SIZE + 9, 2 * BLOCK_SIZE + 22);
        final boolean r = rb1.subsetOf(rb2);
        assertFalse(r);
    }

    @Test
    public void testSubsetOf3() {
        for (int j = -1; j <= 1; j += 2) {
            final RspBitmap rb1 = new RspBitmap();
            final RspBitmap rb2 = new RspBitmap();
            for (int i = 2; i < 10; ++i) {
                rb1.add(i * BLOCK_SIZE + 10);
                rb2.add((i + j) * BLOCK_SIZE + 10);
            }
            final String m = "j=" + j;
            boolean r = rb1.subsetOf(rb2);
            assertFalse(m, r);
            r = rb2.subsetOf(rb1);
            assertFalse(m, r);
        }
    }

    @Test
    public void testSubsetOf4() {
        final RspBitmap rb1 = new RspBitmap();
        final RspBitmap rb2 = new RspBitmap();
        rb1.addRange(BLOCK_SIZE, 7 * BLOCK_SIZE);
        rb2.addRange(BLOCK_SIZE, 6 * BLOCK_SIZE);
        assertFalse(rb1.subsetOf(rb2));
        assertTrue(rb2.subsetOf(rb1));
    }

    @Test
    public void testSubsetOf5() {
        final RspBitmap rb1 = new RspBitmap();
        final RspBitmap rb2 = new RspBitmap();
        rb1.addRange(BLOCK_SIZE, 7 * BLOCK_SIZE - 1);
        rb2.addRange(BLOCK_SIZE, 7 * BLOCK_SIZE - 1);
        assertTrue(rb2.subsetOf(rb1));
        rb1.add(8 * BLOCK_SIZE + 10);
        assertTrue(rb2.subsetOf(rb1));
        assertFalse(rb1.subsetOf(rb2));
    }

    @Test
    public void testOr2() {
        final RspBitmap r1 = new RspBitmap();
        final RspBitmap r2 = new RspBitmap();
        r1.addRange(BLOCK_SIZE, 2 * BLOCK_SIZE - 1);
        final long s1 = 2 * BLOCK_SIZE;
        final long e1 = s1 + BLOCK_LAST / 2;
        r1.addRange(s1, e1);
        assertEquals(BLOCK_SIZE + e1 - s1 + 1, r1.getCardinality());
        final long s2 = e1 + 1;
        final long e2 = s1 + BLOCK_LAST;
        r2.addRange(s2, e2);
        r1.orEquals(r2);
        r1.validate();
        assertEquals(2 * BLOCK_SIZE, r1.getCardinality());
    }

    @Test
    public void testLast() {
        final RspBitmap r = new RspBitmap();
        r.addRange(0, 1599);
        assertEquals(1599, r.last());
        assertEquals(r.last(), r.getCardinality() - 1);
        final RspBitmap r2 = new RspBitmap();
        r2.addRange(1600, 1600);
        r.orEquals(r2);
        r.validate();
        assertEquals(1600, r.last());
        assertEquals(r.last(), r.getCardinality() - 1);
        final RspBitmap r3 = new RspBitmap();
        r3.addRange(1601, 1601);
        r.orEquals(r3);
        r.validate();
        assertEquals(1601, r.last());
        assertEquals(r.last(), r.getCardinality() - 1);
    }

    @Test
    public void testAndNotEquals2() {
        final long[] vs = new long[] {2 * BLOCK_SIZE - 2, 4 * BLOCK_SIZE - 2, 19 * BLOCK_SIZE + 2};
        for (long v : vs) {
            final RspBitmap rb1 = new RspBitmap();
            final RspBitmap rb2 = new RspBitmap();
            final long start = BLOCK_SIZE;
            final long end = 20 * BLOCK_SIZE - 1;
            rb1.addRange(start, end);
            final long startCard = end - start + 1;
            assertEquals(startCard, rb1.getCardinality());
            rb2.add(v);
            rb1.andNotEquals(rb2);
            rb1.validate();
            assertEquals(startCard - 1, rb1.getCardinality());
        }
    }

    @Test
    public void testAndNotEquals3() {
        final RspBitmap rb1 = new RspBitmap();
        final RspBitmap rb2 = new RspBitmap();
        final long start = 2 * BLOCK_SIZE + 3;
        final long end = 6 * BLOCK_SIZE + 4;
        rb1.addRange(start, end);
        assertEquals(end - start + 1, rb1.getCardinality());
        rb2.addRange(BLOCK_SIZE, 4 * BLOCK_SIZE - 1);
        final RspBitmap rb3 = rb1.deepCopy();
        rb1.andNotEquals(rb2);
        rb1.validate();
        rb3.removeRange(BLOCK_SIZE, 4 * BLOCK_SIZE - 1);
        assertEquals(rb3, rb1);
    }

    @Test
    public void testAndNotEquals4() {
        final RspBitmap rb1 = new RspBitmap();
        final RspBitmap rb2 = new RspBitmap();
        rb1.addRange(2 * BLOCK_SIZE, 7 * BLOCK_SIZE - 1);
        rb2.addRange(3 * BLOCK_SIZE, 5 * BLOCK_SIZE - 1);
        final RspBitmap rb3 = rb1.deepCopy();
        assertEquals(5 * BLOCK_SIZE, rb1.getCardinality());
        rb1.andNotEquals(rb2);
        rb1.validate();
        rb3.removeRange(3 * BLOCK_SIZE, 5 * BLOCK_SIZE - 1);
        assertEquals(rb3, rb1);
    }

    @Test
    public void testAndNotEquals5() {
        final RspBitmap rb1 = new RspBitmap();
        final RspBitmap rb2 = new RspBitmap();
        rb1.addRange(2 * BLOCK_SIZE, 9 * BLOCK_SIZE - 1);
        rb2.add(3 * BLOCK_SIZE);
        rb2.add(5 * BLOCK_SIZE);
        rb1.andNotEquals(rb2);
        rb1.validate();
        assertEquals(7 * BLOCK_SIZE - 2, rb1.getCardinality());
    }

    @Test
    public void testAndEquals3() {
        final RspBitmap rb1 = new RspBitmap();
        final RspBitmap rb2 = new RspBitmap();
        rb1.addRange(2 * BLOCK_SIZE, 9 * BLOCK_SIZE - 1);
        assertEquals(7 * BLOCK_SIZE, rb1.getCardinality());
        rb2.addRange(3 * BLOCK_SIZE + 1, 3 * BLOCK_SIZE + 3);
        rb1.andEquals(rb2);
        rb1.validate();
        assertEquals(3, rb1.getCardinality());
    }

    @Test
    public void testOrEquals() {
        final RspBitmap rb1 = new RspBitmap();
        final RspBitmap rb2 = new RspBitmap();
        rb1.addRange(BLOCK_SIZE, BLOCK_SIZE + 2);
        rb2.add(2 * BLOCK_SIZE);
        rb2.add(3 * BLOCK_SIZE);
        rb2.add(4 * BLOCK_SIZE);
        rb2.add(5 * BLOCK_SIZE);
        rb1.orEquals(rb2);
        rb1.validate();
        assertEquals(7, rb1.getCardinality());
    }

    @Test
    public void testOrEquals2() {
        final int len = 6;
        for (int dend1 = BLOCK_LAST - 1; dend1 <= BLOCK_LAST; ++dend1) {
            final String m = "dend1==" + dend1;
            final RspBitmap rb1 = new RspBitmap();
            // do it both for containers and full block spans
            for (int i = len + 1; i <= 2 * len; ++i) {
                final long start = i * BLOCK_SIZE;
                final long end = start + dend1;
                rb1.addRange(start, end);
            }
            for (int s = 0; s <= 2 * len + 1; ++s) {
                final String m2 = m + " && s==" + s;
                for (int dend2 = BLOCK_LAST - 1; dend2 <= BLOCK_LAST; ++dend2) {
                    final String m3 = m2 + " && dend2==" + dend2;
                    for (int dstart2 = 0; dstart2 <= 1; ++dstart2) {
                        final String m4 = m3 + " && dstart2==" + dstart2;
                        final RspBitmap rbBoth = rb1.deepCopy();
                        final RspBitmap rb2 = new RspBitmap();
                        for (int j = s; j <= s + len; ++j) {
                            final long start = j * BLOCK_SIZE + dstart2;
                            final long end = j * BLOCK_SIZE + dend2;
                            rb2.addRange(start, end);
                            rbBoth.addRange(start, end);
                            final String m5 = m4 + " && j==" + j;
                            rb2.validate(m5);
                            rbBoth.validate(m5);
                        }
                        final RspBitmap rbCopy = rb1.deepCopy();
                        rbCopy.orEquals(rb2);
                        rbCopy.validate(m4);
                        assertEquals(m4, rbBoth, rbCopy);
                    }
                }
            }
        }
    }

    @Test
    public void testOrEquals3() {
        final RspBitmap rb1 = new RspBitmap();
        final RspBitmap rb2 = new RspBitmap();
        rb1.add(BLOCK_SIZE + 2);
        rb1.addRange(10 * BLOCK_SIZE, 11 * BLOCK_SIZE - 1);
        rb2.addRange(2 * BLOCK_SIZE, 3 * BLOCK_SIZE - 1);
        rb1.orEquals(rb2);
        rb1.validate();
        assertEquals(1 + 2 * BLOCK_SIZE, rb1.getCardinality());
    }

    @Test
    public void testAndNotEquals6() {
        final int len = 6;
        for (int dend1 = BLOCK_LAST - 1; dend1 <= BLOCK_LAST; ++dend1) {
            final String m = "dend1==" + dend1;
            final RspBitmap rb1 = new RspBitmap();
            // do it both for containers and full block spans
            for (int i = len + 1; i <= 2 * len; ++i) {
                final long start = i * BLOCK_SIZE;
                final long end = start + dend1;
                rb1.addRange(start, end);
            }
            for (int s = 0; s <= 2 * len + 1; ++s) {
                final String m2 = m + " && s==" + s;
                for (int dend2 = BLOCK_LAST - 1; dend2 <= BLOCK_LAST; ++dend2) {
                    final String m3 = m2 + " && dend2==" + dend2;
                    for (int dstart2 = 0; dstart2 <= 1; ++dstart2) {
                        final String m4 = m3 + " && dstart2==" + dstart2;
                        final RspBitmap rbBoth = rb1.deepCopy();
                        final RspBitmap rb2 = new RspBitmap();
                        for (int j = s; j <= s + len; ++j) {
                            final long start = j * BLOCK_SIZE + dstart2;
                            final long end = j * BLOCK_SIZE + dend2;
                            rb2.addRange(start, end);
                            rbBoth.removeRange(start, end);
                            final String m5 = m4 + " && j==" + j;
                            rb2.validate(m5);
                            rbBoth.validate(m5);
                        }
                        final RspBitmap rbCopy = rb1.deepCopy();
                        rbCopy.andNotEquals(rb2);
                        rbCopy.validate(m4);
                        assertEquals(m4, rbBoth, rbCopy);
                    }
                }
            }
        }
    }

    @Test
    public void testUpdate() {
        randomizedTest(RspBitmapTest::doTestUpdate);
    }

    private static void doTestUpdate(final int seed) {
        final String pfxMsg = "doTestUpdate seed=" + seed;
        final Random r = new Random(seed);
        final int count = 100000;
        final int clusterWidth = 150;
        final int jumpOneIn = 50;
        final RspBitmap rb1 =
                populateRandom(pfxMsg, r, count, 1, 10 * count, clusterWidth, jumpOneIn);
        final int steps = 300;
        for (int i = 0; i < steps; ++i) {
            final RspBitmap rbPlus = populateRandom(pfxMsg, r, count / 10, 1, 10 * count, clusterWidth, jumpOneIn);
            final RspBitmap rbMinus = populateRandom(pfxMsg, r, count / 10, 1, 10 * count, clusterWidth, jumpOneIn);
            rbPlus.andNotEquals(rbMinus);
            final String m = pfxMsg + ", i=" + i;
            final RspBitmap preCheck = rbPlus.deepCopy().andEquals(rbMinus);
            assertTrue(m, preCheck.isEmpty());
            rbPlus.validate();
            rb1.andNotEquals(rbMinus);
            rb1.orEquals(rbPlus);
            rb1.validate(m);
            RspBitmap rbCheck = rb1.deepCopy();
            rbCheck.andEquals(rbMinus);
            assertTrue(m, rbCheck.isEmpty());
            rbCheck = rb1.deepCopy();
            rbCheck.andEquals(rbPlus);
            assertEquals(m, rbPlus, rbCheck);
        }
    }

    @Test
    public void testRangeBatchIteratorWithBigBitmaps() {
        randomizedTest(RspBitmapTest::doTestRangeBatchIteratorWithBigBitmaps);
    }

    private static void doTestRangeBatchIteratorWithBigBitmaps(final int seed) {
        final String pfxMsg = "doTesRangeBatchIterator seed==" + seed;
        final Random r = new Random(seed);
        final int count = 10000;
        final int clusterWidth = 150;
        final int jumpOneIn = 50;
        final int steps = 300;
        for (int i = 0; i < steps; ++i) {
            final RspBitmap rb =
                    populateRandom(pfxMsg, r, count, 1, 10 * count, clusterWidth, jumpOneIn);
            final WritableLongChunk<OrderedRowKeyRanges> chunk =
                    WritableLongChunk.makeWritableChunk(2 * count);
            final RspRangeBatchIterator rbit = rb.getRangeBatchIterator(0, rb.getCardinality());
            final int ret = rbit.fillRangeChunk(chunk, 0);
            final RspRangeIterator rit = rb.getRangeIterator();
            int n = 0;
            final String m = pfxMsg + " && i=" + i;
            while (rit.hasNext()) {
                rit.next();
                final String m2 = m + " && n=" + n;
                assertEquals(m2, rit.start(), chunk.get(n));
                assertEquals(m2, rit.end(), chunk.get(n + 1));
                n += 2;
            }
            assertEquals(m, n, 2 * ret);
        }
    }

    @Test
    public void testRspRangeBatchIterator0() {
        final RspBitmap rb = new RspBitmap();
        final int k = 3;
        // each one of these adds should be adding on a separate span.
        final long min;
        rb.append(min = k * BLOCK_SIZE + BLOCK_LAST);
        rb.appendRange((k + 1) * BLOCK_SIZE, (k + 1) * BLOCK_SIZE + BLOCK_LAST);
        final long max;
        rb.append(max = (k + 2) * BLOCK_SIZE);
        final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(2);
        final RspRangeBatchIterator rit = rb.getRangeBatchIterator(0, rb.getCardinality());
        final int r = rit.fillRangeChunk(chunk, 0);
        assertEquals(1, r);
        assertEquals(min, chunk.get(0));
        assertEquals(max, chunk.get(1));
    }

    @Test
    public void testRspRangeBatchIteratorWithInterestingSpanBoundaries() {
        randomizedTest(RspBitmapTest::doTestRspRangeBatchIteratorWithInterestingSpanBoundaries);
    }

    private static void checkRangeBatchIterator(final RspBitmap rb, final String m) {
        final RspRangeBatchIterator rit = rb.getRangeBatchIterator(0, rb.getCardinality());
        final RspRangeIterator it = rb.getRangeIterator();
        final int sz = 8;
        final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(sz);
        int batch = 0;
        int rangeRank = 0;
        while (rit.hasNext()) {
            int n = rit.fillRangeChunk(chunk, 0);
            final String m2 = m + " && batch==" + batch + " && rangeRank==" + rangeRank;
            for (int r = 0; r < n; ++r) {
                final String m3 = m2 + " && r==" + r;
                assertTrue(m3, it.hasNext());
                it.next();
                final long is = it.start();
                final long ie = it.end();
                final long cs = chunk.get(2 * r);
                final long ce = chunk.get(2 * r + 1);
                assertEquals(m3, is, cs);
                assertEquals(m3, ie, ce);
                ++rangeRank;
            }
            ++batch;
        }
        assertFalse(it.hasNext());
    }

    private static void doTestRspRangeBatchIteratorWithInterestingSpanBoundaries(final int seed) {
        final Random r = new Random(seed);
        final int nblocks = 60;
        final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.75f, 3);
        final String m = "seed=" + seed;
        checkRangeBatchIterator(rb, m);
    }

    @Test
    public void testContains() {
        final long[] vs =
                new long[] {3, 4, 5, 8, 10, 12, 29, 31, 44, 59, 60, 61, 72, 65537, 65539, 65536 * 3, 65536 * 3 + 5};
        final RspBitmap rb = new RspBitmap();
        for (long v : vs) {
            rb.add(v);
        }
        for (long v : vs) {
            assertTrue(rb.contains(v));
        }
    }

    @Test
    public void testRangesCountUpperBound() {
        final RspBitmap rb = new RspBitmap();
        final int max = 4096; // max size of an RB ArrayContainer, sadly private on org.roaringbitmap pkg.
        final int initialCount = max - 1;
        for (int i = 0; i < 2 * initialCount; i += 2) {
            rb.add(i); // singleton ranges.
        }
        assertTrue(max - 1 <= rb.rangesCountUpperBound());
        for (int i = 2 * initialCount; i < BLOCK_SIZE; i += 2) {
            rb.add(i);
            assertTrue(rb.getCardinality() <= rb.rangesCountUpperBound());
        }
        long prevRanges = rb.getCardinality();
        for (int i = 1; i < BLOCK_SIZE; i += 2) {
            rb.add(i);
            --prevRanges;
            assertTrue(prevRanges <= rb.rangesCountUpperBound());
        }
        assertEquals(BLOCK_SIZE, rb.getCardinality());
    }

    @FunctionalInterface
    private interface BuildOp {
        void bop(final RspBitmap out, final RspBitmap in);
    }

    private static void doTestBatchBuilderOp(final int seed, final String name, final BuildOp bop) {
        final String m = "name==" + name + " && seed==" + seed;
        final Random r = new Random(seed);
        final int count = BLOCK_SIZE * 3;
        final int clusterWidth = 150;
        final int jumpOneIn = 50;
        final int steps = 300;
        for (int i = 0; i < steps; ++i) {
            final RspBitmap rb =
                    populateRandom(m, r, count, 1, 10 * count, clusterWidth, jumpOneIn);
            final RspBitmap result = new RspBitmap();
            bop.bop(result, rb);
            result.finishMutations();
            final String m2 = m + " && i==" + i;
            assertEquals(m2, rb.getCardinality(), result.getCardinality());
            assertTrue(m2, rb.subsetOf(result));
        }
    }

    @Test
    public void testAddRanges() {
        randomizedTest(RspBitmapTest::doTestAddRanges);
    }

    private static void doTestAddRanges(final int seed) {
        doTestBatchBuilderOp(seed, "appendRanges", (out, in) -> out.addRangesUnsafeNoWriteCheck(in.ixRangeIterator()));
    }

    @Test
    public void testRangeConstructor() {
        final long[] boundaries =
                {BLOCK_SIZE, 2 * BLOCK_SIZE, 3 * BLOCK_SIZE, 4 * BLOCK_SIZE, 8 * BLOCK_SIZE, 9 * BLOCK_SIZE};
        for (int i = 0; i < boundaries.length; ++i) {
            for (int j = i; j < boundaries.length; ++j) {
                final long ki = boundaries[i];
                final long kj = boundaries[j];
                for (int di = -2; di <= 2; ++di) {
                    for (int dj = -2; dj <= 2; ++dj) {
                        final long s = ki + di;
                        final long e = kj + dj;
                        if (s < 0 || e < s) {
                            continue;
                        }
                        final RspBitmap rb = new RspBitmap(s, e);
                        final String m = "s==" + s + " && e==" + e;
                        rb.validate(m);
                        assertEquals(e - s + 1, rb.getCardinality());
                    }
                }
            }
        }
    }

    @Test
    public void testAddRangeRegression0() {
        final RspBitmap rb = new RspBitmap(0, BLOCK_LAST);
        rb.add(BLOCK_SIZE + 10);
        rb.add(2 * BLOCK_SIZE);
        rb.validate();
        rb.addRange(BLOCK_SIZE, 4 * BLOCK_SIZE - 1);
        rb.validate();
    }

    @Test
    public void testAddRangeRegression1() {
        final RspBitmap rb = new RspBitmap(0, BLOCK_LAST);
        rb.add(BLOCK_SIZE);
        rb.addRange(2 * BLOCK_SIZE, 7 * BLOCK_SIZE);
        rb.addRange(BLOCK_SIZE + 1, 3 * BLOCK_SIZE + 1);
        rb.validate();
    }

    @Test
    public void testSubrangeByValueRegression0() {
        final RspBitmap rb = new RspBitmap(BLOCK_SIZE, 2 * BLOCK_SIZE + BLOCK_LAST);
        final RspBitmap rb2 = rb.subrangeByValue(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + BLOCK_LAST);
        rb2.validate();
    }

    private static void addRangeRandom(final int spans, final Random r) {
        final RspBitmap rb = new RspBitmap();
        final int fullSz = spans * BLOCK_SIZE;
        int i = 0;
        while (rb.getCardinality() < fullSz) {
            final long start = r.nextInt(fullSz);
            final long len = r.nextInt(BLOCK_SIZE);
            final long end = Math.min(start + len - 1, fullSz - 1);
            if (end < start) {
                continue;
            }
            rb.addRange(start, end);
            rb.validate("i==" + i);
            ++i;
        }
    }

    @Test
    public void testAddRangeRandom() {
        randomizedTest(RspBitmapTest::doTestAddRangeRandom);
    }

    private static void doTestAddRangeRandom(final int seed) {
        final int spans = 3;
        final int iterations = 200;
        final Random r = new Random(seed);
        for (int iter = 0; iter < iterations; ++iter) {
            addRangeRandom(spans, r);
        }
    }

    @Test
    public void testForEachLongRangeCompactsRangesAcrossContainers() {
        final RspBitmap rb = new RspBitmap();
        rb.addRange(BLOCK_LAST, BLOCK_SIZE);
        rb.add(3 * BLOCK_SIZE - 1);
        rb.addRange(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + BLOCK_LAST);
        rb.add(4 * BLOCK_SIZE);
        final MutableInt rangeCount = new MutableInt(0);
        final RspBitmap rb2 = new RspBitmap();
        rb.forEachLongRange((final long start, final long end) -> {
            rangeCount.increment();
            rb2.addRange(start, end);
            return true;
        });
        assertEquals(2, rangeCount.intValue());
        assertEquals(rb, rb2);
    }

    @Test
    public void testAndEquals() {
        randomizedTest(RspBitmapTest::doTestAndEquals);
    }

    private static TLongSet intersect(final TLongSet s1, TLongSet s2) {
        final TLongHashSet ans = new TLongHashSet();
        s1.forEach((final long v) -> {
            if (s2.contains(v)) {
                ans.add(v);
            }
            return true;
        });
        return ans;
    }

    private static void doTestAndEquals(final int seed) {
        final Random r = new Random(seed);
        final int runs = 30;
        final int nblocks = 20;
        for (int run = 0; run < runs; ++run) {
            final String s = "run==" + run;
            final TLongSet set = new TLongHashSet();
            final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 100,
                    0, 0, set);
            final TLongSet set2 = new TLongHashSet();
            final RspBitmap rb2 = getRandomRspBitmap(nblocks, r, 0.90f, 100,
                    0, 0, set2);
            rb.andEquals(rb2);
            final TLongSet iset = intersect(set, set2);
            assertEquals(s, iset.size(), rb.getCardinality());
            iset.forEach((final long v) -> {
                assertTrue(s, rb.contains(v));
                return true;
            });
        }
    }

    @Test
    public void testRspSequentialBuilderAccumulatesFullBlockSpans() {
        final OrderedLongSetBuilderSequential b = new OrderedLongSetBuilderSequential();
        b.appendKey(3);
        b.appendRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        b.appendKey(2 * BLOCK_SIZE);
        b.appendRange(2 * BLOCK_SIZE + 1, 2 * BLOCK_SIZE + BLOCK_LAST);
        final RspBitmap rb = b.getRspBitmap();
        rb.validate();
        assertEquals(2 * BLOCK_SIZE + 1, rb.getCardinality());
    }

    @Test
    public void testRspSequentialBuilderAccumulatesFullBlockSpans2() {
        final OrderedLongSetBuilderSequential b = new OrderedLongSetBuilderSequential();
        b.appendKey(3);
        b.appendRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        b.appendKey(2 * BLOCK_SIZE);
        b.appendRange(2 * BLOCK_SIZE + 1, 3 * BLOCK_SIZE + BLOCK_LAST);
        b.appendKey(4 * BLOCK_SIZE);
        final RspBitmap rb = b.getRspBitmap();
        rb.validate();
        assertEquals(3 * BLOCK_SIZE + 2, rb.getCardinality());
    }

    @Test
    public void testGetKeysForPositions() {
        randomizedTest(RspBitmapTest::doTestGetKeysForPositions);
    }

    private static void doTestGetKeysForPositions(final int seed) {
        final Random r = new Random(seed);
        final int runs = 30;
        final int nblocks = 20;
        for (int run = 0; run < runs; ++run) {
            final String s = "run==" + run;
            final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 100,
                    0, 0);
            final long card = rb.getCardinality();
            final RspBitmap positions = new RspBitmap();
            for (long j = 0; j < card; ++j) {
                if (r.nextBoolean()) {
                    positions.appendUnsafe(j);
                }
            }
            positions.finishMutationsAndOptimize();
            final RspBitmap result = new RspBitmap();
            final LongConsumer lc = (final long v) -> result.appendUnsafe(v);
            rb.getKeysForPositions(positions.getIterator(), lc);
            result.finishMutationsAndOptimize();
            assertEquals(positions.getCardinality(), result.getCardinality());
            final RspIterator positionsIter = positions.getIterator();
            result.forEachLong((final long v) -> {
                assertTrue(positionsIter.hasNext());
                final long pos = positionsIter.nextLong();
                assertEquals(rb.get(pos), v);
                return true;
            });
            assertFalse(positionsIter.hasNext());
        }
    }

    @Test
    public void test32kBoundaries() {
        final long start = 32 * 1024 - 1;
        final long end = 32 * 1024 + 10;
        RspBitmap rb = new RspBitmap(start + 1, end);
        rb.add(start);
        RspRangeIterator it = rb.getRangeIterator();
        assertTrue(it.hasNext());
        it.next();
        long s = it.start();
        long e = it.end();
        assertEquals(start, s);
        assertEquals(end, e);
        assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorAdvanceRegression0() {
        final RspBitmap rb = new RspBitmap();
        rb.add(BLOCK_SIZE + BLOCK_LAST - 3);
        // 1 short of the last possible element on the block, on a range of its own, and not the first element.
        rb.add(BLOCK_SIZE + BLOCK_LAST - 1);
        rb.add(2 * BLOCK_SIZE);
        final RspRangeIterator rangeIter = rb.getRangeIterator();
        assertTrue(rangeIter.advance(BLOCK_SIZE + BLOCK_LAST));
        assertEquals(2 * BLOCK_SIZE, rangeIter.start());
        assertFalse(rangeIter.hasNext());
    }

    @Test
    public void testTwoElementContainerIter() {
        RspBitmap rb = new RspBitmap();
        rb = rb.add(4);
        rb = rb.add(6);
        final RspRangeIterator rit = rb.getRangeIterator();
        assertTrue(rit.hasNext());
        rit.next();
        assertEquals(4, rit.start());
        assertEquals(4, rit.end());
        assertTrue(rit.hasNext());
        rit.next();
        assertEquals(6, rit.start());
        assertEquals(6, rit.end());
        assertFalse(rit.hasNext());
    }

    @Test
    public void testSingleRangeContainerForEach() {
        RspBitmap rb = new RspBitmap();
        // { 1073741817-1073741823,1073741825-1073741826 }
        final long start1 = 1073741817L;
        final long end1 = 1073741823L;
        rb = rb.addRange(start1, end1);
        final long start2 = 1073741825L;
        final long end2 = 1073741826L;
        rb = rb.addRange(start2, end2);
        final MutableLong prev = new MutableLong(-1);
        final LongAbortableConsumer lac = (final long v) -> {
            final long p = prev.longValue();
            final String m = "p==" + p + " && v==" + v;
            assertTrue(m, v <= end2);
            assertTrue(m, p < v);
            if (p == -1) {
                assertEquals(m, start1, v);
            } else if (p == end1) {
                assertEquals(m, start2, v);
            } else {
                assertEquals(m, p + 1, v);
            }
            prev.setValue(v);
            return true;
        };
        rb.forEachLong(lac);
        final RspIterator rit = rb.getIterator();
        prev.setValue(-1);
        rit.forEachLong(lac);
    }

    @Test
    public void testSingleRangeContainerIter() {
        RspBitmap rb = new RspBitmap();
        // { 1073741817-1073741823,1073741825-1073741826 }
        final long start1 = 1073741817L;
        final long end1 = 1073741823L;
        rb = rb.addRange(start1, end1);
        final long start2 = 1073741825L;
        final long end2 = 1073741826L;
        rb = rb.addRange(start2, end2);
        long p = -1;
        final RspIterator it = rb.getIterator();
        int count = 0;
        while (it.hasNext()) {
            ++count;
            final long v = it.nextLong();
            final String m = "p==" + p + " && v==" + v;
            assertTrue(m, v <= end2);
            assertTrue(m, p < v);
            if (p == -1) {
                assertEquals(m, start1, v);
            } else if (p == end1) {
                assertEquals(m, start2, v);
            } else {
                assertEquals(m, p + 1, v);
            }
            p = v;
        }
        assertEquals(count, rb.getCardinality());
    }

    @Test
    public void testInvertCase0() {
        RspBitmap rb = new RspBitmap();
        rb = rb.addRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        rb = rb.add(2 * BLOCK_SIZE + BLOCK_LAST);
        rb = rb.addRange(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + BLOCK_LAST);
        final OrderedLongSet.BuilderSequential b = new OrderedLongSetBuilderSequential();
        rb.invert(b, new WritableRowSetImpl(rb).rangeIterator(), rb.getCardinality());
        final OrderedLongSet timpl = b.getOrderedLongSet();
        assertEquals(rb.getCardinality(), timpl.ixCardinality());
        assertTrue(timpl.ixContainsRange(0, rb.getCardinality() - 1));
    }

    @Test
    public void testReverseIteratorSkipsBlock() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.addRange(0 * BLOCK_SIZE + 5, 0 * BLOCK_SIZE + 10);
        rb = rb.addRange(2 * BLOCK_SIZE + 5, 2 * BLOCK_SIZE + 10);
        final RspReverseIterator it = rb.getReverseIterator();
        assertTrue(it.advance(1 * BLOCK_SIZE + 15));
        assertEquals("it.next() == 10", it.current(), 10);
    }

    @Test
    public void testReverseIteratorTwoFullBlockSpans() {
        RspBitmap rb = RspBitmap.makeEmpty();
        final long[][] ranges =
                new long[][] {new long[] {0, BLOCK_LAST}, new long[] {2 * BLOCK_SIZE, 2 * BLOCK_SIZE + BLOCK_LAST}};
        rb = rb.addRange(0, BLOCK_LAST);
        rb = rb.addRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + BLOCK_LAST);
        final RspReverseIterator it = rb.getReverseIterator();
        for (int i = ranges.length - 1; i >= 0; --i) {
            long[] range = ranges[i];
            for (long v = range[1]; v >= range[0]; --v) {
                assertTrue(it.hasNext());
                it.next();
                assertEquals(v, it.current());
            }
        }
    }

    // prob.length + 1 == blocks.length
    private static void randomBlocks(
            final RspBitmap[] rbs, final Random rand, final float[] probs, final RspBitmap[] blocks) {
        BLOCKS: for (int i = 0; i < rbs.length; ++i) {
            final long blockOffset = i * BLOCK_SIZE;
            for (int b = 0; b < probs.length; ++b) {
                if (rand.nextFloat() <= probs[b]) {
                    rbs[i] = blocks[b].applyOffsetOnNew(blockOffset);
                    continue BLOCKS;
                }
            }
            rbs[i] = blocks[probs.length].applyOffsetOnNew(blockOffset);
        }
    }

    private static RspBitmap foldOr(final RspBitmap[] rbs) {
        RspBitmap accum = RspBitmap.makeEmpty();
        for (RspBitmap rb : rbs) {
            accum.orEqualsUnsafeNoWriteCheck(rb);
        }
        accum.finishMutations();
        return accum;
    }

    private static void someRandomBlocks(final Random rand, final RspBitmap[] rsps) {
        final float[] probs = {0.33F, 0.33F, 0.33F};
        final RspBitmap[] blocks = new RspBitmap[] {
                RspBitmap.makeSingleRange(0, BLOCK_LAST),
                RspBitmap.makeSingleRange(0, BLOCK_LAST / 2),
                RspBitmap.makeSingleRange(BLOCK_LAST / 2 + 1, BLOCK_LAST),
                RspBitmap.makeSingleRange(BLOCK_LAST / 3, 2 * BLOCK_LAST / 3),
        };
        randomBlocks(rsps, rand, probs, blocks);
    }

    private static void testSimilarOp(final Random rand, final int blockCount, BiConsumer<RspBitmap, RspBitmap> op) {
        final RspBitmap[] rsps0 = new RspBitmap[blockCount];
        someRandomBlocks(rand, rsps0);
        RspBitmap rsp0 = foldOr(rsps0);
        final RspBitmap[] rsps1 = new RspBitmap[blockCount];
        someRandomBlocks(rand, rsps1);
        RspBitmap rsp1 = foldOr(rsps1);
        for (int i = 0; i < blockCount; ++i) {
            op.accept(rsps0[i], rsps1[i]);
            rsps0[i].finishMutations();
        }
        RspBitmap resultByBlock = foldOr(rsps0);
        op.accept(rsp0, rsp1);
        rsp0.finishMutations();
        assertTrue(resultByBlock.equals(rsp0));
    }

    @Test
    public void testSimilarRspsAndNot() {
        final Random rand = new Random(seed0);
        final int blockCount = 1024;
        testSimilarOp(rand, blockCount,
                (final RspBitmap rb1, final RspBitmap rb2) -> rb1.andNotEqualsUnsafeNoWriteCheck(rb2));
    }

    @Test
    public void testSimilarRspsAnd() {
        final Random rand = new Random(seed0 + 1);
        final int blockCount = 1024;
        testSimilarOp(rand, blockCount,
                (final RspBitmap rb1, final RspBitmap rb2) -> rb1.andEqualsUnsafeNoWriteCheck(rb2));
    }

    @Test
    public void testRemoveRangeManyBlocks() {
        final Random rand = new Random(seed0 + 2);
        final int blockCount = 1024;
        final RspBitmap[] rsps0 = new RspBitmap[blockCount];
        someRandomBlocks(rand, rsps0);
        final RspBitmap rsp0 = foldOr(rsps0);
        final float prob = 0.25F;
        for (int i = 0; i < blockCount; ++i) {
            for (int j = i; j < blockCount; ++j) {
                if (rand.nextFloat() > prob) {
                    continue;
                }
                // we will remove blocks i to j, inclusive.
                RspBitmap rsp = rsp0.deepCopy();
                rsp.removeRangeUnsafeNoWriteCheck(i * BLOCK_SIZE, j * BLOCK_SIZE + BLOCK_LAST);
                rsp.finishMutations();
                RspBitmap resultByBlock = RspBitmap.makeEmpty();
                for (int k = 0; k < blockCount; ++k) {
                    if (i <= k && k <= j) {
                        continue;
                    }
                    resultByBlock.orEqualsUnsafeNoWriteCheck(rsps0[k]);
                }
                resultByBlock.finishMutations();
                assertTrue("i==" + i + " && j==" + j, resultByBlock.equals(rsp));
            }
        }
    }

    @Test
    public void testRemoveRanges() {
        final Random rand = new Random(seed0);
        final int blockCount = 1024;
        testSimilarOp(rand, blockCount,
                (final RspBitmap rb1, final RspBitmap rb2) -> rb1
                        .removeRangesUnsafeNoWriteCheck(rb2.ixRangeIterator()));
    }

    @Test
    public void testRemoveRangesEndsUpEmptyEarly() {
        final RspBitmap rb0 = RspBitmap.makeSingleRange(12, 15);
        RspBitmap rb1 = RspBitmap.makeEmpty();
        rb1 = rb1.addRange(11, 20);
        rb1 = rb1.addRange(25, 40);
        rb0.removeRangesUnsafeNoWriteCheck(rb1.ixRangeIterator());
        rb0.finishMutations();
        assertTrue(rb0.isEmpty());
    }

    @Test
    public void testOrManyNonOverlappingContainers() {
        final float[] probs = {0.5F, 0.33F, 0.33F, 0.33F};
        final RspBitmap[] blocks = new RspBitmap[] {
                RspBitmap.makeEmpty(),
                RspBitmap.makeSingleRange(0, BLOCK_LAST),
                RspBitmap.makeSingleRange(0, BLOCK_LAST / 2),
                RspBitmap.makeSingleRange(BLOCK_LAST / 2 + 1, BLOCK_LAST),
                RspBitmap.makeSingleRange(BLOCK_LAST / 3, 2 * BLOCK_LAST / 3),
        };
        final int blockCount = 1024;
        final Random rand = new Random(seed0 + 4);
        final RspBitmap[] rsps0 = new RspBitmap[blockCount];
        randomBlocks(rsps0, rand, probs, blocks);
        RspBitmap rsp0 = foldOr(rsps0);
        final RspBitmap[] rsps1 = new RspBitmap[blockCount];
        randomBlocks(rsps1, rand, probs, blocks);
        RspBitmap rsp1 = foldOr(rsps1);
        for (int i = 0; i < blockCount; ++i) {
            rsps0[i].orEqualsUnsafeNoWriteCheck(rsps1[i]);
            rsps0[i].finishMutations();
        }
        RspBitmap resultByBlock = foldOr(rsps0);
        rsp0.orEqualsUnsafeNoWriteCheck(rsp1);
        rsp0.finishMutations();
        assertTrue(resultByBlock.equals(rsp0));
    }

    @Test
    public void testOrEqualsRegression0() {
        RspBitmap r0 = RspBitmap.makeEmpty();
        r0 = r0.addRange(BLOCK_SIZE, 5 * BLOCK_SIZE - 1);
        r0 = r0.addRange(7 * BLOCK_SIZE, 10 * BLOCK_SIZE - 1);

        RspBitmap r1 = RspBitmap.makeEmpty();
        for (int k = 1; k <= 10; ++k) {
            r1 = r1.add(k * BLOCK_SIZE);
        }

        RspBitmap r3 = r0.orEquals(r1);
        r3.validate();
    }

    @Test
    public void testAppendMergesAdjacent() {
        RspBitmap r0 = RspBitmap.makeEmpty();
        r0 = r0.addRange(3, 4);
        r0 = r0.addRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        r0 = r0.addRange(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + BLOCK_LAST);
        RspBitmap r1 = RspBitmap.makeSingleRange(4 * BLOCK_SIZE, 4 * BLOCK_SIZE + BLOCK_LAST);
        final long cardBefore = r0.getCardinality();
        final boolean worked = r0.tryAppendShiftedUnsafeNoWriteCheck(0, r1, false);
        assertTrue(worked);
        r0.finishMutationsAndOptimize();
        r0.validate();
        final long cardAfter = r0.getCardinality();
        assertEquals(cardBefore + BLOCK_SIZE, cardAfter);
    }

    @Test
    public void testAppendMergesAdjacentFullBlocks() {
        RspBitmap r0 = RspBitmap.makeEmpty();
        r0 = r0.addRange(3, 4);
        r0 = r0.addRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        r0 = r0.addRange(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + BLOCK_LAST);
        RspBitmap r1 = RspBitmap.makeSingleRange(5 * BLOCK_SIZE, 5 * BLOCK_SIZE + BLOCK_LAST);
        r1 = r1.append(7 * BLOCK_SIZE + 3);
        final long cardBefore = r0.getCardinality();
        final boolean worked = r0.tryAppendShiftedUnsafeNoWriteCheck(0, r1, false);
        assertTrue(worked);
        r0.finishMutationsAndOptimize();
        r0.validate();
        final long cardAfter = r0.getCardinality();
        assertEquals(cardBefore + BLOCK_SIZE + 1, cardAfter);
    }

    @Test
    public void testAppendShifted() {
        RspBitmap r0 = RspBitmap.makeSingleRange(20, 39);
        RspBitmap r1 = RspBitmap.makeSingleRange(21, 40);
        r0.appendShiftedUnsafeNoWriteCheck(BLOCK_SIZE, r1, false);
        r0.finishMutationsAndOptimize();
        assertEquals(40, r0.getCardinality());
        assertTrue(r0.containsRange(20, 39));
        assertTrue(r0.containsRange(BLOCK_SIZE + 21, BLOCK_SIZE + 40));
    }

    @Test
    public void testOffsetCtorWithNullSpans() {
        RspBitmap r0 = RspBitmap.makeSingleRange(10, 19);
        r0 = r0.add(BLOCK_SIZE + 10);
        r0 = r0.addRange(2 * BLOCK_SIZE + 10, 2 * BLOCK_SIZE + 19);
        r0 = r0.add(3 * BLOCK_SIZE + 10);
        RspBitmap r1 = r0.make(r0, 1, 0, 3, 1);
        r1.validate();
        assertEquals(12L, r1.getCardinality());

        RspBitmap r2 = r0.make(r0, 3, 0, 3, 0);
        r2.validate();
        assertEquals(1L, r2.getCardinality());

        RspBitmap r3 = r0.make(r0, 0, 0, 3, 0);
        r2.validate();
        assertEquals(r0.getCardinality(), r3.getCardinality());
        assertEquals(r0, r3);
    }

    @Test
    public void testRangeIteratorWithNullSpans() {
        RspBitmap r0 = RspBitmap.makeSingleRange(10, 19);
        r0 = r0.add(BLOCK_SIZE + 10);
        r0 = r0.addRange(2 * BLOCK_SIZE + 10, 2 * BLOCK_SIZE + 19);
        r0 = r0.add(3 * BLOCK_SIZE + 10);
        try (RspRangeIterator it = r0.getRangeIterator()) {
            assertTrue(it.hasNext());
            it.next();
            assertEquals(10L, it.start());
            assertEquals(19L, it.end());
            assertTrue(it.hasNext());
            it.next();
            assertEquals(BLOCK_SIZE + 10L, it.start());
            assertEquals(it.start(), it.end());
            assertTrue(it.hasNext());
            it.next();
            assertEquals(2 * BLOCK_SIZE + 10L, it.start());
            assertEquals(2 * BLOCK_SIZE + 19L, it.end());
            assertTrue(it.hasNext());
            it.next();
            assertEquals(3 * BLOCK_SIZE + 10L, it.start());
            assertEquals(it.start(), it.end());
            assertFalse(it.hasNext());
        }

        RspBitmap r1 = RspBitmap.makeSingleRange(10, 19);
        r1 = r1.add(BLOCK_SIZE + 10);
        r1 = r1.addRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + BLOCK_LAST);
        r1 = r1.add(3 * BLOCK_SIZE);
        try (RspRangeIterator it = r1.getRangeIterator()) {
            assertTrue(it.hasNext());
            it.next();
            assertEquals(10L, it.start());
            assertEquals(19L, it.end());
            assertTrue(it.hasNext());
            it.next();
            assertEquals(BLOCK_SIZE + 10L, it.start());
            assertEquals(it.start(), it.end());
            assertTrue(it.hasNext());
            it.next();
            assertEquals(2 * BLOCK_SIZE, it.start());
            assertEquals(2 * BLOCK_SIZE + BLOCK_LAST + 1, it.end());
            assertFalse(it.hasNext());
        }

        RspBitmap r2 = new RspBitmap();
        r2 = r2.addRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        r2 = r2.add(2 * BLOCK_SIZE + BLOCK_LAST);
        r2 = r2.addRange(3 * BLOCK_SIZE, 3 * BLOCK_SIZE + BLOCK_LAST);
        try (RspRangeIterator it = r2.getRangeIterator()) {
            assertTrue(it.hasNext());
            it.next();
            assertEquals(BLOCK_SIZE, it.start());
            assertEquals(BLOCK_SIZE + BLOCK_LAST, it.end());
            assertTrue(it.hasNext());
            it.next();
            assertEquals(2 * BLOCK_SIZE + BLOCK_LAST, it.start());
            assertEquals(3 * BLOCK_SIZE + BLOCK_LAST, it.end());
            assertFalse(it.hasNext());
        }
    }

    @Test
    public void testOffsetCtorCoverage() {
        RspBitmap r0 = RspBitmap.makeSingleRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        RspBitmap r1 = new RspBitmap(r0, 0, 0, 0, BLOCK_LAST);
        assertEquals(r0, r1);
    }

    @Test
    public void testSubsetOfRegression0() {
        RspBitmap r0 = RspBitmap.makeSingleRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        RspBitmap r1 = RspBitmap.makeSingleRange(BLOCK_SIZE + 3, BLOCK_SIZE + 4);
        // ensure cardinalities are compatible with subset.
        r1 = r1.appendRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + BLOCK_LAST);
        assertFalse(r0.subsetOf(r1));
    }

    @Test
    public void testFindNullSpanCoverage() {
        RspBitmap r0 = RspBitmap.makeSingleRange(BLOCK_SIZE, BLOCK_SIZE + 9);
        r0 = r0.add(2 * BLOCK_SIZE + 20);
        r0 = r0.addRange(3 * BLOCK_SIZE + 11, 3 * BLOCK_SIZE + 13);
        final long f0 = r0.find(2 * BLOCK_SIZE + 19);
        assertEquals(~10L, f0);
        final long f1 = r0.find(2 * BLOCK_SIZE + 20);
        assertEquals(10L, f1);
        final long f2 = r0.find(2 * BLOCK_SIZE + 21);
        assertEquals(~11L, f2);
    }

    @Test
    public void testSubsetOfNullSpanCoverage() {
        final long v1 = BLOCK_SIZE + 1;
        final long v2 = BLOCK_SIZE + 2;
        RspBitmap r1 = RspBitmap.makeSingle(v1);
        RspBitmap r2 = RspBitmap.makeSingle(v2);
        assertFalse(r1.subsetOf(r2));
        r2 = r2.add(v2 + 1);
        assertFalse(r1.subsetOf(r2));
        assertFalse(r2.subsetOf(r1));
    }

    @Test
    public void testOffsetCtorCoverage2() {
        RspBitmap r0 = RspBitmap.makeSingleRange(BLOCK_SIZE, BLOCK_SIZE + BLOCK_LAST);
        RspBitmap r1 = new RspBitmap(r0, 0, 10, 0, 10);
        r1.validate();
        assertEquals(1, r1.getCardinality());
        assertEquals(10L + BLOCK_SIZE, r1.first());

        r0 = RspBitmap.makeSingleRange(BLOCK_SIZE, 3 * BLOCK_SIZE);
        r1 = new RspBitmap(r0, 0, BLOCK_LAST, 1, 0);
        r1.validate();
        assertEquals(BLOCK_SIZE + 2, r1.getCardinality());

        r0 = RspBitmap.makeSingleRange(BLOCK_LAST, 2 * BLOCK_SIZE + BLOCK_LAST);
        r1 = new RspBitmap(r0, 0, 0, 1, 0);
        r1.validate();
        assertEquals(2, r1.getCardinality());

        r0 = RspBitmap.makeSingleRange(BLOCK_LAST, 3 * BLOCK_SIZE + BLOCK_LAST);
        r1 = new RspBitmap(r0, 0, 0, 1, BLOCK_SIZE);
        r1.validate();
        assertEquals(2 + BLOCK_SIZE, r1.getCardinality());
    }

    @Test
    public void testSubsetOfNullSpanCoverage2() {
        RspBitmap r0 = RspBitmap.makeSingle(BLOCK_SIZE + 10);
        r0 = r0.add(2 * BLOCK_SIZE + 20);
        RspBitmap r1 = RspBitmap.makeSingleRange(BLOCK_SIZE + 9, BLOCK_SIZE + 10);
        assertFalse(r1.subsetOf(r0));
    }

    @Test
    public void testContainsRangeNullSpanCoverage() {
        RspBitmap r0 = RspBitmap.makeSingle(BLOCK_SIZE + 10);
        r0 = r0.add(2 * BLOCK_SIZE);
        assertFalse(r0.containsRange(BLOCK_SIZE + 11, BLOCK_SIZE + 12));
        assertFalse(r0.containsRange(BLOCK_SIZE + 9, BLOCK_SIZE + 12));
        r0 = RspBitmap.makeSingle(BLOCK_SIZE + BLOCK_LAST);
        r0 = r0.add(2 * BLOCK_SIZE);
        assertTrue(r0.containsRange(BLOCK_SIZE + BLOCK_LAST, 2 * BLOCK_SIZE));
    }

    @Test
    public void testOverlapsRangeNullSpanCoverage() {
        RspBitmap r0 = RspBitmap.makeSingle(2 * BLOCK_SIZE + BLOCK_LAST - 2);
        r0 = r0.add(BLOCK_LAST);
        assertTrue(r0.overlapsRange(BLOCK_SIZE, 2 * BLOCK_SIZE + BLOCK_LAST - 1));
    }

    @Test
    public void testOverlapsRangeCoverage() {
        RspBitmap r0 = RspBitmap.makeSingleRange(BLOCK_SIZE + 1, 2 * BLOCK_SIZE + BLOCK_LAST);
        assertFalse(r0.overlapsRange(BLOCK_SIZE, BLOCK_SIZE));
        assertTrue(r0.overlapsRange(2 * BLOCK_SIZE - 1, 2 * BLOCK_SIZE - 1));
        assertTrue(r0.overlapsRange(2 * BLOCK_SIZE + BLOCK_LAST, 2 * BLOCK_SIZE + BLOCK_LAST));
        assertFalse(r0.overlapsRange(BLOCK_LAST, BLOCK_LAST));
        assertFalse(r0.overlapsRange(3 * BLOCK_SIZE, 3 * BLOCK_SIZE));
    }

    @Test
    public void testOverlapsNullSpanCoverage() {
        RspBitmap r0 = RspBitmap.makeSingleRange(10, 20);
        r0 = r0.add(BLOCK_SIZE + 10);
        RspBitmap r1 = RspBitmap.makeSingle(BLOCK_SIZE + 9);
        assertFalse(r0.overlaps(r1));
        assertFalse(r1.overlaps(r0));
        r1 = RspBitmap.makeSingle(BLOCK_SIZE + 10);
        assertTrue(r0.overlaps(r1));
        assertTrue(r1.overlaps(r0));
        r0 = r0.add(BLOCK_SIZE + 11);
        assertTrue(r0.overlaps(r1));
        assertTrue(r1.overlaps(r0));
        r1 = r1.addRange(7, 9);
        assertTrue(r0.overlaps(r1));
        assertTrue(r1.overlaps(r0));
    }

    @Test
    public void testOrEqualsSpanCoverage() {
        RspBitmap r0 = RspBitmap.makeSingle(10);
        r0 = r0.add(2 * BLOCK_SIZE);
        RspBitmap r1 = RspBitmap.makeSingle(BLOCK_SIZE + 10);
        RspBitmap r0c = r0.deepCopy();
        r0c = r0c.orEquals(r1);
        assertEquals(3, r0c.getCardinality());
        assertEquals(10, r0c.first());
        assertEquals(2 * BLOCK_SIZE, r0c.last());
        assertEquals(BLOCK_SIZE + 10, r0c.get(1));
    }

    private static RspBitmap vs2rb(final long... vs) {
        RspBitmap rb = new RspBitmap();
        long pendingStart = -1;
        long prev = -1;
        for (long v : vs) {
            if (v < 0) {
                if (pendingStart < 0 || pendingStart >= -v) {
                    throw new IllegalStateException("v=" + v + ", prev=" + prev);
                }
                rb = rb.appendRange(pendingStart, -v);
                pendingStart = -1;
                prev = -v;
            } else {
                if (prev != -1 && prev + 1 >= v) {
                    throw new IllegalStateException("v=" + v + ", prev=" + prev);
                }
                if (pendingStart != -1) {
                    rb = rb.append(pendingStart);
                }
                pendingStart = v;
                prev = v;
            }
        }
        if (pendingStart != -1) {
            rb = rb.append(pendingStart);
        }
        return rb;
    }

    @Test
    public void testAndEqualsSpanCoverage() {
        RspBitmap r0 = RspBitmap.makeSingleRange(2 * BLOCK_SIZE, 2 * BLOCK_SIZE + BLOCK_LAST);
        r0 = r0.addRange(5 * BLOCK_SIZE, 7 * BLOCK_SIZE + BLOCK_LAST);
        RspBitmap r1 = RspBitmap.makeSingle(2 * BLOCK_SIZE);
        r1 = r1.addRange(2 * BLOCK_SIZE + BLOCK_LAST, 5 * BLOCK_SIZE + BLOCK_LAST);
        r1 = r1.addRange(7 * BLOCK_SIZE, 7 * BLOCK_SIZE + 2);
        RspBitmap intersect = r1.deepCopy().andEquals(r0);
        intersect.validate();
        RspBitmap expected = vs2rb(
                2 * BLOCK_SIZE,
                2 * BLOCK_SIZE + BLOCK_LAST,
                5 * BLOCK_SIZE, -(5 * BLOCK_SIZE + BLOCK_LAST),
                7 * BLOCK_SIZE, -(7 * BLOCK_SIZE + 2));
        assertEquals(expected, intersect);
    }

    @Test
    public void testRemoveRangeNullSpanCoverage() {
        RspBitmap r0 = RspBitmap.makeSingleRange(BLOCK_SIZE, 3 * BLOCK_SIZE + BLOCK_LAST);
        RspBitmap r1 = r0.deepCopy().removeRange(BLOCK_SIZE + 1, 4 * BLOCK_SIZE);
        assertEquals(1, r1.getCardinality());
        assertEquals(BLOCK_SIZE, r1.first());
    }

    @Test
    public void testSubrangeByKeyCoverage() {
        RspBitmap rb = RspBitmap.makeSingleRange(BLOCK_SIZE, 3 * BLOCK_SIZE + BLOCK_LAST);
        final long s = BLOCK_SIZE + BLOCK_LAST;
        final long e = s + 1;
        RspBitmap sub = rb.subrangeByValue(s, e);
        sub.validate();
        assertEquals(e - s + 1, sub.getCardinality());
        assertEquals(s, sub.first());
        assertEquals(e, sub.last());
    }

    @Test
    public void testAddRangeRegression2() {
        RspBitmap rb0 = vs2rb(65526, 65536, 131072);
        rb0 = rb0.addRange(65522, 65525);
        rb0.validate();
    }

    @Test
    public void testAddValuesChunk() {
        RspBitmap r0 = RspBitmap.makeEmpty();
        r0 = r0.addRange(0, BLOCK_SIZE + 9999).addRange(BLOCK_SIZE + 10001, BLOCK_SIZE + BLOCK_LAST)
                .add(3 * BLOCK_SIZE);
        final long origCard = r0.getCardinality();
        try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(3)) {
            chunk.set(0, BLOCK_SIZE + 10000);
            chunk.set(1, BLOCK_SIZE + 10001);
            chunk.set(2, 2 * BLOCK_SIZE);
            r0.addValuesUnsafeNoWriteCheck(chunk, 0, 3);
            r0.finishMutations();
            r0.validate();
            assertEquals(origCard + 2, r0.getCardinality());
            assertTrue(r0.containsRange(0, BLOCK_SIZE + 10001));
            assertTrue(r0.find(BLOCK_SIZE + BLOCK_LAST) >= 0);
            assertTrue(r0.find(2 * BLOCK_SIZE) >= 0);
            assertTrue(r0.find(3 * BLOCK_SIZE) >= 0);
        }
    }

    @Test
    public void testRowSequenceByValue() {
        randomizedTest(RspBitmapTest::doTestRowSequenceByValue);
    }

    private static void doTestRowSequenceByValue(final int seed) {
        final Random r = new Random(seed);
        final int nruns = 40;
        for (int nrun = 0; nrun < nruns; ++nrun) {
            final int nblocks = r.nextBoolean() ? (52 + RspBitmap.accNullThreshold) : RspArray.accNullThreshold;
            final RspBitmap rb = getRandomRspBitmap(nblocks, r, 0.90f, 2000,
                    0, 0);
            final long rbc = rb.getCardinality();
            final int ntests = 200;
            final String m = "seed==" + seed + " && nrun==" + nrun;
            for (int t = 0; t < ntests; ++t) {
                final long posStart = r.nextInt((int) rbc);
                final long posEnd = posStart + r.nextInt((int) (rbc - posStart));
                final long start = rb.get(posStart);
                final long end = rb.get(posEnd);
                final String m2 = m + " && t==" + t + " && start==" + start + " && end==" + end;
                RspBitmap rbs = rb.subrangeByValue(start, end);
                try (final RspRangeIterator it = rbs.getRangeIterator();
                        final RowSequence rs = rb.getRowSequenceByKeyRange(start, end)) {
                    rs.forEachRowKeyRange((final long rstart, final long rend) -> {
                        final String m3 = m2 + " && rstart==" + rstart + " && rend==" + rend;
                        assertTrue(m3, it.hasNext());
                        it.next();
                        assertEquals(m3, it.start(), rstart);
                        assertEquals(m3, it.end(), rend);
                        return true;
                    });
                    assertFalse(it.hasNext());
                }
                final long rbsCard = rbs.getCardinality();
                try (final RspRangeIterator it = rbs.getRangeIterator();
                        final RowSequence rs = rb.asRowSequence();
                        final RowSequence.Iterator rsIt = rs.getRowSequenceIterator()) {
                    final boolean wasOk = rsIt.advance(start);
                    assertTrue(wasOk);
                    final RowSequence rs2;
                    if (r.nextBoolean()) {
                        rs2 = rsIt.getNextRowSequenceThrough(end);
                    } else {
                        rs2 = rsIt.getNextRowSequenceWithLength(rbsCard);
                    }
                    assertEquals(m2, rbsCard, rs2.size());
                    rs2.forEachRowKeyRange((final long rstart, final long rend) -> {
                        final String m3 = m2 + " && rstart==" + rstart + " && rend==" + rend;
                        assertTrue(m3, it.hasNext());
                        it.next();
                        assertEquals(m3, it.start(), rstart);
                        assertEquals(m3, it.end(), rend);
                        return true;
                    });
                    assertFalse(it.hasNext());
                }
            }
        }
    }

    @Test
    public void testSequentialBuilderRegression0() {
        final RspBitmapBuilderSequential b = new RspBitmapBuilderSequential();
        b.appendKey(1);
        b.appendKey(BLOCK_SIZE);
        b.appendRange(BLOCK_SIZE + BLOCK_LAST - 1, BLOCK_SIZE + BLOCK_SIZE);
        final OrderedLongSet t = b.getOrderedLongSet();
        t.ixValidate();
        assertEquals(5, t.ixCardinality());
    }

    @Test
    public void testInvertNoAccForCoverage() {
        // 1 full block span in the middle
        final RspBitmap rb = vs2rb(10, BLOCK_SIZE, -(BLOCK_SIZE + BLOCK_LAST + 5), 3 * BLOCK_SIZE + 10);
        final RspBitmap picks = vs2rb(10, BLOCK_SIZE + 1, -(BLOCK_SIZE + BLOCK_LAST + 5), 3 * BLOCK_SIZE + 10);
        final RspBitmapBuilderSequential builder = new RspBitmapBuilderSequential();
        rb.invert(builder, picks.ixRangeIterator(), rb.getCardinality() - 1);
        final RspBitmap result = (RspBitmap) builder.getOrderedLongSet();
        assertEquals(picks.getCardinality(), result.getCardinality());
        assertEquals(0, result.get(0));
        assertEquals(2, result.get(1));
        assertEquals(3, result.get(2));
        assertEquals(rb.getCardinality() - 1, result.get(picks.getCardinality() - 1));
    }

    @Test
    public void testAddValuesUnsafeRegression() {
        RspBitmap rb = new RspBitmap();
        rb = rb.add(3);
        final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(4);
        chunk.setSize(0);
        chunk.add(4);
        chunk.add(BLOCK_SIZE + 10);
        // we saw: "java.lang.IllegalStateException: iv1=3, iv2=4"
        rb.addValuesUnsafeNoWriteCheck(chunk, 0, 2);
    }

    private RspBitmap fromBlocksAsBits(final int nblocks, final int bits) {
        RspBitmap r = RspBitmap.makeEmpty();
        for (int b = 0; b < nblocks; ++b) {
            if ((bits & (1 << b)) != 0) {
                final long blockFirst = BLOCK_SIZE * (long) b;
                final long blockLast = blockFirst + BLOCK_LAST;
                r = r.addRange(blockFirst, blockLast);
            }
        }
        r.validate();
        return r;
    }

    @Test
    public void testBinaryOpsWithFullSpans() {
        final Map<String, BiFunction<RspBitmap, RspBitmap, RspBitmap>> ops = new HashMap<>();
        ops.put("andNotEquals", RspBitmap::andNotEquals);
        ops.put("andEquals", RspBitmap::andEquals);
        ops.put("orEquals", RspBitmap::orEquals);
        final int nblocks = 9;
        final int maxBlocksAsBitsSpec = 1 << nblocks;
        for (String opName : ops.keySet()) {
            final BiFunction<RspBitmap, RspBitmap, RspBitmap> op = ops.get(opName);
            for (int firstBlocksAsBitsSpec = 0; firstBlocksAsBitsSpec < maxBlocksAsBitsSpec; ++firstBlocksAsBitsSpec) {
                for (int secondBlocksAsBitsSpec =
                        0; secondBlocksAsBitsSpec < maxBlocksAsBitsSpec; ++secondBlocksAsBitsSpec) {
                    final RspBitmap r1 = fromBlocksAsBits(nblocks, firstBlocksAsBitsSpec);
                    final RspBitmap r2 = fromBlocksAsBits(nblocks, secondBlocksAsBitsSpec);
                    final RspBitmap result = op.apply(r1, r2);
                    final String msg = opName
                            + " && firstBlocksAsBitsSpec==" + firstBlocksAsBitsSpec
                            + " && secondBlocksAsBitsSpec==" + secondBlocksAsBitsSpec;
                    result.validate(msg);
                    final int resultBits;
                    switch (opName) {
                        case "andNotEquals":
                            resultBits = firstBlocksAsBitsSpec & ~secondBlocksAsBitsSpec;
                            break;
                        case "andEquals":
                            resultBits = firstBlocksAsBitsSpec & secondBlocksAsBitsSpec;
                            break;
                        case "orEquals":
                            resultBits = firstBlocksAsBitsSpec | secondBlocksAsBitsSpec;
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                    final RspBitmap expected = fromBlocksAsBits(nblocks, resultBits);
                    assertEquals(msg, expected, result);
                }
            }
        }
    }

    @Test
    public void testBinarySearchWithSingleValueInLastSpan() {
        final long v0 = 10;
        final long v1 = 10 + BLOCK_SIZE;
        final RspBitmap rsp = vs2rb(v0, v1);
        final RowSet r = new WritableRowSetImpl(rsp);
        final RowSet.SearchIterator sit = r.searchIterator();
        final long ans1 = sit.binarySearchValue((long k, int ignored) -> Long.compare(v0, k), 1);
        assertEquals(v0, ans1);
        final long ans2 = sit.binarySearchValue((long k, int ignored) -> Long.compare(v1 - 1, k), 1);
        assertEquals(v0, ans2);
        final long ans3 = sit.binarySearchValue((long k, int ignored) -> Long.compare(v1, k), 1);
        assertEquals(v1, ans3);
    }

    @Test
    public void testBinarySearchWithFullBlockSpanInLastSpan() {
        final long value0 = 10;
        final long rangeStart1 = BLOCK_SIZE;
        final long rangeLast1 = BLOCK_SIZE + BLOCK_LAST;
        final RspBitmap rsp = vs2rb(value0, rangeStart1, -rangeLast1);
        final RowSet r = new WritableRowSetImpl(rsp);
        final RowSet.SearchIterator sit = r.searchIterator();
        final long ans1 = sit.binarySearchValue((long k, int ignored) -> Long.compare(value0, k), 1);
        assertEquals(value0, ans1);
        final long ans2 = sit.binarySearchValue((long k, int ignored) -> Long.compare(rangeStart1 - 1, k), 1);
        assertEquals(value0, ans2);
        final long ans3 = sit.binarySearchValue((long k, int ignored) -> Long.compare(rangeStart1, k), 1);
        assertEquals(rangeStart1, ans3);
    }

    private void testBiOpSimilarPrefix(
            BiFunction<RspBitmap, RspBitmap, RspBitmap> bitmapFun,
            BiFunction<Boolean, Boolean, Boolean> boolFun) {
        final IntConsumer testFun = (final int seed) -> {
            final String m = "seed==" + seed;
            final Random rand = new Random(seed);
            final int nblocks = (rand.nextInt(4) == 0)
                    ? RspBitmap.accNullThreshold
                    : (12 + RspBitmap.accNullThreshold);
            final RspBitmap rb1 = getRandomRspBitmap(
                    nblocks, rand, 0.90f, 2000, 0, 12);
            for (int common = 0; common <= rb1.size; ++common) {
                final String m1 = m + " && common==" + common;
                RspBitmap rb2;
                if (common == 0) {
                    rb2 = RspBitmap.makeEmpty();
                } else {
                    rb2 = new RspBitmap(rb1, 0, common - 1);
                }
                for (int i = 0; i < rb1.size; ++i) {
                    final long k = (common == 0) ? (rb1.first() & ~BLOCK_LAST) : (rb1.spanInfos[i] & ~BLOCK_LAST);
                    final int start = rand.nextInt(BLOCK_SIZE);
                    final int end = start + rand.nextInt(BLOCK_SIZE - start);
                    rb2 = rb2.addRange(k + start, k + end);
                }
                rb2.validate();
                testBiOp(m1, bitmapFun, boolFun, rb1, rb2);
            }
        };
        randomizedTest(testFun);
    }

    @Test
    public void testOrSimilarPrefix() {
        testBiOpSimilarPrefix(RspBitmap::or, (Boolean b1, Boolean b2) -> b1 || b2);
    }

    @Test
    public void testAndNotSimilarPrefix() {
        testBiOpSimilarPrefix(RspBitmap::andNot, (Boolean b1, Boolean b2) -> b1 && !b2);
    }

    @Test
    public void testAndNotSimilarPrefixForCoverage() {
        // need to cover the case of exactly the same.
        {
            final RspBitmap r1 = RspBitmap.makeSingleRange(10L, 20L);
            final RspBitmap r2 = r1.deepCopy();
            final OrderedLongSet o = r1.ixMinusOnNew(r2);
            assertTrue(o.ixIsEmpty());
        }

        // need to cover the case of different Long spans.
        {
            final long spanLengthThatNeedsLong = 1L << (BITS_PER_BLOCK + 1);
            final long r1Start = BLOCK_SIZE;
            final long r1End = spanLengthThatNeedsLong * BLOCK_SIZE + BLOCK_LAST;
            final RspBitmap r1 = RspBitmap.makeSingleRange(r1Start, r1End);
            RspBitmap r2 = RspBitmap.makeSingleRange(r1Start, r1End);
            final long lonelyLastValue = 20L + (spanLengthThatNeedsLong + 3) * BLOCK_SIZE;
            r2 = r2.add(lonelyLastValue);
            OrderedLongSet minus = r2.ixMinusOnNew(r1);
            assertEquals(1, minus.ixCardinality());
            assertEquals(lonelyLastValue, minus.ixFirstKey());

            r2 = RspBitmap.makeSingleRange(r1Start, r1End + BLOCK_SIZE);
            r2 = r2.add(lonelyLastValue);
            minus = r2.ixMinusOnNew(r1);
            assertEquals(1 + BLOCK_SIZE, minus.ixCardinality());
            assertEquals(r1End + 1, minus.ixFirstKey());
            assertEquals(lonelyLastValue, minus.ixLastKey());
        }
    }

    @Test
    public void testRspArrayCtorBySpanRangeForCoverage() {
        // need to cover the case of ArrayContainer as short[]
        RspBitmap r1 = RspBitmap.makeEmpty();
        // Trickle some values in
        for (long x = 0; x < ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD; ++x) {
            r1 = r1.add(1 + 2 * x);
        }
        final RspBitmap r2 = new RspBitmap(r1, 0, 0);
        assertEquals(r2.ixCardinality(), r1.ixCardinality());
        assertTrue(r2.ixMinusOnNew(r1).ixIsEmpty());
    }

    @Test
    public void testRegressionGitHubIssue2517() {
        final SortedRanges sr = SortedRanges.makeSingleRange(10, 20);
        final RspBitmap other = RspBitmap.makeSingleRange(65536 + 10, 65536 + 11);
        OrderedLongSet olset = sr.ixInsertWithShift(0, other);
        olset = olset.ixAppendRange(65536 + 200, 65536 + 300);
        assertEquals(2, other.getCardinality());
        assertEquals(65536 + 10, other.first());
        assertEquals(65536 + 11, other.last());
    }
}
