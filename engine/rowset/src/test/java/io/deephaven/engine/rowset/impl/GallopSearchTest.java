//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link RowSet.SearchIterator#binarySearchValue} with and without {@link RowSetUtils#GALLOP_SEARCH}: a sequence of
 * ascending targets searched with one iterator returns, for each target, the largest key at or below it when that key
 * is at or after the iterator's current value, and -1 (leaving the iterator in place) otherwise. Row sets cover a
 * single range, sorted ranges, and RSP spans of full blocks and of run, array and bitmap containers.
 */
public class GallopSearchTest {
    private static final long BS = RspArray.BLOCK_SIZE;
    private static final long BASE = 1L << 30;

    private boolean savedGallopSearch;

    @Before
    public void setUp() {
        savedGallopSearch = RowSetUtils.GALLOP_SEARCH;
    }

    @After
    public void tearDown() {
        RowSetUtils.GALLOP_SEARCH = savedGallopSearch;
    }

    @Test
    public void testBinary() {
        checkAll(false);
    }

    @Test
    public void testGallop() {
        checkAll(true);
    }

    private static void checkAll(final boolean gallop) {
        RowSetUtils.GALLOP_SEARCH = gallop;
        final Random random = new Random(0x6A110B);
        for (int trial = 0; trial < 40; ++trial) {
            for (final long[][] ranges : shapes(random)) {
                final long[] keys = keysOf(ranges);
                check(rspOf(ranges), keys, random);
                if (ranges.length <= 100) {
                    check(sortedRangesOf(ranges), keys, random);
                }
                if (ranges.length == 1) {
                    check(singleRangeOf(ranges[0][0], ranges[0][1]), keys, random);
                }
            }
        }
    }

    private static List<long[][]> shapes(final Random random) {
        final List<long[][]> shapes = new ArrayList<>();
        // One range across many full blocks.
        shapes.add(new long[][] {{BASE + random.nextInt(100), BASE + 20 * BS + random.nextInt(100)}});
        // Runs of 64 every 128 keys across several spans: run containers.
        shapes.add(strided(BASE, 64, 128, 5 * (int) BS / 128));
        // Runs of 64 every 128 keys, a short one.
        shapes.add(strided(BASE + 7, 64, 128, 40));
        // Sparse singletons: array containers.
        shapes.add(randomKeys(random, BASE, 4 * BS, 600));
        // A few sparse singletons, few enough for sorted ranges.
        shapes.add(randomKeys(random, BASE, 4 * BS, 80));
        // Half the keys of a span: bitmap containers.
        shapes.add(randomKeys(random, BASE, 2 * BS, (int) BS));
        // Full blocks next to run containers.
        final long[][] mixed = strided(BASE + 3 * BS, 64, 128, 2 * (int) BS / 128);
        final long[][] withFull = new long[mixed.length + 1][];
        withFull[0] = new long[] {BASE, BASE + 3 * BS - 1};
        System.arraycopy(mixed, 0, withFull, 1, mixed.length);
        shapes.add(withFull);
        return shapes;
    }

    private static long[][] strided(final long first, final int runSize, final int stride, final int runs) {
        final long[][] ranges = new long[runs][];
        for (int ii = 0; ii < runs; ++ii) {
            final long start = first + (long) ii * stride;
            ranges[ii] = new long[] {start, start + runSize - 1};
        }
        return ranges;
    }

    private static long[][] randomKeys(final Random random, final long first, final long width, final int count) {
        final long[] keys = random.longs(count, first, first + width).sorted().distinct().toArray();
        final List<long[]> ranges = new ArrayList<>();
        for (final long key : keys) {
            if (!ranges.isEmpty() && ranges.get(ranges.size() - 1)[1] == key - 1) {
                ranges.get(ranges.size() - 1)[1] = key;
            } else {
                ranges.add(new long[] {key, key});
            }
        }
        return ranges.toArray(new long[0][]);
    }

    private static long[] keysOf(final long[][] ranges) {
        long count = 0;
        for (final long[] range : ranges) {
            count += range[1] - range[0] + 1;
        }
        final long[] keys = new long[(int) count];
        int pos = 0;
        for (final long[] range : ranges) {
            for (long key = range[0]; key <= range[1]; ++key) {
                keys[pos++] = key;
            }
        }
        return keys;
    }

    private static void check(final RowSet rowSet, final long[] keys, final Random random) {
        assertEquals(keys.length, rowSet.size());
        final long first = keys[0];
        final long last = keys[keys.length - 1];
        // Target spacing from a few keys apart to far beyond the row set's width.
        final long meanGap = 1L + (long) Math.pow(2, random.nextInt(24));
        final long[] targets = new long[1 + random.nextInt(2000)];
        long target = first - 1 - random.nextInt(3);
        int numTargets = 0;
        while (numTargets < targets.length) {
            targets[numTargets++] = target;
            target += (long) (random.nextDouble() * 2 * meanGap);
            if (random.nextInt(8) == 0) {
                // A key of the row set, so that some probes compare equal.
                target = Math.max(target, keys[random.nextInt(keys.length)]);
            }
            if (target > last + 2) {
                break;
            }
        }
        long current = -1;
        try (final RowSet.SearchIterator iterator = rowSet.searchIterator()) {
            for (int ii = 0; ii < numTargets; ++ii) {
                final long searched = targets[ii];
                final RowSet.TargetComparator comp = (final long rKey, final int dir) -> {
                    assertTrue("probe of a key outside the row set: " + rKey, rowSet.find(rKey) >= 0);
                    return Long.compare(searched, rKey) * dir;
                };
                final long result = iterator.binarySearchValue(comp, 1);
                if (current == -1) {
                    current = first;
                }
                final long expected;
                if (searched < current) {
                    expected = -1;
                } else {
                    final int index = Arrays.binarySearch(keys, searched);
                    expected = keys[index >= 0 ? index : -index - 2];
                    current = expected;
                }
                assertEquals("target " + searched + " in " + rowSet, expected, result);
                if (expected != -1) {
                    assertEquals(expected, iterator.currentValue());
                }
            }
        }
    }
}
