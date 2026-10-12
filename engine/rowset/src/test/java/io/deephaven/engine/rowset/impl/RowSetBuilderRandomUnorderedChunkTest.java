//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

/**
 * Adding a slice of a chunk of row keys in any order to a random builder gives the same row set as adding each key on
 * its own, in every state of the builder: a pending range, a pending {@link SortedRanges}, and an inner builder. The
 * keys mix runs of consecutive keys, descending keys, duplicates and keys spread wider than an {@link RspBitmap} block.
 */
public class RowSetBuilderRandomUnorderedChunkTest {
    private static final int SEEDS = 10;
    private static final int STEPS = 60;

    @Test
    public void testUnorderedChunkSlicesMatchIndividualKeys() {
        for (int seed = 0; seed < SEEDS; ++seed) {
            final Random random = new Random(seed);
            final RowSetBuilderRandom chunkBuilder = RowSetFactory.builderRandom();
            final RowSetBuilderRandom keyBuilder = RowSetFactory.builderRandom();
            final TreeSet<Long> expected = new TreeSet<>();
            final long keySpace = 1L << 40;

            for (int step = 0; step < STEPS; ++step) {
                final int action = random.nextInt(10);
                if (action == 0) {
                    final long key = nextKey(random, keySpace);
                    chunkBuilder.addKey(key);
                    keyBuilder.addKey(key);
                    expected.add(key);
                } else if (action == 1) {
                    final long first = nextKey(random, keySpace - 100);
                    final long last = first + random.nextInt(100);
                    chunkBuilder.addRange(first, last);
                    keyBuilder.addRange(first, last);
                    for (long key = first; key <= last; ++key) {
                        expected.add(key);
                    }
                } else {
                    final long[] keys = unorderedKeys(random, keySpace);
                    // surround the keys with values that are not part of the slice
                    final int before = random.nextInt(4);
                    final long[] padded = new long[before + keys.length + random.nextInt(4)];
                    Arrays.fill(padded, -1);
                    System.arraycopy(keys, 0, padded, before, keys.length);
                    chunkBuilder.addRowKeysChunk(LongChunk.<RowKeys>chunkWrap(padded), before, keys.length);
                    for (final long key : keys) {
                        keyBuilder.addKey(key);
                        expected.add(key);
                    }
                }
            }

            try (final WritableRowSet fromChunks = chunkBuilder.build();
                    final WritableRowSet fromKeys = keyBuilder.build()) {
                assertEquals("seed " + seed, fromKeys, fromChunks);
                assertEquals("seed " + seed, expected.size(), fromChunks.size());
                fromChunks.validate();
            }
        }
    }

    /**
     * Unordered chunks added after the builder has moved to its inner builder are merged with what it holds.
     */
    @Test
    public void testUnorderedChunkAfterInnerBuilder() {
        final RowSetBuilderRandom escalated = RowSetFactory.builderRandom();
        final RowSetBuilderRandom reference = RowSetFactory.builderRandom();
        final int scattered = 4 * SortedRanges.MAX_CAPACITY;
        for (int ii = scattered - 1; ii >= 0; --ii) {
            escalated.addKey(ii * 3L);
            reference.addKey(ii * 3L);
        }
        final long[] descending = new long[1000];
        for (int ii = 0; ii < descending.length; ++ii) {
            descending[ii] = 2 + (descending.length - ii) * 3L;
        }
        escalated.addRowKeysChunk(LongChunk.<RowKeys>chunkWrap(descending), 0, descending.length);
        for (final long key : descending) {
            reference.addKey(key);
        }
        try (final WritableRowSet built = escalated.build();
                final WritableRowSet expected = reference.build()) {
            assertEquals(expected, built);
            built.validate();
        }
    }

    private static long nextKey(final Random random, final long keySpace) {
        return (long) (random.nextDouble() * keySpace);
    }

    /**
     * Runs of consecutive keys and single keys, with gaps from none to wider than an {@link RspBitmap} block, whose
     * runs are shuffled, sometimes reversed, and sometimes repeated.
     */
    private static long[] unorderedKeys(final Random random, final long keySpace) {
        final int runCount = random.nextInt(4) == 0 ? 1 + random.nextInt(4) : 1 + random.nextInt(400);
        final int maxGap = new int[] {1, 3, 200, 140_000}[random.nextInt(4)];
        final List<long[]> runs = new ArrayList<>();
        long key = nextKey(random, keySpace / 2);
        for (int ri = 0; ri < runCount && key < keySpace - 64; ++ri) {
            final long[] run = new long[1 + random.nextInt(16)];
            for (int ii = 0; ii < run.length; ++ii) {
                run[ii] = key++;
            }
            if (random.nextInt(4) == 0) {
                for (int ii = 0; ii < run.length / 2; ++ii) {
                    final long swap = run[ii];
                    run[ii] = run[run.length - 1 - ii];
                    run[run.length - 1 - ii] = swap;
                }
            }
            runs.add(run);
            if (random.nextInt(8) == 0) {
                runs.add(run.clone());
            }
            key += random.nextInt(maxGap);
        }
        Collections.shuffle(runs, random);
        return runs.stream().flatMapToLong(Arrays::stream).toArray();
    }
}
