//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Adding an ordered chunk of row keys to a random builder gives the same row set as adding each key on its own, for
 * whole long chunks, long chunk slices and int chunks, in every state of the builder: a pending range, a pending
 * {@link SortedRanges}, and an inner builder. The built row sets span single ranges, {@link SortedRanges} and
 * {@link RspBitmap} sets.
 */
public class RowSetBuilderRandomOrderedChunkTest {
    private static final int SEEDS = 10;
    private static final int STEPS = 60;

    private enum ChunkKind {
        LONG, LONG_SLICE, INT
    }

    @Test
    public void testOrderedChunksMatchIndividualKeys() {
        for (int seed = 0; seed < SEEDS; ++seed) {
            final Random random = new Random(seed);
            final RowSetBuilderRandom chunkBuilder = RowSetFactory.builderRandom();
            final RowSetBuilderRandom keyBuilder = RowSetFactory.builderRandom();
            final TreeSet<Long> expected = new TreeSet<>();
            // int chunks hold keys below Integer.MAX_VALUE, so keep every key in that range for a seed that uses them
            final boolean intKeys = seed % 2 == 0;
            final long keySpace = intKeys ? Integer.MAX_VALUE : 1L << 40;

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
                    final long[] keys = orderedKeys(random, keySpace);
                    final ChunkKind kind = intKeys ? ChunkKind.values()[random.nextInt(3)]
                            : ChunkKind.values()[random.nextInt(2)];
                    addChunk(chunkBuilder, keys, kind, random);
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
     * A single run of keys added to a fresh builder stays a single range, and a chunk added after the builder holds an
     * inner builder is merged with what it holds.
     */
    @Test
    public void testChunkStates() {
        final RowSetBuilderRandom contiguous = RowSetFactory.builderRandom();
        contiguous.addOrderedRowKeysChunk(LongChunk.chunkWrap(new long[] {5, 6, 7, 8}));
        try (final WritableRowSet built = contiguous.build()) {
            assertEquals(RowSetFactory.fromRange(5, 8), built);
            assertTrue(((WritableRowSetImpl) built).getInnerSet() instanceof SingleRange);
        }

        // many scattered ranges move the builder to its inner builder before the chunk arrives
        final RowSetBuilderRandom escalated = RowSetFactory.builderRandom();
        final RowSetBuilderRandom reference = RowSetFactory.builderRandom();
        final int scattered = 4 * SortedRanges.MAX_CAPACITY;
        for (int ii = scattered - 1; ii >= 0; --ii) {
            escalated.addKey(ii * 3L);
            reference.addKey(ii * 3L);
        }
        final long[] sparse = new long[1000];
        for (int ii = 0; ii < sparse.length; ++ii) {
            sparse[ii] = 1 + ii * 70_000L;
        }
        escalated.addOrderedRowKeysChunk(LongChunk.chunkWrap(sparse));
        final long[] dense = new long[1000];
        for (int ii = 0; ii < dense.length; ++ii) {
            dense[ii] = 2 + ii * 3L;
        }
        escalated.addOrderedRowKeysChunk(LongChunk.chunkWrap(dense));
        escalated.addOrderedRowKeysChunk(LongChunk.chunkWrap(new long[] {1L << 40, (1L << 40) + 1}));
        for (final long[] keys : new long[][] {sparse, dense, {1L << 40, (1L << 40) + 1}}) {
            for (final long key : keys) {
                reference.addKey(key);
            }
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
     * Increasing keys whose gaps range from none, which forms runs, to wider than an {@link RspBitmap} block.
     */
    private static long[] orderedKeys(final Random random, final long keySpace) {
        final int size = random.nextInt(4) == 0 ? 1 + random.nextInt(8) : 1 + random.nextInt(6000);
        final int maxGap = new int[] {1, 3, 200, 140_000}[random.nextInt(4)];
        final long[] keys = new long[size];
        long key = nextKey(random, keySpace / 2);
        int count = 0;
        while (count < size && key < keySpace) {
            keys[count++] = key;
            key += 1 + random.nextInt(maxGap);
        }
        return Arrays.copyOf(keys, count);
    }

    private static void addChunk(final RowSetBuilderRandom builder, final long[] keys, final ChunkKind kind,
            final Random random) {
        switch (kind) {
            case LONG:
                builder.addOrderedRowKeysChunk(LongChunk.<OrderedRowKeys>chunkWrap(keys));
                break;
            case LONG_SLICE: {
                // surround the keys with values that are not part of the slice
                final int before = random.nextInt(4);
                final long[] padded = new long[before + keys.length + random.nextInt(4)];
                Arrays.fill(padded, -1);
                System.arraycopy(keys, 0, padded, before, keys.length);
                builder.addOrderedRowKeysChunk(LongChunk.<OrderedRowKeys>chunkWrap(padded), before, keys.length);
                break;
            }
            case INT: {
                final int[] intKeys = new int[keys.length];
                for (int ii = 0; ii < keys.length; ++ii) {
                    intKeys[ii] = (int) keys[ii];
                }
                builder.addOrderedRowKeysChunk(IntChunk.<OrderedRowKeys>chunkWrap(intKeys));
                break;
            }
        }
    }
}
