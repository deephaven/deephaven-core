//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Concurrent readers of an unmodified {@link RowSequenceAsChunkImpl} must each get a chunk holding its keys or ranges
 * from {@link RowSequence#asRowKeyChunk()} and {@link RowSequence#asRowKeyRangesChunk()}, including when they are the
 * first callers after a modification and so race to build the cached chunk.
 */
public class RowSequenceAsChunkConcurrencyTest {
    private static final int READERS = 4;
    private static final int ITERATIONS = 2_000;
    private static final int KEYS = 4_096;

    @Test
    public void testConcurrentRowKeyChunk() throws Exception {
        runConcurrentReaders(RowSequence::asRowKeyChunk, RowSequenceAsChunkConcurrencyTest::keysOf);
    }

    @Test
    public void testConcurrentRowKeyRangesChunk() throws Exception {
        runConcurrentReaders(RowSequence::asRowKeyRangesChunk, RowSequenceAsChunkConcurrencyTest::rangesOf);
    }

    private static <ATTR extends Any> void runConcurrentReaders(
            final Function<RowSequence, LongChunk<ATTR>> reader,
            final Function<RowSequence, long[]> expectedOf) throws Exception {
        final Random random = new Random(0x23886);
        final ExecutorService executor = Executors.newFixedThreadPool(READERS);
        try (final WritableRowSet rowSet = RowSetFactory.empty()) {
            long nextKey = 0;
            for (int ki = 0; ki < KEYS; ++ki) {
                nextKey += 1 + random.nextInt(3);
                rowSet.insert(nextKey);
            }
            final CyclicBarrier barrier = new CyclicBarrier(READERS);
            for (int ii = 0; ii < ITERATIONS; ++ii) {
                // Populate the cache, then modify the row set so that the readers below race to rebuild it.
                reader.apply(rowSet);
                if (random.nextBoolean() && rowSet.size() > KEYS / 2) {
                    rowSet.remove(rowSet.get(random.nextInt(rowSet.intSize())));
                } else {
                    nextKey += 1 + random.nextInt(3);
                    rowSet.insert(nextKey);
                }
                final long[] expected = expectedOf.apply(rowSet);

                final List<Future<String>> results = new ArrayList<>(READERS);
                for (int ri = 0; ri < READERS; ++ri) {
                    results.add(executor.submit(() -> {
                        barrier.await();
                        return mismatch(reader.apply(rowSet), expected);
                    }));
                }
                for (final Future<String> result : results) {
                    assertNull("iteration " + ii, result.get());
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static String mismatch(final LongChunk<?> actual, final long[] expected) {
        if (actual.size() != expected.length) {
            return "size " + actual.size() + " != " + expected.length;
        }
        for (int ii = 0; ii < expected.length; ++ii) {
            if (actual.get(ii) != expected[ii]) {
                return "element " + ii + ": " + actual.get(ii) + " != " + expected[ii];
            }
        }
        return null;
    }

    private static long[] keysOf(final RowSequence rowSequence) {
        final long[] keys = new long[rowSequence.intSize()];
        final MutableInt next = new MutableInt();
        rowSequence.forAllRowKeys(key -> keys[next.getAndIncrement()] = key);
        assertEquals(keys.length, next.intValue());
        return keys;
    }

    private static long[] rangesOf(final RowSequence rowSequence) {
        final LongArrayList ranges = new LongArrayList();
        rowSequence.forAllRowKeyRanges((first, last) -> {
            ranges.add(first);
            ranges.add(last);
        });
        return ranges.toLongArray();
    }
}
