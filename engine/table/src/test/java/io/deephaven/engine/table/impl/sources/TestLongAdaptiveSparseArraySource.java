//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharacterAdaptiveSparseArraySource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.mutable.MutableInt;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestLongAdaptiveSparseArraySource extends AbstractLongColumnSourceTest {

    /** Row keys near zero, spread over many leaves, and far away, so that each page table level holds many nodes. */
    private static final long[] KEY_REGION_STARTS = {0, 1L << 26, 1L << 41};
    private static final int KEY_REGION_SIZE = 40 * BLOCK_SIZE;

    @NotNull
    @Override
    LongAdaptiveSparseArraySource makeTestSource() {
        return new LongAdaptiveSparseArraySource();
    }

    @Test
    public void confirmAliasingForbidden() {
        final LongAdaptiveSparseArraySource source = makeTestSource();
        final Random random = new Random(0);
        // a full leaf, which getChunk aliases
        for (int ii = 0; ii < BLOCK_SIZE; ++ii) {
            source.set(ii, randomValue(random));
        }
        try (final RowSet srcKeys = RowSetFactory.fromRange(20, 80);
                final RowSet destKeys = RowSetFactory.fromRange(21, 81);
                final ChunkSource.GetContext srcContext = source.makeGetContext(BLOCK_SIZE);
                final ChunkSink.FillFromContext destContext = source.makeFillFromContext(BLOCK_SIZE)) {
            final Chunk<? extends Values> chunk = source.getChunk(srcContext, srcKeys);
            assertTrue(io.deephaven.engine.table.impl.DefaultGetContext.isMyResettableChunk(srcContext, chunk));
            try {
                source.fillFromChunk(destContext, chunk, destKeys);
                fail("expected an UnsupportedOperationException");
            } catch (UnsupportedOperationException expected) {
                // the chunk aliases the leaf it would be written to
            }
        }
    }

    @Test
    public void testDefaultThresholds() {
        checkAgainstMap(makeTestSource(), 0);
    }

    @Test
    public void testThresholds() {
        for (final int compactThreshold : new int[] {1, 4, 64, BLOCK_SIZE}) {
            for (final int sortedThreshold : new int[] {1, 4, 16}) {
                checkAgainstMap(new LongAdaptiveSparseArraySource(sortedThreshold, compactThreshold),
                        compactThreshold * 37 + sortedThreshold);
            }
        }
    }

    private static long randomValue(final Random random) {
        return (long) random.nextInt(1000);
    }

    private static void checkValue(final String message, final long expected, final long actual) {
        if (expected != actual) {
            fail(message + ": expected " + expected + " but was " + actual);
        }
    }

    private static long randomKey(final Random random) {
        return KEY_REGION_STARTS[random.nextInt(KEY_REGION_STARTS.length)] + random.nextInt(KEY_REGION_SIZE);
    }

    /**
     * @return a random row set whose density varies by leaf, so that some leaves stay compact and others fill up
     */
    private static RowSet randomRowSet(final Random random, final int maxSize) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        final int size = 1 + random.nextInt(maxSize);
        final long start = KEY_REGION_STARTS[random.nextInt(KEY_REGION_STARTS.length)];
        for (int ii = 0; ii < size; ++ii) {
            final long leafStart = start + (long) random.nextInt(KEY_REGION_SIZE / BLOCK_SIZE) * BLOCK_SIZE;
            final int span = random.nextBoolean() ? BLOCK_SIZE : 16;
            final long key = leafStart + random.nextInt(span);
            if (random.nextInt(4) == 0) {
                builder.addRange(key, key + random.nextInt(64));
            } else {
                builder.addKey(key);
            }
        }
        return builder.build();
    }

    private static void checkAgainstMap(final LongAdaptiveSparseArraySource source, final int seed) {
        final Random random = new Random(seed);
        final Long2LongOpenHashMap expected = new Long2LongOpenHashMap();
        expected.defaultReturnValue(NULL_LONG);

        for (int step = 0; step < 400; ++step) {
            applyRandomOperation(source, expected, random);
            if (step % 20 == 0) {
                assertMatches(source, expected, random, false);
            }
        }
        assertMatches(source, expected, random, false);
    }

    @Test
    public void testPreviousValues() {
        for (final int compactThreshold : new int[] {4, 64,
                LongAdaptiveSparseArraySource.DEFAULT_COMPACT_THRESHOLD}) {
            for (final int sortedThreshold : new int[] {1, 16}) {
                checkPreviousValues(new LongAdaptiveSparseArraySource(sortedThreshold, compactThreshold),
                        compactThreshold * 37 + sortedThreshold);
            }
        }
    }

    /**
     * Apply random operations in update cycles, checking that previous values are the values at the start of each
     * cycle.
     */
    private static void checkPreviousValues(final LongAdaptiveSparseArraySource source, final int seed) {
        final Random random = new Random(seed);
        final Long2LongOpenHashMap expected = new Long2LongOpenHashMap();
        expected.defaultReturnValue(NULL_LONG);
        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int cycle = 0; cycle < 30; ++cycle) {
            final Long2LongOpenHashMap expectedPrev = new Long2LongOpenHashMap(expected);
            expectedPrev.defaultReturnValue(NULL_LONG);
            updateGraph.startCycleForUnitTests();
            final int operations = random.nextInt(15);
            for (int step = 0; step < operations; ++step) {
                applyRandomOperation(source, expected, random);
                if (step % 5 == 0) {
                    assertMatches(source, expected, random, false);
                    assertMatches(source, expectedPrev, random, true);
                }
            }
            assertMatches(source, expected, random, false);
            assertMatches(source, expectedPrev, random, true);
            updateGraph.completeCycleForUnitTests();
            assertMatches(source, expected, random, true);
        }
    }

    @Test
    public void testClearBlocks() {
        final LongAdaptiveSparseArraySource source = makeTestSource();
        source.startTrackingPrevValues();
        final Random random = new Random(0);
        // a full leaf, a bitmap leaf and a sorted leaf, in that order
        final long[] leafSizes = {BLOCK_SIZE, 100, 3};
        for (int li = 0; li < leafSizes.length; ++li) {
            for (int ii = 0; ii < leafSizes[li]; ++ii) {
                source.set(li * (long) BLOCK_SIZE + ii, randomValue(random));
            }
        }
        final long liveValue = source.getLong(2L * BLOCK_SIZE);
        final long sizeBefore = source.estimateSize();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            try (final RowSet removed = RowSetFactory.fromRange(0, 2L * BLOCK_SIZE - 1);
                    final RowSet live = RowSetFactory.fromKeys(2L * BLOCK_SIZE)) {
                source.setNull(removed);
                SparseBlockColumnSource.clearBlocksWithoutLiveRows(removed, live, source);
            }
        });
        assertTrue(source.estimateSize() < sizeBefore);
        checkValue("live", liveValue, source.getLong(2L * BLOCK_SIZE));
        checkValue("cleared", NULL_LONG, source.getLong(0));
        checkValue("cleared", NULL_LONG, source.getLong(BLOCK_SIZE));
    }

    private static void applyRandomOperation(
            final LongAdaptiveSparseArraySource source,
            final Long2LongOpenHashMap expected,
            final Random random) {
        switch (random.nextInt(6)) {
            case 0: {
                final long key = randomKey(random);
                final long value = randomValue(random);
                source.set(key, value);
                expected.put(key, value);
                break;
            }
            case 1: {
                final long key = randomKey(random);
                source.setNull(key);
                expected.remove(key);
                break;
            }
            case 2: {
                try (final RowSet keys = randomRowSet(random, 200)) {
                    source.setNull(keys);
                    keys.forAllRowKeys(expected::remove);
                }
                break;
            }
            case 3: {
                final long first = randomKey(random);
                final long delta =
                        random.nextBoolean() ? 1 + random.nextInt(BLOCK_SIZE) : -1 - random.nextInt(BLOCK_SIZE);
                try (final RowSet keys = RowSetFactory.fromRange(Math.max(0, first), first + random.nextInt(300))) {
                    if (keys.firstRowKey() + delta < 0) {
                        break;
                    }
                    source.shift(keys, delta);
                    final Long2LongOpenHashMap shifted = new Long2LongOpenHashMap();
                    shifted.defaultReturnValue(NULL_LONG);
                    keys.forAllRowKeys(key -> {
                        if (expected.containsKey(key)) {
                            shifted.put(key + delta, expected.remove(key));
                        } else {
                            shifted.put(key + delta, NULL_LONG);
                        }
                    });
                    shifted.forEach((key, value) -> {
                        if (value == NULL_LONG) {
                            expected.remove((long) key);
                        } else {
                            expected.put((long) key, value);
                        }
                    });
                }
                break;
            }
            case 4: {
                try (final RowSet keys = randomRowSet(random, 3000);
                        final WritableLongChunk<Values> values =
                                WritableLongChunk.makeWritableChunk(keys.intSize());
                        final ChunkSink.FillFromContext context = source.makeFillFromContext(keys.intSize())) {
                    final MutableInt position = new MutableInt(0);
                    keys.forAllRowKeys(key -> {
                        final long value = randomValue(random);
                        values.set(position.getAndIncrement(), value);
                        expected.put(key, value);
                    });
                    source.fillFromChunk(context, values, keys);
                }
                break;
            }
            default: {
                final int count = 1 + random.nextInt(200);
                try (final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(count);
                        final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(count);
                        final ChunkSink.FillFromContext context = source.makeFillFromContext(count)) {
                    for (int ii = 0; ii < count; ++ii) {
                        final long key = randomKey(random);
                        final long value = randomValue(random);
                        keys.set(ii, key);
                        values.set(ii, value);
                        expected.put(key, value);
                    }
                    source.fillFromChunkUnordered(context, values, keys);
                }
            }
        }
    }

    /**
     * Check the current values, or the previous values if {@code usePrev}, of {@code source} against {@code expected}
     * through each way of reading them.
     */
    private static void assertMatches(
            final LongAdaptiveSparseArraySource source,
            final Long2LongOpenHashMap expected,
            final Random random,
            final boolean usePrev) {
        for (int ii = 0; ii < 500; ++ii) {
            final long key = randomKey(random);
            checkValue("key " + key, expected.get(key), usePrev ? source.getPrevLong(key) : source.getLong(key));
        }
        for (final long key : expected.keySet()) {
            checkValue("key " + key, expected.get(key), usePrev ? source.getPrevLong(key) : source.getLong(key));
        }

        // a row set with runs takes the by-ranges path, and one of single keys the by-keys path
        try (final RowSet keys = randomRowSet(random, 5000);
                final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(keys.intSize());
                final ChunkSource.FillContext context = source.makeFillContext(keys.intSize())) {
            if (usePrev) {
                source.fillPrevChunk(context, values, keys);
            } else {
                source.fillChunk(context, values, keys);
            }
            assertEquals(keys.intSize(), values.size());
            final MutableInt position = new MutableInt(0);
            keys.forAllRowKeys(
                    key -> checkValue("key " + key, expected.get(key), values.get(position.getAndIncrement())));
        }
        try (final RowSet keys = RowSetFactory.fromKeys(expected.keySet().toLongArray());
                final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(keys.intSize());
                final ChunkSource.FillContext context = source.makeFillContext(keys.intSize())) {
            if (usePrev) {
                source.fillPrevChunk(context, values, keys);
            } else {
                source.fillChunk(context, values, keys);
            }
            final MutableInt position = new MutableInt(0);
            keys.forAllRowKeys(
                    key -> checkValue("key " + key, expected.get(key), values.get(position.getAndIncrement())));
        }

        final long leafStart = randomKey(random) & ~(BLOCK_SIZE - 1L);
        try (final RowSet keys = RowSetFactory.fromRange(leafStart + 10, leafStart + 500);
                final ChunkSource.GetContext context = source.makeGetContext(keys.intSize())) {
            final LongChunk<? extends Values> values =
                    (usePrev ? source.getPrevChunk(context, keys) : source.getChunk(context, keys)).asLongChunk();
            final MutableInt position = new MutableInt(0);
            keys.forAllRowKeys(
                    key -> checkValue("key " + key, expected.get(key), values.get(position.getAndIncrement())));
        }

        final int count = 300;
        try (final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(count);
                final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(count);
                final ChunkSource.FillContext context = source.makeFillContext(count)) {
            for (int ii = 0; ii < count; ++ii) {
                keys.set(ii, random.nextInt(10) == 0 ? RowSequence.NULL_ROW_KEY : randomKey(random));
            }
            keys.setSize(count);
            if (usePrev) {
                source.fillPrevChunkUnordered(context, values, keys);
            } else {
                source.fillChunkUnordered(context, values, keys);
            }
            for (int ii = 0; ii < count; ++ii) {
                final long key = keys.get(ii);
                final long value = key == RowSequence.NULL_ROW_KEY ? NULL_LONG : expected.get(key);
                checkValue("key " + key, value, values.get(ii));
            }
        }
    }
}
