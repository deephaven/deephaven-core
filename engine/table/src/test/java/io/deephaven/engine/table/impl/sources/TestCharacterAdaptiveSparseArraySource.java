//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.mutable.MutableInt;
import it.unimi.dsi.fastutil.longs.Long2CharOpenHashMap;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK0_SHIFT;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK0_SIZE;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK1_SHIFT;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK1_SIZE;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK2_SIZE;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCharacterAdaptiveSparseArraySource extends AbstractCharacterColumnSourceTest {

    /** Row keys near zero, spread over many leaves, and far away, so that each page table level holds many nodes. */
    private static final long[] KEY_REGION_STARTS = {0, 1L << 26, 1L << 41};
    private static final int KEY_REGION_SIZE = 40 * BLOCK_SIZE;
    private static final int SORTED_THRESHOLD = CharacterSparseArraySource.DEFAULT_SORTED_THRESHOLD;
    private static final int COMPACT_THRESHOLD = CharacterSparseArraySource.DEFAULT_COMPACT_THRESHOLD;

    /**
     * @return a source with the default thresholds, whose leaves may be compact
     */
    @NotNull
    @Override
    CharacterSparseArraySource makeTestSource() {
        return new CharacterSparseArraySource(SORTED_THRESHOLD, COMPACT_THRESHOLD);
    }

    @Test
    public void confirmAliasingForbidden() {
        final CharacterSparseArraySource source = makeTestSource();
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
        for (final int compactThreshold : new int[] {0, 1, 4, 64, BLOCK_SIZE}) {
            for (final int sortedThreshold : new int[] {1, 4, 16}) {
                checkAgainstMap(new CharacterSparseArraySource(sortedThreshold, compactThreshold),
                        compactThreshold * 37 + sortedThreshold);
            }
        }
    }

    private static char randomValue(final Random random) {
        return (char) random.nextInt(1000);
    }

    private static void checkValue(final String message, final char expected, final char actual) {
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

    private static void checkAgainstMap(final CharacterSparseArraySource source, final int seed) {
        final Random random = new Random(seed);
        final Long2CharOpenHashMap expected = new Long2CharOpenHashMap();
        expected.defaultReturnValue(NULL_CHAR);

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
        for (final int compactThreshold : new int[] {0, 4, 64,
                CharacterSparseArraySource.DEFAULT_COMPACT_THRESHOLD}) {
            for (final int sortedThreshold : new int[] {1, 16}) {
                checkPreviousValues(new CharacterSparseArraySource(sortedThreshold, compactThreshold),
                        compactThreshold * 37 + sortedThreshold);
            }
        }
    }

    /**
     * Apply random operations in update cycles, checking that previous values are the values at the start of each
     * cycle.
     */
    private static void checkPreviousValues(final CharacterSparseArraySource source, final int seed) {
        final Random random = new Random(seed);
        final Long2CharOpenHashMap expected = new Long2CharOpenHashMap();
        expected.defaultReturnValue(NULL_CHAR);
        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int cycle = 0; cycle < 30; ++cycle) {
            final Long2CharOpenHashMap expectedPrev = new Long2CharOpenHashMap(expected);
            expectedPrev.defaultReturnValue(NULL_CHAR);
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
    public void testParallelPopulation() throws Exception {
        for (final Preparation preparation : Preparation.values()) {
            for (final int compactThreshold : new int[] {0, 4,
                    CharacterSparseArraySource.DEFAULT_COMPACT_THRESHOLD}) {
                checkParallelPopulation(new CharacterSparseArraySource(16, compactThreshold), preparation,
                        compactThreshold * 3 + preparation.ordinal());
            }
        }
    }

    /** How a test prepares a source for parallel population. */
    private enum Preparation {
        /** For writers whose divisions are aligned to leaves. */
        ALIGNED,
        /** For writers whose divisions end anywhere. */
        UNALIGNED,
        /** For writers whose divisions end anywhere, building the leaves in parallel divisions aligned to leaves. */
        IN_DIVISIONS
    }

    /**
     * @return the end of a division of {@code rows} from position {@code start}, about {@code length} rows long, moved
     *         to the next leaf boundary if {@code aligned}
     */
    private static long divisionEnd(final RowSet rows, final long start, final int length, final boolean aligned) {
        long end = Math.min(rows.size(), start + length);
        if (aligned && end < rows.size()) {
            final long leafEnd = rows.get(end - 1) | (BLOCK_SIZE - 1);
            end = rows.find(leafEnd + 1);
            end = end < 0 ? -end - 1 : end;
        }
        return end;
    }

    /**
     * Populate random rows from several threads after preparing for parallel population, in update cycles, and check
     * the current and previous values.
     */
    private static void checkParallelPopulation(
            final CharacterSparseArraySource source,
            final Preparation preparation,
            final int seed) throws Exception {
        final boolean aligned = preparation == Preparation.ALIGNED;
        final Random random = new Random(seed);
        final Long2CharOpenHashMap expected = new Long2CharOpenHashMap();
        expected.defaultReturnValue(NULL_CHAR);
        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            for (int cycle = 0; cycle < 20; ++cycle) {
                final Long2CharOpenHashMap expectedPrev = new Long2CharOpenHashMap(expected);
                expectedPrev.defaultReturnValue(NULL_CHAR);
                updateGraph.startCycleForUnitTests();
                try (final RowSet rows = randomRowSet(random, 4000)) {
                    final char[] values = new char[rows.intSize()];
                    final MutableInt position = new MutableInt(0);
                    rows.forAllRowKeys(key -> {
                        final char value = randomValue(random);
                        values[position.get()] = value;
                        position.increment();
                        expected.put(key, value);
                    });
                    if (preparation == Preparation.IN_DIVISIONS) {
                        source.prepareForParallelPopulationInDivisions(rows);
                        final List<Future<?>> preparations = new ArrayList<>();
                        long start = 0;
                        while (start < rows.size()) {
                            final long end = divisionEnd(rows, start, 1 + random.nextInt(600), true);
                            final RowSet division = rows.subSetByPositionRange(start, end);
                            preparations.add(executor.submit(() -> {
                                try (final RowSet ignored = division) {
                                    source.prepareDivisionForParallelPopulation(division);
                                }
                            }));
                            start = end;
                        }
                        for (final Future<?> future : preparations) {
                            future.get();
                        }
                    } else {
                        source.prepareForParallelPopulation(rows, aligned);
                    }
                    // aligned divisions end at a leaf boundary, others anywhere
                    final List<Future<?>> futures = new ArrayList<>();
                    long start = 0;
                    while (start < rows.size()) {
                        final long end = divisionEnd(rows, start, 1 + random.nextInt(300), aligned);
                        final long divisionStart = start;
                        final long divisionEnd = end;
                        // a division is written in pieces, which may end inside a leaf
                        final int pieceSize = 1 + random.nextInt(48);
                        futures.add(executor.submit(() -> fillDivision(source, rows, values, divisionStart,
                                divisionEnd, pieceSize)));
                        start = end;
                    }
                    for (final Future<?> future : futures) {
                        future.get();
                    }
                }
                assertMatches(source, expected, random, false);
                assertMatches(source, expectedPrev, random, true);
                updateGraph.completeCycleForUnitTests();
                assertMatches(source, expected, random, true);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Populate leaves that each become full blocks from several threads at once, in pieces that end inside the leaves,
     * so that each thread's first piece briefly makes a compact leaf in the same, newly prepared, bottom page table
     * level.
     */
    @Test
    public void testParallelPopulationOfSplitFullLeaves() throws Exception {
        final int threads = 4;
        final int leavesPerThread = 4;
        final int rowsPerLeaf = CharacterSparseArraySource.DEFAULT_COMPACT_THRESHOLD + 72;
        final int pieceSize = CharacterSparseArraySource.DEFAULT_COMPACT_THRESHOLD - 31;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            for (int trial = 0; trial < 200; ++trial) {
                final Random random = new Random(trial);
                final CharacterSparseArraySource source = makeTestSource();
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                for (int leaf = 0; leaf < threads * leavesPerThread; ++leaf) {
                    builder.appendRange((long) leaf * BLOCK_SIZE, (long) leaf * BLOCK_SIZE + rowsPerLeaf - 1);
                }
                try (final RowSet rows = builder.build()) {
                    final Long2CharOpenHashMap expected = new Long2CharOpenHashMap();
                    expected.defaultReturnValue(NULL_CHAR);
                    final char[] values = new char[rows.intSize()];
                    final MutableInt position = new MutableInt(0);
                    rows.forAllRowKeys(key -> {
                        final char value = randomValue(random);
                        values[position.get()] = value;
                        position.increment();
                        expected.put(key, value);
                    });
                    source.prepareForParallelPopulation(rows, true);
                    final CountDownLatch startLatch = new CountDownLatch(1);
                    final List<Future<?>> futures = new ArrayList<>();
                    final long divisionSize = (long) leavesPerThread * rowsPerLeaf;
                    for (int ti = 0; ti < threads; ++ti) {
                        final long divisionStart = ti * divisionSize;
                        futures.add(executor.submit(() -> {
                            startLatch.await();
                            fillDivision(source, rows, values, divisionStart, divisionStart + divisionSize,
                                    pieceSize);
                            return null;
                        }));
                    }
                    startLatch.countDown();
                    for (final Future<?> future : futures) {
                        future.get();
                    }
                    assertMatches(source, expected, random, false);
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Write positions {@code start} through {@code end - 1} of {@code rows} with their {@code values},
     * {@code pieceSize} rows at a time.
     */
    private static void fillDivision(
            final CharacterSparseArraySource source,
            final RowSet rows,
            final char[] values,
            final long start,
            final long end,
            final int pieceSize) {
        try (final RowSet division = rows.subSetByPositionRange(start, end);
                final RowSequence.Iterator pieces = division.getRowSequenceIterator();
                final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(pieceSize);
                final ChunkSink.FillFromContext context = source.makeFillFromContext(pieceSize)) {
            int position = (int) start;
            while (pieces.hasMore()) {
                final RowSequence piece = pieces.getNextRowSequenceWithLength(pieceSize);
                chunk.copyFromTypedArray(values, position, 0, piece.intSize());
                chunk.setSize(piece.intSize());
                source.fillFromChunk(context, chunk, piece);
                position += piece.intSize();
            }
        }
    }

    @Test
    public void testClearBlocks() {
        final CharacterSparseArraySource source = makeTestSource();
        source.startTrackingPrevValues();
        final Random random = new Random(0);
        // a full leaf, a bitmap leaf and a sorted leaf, in that order
        final long[] leafSizes = {BLOCK_SIZE, 100, 3};
        for (int li = 0; li < leafSizes.length; ++li) {
            for (int ii = 0; ii < leafSizes[li]; ++ii) {
                source.set(li * (long) BLOCK_SIZE + ii, randomValue(random));
            }
        }
        final char liveValue = source.getChar(2L * BLOCK_SIZE);
        final long sizeBefore = source.estimateSize();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            try (final RowSet removed = RowSetFactory.fromRange(0, 2L * BLOCK_SIZE - 1);
                    final RowSet live = RowSetFactory.fromKeys(2L * BLOCK_SIZE)) {
                source.setNull(removed);
                SparseArrayColumnSource.clearBlocksWithoutLiveRows(removed, live, source);
            }
        });
        assertTrue(source.estimateSize() < sizeBefore);
        checkValue("live", liveValue, source.getChar(2L * BLOCK_SIZE));
        checkValue("cleared", NULL_CHAR, source.getChar(0));
        checkValue("cleared", NULL_CHAR, source.getChar(BLOCK_SIZE));
    }

    @Test
    public void testClearNeverWrittenStructures() {
        final CharacterSparseArraySource source = makeTestSource();
        final char value = randomValue(new Random(0));
        source.set(0, value);
        source.startTrackingPrevValues();
        final long sizeBefore = source.estimateSize();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            // a leaf, a block2 structure and a block1 structure that were never written
            try (final RowSet candidates = RowSetFactory.fromKeys(BLOCK_SIZE, 1L << BLOCK1_SHIFT, 1L << BLOCK0_SHIFT);
                    final RowSet live = RowSetFactory.fromKeys(0)) {
                SparseArrayColumnSource.clearBlocksWithoutLiveRows(candidates, live, source);
            }
        });
        assertEquals(sizeBefore, source.estimateSize());
        checkValue("live", value, source.getChar(0));
    }

    /**
     * Write every fourth leaf of one bottom page table level in an order that adds leaves among those it holds, release
     * some, and write more until it holds a quarter of its leaves, spread across the whole level, checking current and
     * previous values throughout.
     */
    @Test
    public void testManyLeavesInOneBottomLevel() {
        final List<Long> leafStarts = new ArrayList<>();
        for (int leaf = 0; leaf < BLOCK2_SIZE; leaf += 4) {
            leafStarts.add((long) leaf * BLOCK_SIZE);
        }
        checkManyChildren(leafStarts);
    }

    /**
     * Write a leaf in each of a quarter of the bottom levels of one middle page table level, as
     * {@link #testManyLeavesInOneBottomLevel()} writes the leaves of one bottom level.
     */
    @Test
    public void testManyBottomLevelsInOneMiddleLevel() {
        final Random random = new Random(1);
        final List<Long> leafStarts = new ArrayList<>();
        for (int level2 = 0; level2 < BLOCK1_SIZE / 4; ++level2) {
            leafStarts.add(((long) level2 << BLOCK1_SHIFT) + (long) random.nextInt(BLOCK2_SIZE) * BLOCK_SIZE);
        }
        checkManyChildren(leafStarts);
    }

    /**
     * Write a leaf under each of many middle page table levels spread across the top level, as
     * {@link #testManyLeavesInOneBottomLevel()} writes the leaves of one bottom level.
     */
    @Test
    public void testManyMiddleLevels() {
        final Random random = new Random(2);
        final List<Long> leafStarts = new ArrayList<>();
        random.ints(0, BLOCK0_SIZE).distinct().limit(600).forEach(level1 -> leafStarts.add(
                ((long) level1 << BLOCK0_SHIFT) + ((long) random.nextInt(BLOCK1_SIZE) << BLOCK1_SHIFT)));
        checkManyChildren(leafStarts);
    }

    /**
     * Write leaves under top level children that are first too sparse for a window and then dense enough for one, so
     * that the top level moves between a window and ranked children in both directions.
     */
    @Test
    public void testTopLevelWindowAndRanks() {
        final List<Long> leafStarts = new ArrayList<>();
        for (int level1 = 0; level1 < 200; ++level1) {
            leafStarts.add((long) level1 << BLOCK0_SHIFT);
        }
        for (int level1 = 1000; level1 < 1010; ++level1) {
            leafStarts.add((long) level1 << BLOCK0_SHIFT);
        }
        checkManyChildren(leafStarts);
    }

    /**
     * Write bottom levels of a middle level that are first too sparse for a window and then dense enough for one, as
     * {@link #testTopLevelWindowAndRanks()} does for the top level.
     */
    @Test
    public void testMiddleLevelWindowAndRanks() {
        final List<Long> leafStarts = new ArrayList<>();
        for (int level2 = 0; level2 < 300; ++level2) {
            leafStarts.add((long) level2 << BLOCK1_SHIFT);
        }
        for (int level2 = 2000; level2 < 2010; ++level2) {
            leafStarts.add((long) level2 << BLOCK1_SHIFT);
        }
        checkManyChildren(leafStarts);
    }

    /**
     * Write leaves of a bottom level that are first too sparse for a window and then dense enough for one, as
     * {@link #testTopLevelWindowAndRanks()} does for the top level.
     */
    @Test
    public void testBottomLevelWindowAndRanks() {
        final List<Long> leafStarts = new ArrayList<>();
        for (int leaf = 0; leaf < 300; ++leaf) {
            leafStarts.add((long) leaf * BLOCK_SIZE);
        }
        for (int leaf = 2000; leaf < 2010; ++leaf) {
            leafStarts.add((long) leaf * BLOCK_SIZE);
        }
        checkManyChildren(leafStarts);
    }

    /**
     * Write the leaves starting at {@code leafStarts} in a shuffled order, release some, and write the rest, changing
     * the leaves already held, then release some of those, checking current and previous values throughout.
     */
    private void checkManyChildren(final List<Long> leafStarts) {
        final CharacterSparseArraySource source = makeTestSource();
        final Random random = new Random(0);
        final Long2CharOpenHashMap expected = new Long2CharOpenHashMap();
        expected.defaultReturnValue(NULL_CHAR);
        final List<Long> leaves = new ArrayList<>(leafStarts);
        Collections.shuffle(leaves, random);
        final List<Long> firstLeaves = leaves.subList(0, leaves.size() / 3);
        final List<Long> laterLeaves = leaves.subList(leaves.size() / 3, leaves.size());

        writeLeavesInChunks(source, expected, random, firstLeaves);
        checkLeaves(source, expected, expected);

        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        // release every other leaf written so far
        updateGraph.runWithinUnitTestCycle(() -> {
            final Long2CharOpenHashMap previous = new Long2CharOpenHashMap(expected);
            previous.defaultReturnValue(NULL_CHAR);
            releaseLeaves(source, expected, firstLeaves, 2);
            checkLeaves(source, expected, previous);
        });
        checkLeaves(source, expected, expected);

        // write the remaining leaves, past the number at which a level indexes its children by child index, and change
        // a value in each leaf already held
        updateGraph.runWithinUnitTestCycle(() -> {
            final Long2CharOpenHashMap previous = new Long2CharOpenHashMap(expected);
            previous.defaultReturnValue(NULL_CHAR);
            for (final long key : previous.keySet().toLongArray()) {
                if ((key & (BLOCK_SIZE - 1)) == 0) {
                    final char value = randomValue(random);
                    source.set(key, value);
                    expected.put(key, value);
                }
            }
            writeLeavesInChunks(source, expected, random, laterLeaves);
            checkLeaves(source, expected, previous);
        });
        checkLeaves(source, expected, expected);

        // release some of the later leaves
        updateGraph.runWithinUnitTestCycle(() -> {
            final Long2CharOpenHashMap previous = new Long2CharOpenHashMap(expected);
            previous.defaultReturnValue(NULL_CHAR);
            releaseLeaves(source, expected, laterLeaves, 3);
            checkLeaves(source, expected, previous);
        });
        checkLeaves(source, expected, expected);
    }

    /**
     * Remove every row of every {@code step}th leaf of {@code leafStarts} from {@code source} and {@code expected}, and
     * release the structures that hold no remaining row.
     */
    private static void releaseLeaves(
            final CharacterSparseArraySource source,
            final Long2CharOpenHashMap expected,
            final List<Long> leafStarts,
            final int step) {
        final RowSetBuilderRandom removedBuilder = RowSetFactory.builderRandom();
        for (int ii = 0; ii < leafStarts.size(); ii += step) {
            final long leafStart = leafStarts.get(ii);
            removedBuilder.addRange(leafStart, leafStart + BLOCK_SIZE - 1);
        }
        try (final RowSet removed = removedBuilder.build()) {
            removed.forAllRowKeys(expected::remove);
            try (final RowSet live = RowSetFactory.fromKeys(expected.keySet().toLongArray())) {
                source.setNull(removed);
                SparseArrayColumnSource.clearBlocksWithoutLiveRows(removed, live, source);
            }
        }
    }

    /**
     * Write the leaves starting at {@code leafStarts}, a chunk of them at a time, each chunk in ascending order: a few
     * values in most leaves, more in some, and every value in a few.
     */
    private static void writeLeavesInChunks(
            final CharacterSparseArraySource source,
            final Long2CharOpenHashMap expected,
            final Random random,
            final List<Long> leafStarts) {
        int next = 0;
        while (next < leafStarts.size()) {
            final int end = Math.min(leafStarts.size(), next + 1 + random.nextInt(200));
            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            for (final long leafStart : leafStarts.subList(next, end)) {
                final int kind = random.nextInt(50);
                final int count = kind == 0 ? BLOCK_SIZE : kind < 10 ? 40 : 1 + random.nextInt(3);
                for (int ii = 0; ii < count; ++ii) {
                    builder.addKey(leafStart + (count == BLOCK_SIZE ? ii : random.nextInt(BLOCK_SIZE)));
                }
                // the first key of each leaf is always written, so that each leaf can be changed by its first key
                builder.addKey(leafStart);
            }
            try (final RowSet keys = builder.build();
                    final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(keys.intSize());
                    final ChunkSink.FillFromContext context = source.makeFillFromContext(keys.intSize())) {
                final MutableInt position = new MutableInt(0);
                keys.forAllRowKeys(key -> {
                    final char value = randomValue(random);
                    values.set(position.getAndIncrement(), value);
                    expected.put(key, value);
                });
                source.fillFromChunk(context, values, keys);
            }
            next = end;
        }
    }

    /**
     * Check every row key of {@code current} and {@code previous} against the current and previous values of
     * {@code source}, one at a time and in a chunk.
     */
    private static void checkLeaves(
            final CharacterSparseArraySource source,
            final Long2CharOpenHashMap current,
            final Long2CharOpenHashMap previous) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        current.keySet().forEach(builder::addKey);
        previous.keySet().forEach(builder::addKey);
        try (final RowSet keys = builder.build();
                final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(keys.intSize());
                final ChunkSource.FillContext context = source.makeFillContext(keys.intSize())) {
            keys.forAllRowKeys(key -> {
                checkValue("key " + key, current.get(key), source.getChar(key));
                checkValue("prev key " + key, previous.get(key), source.getPrevChar(key));
            });
            source.fillChunk(context, values, keys);
            final MutableInt position = new MutableInt(0);
            keys.forAllRowKeys(
                    key -> checkValue("key " + key, current.get(key), values.get(position.getAndIncrement())));
            source.fillPrevChunk(context, values, keys);
            position.set(0);
            keys.forAllRowKeys(
                    key -> checkValue("prev key " + key, previous.get(key), values.get(position.getAndIncrement())));
        }
    }

    private static void applyRandomOperation(
            final CharacterSparseArraySource source,
            final Long2CharOpenHashMap expected,
            final Random random) {
        switch (random.nextInt(6)) {
            case 0: {
                final long key = randomKey(random);
                final char value = randomValue(random);
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
                    final Long2CharOpenHashMap shifted = new Long2CharOpenHashMap();
                    shifted.defaultReturnValue(NULL_CHAR);
                    keys.forAllRowKeys(key -> {
                        if (expected.containsKey(key)) {
                            shifted.put(key + delta, expected.remove(key));
                        } else {
                            shifted.put(key + delta, NULL_CHAR);
                        }
                    });
                    shifted.forEach((key, value) -> {
                        if (value == NULL_CHAR) {
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
                        final WritableCharChunk<Values> values =
                                WritableCharChunk.makeWritableChunk(keys.intSize());
                        final ChunkSink.FillFromContext context = source.makeFillFromContext(keys.intSize())) {
                    final MutableInt position = new MutableInt(0);
                    keys.forAllRowKeys(key -> {
                        final char value = randomValue(random);
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
                        final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(count);
                        final ChunkSink.FillFromContext context = source.makeFillFromContext(count)) {
                    for (int ii = 0; ii < count; ++ii) {
                        final long key = randomKey(random);
                        final char value = randomValue(random);
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
            final CharacterSparseArraySource source,
            final Long2CharOpenHashMap expected,
            final Random random,
            final boolean usePrev) {
        for (int ii = 0; ii < 500; ++ii) {
            final long key = randomKey(random);
            checkValue("key " + key, expected.get(key), usePrev ? source.getPrevChar(key) : source.getChar(key));
        }
        for (final long key : expected.keySet()) {
            checkValue("key " + key, expected.get(key), usePrev ? source.getPrevChar(key) : source.getChar(key));
        }

        // a row set with runs takes the by-ranges path, and one of single keys the by-keys path
        try (final RowSet keys = randomRowSet(random, 5000);
                final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(keys.intSize());
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
                final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(keys.intSize());
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
            final CharChunk<? extends Values> values =
                    (usePrev ? source.getPrevChunk(context, keys) : source.getChunk(context, keys)).asCharChunk();
            final MutableInt position = new MutableInt(0);
            keys.forAllRowKeys(
                    key -> checkValue("key " + key, expected.get(key), values.get(position.getAndIncrement())));
        }

        final int count = 300;
        try (final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(count);
                final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(count);
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
                final char value = key == RowSequence.NULL_ROW_KEY ? NULL_CHAR : expected.get(key);
                checkValue("key " + key, value, values.get(ii));
            }
        }
    }
}
