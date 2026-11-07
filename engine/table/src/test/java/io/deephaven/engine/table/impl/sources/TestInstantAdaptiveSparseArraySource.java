//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import java.util.stream.LongStream;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.ForcedParallelSelect;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestInstantAdaptiveSparseArraySource {
    private static final int KEY_RANGE = 8 * BLOCK_SIZE;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * @return a source over a long source with the default thresholds, whose leaves may be compact
     */
    private static InstantAdaptiveSparseArraySource newSource() {
        return new InstantAdaptiveSparseArraySource();
    }

    private static Instant randomInstant(final Random random) {
        return random.nextInt(10) == 0 ? null : DateTimeUtils.epochNanosToInstant(random.nextLong() >>> 2);
    }

    private static RowSet randomRowSet(final Random random) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        final int size = 1 + random.nextInt(2000);
        for (int ii = 0; ii < size; ++ii) {
            final long key = random.nextInt(KEY_RANGE);
            if (random.nextInt(4) == 0) {
                builder.addRange(key, key + random.nextInt(64));
            } else {
                builder.addKey(key);
            }
        }
        return builder.build();
    }

    @Test
    public void testFills() {
        final Random random = new Random(0);
        final InstantAdaptiveSparseArraySource source = newSource();
        final Map<Long, Instant> expected = new HashMap<>();
        for (int step = 0; step < 50; ++step) {
            if (random.nextBoolean()) {
                try (final RowSet keys = randomRowSet(random);
                        final WritableObjectChunk<Instant, Values> values =
                                WritableObjectChunk.makeWritableChunk(keys.intSize());
                        final ChunkSink.FillFromContext context = source.makeFillFromContext(keys.intSize())) {
                    final MutableInt position = new MutableInt(0);
                    keys.forAllRowKeys(key -> {
                        final Instant value = randomInstant(random);
                        values.set(position.getAndIncrement(), value);
                        expected.put(key, value);
                    });
                    source.fillFromChunk(context, values, keys);
                }
            } else {
                final int count = 1 + random.nextInt(200);
                try (final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(count);
                        final WritableObjectChunk<Instant, Values> values =
                                WritableObjectChunk.makeWritableChunk(count);
                        final ChunkSink.FillFromContext context = source.makeFillFromContext(count)) {
                    for (int ii = 0; ii < count; ++ii) {
                        final long key = random.nextInt(KEY_RANGE);
                        final Instant value = randomInstant(random);
                        keys.set(ii, key);
                        values.set(ii, value);
                        expected.put(key, value);
                    }
                    source.fillFromChunkUnordered(context, values, keys);
                }
            }
            assertMatches(source, expected, random);
        }
    }

    @Test
    public void testFillsFromLongerChunk() {
        final InstantAdaptiveSparseArraySource source = newSource();
        final Instant value = Instant.ofEpochSecond(1_700_000_000L, 123);
        // Instant.MAX has no nanosecond value, so converting it would throw
        try (final WritableObjectChunk<Instant, Values> values = WritableObjectChunk.makeWritableChunk(2);
                final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(1);
                final ChunkSink.FillFromContext context = source.makeFillFromContext(2)) {
            values.set(0, value);
            values.set(1, Instant.MAX);
            source.fillFromChunk(context, values, RowSequenceFactory.forRange(5, 5));
            assertEquals(value, source.get(5));
            keys.set(0, 7);
            source.fillFromChunkUnordered(context, values, keys);
            assertEquals(value, source.get(7));
            keys.setSize(0);
            values.set(0, Instant.MAX);
            source.fillFromChunkUnordered(context, values, keys);
            source.fillFromChunk(context, values, RowSequenceFactory.EMPTY);
        }
        assertNull(source.get(6));
    }

    private static void assertMatches(final InstantAdaptiveSparseArraySource source,
            final Map<Long, Instant> expected, final Random random) {
        try (final RowSet keys = randomRowSet(random);
                final WritableObjectChunk<Instant, Values> values =
                        WritableObjectChunk.makeWritableChunk(keys.intSize());
                final ChunkSource.FillContext context = source.makeFillContext(keys.intSize())) {
            source.fillChunk(context, values, keys);
            assertEquals(keys.intSize(), values.size());
            final MutableInt position = new MutableInt(0);
            keys.forAllRowKeys(key -> {
                assertEquals(expected.get(key), values.get(position.get()));
                assertEquals(expected.get(key), source.get(key));
                position.increment();
            });
        }
        final int count = 300;
        try (final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(count);
                final WritableObjectChunk<Instant, Values> values = WritableObjectChunk.makeWritableChunk(count);
                final ChunkSource.FillContext context = source.makeFillContext(count)) {
            for (int ii = 0; ii < count; ++ii) {
                keys.set(ii, random.nextInt(10) == 0 ? RowSequence.NULL_ROW_KEY : random.nextInt(KEY_RANGE));
            }
            source.fillChunkUnordered(context, values, keys);
            assertEquals(count, values.size());
            for (int ii = 0; ii < count; ++ii) {
                final long key = keys.get(ii);
                assertEquals(key == RowSequence.NULL_ROW_KEY ? null : expected.get(key), values.get(ii));
            }
        }
    }

    @Test
    public void testPreviousValues() {
        final InstantAdaptiveSparseArraySource source = newSource();
        final Instant first = DateTimeUtils.epochNanosToInstant(1000);
        final Instant second = DateTimeUtils.epochNanosToInstant(2000);
        source.set(7, first);
        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            source.set(7, second);
            source.set(8, second);
            assertEquals(first, source.getPrev(7));
            assertNull(source.getPrev(8));
            try (final RowSet keys = RowSetFactory.fromRange(6, 8);
                    final WritableObjectChunk<Instant, Values> values = WritableObjectChunk.makeWritableChunk(3);
                    final ChunkSource.FillContext context = source.makeFillContext(3);
                    final WritableLongChunk<RowKeys> unordered = WritableLongChunk.makeWritableChunk(3)) {
                source.fillPrevChunk(context, values, keys);
                assertNull(values.get(0));
                assertEquals(first, values.get(1));
                assertNull(values.get(2));
                unordered.set(0, 8);
                unordered.set(1, 7);
                unordered.set(2, 6);
                source.fillPrevChunkUnordered(context, values, unordered);
                assertNull(values.get(0));
                assertEquals(first, values.get(1));
                assertNull(values.get(2));
            }
        });
        assertEquals(second, source.getPrev(7));
        assertEquals(second, source.getPrev(8));
    }

    @Test
    public void testReinterpret() {
        final InstantAdaptiveSparseArraySource source = newSource();
        source.set(3, DateTimeUtils.epochNanosToInstant(12345));
        final ColumnSource<Long> nanos = source.reinterpret(long.class);
        assertTrue(nanos instanceof LongAdaptiveSparseArraySource);
        assertSame(nanos, ReinterpretUtils.maybeConvertToPrimitive(source));
        assertSame(nanos, source.toEpochNano());
        assertSame(source, source.toInstant());
        assertEquals(12345, nanos.getLong(3));
        assertEquals(DateTimeUtils.epochNanosToZonedDateTime(12345, ZoneId.of("UTC")),
                source.toZonedDateTime(ZoneId.of("UTC")).get(3));
        assertEquals(DateTimeUtils.epochNanosToZonedDateTime(12345, ZoneId.of("UTC")).toLocalDate(),
                source.toLocalDate(ZoneId.of("UTC")).get(3));
        assertEquals(DateTimeUtils.epochNanosToZonedDateTime(12345, ZoneId.of("UTC")).toLocalTime(),
                source.toLocalTime(ZoneId.of("UTC")).get(3));
    }

    @Test
    public void testParallelPopulation() throws Exception {
        checkParallelPopulation(true);
        checkParallelPopulation(false);
    }

    /**
     * Populate random rows from several threads after preparing for parallel population, in update cycles, and check
     * the current and previous values. Aligned divisions are whole leaves; others end anywhere.
     */
    private static void checkParallelPopulation(final boolean aligned) throws Exception {
        final InstantAdaptiveSparseArraySource source = newSource();
        assertEquals(BLOCK_SIZE, source.parallelPopulationAlignment());
        final Random random = new Random(aligned ? 1 : 0);
        final Map<Long, Instant> expected = new HashMap<>();
        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            for (int cycle = 0; cycle < 6; ++cycle) {
                final Map<Long, Instant> expectedPrev = new HashMap<>(expected);
                updateGraph.startCycleForUnitTests();
                try (final RowSet rows = randomRowSet(random)) {
                    final Instant[] values = new Instant[rows.intSize()];
                    for (int ii = 0; ii < values.length; ++ii) {
                        values[ii] = randomInstant(random);
                        expected.put(rows.get(ii), values[ii]);
                    }
                    if (aligned) {
                        source.prepareForParallelPopulation(rows, true);
                    } else {
                        source.prepareForParallelPopulation(rows);
                    }
                    final List<Future<?>> futures = new ArrayList<>();
                    long start = 0;
                    while (start < rows.size()) {
                        long end = Math.min(rows.size(), start + 1 + random.nextInt(300));
                        if (aligned && end < rows.size()) {
                            end = rows.find((rows.get(end - 1) | (BLOCK_SIZE - 1)) + 1);
                            end = end < 0 ? -end - 1 : end;
                        }
                        final long divisionStart = start;
                        final long divisionEnd = end;
                        futures.add(executor.submit(() -> fillDivision(source, rows, values, divisionStart,
                                divisionEnd)));
                        start = end;
                    }
                    for (final Future<?> future : futures) {
                        future.get();
                    }
                }
                assertAllMatch(source, expected, expectedPrev);
                updateGraph.completeCycleForUnitTests();
                assertAllMatch(source, expected, expected);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Write positions {@code start} through {@code end - 1} of {@code rows} with their {@code values}.
     */
    private static void fillDivision(
            final InstantAdaptiveSparseArraySource source,
            final RowSet rows,
            final Instant[] values,
            final long start,
            final long end) {
        final int size = (int) (end - start);
        try (final RowSet division = rows.subSetByPositionRange(start, end);
                final WritableObjectChunk<Instant, Values> chunk = WritableObjectChunk.makeWritableChunk(size);
                final ChunkSink.FillFromContext context = source.makeFillFromContext(size)) {
            chunk.copyFromTypedArray(values, (int) start, 0, size);
            source.fillFromChunk(context, chunk, division);
        }
    }

    private static void assertAllMatch(
            final InstantAdaptiveSparseArraySource source,
            final Map<Long, Instant> expected,
            final Map<Long, Instant> expectedPrev) {
        for (long key = 0; key < KEY_RANGE + 64; ++key) {
            assertEquals(expected.get(key), source.get(key));
            assertEquals(expectedPrev.get(key), source.getPrev(key));
        }
    }

    @Test
    public void testStructure() {
        final InstantAdaptiveSparseArraySource source = newSource();
        final LongAdaptiveSparseArraySource nanos = (LongAdaptiveSparseArraySource) source.toEpochNano();
        assertEquals(0, nanos.estimateSize());
        source.ensureCapacity(4L * BLOCK_SIZE, true);
        assertEquals(0, nanos.estimateSize());
        assertEquals(NULL_LONG, source.getLong(3L * BLOCK_SIZE));
        // a full leaf, then a leaf with a few values
        for (long key = 0; key < BLOCK_SIZE + 5; ++key) {
            source.set(key, key * 1000);
        }
        source.setNull(7);
        assertNull(source.get(7));
        assertEquals(NULL_LONG, source.getLong(7));
        assertEquals(8000, source.getLong(8));
        assertEquals(DateTimeUtils.epochNanosToInstant(8000), source.get(8));
        assertTrue(nanos.estimateSize() > 0);

        // move the second leaf's values up by two leaves
        try (final RowSet second = RowSetFactory.fromRange(BLOCK_SIZE, BLOCK_SIZE + 4)) {
            source.shift(second, 2L * BLOCK_SIZE);
        }
        for (long key = BLOCK_SIZE; key < BLOCK_SIZE + 5; ++key) {
            assertEquals(NULL_LONG, source.getLong(key));
            assertEquals(key * 1000, source.getLong(key + 2L * BLOCK_SIZE));
        }

        source.startTrackingPrevValues();
        final long sizeBefore = nanos.estimateSize();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            try (final RowSet removed = RowSetFactory.fromRange(0, 2L * BLOCK_SIZE - 1);
                    final RowSet live = RowSetFactory.fromRange(3L * BLOCK_SIZE, 3L * BLOCK_SIZE + 4)) {
                source.setNull(removed);
                assertEquals(8000, source.getPrevLong(8));
                SparseArrayColumnSource.clearBlocksWithoutLiveRows(removed, live, nanos);
            }
        });
        assertTrue(nanos.estimateSize() < sizeBefore);
        assertEquals(NULL_LONG, source.getPrevLong(8));
        for (long key = 0; key < BLOCK_SIZE; ++key) {
            assertNull(source.get(key));
        }
        for (long key = BLOCK_SIZE; key < BLOCK_SIZE + 5; ++key) {
            assertEquals(key * 1000, source.getLong(key + 2L * BLOCK_SIZE));
        }

        assertFalse(source.isImmutable());
        source.setImmutable();
        assertTrue(source.isImmutable());
        assertTrue(source.toEpochNano().isImmutable());
    }

    @Test
    public void testUpdate() {
        checkUpdate();
    }

    @Test
    public void testParallelUpdate() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.resetForUnitTests(false, true, 0, 4, 2, 1);
        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, 4);
        try (final SafeCloseable ignored = ExecutionContext.getContext().withOperationInitializer(threadPool).open();
                final SafeCloseable ignored2 = threadPool::shutdown;
                final SafeCloseable ignored3 = new ForcedParallelSelect(100)) {
            checkUpdate();
        }
    }

    private static void checkUpdate() {
        final boolean startAdaptive = SparseMemoryColumnSourceFactory.USE_ADAPTIVE;
        try {
            SparseMemoryColumnSourceFactory.USE_ADAPTIVE = true;
            final QueryTable table = TstUtils.testRefreshingTable(
                    RowSetFactory.fromKeys(LongStream.range(0, 3000).map(key -> key * 3).toArray()).toTracking());
            final Table updated = table.update("T = k % 7 == 0 ? null : DateTimeUtils.epochNanosToInstant(k * 1000)");
            final ColumnSource<Instant> column = updated.getColumnSource("T", Instant.class);
            assertTrue(column instanceof InstantAdaptiveSparseArraySource);
            assertTableEquals(table.view("T = k % 7 == 0 ? null : DateTimeUtils.epochNanosToInstant(k * 1000)"),
                    updated);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            for (int cycle = 0; cycle < 4; ++cycle) {
                final long firstKey = 9000 + cycle * 1000L;
                updateGraph.runWithinUnitTestCycle(() -> {
                    final RowSet added = RowSetFactory.fromRange(firstKey, firstKey + 999);
                    final RowSet removed = RowSetFactory.fromRange(firstKey - 9000, firstKey - 8001);
                    removeRows(table, removed);
                    addToTable(table, added);
                    table.notifyListeners(added, removed.intersect(table.getRowSet().prev()), i());
                });
                assertTableEquals(table.view("T = k % 7 == 0 ? null : DateTimeUtils.epochNanosToInstant(k * 1000)"),
                        updated);
            }
        } finally {
            SparseMemoryColumnSourceFactory.USE_ADAPTIVE = startAdaptive;
        }
    }
}
