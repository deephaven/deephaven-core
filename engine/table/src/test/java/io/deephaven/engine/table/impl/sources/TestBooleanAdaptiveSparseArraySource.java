//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import java.util.stream.LongStream;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.ForcedParallelSelect;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestBooleanAdaptiveSparseArraySource extends AbstractBooleanColumnSourceTest {
    private static final int KEY_RANGE = 8 * BLOCK_SIZE;

    /**
     * @return a source over a byte source with the default thresholds, whose leaves may be compact
     */
    @NotNull
    @Override
    BooleanAdaptiveSparseArraySource makeTestSource() {
        return new BooleanAdaptiveSparseArraySource(
                new ByteAdaptiveSparseArraySource(ByteAdaptiveSparseArraySource.DEFAULT_SORTED_THRESHOLD,
                        ByteAdaptiveSparseArraySource.DEFAULT_COMPACT_THRESHOLD));
    }

    private static Boolean randomBoolean(final Random random) {
        final int choice = random.nextInt(3);
        return choice == 0 ? null : choice == 1;
    }

    @Test
    public void testReinterpret() {
        final BooleanAdaptiveSparseArraySource source = makeTestSource();
        source.set(3, true);
        source.set(5, false);
        final ColumnSource<Byte> bytes = source.reinterpret(byte.class);
        assertTrue(bytes instanceof ByteAdaptiveSparseArraySource);
        assertSame(bytes, ReinterpretUtils.maybeConvertToPrimitive(source));
        assertSame(Boolean.TRUE, source.get(3));
        assertSame(Boolean.FALSE, source.get(5));
        assertSame(null, source.get(4));
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
    private void checkParallelPopulation(final boolean aligned) throws Exception {
        final BooleanAdaptiveSparseArraySource source = makeTestSource();
        assertEquals(BLOCK_SIZE, source.parallelPopulationAlignment());
        final Random random = new Random(aligned ? 1 : 0);
        final Map<Long, Boolean> expected = new HashMap<>();
        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            for (int cycle = 0; cycle < 6; ++cycle) {
                final Map<Long, Boolean> expectedPrev = new HashMap<>(expected);
                updateGraph.startCycleForUnitTests();
                try (final RowSet rows = randomRowSet(random)) {
                    final Boolean[] values = new Boolean[rows.intSize()];
                    for (int ii = 0; ii < values.length; ++ii) {
                        values[ii] = randomBoolean(random);
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
                assertMatches(source, expected, expectedPrev);
                updateGraph.completeCycleForUnitTests();
                assertMatches(source, expected, expected);
            }
        } finally {
            executor.shutdownNow();
        }
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

    /**
     * Write positions {@code start} through {@code end - 1} of {@code rows} with their {@code values}.
     */
    private static void fillDivision(
            final BooleanAdaptiveSparseArraySource source,
            final RowSet rows,
            final Boolean[] values,
            final long start,
            final long end) {
        final int size = (int) (end - start);
        try (final RowSet division = rows.subSetByPositionRange(start, end);
                final WritableObjectChunk<Boolean, Values> chunk = WritableObjectChunk.makeWritableChunk(size);
                final ChunkSink.FillFromContext context = source.makeFillFromContext(size)) {
            chunk.copyFromTypedArray(values, (int) start, 0, size);
            source.fillFromChunk(context, chunk, division);
        }
    }

    private static void assertMatches(
            final BooleanAdaptiveSparseArraySource source,
            final Map<Long, Boolean> expected,
            final Map<Long, Boolean> expectedPrev) {
        for (long key = 0; key < KEY_RANGE + 64; ++key) {
            assertSame(expected.get(key), source.get(key));
            assertSame(expectedPrev.get(key), source.getPrev(key));
        }
    }

    @Test
    public void testStructure() {
        final BooleanAdaptiveSparseArraySource source = makeTestSource();
        assertEquals(0, source.estimateSize());
        // a full leaf, then a leaf with a few values
        final Map<Long, Boolean> expected = new HashMap<>();
        for (long key = 0; key < BLOCK_SIZE + 5; ++key) {
            final Boolean value = key % 3 == 0 ? null : key % 3 == 1;
            source.set(key, value);
            expected.put(key, value);
        }
        assertTrue(source.estimateSize() > 0);

        // move the second leaf's values up by two leaves
        try (final RowSet second = RowSetFactory.fromRange(BLOCK_SIZE, BLOCK_SIZE + 4)) {
            source.shift(second, 2L * BLOCK_SIZE);
        }
        for (long key = BLOCK_SIZE; key < BLOCK_SIZE + 5; ++key) {
            assertSame(null, source.get(key));
            assertSame(expected.get(key), source.get(key + 2L * BLOCK_SIZE));
        }

        source.startTrackingPrevValues();
        final long sizeBefore = source.estimateSize();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            try (final RowSet removed = RowSetFactory.fromRange(0, 2L * BLOCK_SIZE - 1);
                    final RowSet live = RowSetFactory.fromRange(3L * BLOCK_SIZE, 3L * BLOCK_SIZE + 4)) {
                source.setNull(removed);
                SparseArrayColumnSource.clearBlocksWithoutLiveRows(removed, live,
                        (ByteAdaptiveSparseArraySource) source.reinterpret(byte.class));
            }
        });
        assertTrue(source.estimateSize() < sizeBefore);
        for (long key = 0; key < BLOCK_SIZE; ++key) {
            assertSame(null, source.get(key));
        }
        for (long key = BLOCK_SIZE; key < BLOCK_SIZE + 5; ++key) {
            assertSame(expected.get(key), source.get(key + 2L * BLOCK_SIZE));
        }

        assertFalse(source.isImmutable());
        source.setImmutable();
        assertTrue(source.isImmutable());
        assertTrue(source.reinterpret(byte.class).isImmutable());
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
            final Table updated = table.update("B = k % 7 == 0 ? null : k % 7 < 4");
            final ColumnSource<Boolean> column = updated.getColumnSource("B", Boolean.class);
            assertTrue(column instanceof BooleanAdaptiveSparseArraySource);
            assertTableEquals(table.view("B = k % 7 == 0 ? null : k % 7 < 4"), updated);

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
                assertTableEquals(table.view("B = k % 7 == 0 ? null : k % 7 < 4"), updated);
            }
        } finally {
            SparseMemoryColumnSourceFactory.USE_ADAPTIVE = startAdaptive;
        }
    }
}
