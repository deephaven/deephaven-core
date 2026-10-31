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
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestInstantAdaptiveSparseArraySource {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static Instant randomInstant(final Random random) {
        return random.nextInt(10) == 0 ? null : DateTimeUtils.epochNanosToInstant(random.nextLong() >>> 2);
    }

    private static RowSet randomRowSet(final Random random) {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        final int size = 1 + random.nextInt(2000);
        for (int ii = 0; ii < size; ++ii) {
            final long key = random.nextInt(8 * BLOCK_SIZE);
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
        final InstantAdaptiveSparseArraySource source = new InstantAdaptiveSparseArraySource();
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
                        final long key = random.nextInt(8 * BLOCK_SIZE);
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
                keys.set(ii, random.nextInt(10) == 0 ? RowSequence.NULL_ROW_KEY : random.nextInt(8 * BLOCK_SIZE));
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
        final InstantAdaptiveSparseArraySource source = new InstantAdaptiveSparseArraySource();
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
        final InstantAdaptiveSparseArraySource source = new InstantAdaptiveSparseArraySource();
        source.set(3, DateTimeUtils.epochNanosToInstant(12345));
        final ColumnSource<Long> nanos = source.reinterpret(long.class);
        assertTrue(nanos instanceof LongAdaptiveSparseArraySource);
        assertSame(nanos, ReinterpretUtils.maybeConvertToPrimitive(source));
        assertSame(nanos, source.toEpochNano());
        assertSame(source, source.toInstant());
        assertEquals(12345, nanos.getLong(3));
        assertEquals(DateTimeUtils.epochNanosToZonedDateTime(12345, ZoneId.of("UTC")),
                source.toZonedDateTime(ZoneId.of("UTC")).get(3));
    }

    @Test
    public void testUpdate() {
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
